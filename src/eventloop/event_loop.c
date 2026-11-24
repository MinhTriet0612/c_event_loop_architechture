
#include "eventloop/event_loop.h"

#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>
#include <sys/event.h>
#include <sys/socket.h>
#include <unistd.h>

#define WAKE_IDENT 1 // EVFILT_USER id used by workers to wake the loop
#define MAX_EVENTS 64
#define RBUF_SIZE 4096

struct conn {
  loop_t *loop;
  int fd;

  char rbuf[RBUF_SIZE]; // partial input, until a full line arrives
  size_t rlen;

  char *wbuf;  // pending output (partial writes)
  size_t wlen; // bytes in wbuf
  size_t woff; // bytes of wbuf already sent
  size_t wcap;
  int want_write; // EVFILT_WRITE registered?

  int closed;       // fd closed, struct kept until pending_jobs == 0
  int pending_jobs; // jobs in the pool that point to this conn

  struct conn *prev, *next; // all conns, for sweep + shutdown
};

struct loop {
  int kq;
  int listen_fd;
  int running;
  int need_sweep;
  line_fn on_line;
  conn_t *conns;

  thread_pool_t pool;

  // SHARED with workers: protected by done_mutex
  pthread_mutex_t done_mutex;
  job_t *done_head, *done_tail;
};

// ---------- helpers ----------

static int set_nonblock(int fd) {
  int flags = fcntl(fd, F_GETFL, 0);
  if (flags < 0) {
    return -1;
  }
  return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

static int kq_change(int kq, uintptr_t ident, int16_t filter, uint16_t flags,
                     uint32_t fflags, void *udata) {
  struct kevent ev;
  EV_SET(&ev, ident, filter, flags, fflags, 0, udata);
  return kevent(kq, &ev, 1, NULL, 0, NULL);
}

static int open_listener(int port) {
  int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    return -1;
  }

  int one = 1;
  setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

  struct sockaddr_in addr;
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = htons((uint16_t)port);

  if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0 ||
      listen(fd, SOMAXCONN) < 0 || set_nonblock(fd) < 0) {
    close(fd);
    return -1;
  }
  return fd;
}

// ---------- connections (loop thread only) ----------

static conn_t *conn_new(loop_t *loop, int fd) {
  conn_t *c = calloc(1, sizeof(*c));
  if (c == NULL) {
    return NULL;
  }
  c->loop = loop;
  c->fd = fd;

  c->next = loop->conns;
  if (loop->conns != NULL) {
    loop->conns->prev = c;
  }
  loop->conns = c;
  return c;
}

static void conn_free(loop_t *loop, conn_t *c) {
  if (c->prev != NULL) {
    c->prev->next = c->next;
  } else {
    loop->conns = c->next;
  }
  if (c->next != NULL) {
    c->next->prev = c->prev;
  }
  free(c->wbuf);
  free(c);
}

void conn_close(conn_t *c) {
  if (c->closed) {
    return;
  }
  c->closed = 1;
  close(c->fd); // also removes this fd's kqueue registrations
  c->fd = -1;
  c->loop->need_sweep = 1;
}

int conn_is_open(const conn_t *c) { return !c->closed; }

// Free closed conns that no job points to anymore. Runs after each event
// batch, so a later event in the same batch never sees freed memory.
static void sweep_closed(loop_t *loop) {
  if (!loop->need_sweep) {
    return;
  }
  loop->need_sweep = 0;

  conn_t *c = loop->conns;
  while (c != NULL) {
    conn_t *next = c->next;
    if (c->closed && c->pending_jobs == 0) {
      conn_free(loop, c);
    }
    c = next;
  }
}

// ---------- writing ----------

// Send as much of wbuf as the socket takes now.
static int flush_wbuf(conn_t *c) {
  while (c->woff < c->wlen) {
    ssize_t n = write(c->fd, c->wbuf + c->woff, c->wlen - c->woff);
    if (n < 0) {
      if (errno == EINTR) {
        continue;
      }
      if (errno == EAGAIN) {
        break; // socket buffer full, wait for EVFILT_WRITE
      }
      conn_close(c);
      return -1;
    }
    c->woff += (size_t)n;
  }

  if (c->woff == c->wlen) {
    c->woff = 0;
    c->wlen = 0;
  }
  return 0;
}

// Watch EVFILT_WRITE only while there is pending output.
static void update_write_interest(conn_t *c) {
  int need = c->wlen > 0;
  if (need == c->want_write) {
    return;
  }
  kq_change(c->loop->kq, (uintptr_t)c->fd, EVFILT_WRITE,
            need ? EV_ADD : EV_DELETE, 0, c);
  c->want_write = need;
}

int conn_send(conn_t *c, const char *buf, size_t len) {
  if (c->closed) {
    return -1;
  }

  // drop already-sent bytes before appending
  if (c->woff > 0) {
    memmove(c->wbuf, c->wbuf + c->woff, c->wlen - c->woff);
    c->wlen -= c->woff;
    c->woff = 0;
  }

  if (c->wlen + len > c->wcap) {
    size_t cap = c->wcap ? c->wcap : 1024;
    while (cap < c->wlen + len) {
      cap *= 2;
    }
    char *p = realloc(c->wbuf, cap);
    if (p == NULL) {
      conn_close(c);
      return -1;
    }
    c->wbuf = p;
    c->wcap = cap;
  }

  memcpy(c->wbuf + c->wlen, buf, len);
  c->wlen += len;

  if (flush_wbuf(c) < 0) {
    return -1;
  }
  update_write_interest(c);
  return 0;
}

// ---------- event handlers (loop thread) ----------

static void on_accept(loop_t *loop) {
  for (;;) {
    int fd = accept(loop->listen_fd, NULL, NULL);
    if (fd < 0) {
      if (errno == EINTR) {
        continue;
      }
      return; // EAGAIN: no more pending connections
    }

    int one = 1;
    if (set_nonblock(fd) < 0 ||
        setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one)) < 0) {
      close(fd);
      continue;
    }

    conn_t *c = conn_new(loop, fd);
    if (c == NULL) {
      close(fd);
      continue;
    }

    if (kq_change(loop->kq, (uintptr_t)fd, EVFILT_READ, EV_ADD, 0, c) < 0) {
      conn_close(c);
    }
  }
}

static void on_readable(loop_t *loop, conn_t *c) {
  ssize_t n = read(c->fd, c->rbuf + c->rlen, RBUF_SIZE - c->rlen);
  if (n == 0) {
    conn_close(c); // client disconnected
    return;
  }
  if (n < 0) {
    if (errno != EAGAIN && errno != EINTR) {
      conn_close(c);
    }
    return;
  }
  c->rlen += (size_t)n;

  // hand every complete line to the app
  size_t start = 0;
  for (size_t i = 0; i < c->rlen && !c->closed; i++) {
    if (c->rbuf[i] != '\n') {
      continue;
    }
    size_t len = i - start;
    if (len > 0 && c->rbuf[start + len - 1] == '\r') {
      len--;
    }
    c->rbuf[start + len] = '\0';
    loop->on_line(loop, c, c->rbuf + start, len);
    start = i + 1;
  }
  if (c->closed) {
    return;
  }

  // keep the unfinished tail for the next read
  memmove(c->rbuf, c->rbuf + start, c->rlen - start);
  c->rlen -= start;

  if (c->rlen == RBUF_SIZE) {
    conn_close(c); // line too long
  }
}

static void on_writable(conn_t *c) {
  if (flush_wbuf(c) < 0) {
    return;
  }
  update_write_interest(c);
}

// ---------- pool bridge ----------

// WORKER thread: run the job's work.
static void *run_job(void *arg) {
  job_t *job = arg;
  job->work(job);
  return job;
}

// WORKER thread (pool's on_done): post the job back and wake the loop.
// Must not touch job after unlock: the loop may free it right away.
static void post_to_loop(void *result, void *user_data) {
  (void)result;
  job_t *job = user_data;
  loop_t *loop = job->loop;

  pthread_mutex_lock(&loop->done_mutex);
  job->next = NULL;
  if (loop->done_tail != NULL) {
    loop->done_tail->next = job;
  } else {
    loop->done_head = job;
  }
  loop->done_tail = job;
  pthread_mutex_unlock(&loop->done_mutex);

  kq_change(loop->kq, WAKE_IDENT, EVFILT_USER, 0, NOTE_TRIGGER, NULL);
}

// LOOP thread: take the WHOLE done queue (wake-ups coalesce) and finish jobs.
static void drain_done(loop_t *loop) {
  pthread_mutex_lock(&loop->done_mutex);
  job_t *job = loop->done_head;
  loop->done_head = NULL;
  loop->done_tail = NULL;
  pthread_mutex_unlock(&loop->done_mutex);

  while (job != NULL) {
    job_t *next = job->next;
    job->conn->pending_jobs--;
    job->done(job); // if the client left, conn_send is a no-op
    if (job->conn->closed) {
      loop->need_sweep = 1;
    }
    free(job);
    job = next;
  }
}

int loop_submit(loop_t *loop, conn_t *conn, job_work_fn work, job_done_fn done,
                void *data) {
  if (loop == NULL || conn == NULL || work == NULL || done == NULL ||
      conn->closed) {
    return -1;
  }

  job_t *job = malloc(sizeof(*job));
  if (job == NULL) {
    return -1;
  }
  job->loop = loop;
  job->conn = conn;
  job->work = work;
  job->done = done;
  job->data = data;
  job->next = NULL;

  conn->pending_jobs++; // keeps conn alive until done() ran
  if (thread_pool_submit_cb(&loop->pool, run_job, job, post_to_loop, job) !=
      0) {
    conn->pending_jobs--;
    free(job);
    return -1;
  }
  return 0;
}

// ---------- lifecycle ----------

int loop_init(loop_t **out, int port, size_t num_threads, line_fn on_line) {
  if (out == NULL || on_line == NULL) {
    return -1;
  }

  loop_t *loop = calloc(1, sizeof(*loop));
  if (loop == NULL) {
    return -1;
  }
  loop->kq = -1;
  loop->listen_fd = -1;
  loop->on_line = on_line;

  if (pthread_mutex_init(&loop->done_mutex, NULL) != 0) {
    free(loop);
    return -1;
  }

  loop->kq = kqueue();
  if (loop->kq < 0) {
    goto fail;
  }

  loop->listen_fd = open_listener(port);
  if (loop->listen_fd < 0) {
    goto fail;
  }

  // SIGINT/SIGTERM arrive as kqueue events instead of killing the process.
  signal(SIGINT, SIG_IGN);
  signal(SIGTERM, SIG_IGN);

  if (kq_change(loop->kq, (uintptr_t)loop->listen_fd, EVFILT_READ, EV_ADD, 0,
                NULL) < 0 ||
      kq_change(loop->kq, WAKE_IDENT, EVFILT_USER, EV_ADD | EV_CLEAR, 0, NULL) <
          0 ||
      kq_change(loop->kq, SIGINT, EVFILT_SIGNAL, EV_ADD, 0, NULL) < 0 ||
      kq_change(loop->kq, SIGTERM, EVFILT_SIGNAL, EV_ADD, 0, NULL) < 0) {
    goto fail;
  }

  if (thread_pool_init(&loop->pool, num_threads) != 0) {
    goto fail;
  }

  *out = loop;
  return 0;

fail:
  if (loop->listen_fd >= 0) {
    close(loop->listen_fd);
  }
  if (loop->kq >= 0) {
    close(loop->kq);
  }
  pthread_mutex_destroy(&loop->done_mutex);
  free(loop);
  return -1;
}

int loop_run(loop_t *loop) {
  struct kevent events[MAX_EVENTS];
  loop->running = 1;

  while (loop->running) {
    int n = kevent(loop->kq, NULL, 0, events, MAX_EVENTS, NULL); // sleep here
    if (n < 0) {
      if (errno == EINTR) {
        continue;
      }
      return -1;
    }

    for (int i = 0; i < n; i++) {
      struct kevent *ev = &events[i];

      if (ev->filter == EVFILT_USER) {
        drain_done(loop);
      } else if (ev->filter == EVFILT_SIGNAL) {
        loop->running = 0;
      } else if (ev->filter == EVFILT_READ &&
                 (int)ev->ident == loop->listen_fd) {
        on_accept(loop);
      } else {
        conn_t *c = ev->udata;
        if (c == NULL || c->closed) {
          continue; // closed earlier in this batch
        }
        if (ev->flags & EV_ERROR) {
          conn_close(c);
        } else if (ev->filter == EVFILT_READ) {
          on_readable(loop, c);
        } else if (ev->filter == EVFILT_WRITE) {
          on_writable(c);
        }
      }
    }

    sweep_closed(loop);
  }
  return 0;
}

void loop_destroy(loop_t *loop) {
  if (loop == NULL) {
    return;
  }

  close(loop->listen_fd);           // 1. no new clients
  thread_pool_destroy(&loop->pool); // 2. workers finish queued jobs + join
  drain_done(loop);                 // 3. deliver their results

  while (loop->conns != NULL) { // 4. close + free all conns
    conn_close(loop->conns);
    conn_free(loop, loop->conns);
  }

  close(loop->kq); // 5.
  pthread_mutex_destroy(&loop->done_mutex);
  free(loop);
}
