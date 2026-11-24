
#include "eventloop/event_loop.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

// Line protocol:
//   echo <text>  -> reply right away on the loop thread
//   sq <n>       -> n*n computed on a pool worker
//   slow <sec>   -> worker sleeps <sec> (max 10) then replies
//   quit         -> close connection

typedef struct {
  const char *op;
  long n;
  long result;
} calc_t;

// WORKER thread: only touches job->data
static void sq_work(job_t *job) {
  calc_t *c = job->data;
  c->result = c->n * c->n;
}

static void slow_work(job_t *job) {
  calc_t *c = job->data;
  sleep((unsigned)c->n);
  c->result = c->n;
}

// LOOP thread: send the result, free the data
static void reply_done(job_t *job) {
  calc_t *c = job->data;
  char out[64];
  int len = snprintf(out, sizeof(out), "%s %ld = %ld\n", c->op, c->n,
                     c->result);
  conn_send(job->conn, out, (size_t)len); // no-op if the client left
  free(c);
}

static void on_line(loop_t *loop, conn_t *conn, char *line, size_t len) {
  (void)len;

  if (strncmp(line, "echo ", 5) == 0) {
    conn_send(conn, line + 5, strlen(line + 5));
    conn_send(conn, "\n", 1);
    return;
  }

  if (strcmp(line, "quit") == 0) {
    conn_close(conn);
    return;
  }

  job_work_fn work = NULL;
  const char *op = NULL;
  if (strncmp(line, "sq ", 3) == 0) {
    work = sq_work;
    op = "sq";
  } else if (strncmp(line, "slow ", 5) == 0) {
    work = slow_work;
    op = "slow";
  }

  if (work == NULL) {
    conn_send(conn, "unknown command\n", 16);
    return;
  }

  calc_t *c = malloc(sizeof(*c));
  if (c == NULL) {
    conn_send(conn, "error\n", 6);
    return;
  }
  c->op = op;
  c->n = strtol(strchr(line, ' ') + 1, NULL, 10);
  c->result = 0;
  if (work == slow_work && (c->n < 0 || c->n > 10)) {
    c->n = c->n < 0 ? 0 : 10;
  }

  if (loop_submit(loop, conn, work, reply_done, c) != 0) {
    free(c);
    conn_send(conn, "error\n", 6);
  }
}

int main(int argc, char **argv) {
  int port = argc > 1 ? atoi(argv[1]) : 8080;
  loop_t *loop;

  if (loop_init(&loop, port, 4, on_line) != 0) {
    perror("loop_init");
    return 1;
  }

  printf("listening on 127.0.0.1:%d (Ctrl-C to stop)\n", port);
  fflush(stdout);

  loop_run(loop);

  printf("shutting down...\n");
  loop_destroy(loop);
  printf("bye\n");
  return 0;
}
