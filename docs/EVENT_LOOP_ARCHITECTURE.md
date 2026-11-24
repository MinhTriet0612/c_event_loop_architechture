# Event Loop + Thread Pool Architecture (macOS / kqueue)

The main thread runs an event loop (kqueue) for all socket I/O. The existing
thread pool (`thread_pool.c`) runs blocking or CPU-heavy work behind it.
Results come back to the loop thread through a done queue and a wake-up event.

## 1. Why both?

| Need | Tool | Reason |
|---|---|---|
| Many idle connections | kqueue | OS notifies when an fd is ready; 1 thread handles thousands |
| CPU-heavy / blocking work | thread pool | would freeze the loop if run in a callback |

kqueue only tells you "fd is ready". It does not run work. A slow callback
blocks every client.

Same split as libuv:

```
PATH 1: loop thread  -> sockets, pipes, timers, signals (OS can notify)
PATH 2: thread pool  -> file I/O, DNS, CPU work (OS can't notify / blocks)
```

## 2. Big picture

```
┌──────────────────────── MAIN THREAD (event loop) ─────────────────────────┐
│                                                                           │
│   kqueue (kq) watches:                                                    │
│   ┌─────────────┬──────────────┬──────────────┬───────────────┐           │
│   │ listen fd   │ conn fds     │ EVFILT_USER  │ EVFILT_SIGNAL │           │
│   │ READ        │ READ / WRITE │ (wake-up)    │ (SIGINT)      │           │
│   └──────┬──────┴──────┬───────┴──────┬───────┴───────┬───────┘           │
│          ▼             ▼              ▼               ▼                   │
│      on_accept     on_read /      drain_done()    running = 0             │
│                    on_write           │                                   │
│                       │               │ job->done(conn) → write reply     │
│                       │ heavy?        │                                   │
└───────────────────────┼───────────────▲───────────────────────────────────┘
                        │ submit(job)   │ push job + NOTE_TRIGGER
                        ▼               │
          ┌─ thread pool (existing) ────┴──────────────┐
          │ queue → worker0..N → run_job + post_to_loop│
          │           job->work(job)                   │
          │           lock done_mutex; push; unlock    │
          │           kevent(EVFILT_USER, TRIGGER)     │
          └────────────────────────────────────────────┘
```

## 3. Components

| # | Component | Runs on | Job |
|---|---|---|---|
| 1 | Event loop | main thread | owns kqueue, dispatches events |
| 2 | `conn_t` (in kqueue `udata`) | main thread | per-client state, buffers |
| 3 | Thread pool | workers | runs `job->work` |
| 4 | Done queue | shared (mutex) | workers push finished jobs, loop drains |
| 5 | `EVFILT_USER` | shared | worker wakes the sleeping loop |
| 6 | `EVFILT_SIGNAL` | main thread | SIGINT becomes a normal loop event |

## 4. Data structures

```c
typedef struct conn {
  int fd;
  char rbuf[4096]; size_t rlen;       // partial reads accumulate here
  char *wbuf; size_t wlen, woff;      // pending output (partial writes)
  int closed;                         // set on disconnect
  int pending_jobs;                   // free only when closed && pending_jobs == 0
} conn_t;

typedef struct job {
  struct loop *loop;                  // worker finds the done queue
  conn_t *conn;                       // never a raw fd (fd reuse!)
  void (*work)(struct job *);         // runs on WORKER thread
  void (*done)(struct job *);         // runs on LOOP thread
  void *data;                         // input / result
  struct job *next;                   // done-queue link
} job_t;

typedef struct loop {
  int kq, listen_fd, running;
  thread_pool_t pool;
  pthread_mutex_t done_mutex;
  job_t *done_head, *done_tail;
} loop_t;
```

## 5. Flows

### A. Connection and small request (loop only)

```
listen fd readable -> on_accept: accept() -> non-blocking + SO_NOSIGPIPE
                      -> conn_t -> register EVFILT_READ (udata = conn)
conn fd readable   -> on_read: read() into rbuf -> small work? reply now
```

### B. Offload heavy work (loop -> pool)

```
on_read: needs heavy work
  job = malloc; job->conn = conn; conn->pending_jobs++
  thread_pool_submit_cb(&pool, run_job, job, post_to_loop, job)  // returns immediately
  back to kevent()                                    // loop stays free
```

### C. Completion (pool -> loop)

```
worker: job->work(job)                      // blocking / CPU part
worker: lock done_mutex; push job; unlock
worker: kevent(EVFILT_USER, NOTE_TRIGGER)   // wake loop

loop:   USER event -> lock; take WHOLE done queue; unlock
loop:   for each job:
          conn->pending_jobs--
          conn->closed ? skip : job->done(job)     // fd-reuse safe
          if closed && pending_jobs == 0 -> free(conn)
          free(job)
```

### D. Partial write

```
write() sent fewer bytes -> keep rest in wbuf -> register EVFILT_WRITE
conn fd writable -> on_write: send rest -> done? unregister EVFILT_WRITE
```

### E. Shutdown

```
SIGINT -> EVFILT_SIGNAL -> running = 0
1. close listen fd              (no new clients)
2. thread_pool_destroy(&pool)   (workers drain + join; kq still open)
3. drain_done() one last time   (free jobs, update conns)
4. close + free all conns
5. close(kq), destroy done_mutex
```

## 6. Thread rules (most important)

```
┌──────────────────────┬───────────────────────────────────────────┐
│ LOOP thread ONLY     │ conn_t, sockets, kqueue registrations,    │
│                      │ done callbacks            -> no locks     │
├──────────────────────┼───────────────────────────────────────────┤
│ WORKER threads ONLY  │ job->work touches job->data only          │
│                      │ (never conn, never fd)                    │
├──────────────────────┼───────────────────────────────────────────┤
│ SHARED (mutex)       │ done queue (done_mutex), pool queue       │
└──────────────────────┴───────────────────────────────────────────┘
```

## 7. Pitfalls

- Sockets must be non-blocking, otherwise one `read` freezes the loop.
- Wake-ups coalesce: 5 finished jobs may produce 1 event. Always drain the whole done queue.
- `write` can be partial. Buffer the rest and wait for `EVFILT_WRITE`.
- fd reuse: a job must reference `conn_t`, not the fd number.
- Writing to a closed socket raises SIGPIPE. Use `SO_NOSIGPIPE`.
- Use `EVFILT_SIGNAL` for SIGINT. Don't touch loop state in a signal handler.
- Start level-triggered (kqueue default). Use `EV_CLEAR` only for `EVFILT_USER`.

## 8. Build order

```
M0  fix pool unlock bug (thread_pool.c worker)      -> 2 slow tasks run in parallel
M1  kqueue + listen + echo (one conn)               -> nc localhost 8080 echoes
M2  conn_t, multi-client, rbuf/wbuf, SO_NOSIGPIPE   -> 100 clients; kill one mid-write
M3  EVFILT_USER + done queue (plain pthread)        -> callback runs on loop thread
M4  plug in pool: submit_cb, pending_jobs            -> "sleep 2" job doesn't block others
M5  EVFILT_SIGNAL shutdown + leak check             -> Ctrl-C clean exit, `leaks` = 0
```

The thread pool API needs no changes. New code goes in `event_loop.c/.h`.

**Prerequisite (M0):** `thread_pool.c` worker is missing
`pthread_mutex_unlock(&pool->mutex);` after the dequeue. Without it, a worker
runs the task while still holding the mutex, and the loop deadlocks in M4.
