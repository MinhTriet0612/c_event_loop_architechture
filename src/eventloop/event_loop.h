
#ifndef EVENT_LOOP_H
#define EVENT_LOOP_H

#include "threadpool/thread_pool.h"

#include <stddef.h>

typedef struct loop loop_t;
typedef struct conn conn_t;
typedef struct job job_t;

// Called on the LOOP thread for each complete line (without "\n" / "\r\n").
typedef void (*line_fn)(loop_t *loop, conn_t *conn, char *line, size_t len);

typedef void (*job_work_fn)(job_t *job); // runs on a WORKER thread
typedef void (*job_done_fn)(job_t *job); // runs on the LOOP thread, always

// One offloaded task: loop -> pool -> loop.
struct job {
  loop_t *loop;
  conn_t *conn;     // never a raw fd (fd numbers get reused)
  job_work_fn work; // touch only job->data here
  job_done_fn done; // use result, conn_send, free job->data
  void *data;       // input / result, owned by the caller
  struct job *next; // done-queue link
};

// Listen on 127.0.0.1:port with num_threads pool workers.
int loop_init(loop_t **out, int port, size_t num_threads, line_fn on_line);

// Runs until SIGINT / SIGTERM.
int loop_run(loop_t *loop);

// Stops accepting, finishes pending jobs, frees everything.
void loop_destroy(loop_t *loop);

// Offload work to the pool. done() is always called later on the loop
// thread. On error returns -1 and data stays owned by the caller.
int loop_submit(loop_t *loop, conn_t *conn, job_work_fn work, job_done_fn done,
                void *data);

// Loop thread only. Buffers what can't be written now.
// Returns -1 (no-op) if the connection is closed.
int conn_send(conn_t *conn, const char *buf, size_t len);

// Loop thread only.
void conn_close(conn_t *conn);
int conn_is_open(const conn_t *conn);

#endif
