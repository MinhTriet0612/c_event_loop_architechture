
#ifndef THREAD_POOL_H
#define THREAD_POOL_H

#include <pthread.h>
#include <stddef.h>

// A task returns its result as void* (cast back by the caller).
typedef void *(*task_fn)(void *arg);

// Called on the WORKER thread after the task finishes.
typedef void (*done_fn)(void *result, void *user_data);

// One-shot result slot. Caller waits with future_wait, then frees with
// future_destroy.
typedef struct future {
  pthread_mutex_t mutex;
  pthread_cond_t cond;
  int done;
  void *result;
} future_t;

typedef struct task {
  task_fn function;
  void *arg;
  future_t *future; // NULL = no future
  done_fn on_done;  // NULL = no callback
  void *user_data;
  struct task *next;
} task_t;

typedef struct {
  pthread_t *threads;
  size_t thread_count;

  task_t *head;
  task_t *tail;

  pthread_mutex_t mutex;
  pthread_cond_t condition;

  int stopping;
} thread_pool_t;

int thread_pool_init(thread_pool_t *pool, size_t num_threads);

// Fire-and-forget: result is ignored.
int thread_pool_submit(thread_pool_t *pool, task_fn function, void *arg);

// Returns a future, or NULL on error (e.g. pool stopping).
future_t *thread_pool_submit_future(thread_pool_t *pool, task_fn function,
                                    void *arg);

// on_done(result, user_data) runs on a worker thread when the task finishes.
int thread_pool_submit_cb(thread_pool_t *pool, task_fn function, void *arg,
                          done_fn on_done, void *user_data);

// Blocks until the task is done, returns its result.
void *future_wait(future_t *future);

// Call only after future_wait has returned.
void future_destroy(future_t *future);

// future_wait + future_destroy in one call. Do not use the future afterwards.
void *future_get(future_t *future);

void thread_pool_destroy(thread_pool_t *pool);

#endif
