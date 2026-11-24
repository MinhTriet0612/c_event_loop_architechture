
#include "thread_pool.h"

#include <pthread.h>
#include <stdlib.h>

static void *worker(void *arg) {

  // get pool reference from arg
  thread_pool_t *pool = (thread_pool_t *)arg;
  while (1) {
    // get task from queue
    // handle case have no task
    // should we lock first once pick task? okay try it
    // lock mutex
    pthread_mutex_lock(&pool->mutex);

    while (pool->head == NULL && !pool->stopping) { // it always in while loop
      // thread  joins room to sleep and wait for condition signal(for wake up)
      pthread_cond_wait(&pool->condition, &pool->mutex);
    }

    // shutdown and no remaining tasks
    if (pool->stopping && pool->head == NULL) {
      pthread_mutex_unlock(&pool->mutex);
      return NULL;
    }

    // dequeue task
    task_t *task = pool->head;
    pool->head = task->next;

    if (pool->head == NULL) {
      pool->tail = NULL;
    }
    // release lock to allow other threads to access the queue
    pthread_mutex_unlock(&pool->mutex);

    // now on thread just remain one task to execute
    // execute
    void *result = task->function(task->arg);

    // publish result to the future and wake anyone in future_wait
    if (task->future != NULL) {
      future_t *f = task->future;
      pthread_mutex_lock(&f->mutex);
      f->result = result;
      f->done = 1;
      pthread_cond_broadcast(&f->cond);
      pthread_mutex_unlock(&f->mutex);
    }

    // notify via callback (runs on this worker thread)
    if (task->on_done != NULL) {
      task->on_done(result, task->user_data);
    }

    // once done -> we free the task memory (future is owned by the caller)
    free(task);
  }
}

int thread_pool_init(thread_pool_t *pool, size_t num_threads) {
  if (pool == NULL || num_threads == 0) {
    return -1; // can't construct with these values.
  }

  pool->threads = NULL;
  pool->thread_count = 0; // curr we have no threads initialized
  pool->head = NULL;
  pool->tail = NULL;
  pool->stopping = 0;

  if (pthread_mutex_init(&pool->mutex, NULL) != 0) {
    return -1;
  }

  if (pthread_cond_init(&pool->condition, NULL) != 0) {
    pthread_mutex_destroy(&pool->mutex);
    return -1;
  }

  pool->threads = malloc(num_threads * sizeof(pthread_t));

  if (pool->threads == NULL) {
    pthread_cond_destroy(&pool->condition);
    pthread_mutex_destroy(&pool->mutex);
    return -1;
  }

  // in phase allocate thread
  for (size_t i = 0; i < num_threads; i++) {
    if (pthread_create(&pool->threads[i], NULL, worker, pool) != 0) {
      // Stop workers already created.
      pthread_mutex_lock(&pool->mutex);
      pool->stopping = 1;
      pthread_mutex_unlock(&pool->mutex);

      pthread_cond_broadcast(&pool->condition);

      for (size_t j = 0; j < i; j++) {
        pthread_join(pool->threads[j], NULL);
      }

      free(pool->threads);

      pthread_cond_destroy(&pool->condition);
      pthread_mutex_destroy(&pool->mutex);

      return -1;
    }

    pool->thread_count++;
  }
  return 0;
}

static int enqueue(thread_pool_t *pool, task_fn function, void *arg,
                   future_t *future, done_fn on_done, void *user_data) {
  if (pool == NULL || function == NULL) {
    return -1;
  }

  task_t *task = malloc(sizeof(task_t));

  if (task == NULL) {
    return -1;
  }

  task->function = function;
  task->arg = arg;
  task->future = future;
  task->on_done = on_done;
  task->user_data = user_data;
  task->next = NULL;

  pthread_mutex_lock(&pool->mutex);

  if (pool->stopping) {
    pthread_mutex_unlock(&pool->mutex);
    free(task);
    return -1;
  }

  if (pool->tail == NULL) {
    pool->head = task;
    pool->tail = task;
  } else {
    pool->tail->next = task;
    pool->tail = task;
  }

  pthread_mutex_unlock(&pool->mutex);

  pthread_cond_signal(&pool->condition);

  return 0;
}

int thread_pool_submit(thread_pool_t *pool, task_fn function, void *arg) {
  return enqueue(pool, function, arg, NULL, NULL, NULL);
}

int thread_pool_submit_cb(thread_pool_t *pool, task_fn function, void *arg,
                          done_fn on_done, void *user_data) {
  if (on_done == NULL) {
    return -1;
  }
  return enqueue(pool, function, arg, NULL, on_done, user_data);
}

future_t *thread_pool_submit_future(thread_pool_t *pool, task_fn function,
                                    void *arg) {
  future_t *f = malloc(sizeof(future_t));

  if (f == NULL) {
    return NULL;
  }

  f->done = 0;
  f->result = NULL;

  if (pthread_mutex_init(&f->mutex, NULL) != 0) {
    free(f);
    return NULL;
  }

  if (pthread_cond_init(&f->cond, NULL) != 0) {
    pthread_mutex_destroy(&f->mutex);
    free(f);
    return NULL;
  }

  if (enqueue(pool, function, arg, f, NULL, NULL) != 0) {
    future_destroy(f);
    return NULL;
  }

  return f;
}

void *future_wait(future_t *f) {
  pthread_mutex_lock(&f->mutex);

  while (!f->done) {
    pthread_cond_wait(&f->cond, &f->mutex);
  }

  void *result = f->result;
  pthread_mutex_unlock(&f->mutex);

  return result;
}

void future_destroy(future_t *f) {
  if (f == NULL) {
    return;
  }
  pthread_cond_destroy(&f->cond);
  pthread_mutex_destroy(&f->mutex);
  free(f);
}

void *future_get(future_t *f) {
  void *result = future_wait(f);
  future_destroy(f);
  return result;
}

void thread_pool_destroy(thread_pool_t *pool) {
  if (pool == NULL) {
    return;
  }

  pthread_mutex_lock(&pool->mutex);
  pool->stopping = 1;
  pthread_mutex_unlock(&pool->mutex);

  pthread_cond_broadcast(&pool->condition);

  // Wait for all workers to finish.
  for (size_t i = 0; i < pool->thread_count; i++) {
    pthread_join(pool->threads[i], NULL);
  }

  // Free any remaining queued tasks.
  // Normally empty because workers drain the queue.
  task_t *task = pool->head;

  while (task != NULL) {
    task_t *next = task->next;
    free(task);
    task = next;
  }

  free(pool->threads);

  pthread_cond_destroy(&pool->condition);
  pthread_mutex_destroy(&pool->mutex);
}
