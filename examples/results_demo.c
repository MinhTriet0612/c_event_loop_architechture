
#include "threadpool/thread_pool.h"

#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>

// int packed into the pointer: no malloc needed
static void *square(void *arg) {
  intptr_t n = (intptr_t)arg;
  return (void *)(n * n);
}

// runs on a worker thread -> atomic add
static void on_square_done(void *result, void *user_data) {
  atomic_fetch_add((atomic_int *)user_data, (int)(intptr_t)result);
}

int main(void) {
  thread_pool_t pool;

  if (thread_pool_init(&pool, 4) != 0) {
    fprintf(stderr, "Pool initialization failed\n");
    return 1;
  }

  // A: futures
  future_t *futures[10];
  for (intptr_t i = 0; i < 10; i++) {
    futures[i] = thread_pool_submit_future(&pool, square, (void *)i);
  }

  printf("futures:");
  for (int i = 0; i < 10; i++) {
    if (futures[i] == NULL) {
      printf(" ERR");
      continue;
    }
    printf(" %d", (int)(intptr_t)future_get(futures[i]));
  }
  printf("\n");

  // B: callbacks
  atomic_int total = 0;
  for (intptr_t i = 0; i < 10; i++) {
    thread_pool_submit_cb(&pool, square, (void *)i, on_square_done, &total);
  }

  thread_pool_destroy(&pool); // drains + joins, so all callbacks have run

  printf("callback total: %d (expect 285)\n", atomic_load(&total));

  return 0;
}
