
#include "threadpool/thread_pool.h"

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

void *do_work(void *arg) {
  int value = *(int *)arg;

  printf("Task %d running\n", value);

  sleep(1);

  printf("Task %d finished\n", value);

  return NULL;
}

int main(void) {
  thread_pool_t pool;

  if (thread_pool_init(&pool, 4) != 0) {
    fprintf(stderr, "Pool initialization failed\n");
    return 1;
  }

  int *values = malloc(10 * sizeof(int));
  if (values == NULL) {
    return 1;
  }

  for (int i = 0; i < 10; i++) {
    values[i] = i;

    if (thread_pool_submit(&pool, do_work, &values[i]) != 0) {
      fprintf(stderr, "Submit failed\n");
    }
  }

  thread_pool_destroy(&pool);

  free(values);

  return 0;
}
