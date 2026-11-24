CC      = cc
CFLAGS  = -Wall -Wextra -g -pthread -Isrc
BUILD   = build

POOL_SRC = src/threadpool/thread_pool.c

.PHONY: all pool_demo results_demo server clean

all: pool_demo results_demo server

pool_demo: $(BUILD)/pool_demo

$(BUILD)/pool_demo: examples/pool_demo.c $(POOL_SRC) src/threadpool/thread_pool.h | $(BUILD)
	$(CC) $(CFLAGS) examples/pool_demo.c $(POOL_SRC) -o $@

results_demo: $(BUILD)/results_demo

$(BUILD)/results_demo: examples/results_demo.c $(POOL_SRC) src/threadpool/thread_pool.h | $(BUILD)
	$(CC) $(CFLAGS) examples/results_demo.c $(POOL_SRC) -o $@

LOOP_SRC = src/eventloop/event_loop.c

server: $(BUILD)/server

$(BUILD)/server: src/main.c $(LOOP_SRC) src/eventloop/event_loop.h $(POOL_SRC) src/threadpool/thread_pool.h | $(BUILD)
	$(CC) $(CFLAGS) src/main.c $(LOOP_SRC) $(POOL_SRC) -o $@

$(BUILD):
	mkdir -p $(BUILD)

clean:
	rm -rf $(BUILD)
