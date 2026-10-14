/* SPDX-License-Identifier: BSD-3-Clause */

#ifndef __SO_CONSUMER_H__
#define __SO_CONSUMER_H__

#include "ring_buffer.h"
#include "packet.h"

#define LOG_SLOTS 4096
#define LOG_ENTRY_SZ 64
#define LOG_BATCH 128

/* Finished log line waiting for its turn to be written to the file */
typedef struct so_log_slot_t {
	char buf[LOG_ENTRY_SZ];
	int len;
	int ready;
} so_log_slot_t;

typedef struct so_consumer_ctx_t {
	struct so_ring_buffer_t *producer_rb;
	int out_fd;

	pthread_mutex_t *log_mutex;
	pthread_cond_t *log_cond;
	so_log_slot_t *slots;
	int *flushing;
	unsigned long *next_timestamp;
	pthread_mutex_t *deq_mutex;
	unsigned long *next_seq;
} so_consumer_ctx_t;

int create_consumers(pthread_t *tids,
					int num_consumers,
					so_ring_buffer_t *rb,
					const char *out_filename);

#endif /* __SO_CONSUMER_H__ */
