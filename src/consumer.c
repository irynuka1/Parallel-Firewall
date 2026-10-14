// SPDX-License-Identifier: BSD-3-Clause

#include <pthread.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "consumer.h"
#include "ring_buffer.h"
#include "packet.h"
#include "utils.h"

void consumer_thread(so_consumer_ctx_t *ctx)
{
	char buffer[PKT_SZ];
	char batch[LOG_BATCH * LOG_ENTRY_SZ];
	ssize_t size;
	unsigned long seq;

	// Processing the packets
	while (1) {
		// Dequeue and sequence assignment are atomic, so the sequence
		// number reflects the order in which packets entered the buffer
		pthread_mutex_lock((*ctx).deq_mutex);
		size = ring_buffer_dequeue((*ctx).producer_rb, buffer, PKT_SZ);

		// If the size is zero it means that the ring buffer is stopped or empty
		if (size == 0) {
			pthread_mutex_unlock((*ctx).deq_mutex);
			break;
		}
		seq = (*(*ctx).next_seq)++;
		pthread_mutex_unlock((*ctx).deq_mutex);

		DIE(size != PKT_SZ, "packet error");

		struct so_packet_t *pkt = (struct so_packet_t *)buffer;
		int action = process_packet(pkt);
		unsigned long hash = packet_hash(pkt);
		unsigned long timestamp = (*pkt).hdr.timestamp;

		// Creating the log entry
		char log_entry[256];
		int len = snprintf(log_entry, sizeof(log_entry), "%s %016lx %lu\n",
						   RES_TO_STR(action), hash, timestamp);

		// Publish the entry in its slot, then write every entry that is
		// ready in order. Nobody blocks waiting for a predecessor, except
		// when the slot window is full.
		pthread_mutex_lock((*ctx).log_mutex);
		while (seq - *(*ctx).next_timestamp >= LOG_SLOTS)
			pthread_cond_wait((*ctx).log_cond, (*ctx).log_mutex);

		so_log_slot_t *slot = &(*ctx).slots[seq % LOG_SLOTS];

		memcpy((*slot).buf, log_entry, len);
		(*slot).len = len;
		(*slot).ready = 1;

		// Only one consumer at a time drains the ready entries, in order,
		// and writes them in batches outside of the lock
		if (!*(*ctx).flushing) {
			*(*ctx).flushing = 1;

			while (1) {
				size_t n = 0;
				size_t cnt = 0;

				while (cnt < LOG_BATCH &&
				       (*ctx).slots[*(*ctx).next_timestamp % LOG_SLOTS].ready) {
					so_log_slot_t *s = &(*ctx).slots[*(*ctx).next_timestamp % LOG_SLOTS];

					memcpy(batch + n, (*s).buf, (*s).len);
					n += (*s).len;
					(*s).ready = 0;
					(*(*ctx).next_timestamp)++;
					cnt++;
				}

				if (n == 0)
					break;

				pthread_cond_broadcast((*ctx).log_cond);
				pthread_mutex_unlock((*ctx).log_mutex);
				write((*ctx).out_fd, batch, n);
				pthread_mutex_lock((*ctx).log_mutex);
			}

			*(*ctx).flushing = 0;
		}
		pthread_mutex_unlock((*ctx).log_mutex);
	}

	free(ctx);
	pthread_exit(NULL);
}

int create_consumers(pthread_t *tids,
					 int num_consumers,
					 so_ring_buffer_t *rb,
					 const char *out_filename)
{
	// Creating (if needed) and opening the output file with read and write permissions
	int out_fd = open(out_filename, O_RDWR | O_CREAT | O_TRUNC, 0666);

	DIE(out_fd < 0, "output file error");

	// Allocating and initializing the log mutex, condition variable and next timestamp
	pthread_mutex_t *log_mutex = malloc(sizeof(pthread_mutex_t));
	pthread_cond_t *log_cond = malloc(sizeof(pthread_cond_t));
	unsigned long *next_timestamp = malloc(sizeof(unsigned long));

	pthread_mutex_init(log_mutex, NULL);
	pthread_cond_init(log_cond, NULL);
	int *flushing = calloc(1, sizeof(int));

	DIE(flushing == NULL, "calloc flushing");
	so_log_slot_t *slots = calloc(LOG_SLOTS, sizeof(so_log_slot_t));

	DIE(slots == NULL, "calloc slots");
	*next_timestamp = 0;

	pthread_mutex_t *deq_mutex = malloc(sizeof(pthread_mutex_t));
	unsigned long *next_seq = malloc(sizeof(unsigned long));

	pthread_mutex_init(deq_mutex, NULL);
	*next_seq = 0;

	// Creating the consumer threads
	for (int i = 0; i < num_consumers; i++) {
		so_consumer_ctx_t *ctx = malloc(sizeof(so_consumer_ctx_t));

		(*ctx).producer_rb = rb;
		(*ctx).out_fd = out_fd;
		(*ctx).log_mutex = log_mutex;
		(*ctx).log_cond = log_cond;
		(*ctx).slots = slots;
		(*ctx).flushing = flushing;
		(*ctx).next_timestamp = next_timestamp;
		(*ctx).deq_mutex = deq_mutex;
		(*ctx).next_seq = next_seq;

		int rc = pthread_create(&tids[i], NULL, (void *)consumer_thread, ctx);

		DIE(rc != 0, "thread creation error");
	}

	return num_consumers;
}
