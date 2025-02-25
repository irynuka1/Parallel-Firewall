// SPDX-License-Identifier: BSD-3-Clause

#include <pthread.h>
#include <fcntl.h>
#include <unistd.h>

#include "consumer.h"
#include "ring_buffer.h"
#include "packet.h"
#include "utils.h"

void consumer_thread(so_consumer_ctx_t *ctx)
{
	char buffer[PKT_SZ];
	ssize_t size;

	// Processing the packets
	while (1) {
		size = ring_buffer_dequeue((*ctx).producer_rb, buffer, PKT_SZ);

		// If the size is zero it means that the ring buffer is stopped or empty
		if (size == 0)
			break;
		DIE(size != PKT_SZ, "packet error");

		struct so_packet_t *pkt = (struct so_packet_t *)buffer;
		int action = process_packet(pkt);
		unsigned long hash = packet_hash(pkt);
		unsigned long timestamp = (*pkt).hdr.timestamp;

		// Creating the log entry
		char log_entry[256];
		int len = snprintf(log_entry, sizeof(log_entry), "%s %016lx %lu\n",
						   RES_TO_STR(action), hash, timestamp);

		pthread_mutex_lock((*ctx).log_mutex);
		write((*ctx).out_fd, log_entry, len);
		pthread_mutex_unlock((*ctx).log_mutex);
	}

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
	*next_timestamp = 0;

	// Creating the consumer threads
	for (int i = 0; i < num_consumers; i++) {
		so_consumer_ctx_t *ctx = malloc(sizeof(so_consumer_ctx_t));

		(*ctx).producer_rb = rb;
		(*ctx).out_fd = out_fd;
		(*ctx).log_mutex = log_mutex;
		(*ctx).log_cond = log_cond;
		(*ctx).next_timestamp = next_timestamp;

		int rc = pthread_create(&tids[i], NULL, (void *)consumer_thread, ctx);

		DIE(rc != 0, "thread creation error");
	}

	return num_consumers;
}
