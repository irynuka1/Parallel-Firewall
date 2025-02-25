// SPDX-License-Identifier: BSD-3-Clause

#include "ring_buffer.h"
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

// Helper function for data copying
static void ring_buffer_copy(char *ring_data, size_t cap, size_t pos, void *data, size_t size, int is_enqueue)
{
	size_t space_to_end = cap - pos;

	if (size <= space_to_end) {
		if (is_enqueue)
			memcpy(ring_data + pos, data, size);
		else
			memcpy(data, ring_data + pos, size);
	} else {
		// Wrapping around the ring buffer
		if (is_enqueue) {
			memcpy(ring_data + pos, data, space_to_end);
			memcpy(ring_data, (char *)data + space_to_end, size - space_to_end);
		} else {
			memcpy(data, ring_data + pos, space_to_end);
			memcpy((char *)data + space_to_end, ring_data, size - space_to_end);
		}
	}
}

int ring_buffer_init(so_ring_buffer_t *ring, size_t cap)
{
	if (!ring || cap == 0)
		return -1;

	(*ring).data = malloc(cap);
	if (!(*ring).data)
		return -1;

	(*ring).cap = cap;
	(*ring).len = 0;
	(*ring).read_pos = 0;
	(*ring).write_pos = 0;
	(*ring).is_stopped = 0;

	// Checking the initializations
	if (pthread_mutex_init(&(*ring).mutex, NULL) != 0) {
		free((*ring).data);
		return -1;
	}

	if (pthread_cond_init(&(*ring).not_full, NULL) != 0 || pthread_cond_init(&(*ring).not_empty, NULL) != 0) {
		pthread_mutex_destroy(&(*ring).mutex);
		free((*ring).data);
		return -1;
	}

	return 0;
}

ssize_t ring_buffer_enqueue(so_ring_buffer_t *ring, void *data, size_t size)
{
	if (!ring || !data || size == 0)
		return -1;

	pthread_mutex_lock(&(*ring).mutex);

	// Waiting for the ring buffer to free up space
	while ((*ring).len + size > (*ring).cap && !(*ring).is_stopped)
		pthread_cond_wait(&(*ring).not_full, &(*ring).mutex);

	if ((*ring).is_stopped) {
		pthread_mutex_unlock(&(*ring).mutex);
		return 0;
	}

	// Copying data into the ring buffer
	ring_buffer_copy((*ring).data, (*ring).cap, (*ring).write_pos, data, size, 1);

	(*ring).write_pos = ((*ring).write_pos + size) % (*ring).cap;
	(*ring).len += size;

	pthread_cond_signal(&(*ring).not_empty);
	pthread_mutex_unlock(&(*ring).mutex);

	return size;
}

ssize_t ring_buffer_dequeue(so_ring_buffer_t *ring, void *data, size_t size)
{
	if (!ring || !data || size == 0)
		return -1;

	pthread_mutex_lock(&(*ring).mutex);

	// Waiting for the ring buffer to have enough data
	while ((*ring).len < size && !(*ring).is_stopped)
		pthread_cond_wait(&(*ring).not_empty, &(*ring).mutex);

	if ((*ring).len < size && (*ring).is_stopped) {
		pthread_mutex_unlock(&(*ring).mutex);
		return 0;
	}

	// Copying data from the ring buffer
	ring_buffer_copy((*ring).data, (*ring).cap, (*ring).read_pos, data, size, 0);

	(*ring).read_pos = ((*ring).read_pos + size) % (*ring).cap;
	(*ring).len -= size;

	pthread_cond_signal(&(*ring).not_full);
	pthread_mutex_unlock(&(*ring).mutex);

	return size;
}

void ring_buffer_destroy(so_ring_buffer_t *ring)
{
	if (!ring)
		return;

	free((*ring).data);
	pthread_mutex_destroy(&(*ring).mutex);
	pthread_cond_destroy(&(*ring).not_full);
	pthread_cond_destroy(&(*ring).not_empty);
}

void ring_buffer_stop(so_ring_buffer_t *ring)
{
	if (!ring)
		return;

	pthread_mutex_lock(&(*ring).mutex);
	(*ring).is_stopped = 1;
	// Waking up all the threads waiting on the ring buffer
	pthread_cond_broadcast(&(*ring).not_empty);
	pthread_cond_broadcast(&(*ring).not_full);
	pthread_mutex_unlock(&(*ring).mutex);
}
