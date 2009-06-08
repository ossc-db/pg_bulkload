/*-------------------------------------------------------------------------
 *
 * pgut-ipc.h
 *
 * Copyright (c) 2009, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 *
 *-------------------------------------------------------------------------
 */

#ifndef PGUT_IPC_H
#define PGUT_IPC_H

#ifdef WIN32
struct iovec
{
	void  *iov_base;    /* Starting address */
	size_t iov_len;     /* Number of bytes to transfer */
};
#else
#include <sys/uio.h>
#endif

typedef struct Queue	Queue;

extern Queue *QueueCreate(unsigned *key, uint32 size);
extern Queue *QueueOpen(unsigned key);
extern void QueueClose(Queue *self);
extern uint32 QueueRead(Queue *self, void *buffer, uint32 len);
extern bool QueueWrite(Queue *self, const struct iovec iov[], int count, uint32 timeout_msec);

#endif   /* PGUT_IPC_H */
