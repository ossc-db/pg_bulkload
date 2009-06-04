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

typedef struct Queue	Queue;

extern Queue *QueueCreate(const char *path, uint32 size);
extern void QueueClose(Queue *self);
extern uint32 QueueRead(Queue *self, void *buffer, uint32 len);
extern bool QueueWrite(Queue *self, const void *buffer, uint32 len, uint32 timeout_msec);

#endif   /* PGUT_IPC_H */
