/*-------------------------------------------------------------------------
 *
 * pgut-ipc.c
 *
 * Copyright (c) 2009, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#ifdef HAVE_SYS_IPC_H
#include <sys/ipc.h>
#endif
#ifdef HAVE_SYS_SHM_H
#include <sys/shm.h>
#endif
#ifdef HAVE_KERNEL_OS_H
#include <kernel/OS.h>
#endif

#include "pgut-ipc.h"
#include "storage/spin.h"
#include "miscadmin.h"

#ifdef SHM_SHARE_MMU			/* use intimate shared memory on Solaris */
#define PG_SHMAT_FLAGS			SHM_SHARE_MMU
#else
#define PG_SHMAT_FLAGS			0
#endif

#define SPIN_SLEEP_MSEC		10	/* 10ms */

#ifdef WIN32
typedef HANDLE	ShmemHandle;
#else
typedef int		ShmemHandle;
#define DEFAULT_SHMEM_KEY			0xBEEF	/* FIXME */
#endif

typedef struct QueueHeader
{
	uint32		size;		/* size of data */
	uint32		begin;		/* read size */
	uint32		end;		/* written size */
	slock_t		mutex;		/* protects the counters only */
	char		data[1];	/* VARIABLE LENGTH ARRAY - MUST BE LAST */
} QueueHeader;

struct Queue
{
	ShmemHandle		handle;
	QueueHeader	   *header;
	uint32			size;	/* copy of header->size */
};

Queue *
QueueCreate(const char *path, uint32 size)
{
	Queue		   *self;
	ShmemHandle		handle;
	QueueHeader	   *header;

	if (size > 0)
	{
#ifdef WIN32
		handle = CreateFileMapping(INVALID_HANDLE_VALUE, NULL, PAGE_READWRITE, 0, size, path);
		if (handle == NULL)
			elog(ERROR, "CreateFileMapping(%s) failed: errcode=%lu", path, GetLastError());

		header = MapViewOfFile(handle, FILE_MAP_ALL_ACCESS, 0, 0, 0);
		if (header == NULL)
			elog(ERROR, "MapViewOfFile failed: errcode=%lu", GetLastError());
#else
		handle = shmget(DEFAULT_SHMEM_KEY, size, IPC_CREAT | IPC_EXCL | 0600);
		if (handle < 0)
			elog(ERROR, "shmget(id=%d) failed: %m", DEFAULT_SHMEM_KEY);

		header = shmat(handle, NULL, PG_SHMAT_FLAGS);
		if (header == (void *) -1)
			elog(ERROR, "shmat(id=%d) failed: %m", DEFAULT_SHMEM_KEY);
#endif

		header->size = size;
		header->begin = header->end = 0;
		SpinLockInit(&header->mutex);
	}
	else
	{
#ifdef WIN32
		handle = OpenFileMapping(FILE_MAP_ALL_ACCESS, FALSE, path);
		if (handle == NULL)
			elog(ERROR, "OpenFileMapping(%s) failed: errcode=%lu", path, GetLastError());

		header = MapViewOfFile(handle, FILE_MAP_ALL_ACCESS, 0, 0, 0);
		if (header == NULL)
			elog(ERROR, "MapViewOfFile failed: errcode=%lu", GetLastError());
#else
		handle = shmget(DEFAULT_SHMEM_KEY, sizeof(QueueHeader), 0);
		if (handle < 0)
			elog(ERROR, "shmget(id=%d) failed: %m", DEFAULT_SHMEM_KEY);

		header = shmat(handle, NULL, PG_SHMAT_FLAGS);
		if (header == NULL)
			elog(ERROR, "shmat(id=%d) failed: %m", DEFAULT_SHMEM_KEY);
#endif
	}

	self = palloc(sizeof(Queue));
	self->handle = handle;
	self->header = header;
	self->size = header->size;
	return self;
}

void
QueueClose(Queue *self)
{
	if (self)
	{
#ifdef WIN32
		UnmapViewOfFile(self->header);
		CloseHandle(self->handle);
#else
		shmdt(self->header);
		shmctl(self->handle, IPC_RMID, NULL);
#endif
		pfree(self);
	}
}

/*
 * TODO: read size and data at once to reduce spinlocks.
 * TODO: do memcpy out of spinlock.
 */
uint32
QueueRead(Queue *self, void *buffer, uint32 len)
{
	volatile QueueHeader *header = self->header;
	const char *data = (const char *) header->data;
	uint32	size = self->size;
	uint32	begin;
	uint32	end;

	if (len > size)
		elog(ERROR, "read length is too large");

retry:
	SpinLockAcquire(&header->mutex);
	begin = header->begin;
	end = header->end;

	if (begin <= end)
	{
		if (begin + len <= end)
		{
			memcpy(buffer, data + begin, len);
			header->begin += len;
			SpinLockRelease(&header->mutex);
			return len;
		}
	}
	else if (begin + len <= size + end)
	{
		if (begin + len <= size)
		{
			memcpy(buffer, data + begin, len);
			header->begin += len;
		}
		else
		{
			uint32	first = size - begin;
			uint32	second = len - first;
			memcpy(buffer, data + begin, first);
			memcpy((char *) buffer + first, data, second);
			header->begin = second;
		}
		SpinLockRelease(&header->mutex);
		return len;
	}

	/* not enough data yet */
	SpinLockRelease(&header->mutex);
	CHECK_FOR_INTERRUPTS();
	pg_usleep(SPIN_SLEEP_MSEC * 1000);

	goto retry;
}

/*
 * Write length and buffer. (sizeof(len) + len) bytes are written.
 * TODO: do memcpy out of spinlock.
 */
bool
QueueWrite(Queue *self, const void *buffer, uint32 len, uint32 timeout_msec)
{
	volatile QueueHeader *header = self->header;
	char   *data = (char *) header->data;
	uint32	size = self->size;
	uint32	begin;
	uint32	end;
	uint32	total = sizeof(len) + len;
	uint32	sleep_msec = 0;

	if (total > size)
		elog(ERROR, "write length is too large");

retry:
	SpinLockAcquire(&header->mutex);
	begin = header->begin;
	end = header->end;

	if (begin > end)
	{
		if (end + total <= begin)
		{
			memcpy(data + end, &len, sizeof(len));
			memcpy(data + end + sizeof(len), buffer, len);
			header->end += total;
			SpinLockRelease(&header->mutex);
			return true;
		}
	}
	else if (end + total <= size + begin)
	{
		if (end + total <= size)
		{
			/* both continuous */
			memcpy(data + end, &len, sizeof(len));
			memcpy(data + end + sizeof(len), buffer, len);
			header->end += total;
		}
		else if (end + sizeof(len) <= size)
		{
			/* len is continuous & buffer is split */
			uint32	first = size - sizeof(len) - end;
			uint32	second = len - first;
			memcpy(data + end, &len, sizeof(len));
			memcpy(data + end + sizeof(len), buffer, first);
			memcpy(data, ((const char *) buffer) + first, second);
			header->end = second;
		}
		else
		{
			/* len is split & buffer is coninuous */
			uint32	first = size - end;
			uint32	second = sizeof(len) - first;
			memcpy(data + end, &len, first);
			memcpy(data, ((const char *) &len) + first, second);
			memcpy(data + second, buffer, len);
			header->end = second + len;
		}
		SpinLockRelease(&header->mutex);
		return true;
	}

	/* buffer is full. sleep and retry unless timeout */
	SpinLockRelease(&header->mutex);

	if (sleep_msec > timeout_msec)
		return false;	/* timeout */

	CHECK_FOR_INTERRUPTS();
	pg_usleep(SPIN_SLEEP_MSEC * 1000);
	sleep_msec += SPIN_SLEEP_MSEC;
	goto retry;
}
