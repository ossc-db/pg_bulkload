/*-------------------------------------------------------------------------
 *
 * pgut-ipc.c
 *
 * Copyright (c) 2009-2010, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#ifndef WIN32
#include <unistd.h>
#endif

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
#include "storage/ipc.h"
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

static void win32_shmemName(char *name, size_t len, unsigned key)
{
	snprintf(name, len, "pg_bulkload_%u", key);
}
#else
typedef int		ShmemHandle;
#endif

typedef struct QueueHeader
{
	uint32		magic;		/* magic # to identify pgut-queue segments */
#define PGUTShmemMagic	0550
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
QueueCreate(unsigned *key, uint32 size)
{
	Queue		   *self;
	ShmemHandle		handle;
	QueueHeader	   *header;
	unsigned		shmemKey;
#ifdef WIN32
	char	shmemName[MAX_PATH];
#endif

retry:
	shmemKey = (getpid() << 16 | (unsigned) rand());

#ifdef WIN32
	win32_shmemName(shmemName, lengthof(shmemName), shmemKey);

	handle = CreateFileMappingA(INVALID_HANDLE_VALUE, NULL, PAGE_READWRITE, 0,
							   offsetof(QueueHeader, data) + size, shmemName);
	if (handle == NULL)
	{
		if (GetLastError() == ERROR_ALREADY_EXISTS)
		{
			/* conflicted. retry. */
			goto retry;
		}
		elog(ERROR, "CreateFileMapping(%s) failed: errcode=%lu", shmemName, GetLastError());
	}

	header = MapViewOfFile(handle, FILE_MAP_ALL_ACCESS, 0, 0, 0);
	if (header == NULL)
		elog(ERROR, "MapViewOfFile failed: errcode=%lu", GetLastError());
#else
	handle = shmget(shmemKey, offsetof(QueueHeader, data) + size, IPC_CREAT | IPC_EXCL | 0600);
	if (handle < 0)
	{
		if (errno == EEXIST || errno == EACCES
#ifdef EIDRM
			|| errno == EIDRM
#endif
		)
		{
			/* conflicted. retry. */
			goto retry;
		}
		elog(ERROR, "shmget(id=%d) failed: %m", shmemKey);
	}

	header = shmat(handle, NULL, PG_SHMAT_FLAGS);
	if (header == (void *) -1)
		elog(ERROR, "shmat(id=%d) failed: %m", shmemKey);
#endif

	*key = shmemKey;
	header->magic = PGUTShmemMagic;
	header->size = size;
	header->begin = header->end = 0;
	SpinLockInit(&header->mutex);

	self = palloc(sizeof(Queue));
	self->handle = handle;
	self->header = header;
	self->size = header->size;
	return self;
}

Queue *
QueueOpen(unsigned key)
{
	Queue		   *self;
	ShmemHandle		handle;
	QueueHeader	   *header;

#ifdef WIN32
	char	shmemName[MAX_PATH];

	win32_shmemName(shmemName, lengthof(shmemName), key);

	handle = OpenFileMappingA(FILE_MAP_ALL_ACCESS, FALSE, shmemName);
	if (handle == NULL)
		elog(ERROR, "OpenFileMapping(%s) failed: errcode=%lu", shmemName, GetLastError());

	header = MapViewOfFile(handle, FILE_MAP_ALL_ACCESS, 0, 0, 0);
	if (header == NULL)
		elog(ERROR, "MapViewOfFile failed: errcode=%lu", GetLastError());
#else
	handle = shmget(key, sizeof(QueueHeader), 0);
	if (handle < 0)
		elog(ERROR, "shmget(id=%d) failed: %m", key);

	header = shmat(handle, NULL, PG_SHMAT_FLAGS);
	if (header == (void *) -1)
		elog(ERROR, "shmat(id=%d) failed: %m", key);
#endif

	/* check magic number */
	if (header->magic != PGUTShmemMagic)
	{
#ifdef WIN32
		UnmapViewOfFile(header);
		CloseHandle(handle);
#else
		shmdt(header);
		shmctl(handle, IPC_RMID, NULL);
#endif
		elog(ERROR, "segment belongs to a non-pgut app");
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
QueueRead(Queue *self, void *buffer, uint32 len, bool need_lock)
{
	volatile QueueHeader *header = self->header;
	const char *data = (const char *) header->data;
	uint32	size = self->size;
	uint32	begin;
	uint32	end;

	if (len > size)
		elog(ERROR, "read length is too large");

retry:
	if (need_lock)
		SpinLockAcquire(&header->mutex);

	begin = header->begin;
	end = header->end;

	if (begin <= end)
	{
		if (begin + len <= end)
		{
			memcpy(buffer, data + begin, len);
			header->begin += len;
			if (need_lock)
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
		if (need_lock)
			SpinLockRelease(&header->mutex);

		return len;
	}

	/* not enough data yet */
	if (need_lock)
		SpinLockRelease(&header->mutex);

	CHECK_FOR_INTERRUPTS();
	pg_usleep(SPIN_SLEEP_MSEC * 1000);

	goto retry;
}

/*
 * TODO: do memcpy out of spinlock.
 */
bool
QueueWrite(Queue *self, const struct iovec iov[], int count, uint32 timeout_msec, bool need_lock)
{
	volatile QueueHeader *header = self->header;
	char   *data = (char *) header->data;
	char   *dst;
	uint32	size = self->size;
	uint32	begin;
	uint32	end;
	uint32	total;
	uint32	sleep_msec = 0;
	int		i;

	total = 0;
	for (i = 0; i < count; i++)
		total += iov[i].iov_len;

	if (total > size)
		elog(ERROR, "write length is too large");

retry:
	if (need_lock)
		SpinLockAcquire(&header->mutex);

	begin = header->begin;
	end = header->end;
	dst = data + end;

	if (begin > end)
	{
		if (end + total <= begin)
		{
			for (i = 0; i < count; i++)
			{
				memcpy(dst, iov[i].iov_base, iov[i].iov_len);
				dst += iov[i].iov_len;
			}
			header->end += total;
			if (need_lock)
				SpinLockRelease(&header->mutex);

			return true;
		}
	}
	else if (end + total <= size + begin)
	{
		if (end + total <= size)
		{
			/* both continuous */
			for (i = 0; i < count; i++)
			{
				memcpy(dst, iov[i].iov_base, iov[i].iov_len);
				dst += iov[i].iov_len;
			}
		}
		else
		{
			uint32	head;
			uint32	tail = size - end;

			/* first half */
			for (i = 0; i < count && iov[i].iov_len <= tail; i++)
			{
				memcpy(dst, iov[i].iov_base, iov[i].iov_len);
				dst += iov[i].iov_len;
				tail -= iov[i].iov_len;
			}

			/* split element */
			if (i < count)
			{
				head = iov[i].iov_len - tail;
				memcpy(dst, iov[i].iov_base, tail);
				memcpy(data, ((const char *) iov[i].iov_base) + tail, head);
				dst = data + head;
				i++;
			}

			/* second half */
			for (; i < count; i++)
			{
				memcpy(dst, iov[i].iov_base, iov[i].iov_len);
				dst += iov[i].iov_len;
			}
		}
		header->end = dst - data;
		if (need_lock)
			SpinLockRelease(&header->mutex);

		return true;
	}

	/* buffer is full. sleep and retry unless timeout */
	if (need_lock)
		SpinLockRelease(&header->mutex);

	if (sleep_msec > timeout_msec)
		return false;	/* timeout */

	CHECK_FOR_INTERRUPTS();
	pg_usleep(SPIN_SLEEP_MSEC * 1000);
	sleep_msec += SPIN_SLEEP_MSEC;
	goto retry;
}
