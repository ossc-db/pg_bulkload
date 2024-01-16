/*-------------------------------------------------------------------------
 *
 * pgut-ipc.c
 *
 * Copyright (c) 2009-2024, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <unistd.h>

#if PG_VERSION_NUM >= 160000
#include <sys/ipc.h>
#include <sys/shm.h>
#else
#ifdef HAVE_SYS_IPC_H
#include <sys/ipc.h>
#endif
#ifdef HAVE_SYS_SHM_H
#include <sys/shm.h>
#endif
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
typedef int		ShmemHandle;	/* shared memory ID returned by shmget(2) */
#endif

/*
 * QueueHeader represents a queue instance which is usable by multiple
 * processes.  It is designed to be placed on shared memory, and holds data
 * buffer and additional information such as size and current states.
 *
 * Actual data is stored in an array of char following fixed-size portion of
 * QueueHeader structure.  The size of the buffer is fixed at the creation of
 * the queue, and kept in in QueueHeader.size.
 *
 * The buffer is continuous simple array, and used in the manner of so-called
 * "ring buffer".  First data is written at the head of the array, and next
 * will follow it.  Once array is used up, unwritten portion is written at the
 * head of the array.
 *
 *  +---+---+-------+---------------------------+
 *  | 1 | 2 |   3   |           not-used        |
 *  +---+---+-------+---------------------------+
 *
 * After more data has been written, and data #1 ~ #4 has been read, the buffer
 * looks like this:
 *
 *  +-------------+----------+---+--------------+
 *  | 6(2nd half) | not-used | 5 | 6(1st half)  |
 *  +-------------+----------+---+--------------+
 *
 * In the example above, data 6 is written separately, but it can be read as
 * continuous entry on next read request.
 *
 * To manage current status of buffer, we use two pointers; begin and end.
 * Former points data which will be read for next read request, and latter
 * points the head of unused area which is available on next write request.  If
 * they are equal, it means that the queue is completely empty.  Therefore,
 * QueueHeader.end must not catch up QueueHeader.begin even if writer is faster
 * than reader.  This also means that available size is QueueHeader.size - 1
 * bytes.
 *
 * QueueHeader.mutex is used to lock the queue exclusively, if client requests.
 * If reader and writer access a queue concurrently, they must acquire lock.
 *
 * QueueHeader.magic is magic number which identifies pgut-queue segments.
 */
typedef struct QueueHeader
{
	uint32		magic;		/* magic # to identify pgut-queue segments */
#define PGUTShmemMagic	0550
	uint32		size;		/* size of data */
	uint32		begin;		/* position that begins to read on data */
	uint32		end;		/* position that begins to write on data */
	slock_t		mutex;		/* locks shared variables begin, end and data */
	char		data[1];	/* VARIABLE LENGTH ARRAY - MUST BE LAST */
} QueueHeader;

/*
 * Queue is a queue handle for reader and writer.  QueueHeader is placed on
 * shared memory and shared by reader and writer, but Queue is allocated for
 * each reader and writer.
 */
struct Queue
{
	ShmemHandle		handle; /* handle of shared memory used for the queue */
	QueueHeader	   *header;	/* actual queue entity placed on shared memory */
	uint32			size;	/* copy of header->size */
};

/*
 * QueueCreate
 *
 * Create and initialize a queue with given size.  Note that available size is
 * size - 1 bytes, as described in comment of QueueHeader.  The key of shared
 * memory, which is necessary to open the queue by other processes, is
 * automatically determined and stored in key argument.
 */
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

	/*
	 * In order to distinguish whether the queue buffer is full or empty, begin
	 * and end must be different at least 1 byte.  So minimum value allowed as
	 * size argument is 2.
	 */
	if (size < 2)
		elog(ERROR, "queue data size is too small");

retry:
	/* Loop until we find a free IPC key */
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
	/* Attempt to create a new shared memory segment with generated key. */
	handle = shmget(shmemKey, offsetof(QueueHeader, data) + size, IPC_CREAT | IPC_EXCL | 0600);
	if (handle < 0)
	{
		/*
		 * Retry quietly if error indicates a collision with existing segment.
		 * One would expect EEXIST, given that we said IPC_EXCL, but perhaps
		 * we could get a permission violation instead?  Also, EIDRM might
		 * occur if an old seg is slated for destruction but not gone yet.
		 */
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

	/* OK, should be able to attach to the segment */
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

/*
 * QueueOpen
 *
 * Open existing queue which has given key.  If there is no queue with given
 * key, or matched shared memory segment is not created by pgut, error occurs
 * and never return.
 */
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
	handle = shmget(key, 0, 0);
	if (handle < 0)
		elog(ERROR, "shmget(id=%d) failed: %m", key);

	/* OK, should be able to attach to the segment */
	header = shmat(handle, NULL, PG_SHMAT_FLAGS);
	/* failed: must be some other app's */
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

/*
 * QueueClose
 *
 * Close a queue, and release all resources including shared memory segment.
 * This must be called once and only once for a queue.
 */
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
 * QueueRead
 *
 * Read out next len bytes from the queue specified by self into buffer, and
 * return the size of the data which read out in a buffer (always same as given
 * value).
 *
 * When need_lock was true, the queue is locked exclusively during reading the
 * data.  If multiple readers might access the queue concurrently, they must
 * set need_lock true.  Note that concurrent read/write don't require lock.
 *
 * When the queue doesn't have enough data yet, sleep SPIN_SLEEP_MSEC
 * milliseconds at a time until enough data arrive.
 */
uint32
QueueRead(Queue *self, void *buffer, uint32 len, bool need_lock)
{
	/* use volatile pointer to prevent code rearrangement */
	volatile QueueHeader *header = self->header;
	const char *data = (const char *) header->data;
	uint32	size = self->size;
	uint32	begin;
	uint32	end;

	if (len >= size)
		elog(ERROR, "read length is too large");

retry:
	if (need_lock)
		SpinLockAcquire(&header->mutex);

	begin = header->begin;
	end = header->end;

	if (begin <= end)
	{
		/* When written area ends before the end of the buffer. */
		if (begin + len <= end)
		{
			/* Read out all requested data at once. */
			memcpy(buffer, data + begin, len);
			header->begin += len;
			if (need_lock)
				SpinLockRelease(&header->mutex);

			return len;
		}
	}
	else if (begin + len <= size + end)
	{
		/* When written area continues beyond the end of the buffer. */
		if (begin + len <= size)
		{
			/*
			 * Read out all requested data at once, if it's in continuous area.
			 */
			memcpy(buffer, data + begin, len);
			header->begin += len;
		}
		else
		{
			/*
			 * Read out requested data separately, if it continues beyond the
			 * end of the buffer.
			 */
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
 * QueueWrite
 *
 * Write data stored in first count iovecs in iov into the queue specified by
 * self.  The first element in iov is written first, and other elements follow.
 *
 * It returns true on success, including the case that count was zero or less.
 *
 * If total size of requested data is more than the size of the queue itself,
 * error occurs.  Otherwise, the queue doesn't have enough room, QueueWrite
 * sleeps SPIN_SLEEP_MSEC at a time until reader reads enough data.  If total
 * elapsed time exceeded timeout_msec, it gives up and returns false.
 *
 * When need_lock was true, the queue is locked exclusively during writing the
 * data.  If multiple writers might access the queue concurrently, they must
 * set need_lock true.  Note that concurrent read/write don't require lock.
 */
bool
QueueWrite(Queue *self, const struct iovec iov[], int count, uint32 timeout_msec, bool need_lock)
{
	/* use volatile pointer to prevent code rearrangement */
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

	if (total >= size)
		elog(ERROR, "write length is too large");

retry:
	if (need_lock)
		SpinLockAcquire(&header->mutex);

	begin = header->begin;
	end = header->end;
	dst = data + end;

	if (begin > end)
	{
		/* When unwritten area ends before the end of the buffer. */
		if (end + total < begin)
		{
			/* Write each data stored in iov. */
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
	else if (end + total < size + begin)
	{
		/* When unwritten area continues beyond the end of the buffer. */
		if (end + total <= size)
		{
			/*
			 * Write all data sequentially if the queue has enough and
			 * continuous room.
			 */
			for (i = 0; i < count; i++)
			{
				memcpy(dst, iov[i].iov_base, iov[i].iov_len);
				dst += iov[i].iov_len;
			}
		}
		else
		{
			/*
			 * Write data separately, if requested data is longer than
			 * continuous space.  Rest of requested data are written at the
			 * head of the buffer.
			 */
			uint32	head;
			uint32	tail = size - end;

			/*
			 * Write data stored in next iov while available space at the end
			 * of the buffer is enough.
			 */
			for (i = 0; i < count && iov[i].iov_len <= tail; i++)
			{
				memcpy(dst, iov[i].iov_base, iov[i].iov_len);
				dst += iov[i].iov_len;
				tail -= iov[i].iov_len;
			}

			/* Write data in an iov separately. */
			if (i < count)
			{
				head = iov[i].iov_len - tail;
				memcpy(dst, iov[i].iov_base, tail);
				memcpy(data, ((const char *) iov[i].iov_base) + tail, head);
				dst = data + head;
				i++;
			}

			/* Write rest of requested data. */
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
