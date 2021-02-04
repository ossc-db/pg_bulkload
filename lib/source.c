/*
 * pg_bulkload: lib/source.c
 *
 *	  Copyright (c) 2009-2021, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

#include "pg_bulkload.h"

#include <fcntl.h>
#include "pgut/pgut-pthread.h"

#include "access/htup.h"
#include "catalog/pg_type.h"
#include "fmgr.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "tcop/dest.h"
#include "utils/memutils.h"

#include "reader.h"

#include "pgut/pgut-be.h"

#include "storage/fd.h"

extern PGDLLIMPORT CommandDest		whereToSendOutput;

#ifdef _MSC_VER
/*
 * Unfortunately, FrontendProtocol variable is not exported from postgres.a
 * for MSVC. So, we define own entity of the variable and assume protocol
 * version is always 3.
 */
ProtocolVersion	FrontendProtocol = 3;
#endif

/* ========================================================================
 * AsyncSource
 * ========================================================================*/
#define SPIN_SLEEP_MSEC		10
#define READ_UNIT_SIZE		(1024 * 1024)
#define INITIAL_BUF_LEN		(16 * READ_UNIT_SIZE)
#define ERROR_MESSAGE_LEN	1024

typedef struct AsyncSource
{
	Source	base;

	FILE   *fd;
	bool	eof;

	char   *buffer;		/* read buffer */
	int		size;		/* buffer size */
	int		begin;		/* begin of the buffer finished with reading */
	int		end;		/* end of the buffer finished with reading */

	/*
	 * because ereport() does not support multi-thread, the read thread stores
	 * away error messsage in a message buffer.
	 */
	char	errmsg[ERROR_MESSAGE_LEN];

	/*
	 * An independently managed memory context for allocations performed by
	 * AsyncSourceRead
	 */
	MemoryContext context;

	pthread_t		th;
	pthread_mutex_t	lock;
} AsyncSource;

static size_t AsyncSourceRead(AsyncSource *self, void *buffer, size_t len);
static void AsyncSourceClose(AsyncSource *self);
static void *AsyncSourceMain(void *arg);

/* ========================================================================
 * FileSource
 * ========================================================================*/

typedef struct FileSource
{
	Source	base;

	FILE   *fd;
} FileSource;

static size_t FileSourceRead(FileSource *self, void *buffer, size_t len);
static void FileSourceClose(FileSource *self);

/* ========================================================================
 * RemoteSource
 * ========================================================================*/

typedef struct RemoteSource
{
	Source	base;

	bool		eof;
	StringInfo	buffer;
} RemoteSource;

static size_t RemoteSourceRead(RemoteSource *self, void *buffer, size_t len);
static size_t RemoteSourceReadOld(RemoteSource *self, void *buffer, size_t len);
static void RemoteSourceClose(RemoteSource *self);

static Source *CreateAsyncSource(const char *path, TupleDesc desc);
static Source *CreateFileSource(const char *path, TupleDesc desc);
static Source *CreateRemoteSource(const char *path, TupleDesc desc);

static int Wrappered_pq_getbyte(void);
static int Wrappered_pq_getbytes(char *s, size_t len);

Source *
CreateSource(const char *path, TupleDesc desc, bool async_read)
{
	if (pg_strcasecmp(path, "stdin") == 0)
	{
		if (whereToSendOutput != DestRemote)
			ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("local stdin read is not supported")));

		return CreateRemoteSource(NULL, desc);
	}
	else
	{
		if (!is_absolute_path(path))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("relative path not allowed for INPUT: %s", path)));

		if (async_read)
			return CreateAsyncSource(path, desc);

		return CreateFileSource(path, desc);
	}
}

/* ========================================================================
 * AsyncSource
 * ========================================================================*/

static Source *
CreateAsyncSource(const char *path, TupleDesc desc)
{
	AsyncSource *self = palloc0(sizeof(AsyncSource));
	MemoryContext	oldcxt;

	self->base.read = (SourceReadProc) AsyncSourceRead;
	self->base.close = (SourceCloseProc) AsyncSourceClose;

	self->size = INITIAL_BUF_LEN;
	self->begin = 0;
	self->end = 0;
	self->errmsg[0] = '\0';

	/* Create a dedicated context for our allocation needs */
	self->context = AllocSetContextCreate(
							CurrentMemoryContext,
							"AsyncSource",
#if PG_VERSION_NUM >= 90600
									ALLOCSET_DEFAULT_SIZES);
#else
									ALLOCSET_SMALL_MINSIZE,
									ALLOCSET_SMALL_INITSIZE,
									ALLOCSET_DEFAULT_MAXSIZE);
#endif

	/* Must allocate memory for self->buffer in self->context */
	oldcxt = MemoryContextSwitchTo(self->context);
	self->buffer = palloc0(self->size);
	MemoryContextSwitchTo(oldcxt);

	self->eof = false;
	self->fd = AllocateFile(path, "r");
	if (self->fd == NULL)
		ereport(ERROR, (errcode_for_file_access(),
			errmsg("could not open \"%s\" %m", path)));

#if defined(USE_POSIX_FADVISE)
	posix_fadvise(fileno(self->fd), 0, 0, POSIX_FADV_SEQUENTIAL | POSIX_FADV_NOREUSE | POSIX_FADV_WILLNEED);
#endif

	pthread_mutex_init(&self->lock, NULL);

	if (pthread_create(&self->th, NULL, AsyncSourceMain, self) != 0)
		elog(ERROR, "pthread_create");

	return (Source *) self;
}

static size_t
AsyncSourceRead(AsyncSource *self, void *buffer, size_t len)
{
	char   *data;
	int		size;
	int		begin;
	int		end;
	char	errhead;
	size_t	bytesread;
	int		n;

	/* 4 times of the needs size allocate a buffer at least */
	if (self->size < len * 4)
	{
		char   *newbuf;
		int		newsize;
		MemoryContext	oldcxt;

		/* read buffer a multiple of READ_UNIT_SIZE */
		newsize = (len * 4 - 1) -
				  ((len * 4 - 1) / READ_UNIT_SIZE) +
				  READ_UNIT_SIZE;

		/* Switch to the dedicated context for our allocation needs */
		oldcxt = MemoryContextSwitchTo(self->context);
		newbuf = palloc0(newsize);

		pthread_mutex_lock(&self->lock);

		/* copy it in new buffer from old buffer */
		if (self->begin > self->end)
		{
			memcpy(newbuf, self->buffer + self->begin,
				   self->size - self->begin);
			memcpy(newbuf + self->size - self->begin, self->buffer, self->end);
			self->end = self->size - self->begin + self->end;
		}
		else
		{
			memcpy(newbuf, self->buffer + self->begin, self->end - self->begin);
			self->end = self->end - self->begin;
		}

		pfree(self->buffer);
		self->buffer = newbuf;
		self->size = newsize;
		self->begin = 0;

		pthread_mutex_unlock(&self->lock);

		MemoryContextSwitchTo(oldcxt);
	}

	/* this value that a read thread does not change */
	data = self->buffer;
	size = self->size;
	begin = self->begin;

	bytesread = 0;
retry:
	end = self->end;
	errhead = self->errmsg[0];

	/* error in read thread */
	if (errhead != '\0')
	{
		/* wait for error message to be set */
		pthread_mutex_lock(&self->lock);
		pthread_mutex_unlock(&self->lock);

		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("%s", self->errmsg)));
	}

	if (begin < end)
	{
		n = Min(len - bytesread, end - begin);
		memcpy((char *) buffer + bytesread, data + begin, n);
		begin += n;
		bytesread += n;
	}
	else if (begin > end)
	{
		n = Min(len - bytesread, size - begin);
		memcpy((char *) buffer + bytesread, data + begin, n);
		begin += n;
		bytesread += n;

		if (begin == size)
		{
			self->begin = begin = 0;

			if (bytesread < len)
				goto retry;
		}
	}

	self->begin = begin;

	if (bytesread == len || (self->eof && begin == end))
		return bytesread;

	/* not enough data yet */
	CHECK_FOR_INTERRUPTS();
	pg_usleep(SPIN_SLEEP_MSEC * 1000);

	goto retry;
}

static void
AsyncSourceClose(AsyncSource *self)
{
	self->eof = true;

	pthread_mutex_unlock(&self->lock);
	pthread_join(self->th, NULL);

	if (self->fd != NULL && FreeFile(self->fd) < 0)
	{
		ereport(WARNING, (errcode_for_file_access(),
			errmsg("could not close source file: %m")));
	}
	self->fd = NULL;

	if (self->context != NULL)
		MemoryContextDelete(self->context);

	self->buffer = NULL;

	pfree(self);
}

static void *
AsyncSourceMain(void *arg)
{
	AsyncSource   *self = (AsyncSource *) arg;
	size_t			bytesread;
	int				begin;
	int				end;
	int				size;
	int				len;
	char		   *data;

	Assert(self->begin == 0);
	Assert(self->end == 0);

	for (;;)
	{
		pthread_mutex_lock(&self->lock);

		begin = self->begin;
		end = self->end;
		size = self->size;
		data = self->buffer;

		if (begin > end)
		{
			len = begin - end;
			if (len <= READ_UNIT_SIZE)
				len = 0;
		}
		else
		{
			len = size - end;
			if (begin == 0 && len <= READ_UNIT_SIZE)
				len = 0;
		}

		if (len == 0)
		{
			pthread_mutex_unlock(&self->lock);

			if (self->eof)
				break;

			pg_usleep(SPIN_SLEEP_MSEC * 1000);

			continue;
			/* retry */
		}

		len = Min(len, READ_UNIT_SIZE);

		bytesread = fread(data + end, 1, len, self->fd);

		if (ferror(self->fd))
		{
			snprintf(self->errmsg, ERROR_MESSAGE_LEN,
					 "could not read from source file: %m");
			pthread_mutex_unlock(&self->lock);
			return NULL;
		}

		end += bytesread;
		if (end == self->size)
			end = 0;

		self->end = end;

		if (feof(self->fd))
		{
			self->eof = true;
			break;
		}

		if (self->eof)
			break;

		pthread_mutex_unlock(&self->lock);
	}

	pthread_mutex_unlock(&self->lock);

	return NULL;
}

/* ========================================================================
 * FileSource
 * ========================================================================*/

static Source *
CreateFileSource(const char *path, TupleDesc desc)
{
	FileSource *self = palloc0(sizeof(FileSource));
	self->base.read = (SourceReadProc) FileSourceRead;
	self->base.close = (SourceCloseProc) FileSourceClose;

	self->fd = AllocateFile(path, "r");
	if (self->fd == NULL)
		ereport(ERROR, (errcode_for_file_access(),
			errmsg("could not open \"%s\" %m", path)));

#if defined(USE_POSIX_FADVISE)
	posix_fadvise(fileno(self->fd), 0, 0, POSIX_FADV_SEQUENTIAL | POSIX_FADV_NOREUSE | POSIX_FADV_WILLNEED);
#endif

	return (Source *) self;
}

static size_t
FileSourceRead(FileSource *self, void *buffer, size_t len)
{
	size_t	bytesread;

	bytesread = fread(buffer, 1, len, self->fd);
	if (ferror(self->fd))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read from source file: %m")));

	return bytesread;
}

static void
FileSourceClose(FileSource *self)
{
	if (self->fd != NULL && FreeFile(self->fd) < 0)
	{
		ereport(WARNING, (errcode_for_file_access(),
			errmsg("could not close source file: %m")));
	}
	pfree(self);
}

/* ========================================================================
 * RemoteSource
 * ========================================================================*/

#define IsBinaryCopy()	(false)

static Source *
CreateRemoteSource(const char *path, TupleDesc desc)
{
	RemoteSource *self = (RemoteSource *) palloc0(sizeof(RemoteSource));
	self->base.close = (SourceCloseProc) RemoteSourceClose;

	if (PG_PROTOCOL_MAJOR(FrontendProtocol) >= 3)
	{
		/* new way */
		StringInfoData	buf;
		int16			format;
		int				nattrs;
		int				i;

		self->base.read = (SourceReadProc) RemoteSourceRead;

		/* count valid fields */
		for (nattrs = 0, i = 0; i < desc->natts; i++)
		{
#if PG_VERSION_NUM >= 110000
			if (desc->attrs[i].attisdropped)
#else
			if (desc->attrs[i]->attisdropped)
#endif
				continue;
			nattrs++;
		}

		format = (IsBinaryCopy() ? 1 : 0);
		pq_beginmessage(&buf, 'G');
		pq_sendbyte(&buf, format);		/* overall format */
		pq_sendint(&buf, nattrs, 2);
		for (i = 0; i < nattrs; i++)
			pq_sendint(&buf, format, 2);		/* per-column formats */
		pq_endmessage(&buf);
		self->buffer = makeStringInfo();
	}
	else if (PG_PROTOCOL_MAJOR(FrontendProtocol) >= 2)
	{
		self->base.read = (SourceReadProc) RemoteSourceReadOld;

		/* old way */
		if (IsBinaryCopy())
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			errmsg("COPY BINARY is not supported to stdout or from stdin")));
		pq_putemptymessage('G');
	}
	else
	{
		self->base.read = (SourceReadProc) RemoteSourceReadOld;

		/* very old way */
		if (IsBinaryCopy())
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			errmsg("COPY BINARY is not supported to stdout or from stdin")));
		pq_putemptymessage('D');
	}
	/* We *must* flush here to ensure FE knows it can send. */
	pq_flush();

	return (Source *) self;
}

static size_t
RemoteSourceRead(RemoteSource *self, void *buffer, size_t len)
{
	size_t	bytesread;
	size_t	minread = len;

	bytesread = 0;
	while (len > 0 && bytesread < minread && !self->eof)
	{
		int			avail;

		while (self->buffer->cursor >= self->buffer->len)
		{
			/* Try to receive another message */
			int			mtype;

readmessage:
			mtype = Wrappered_pq_getbyte();
			if (mtype == EOF)
				ereport(ERROR,
						(errcode(ERRCODE_CONNECTION_FAILURE),
					 errmsg("unexpected EOF on client connection")));
			if (pq_getmessage(self->buffer, 0))
				ereport(ERROR,
						(errcode(ERRCODE_CONNECTION_FAILURE),
					 errmsg("unexpected EOF on client connection")));
			switch (mtype)
			{
				case 'd':		/* CopyData */
					break;
				case 'c':		/* CopyDone */
					/* COPY IN correctly terminated by frontend */
					self->eof = true;
					return bytesread;
				case 'f':		/* CopyFail */
					ereport(ERROR,
							(errcode(ERRCODE_QUERY_CANCELED),
							 errmsg("COPY from stdin failed: %s",
							   pq_getmsgstring(self->buffer))));
					break;
				case 'H':		/* Flush */
				case 'S':		/* Sync */

					/*
					 * Ignore Flush/Sync for the convenience of client
					 * libraries (such as libpq) that may send those
					 * without noticing that the command they just
					 * sent was COPY.
					 */
					goto readmessage;
				default:
					ereport(ERROR,
							(errcode(ERRCODE_PROTOCOL_VIOLATION),
							 errmsg("unexpected message type 0x%02X during COPY from stdin",
									mtype)));
					break;
			}
		}
		avail = self->buffer->len - self->buffer->cursor;
		if (avail > len)
			avail = len;
		pq_copymsgbytes(self->buffer, (char *) buffer, avail);
		buffer = (void *) ((char *) buffer + avail);
		len -= avail;
		bytesread += avail;
	}

	return bytesread;
}

static size_t
RemoteSourceReadOld(RemoteSource *self, void *buffer, size_t len)
{
	if (Wrappered_pq_getbytes((char *) buffer, 1))
	{
		/* Only a \. terminator is legal EOF in old protocol */
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("unexpected EOF on client connection")));
	}

	return 1;
}

typedef struct AttributeDefinition
{
	char   *name;
	Oid		typid;
	int16	typlen;
	int32	typmod;
} AttributeDefinition;

static void
SendResultDescriptionMessage(AttributeDefinition *attrs, int natts)
{
	int			proto = PG_PROTOCOL_MAJOR(FrontendProtocol);
	int			i;
	StringInfoData buf;

	pq_beginmessage(&buf, 'T'); /* tuple descriptor message type */
	pq_sendint(&buf, natts, 2);	/* # of attrs in tuples */

	for (i = 0; i < natts; ++i)
	{
		pq_sendstring(&buf, attrs[i].name);
		/* column ID info appears in protocol 3.0 and up */
		if (proto >= 3)
		{
			pq_sendint(&buf, 0, 4);
			pq_sendint(&buf, 0, 2);
		}
		/* If column is a domain, send the base type and typmod instead */
		pq_sendint(&buf, attrs[i].typid, sizeof(Oid));
		pq_sendint(&buf, attrs[i].typlen, sizeof(int16));
		/* typmod appears in protocol 2.0 and up */
		if (proto >= 2)
			pq_sendint(&buf, attrs[i].typmod, sizeof(int32));
		/* format info appears in protocol 3.0 and up */
		if (proto >= 3)
			pq_sendint(&buf, 0, 2);
	}

	pq_endmessage(&buf);
}

static void
RemoteSourceClose(RemoteSource *self)
{
	AttributeDefinition attrs[] = {
		{"skip", INT8OID, 8, -1},
		{"count", INT8OID, 8, -1},
		{"parse_errors", INT8OID, 8, -1},
		{"duplicate_new", INT8OID, 8, -1},
		{"duplicate_old", INT8OID, 8, -1},
		{"system_time", FLOAT8OID, 8, -1},
		{"user_time", FLOAT8OID, 8, -1},
		{"duration", FLOAT8OID, 8, -1}
	};

	SendResultDescriptionMessage(attrs, PG_BULKLOAD_COLS);
	pfree(self);
}

/* These blow 2 wrapper functions is required for backward 
 * compatibility beyond the PostgreSQL commit:
 * Be more careful to not lose sync in the FE/BE protocol.*/
static int
Wrappered_pq_getbyte(void)
{
#if (PG_VERSION_NUM >= 90401) \
  || ((PG_VERSION_NUM >= 90306) && (PG_VERSION_NUM < 90400)) \
  || ((PG_VERSION_NUM >= 90210) && (PG_VERSION_NUM < 90300)) \
  || ((PG_VERSION_NUM >= 90115) && (PG_VERSION_NUM < 90200)) \
  || ((PG_VERSION_NUM >= 90019) && (PG_VERSION_NUM < 90100))
             pq_startmsgread();
#endif
            return pq_getbyte();
}

static int
Wrappered_pq_getbytes(char *s, size_t len)
{
#if (PG_VERSION_NUM >= 90401) \
  || ((PG_VERSION_NUM >= 90306) && (PG_VERSION_NUM < 90400)) \
  || ((PG_VERSION_NUM >= 90210) && (PG_VERSION_NUM < 90300)) \
  || ((PG_VERSION_NUM >= 90115) && (PG_VERSION_NUM < 90200)) \
  || ((PG_VERSION_NUM >= 90019) && (PG_VERSION_NUM < 90100))
             pq_startmsgread();
#endif
            return pq_getbytes(s, len);
}
