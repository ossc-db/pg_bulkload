/*
 * pg_bulkload: lib/source.c
 *
 *	  Copyright(C) 2009, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

#include "postgres.h"

#include <fcntl.h>
#include <unistd.h>

#include "access/htup.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "catalog/pg_type.h"
#include "storage/fd.h"

#include "reader.h"
#include "pgut/pgut-ipc.h"

extern PGDLLIMPORT ProtocolVersion	FrontendProtocol;

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

/* ========================================================================
 * QueueSource
 * ========================================================================*/

typedef struct QueueSource
{
	Source	base;

	Queue  *queue;
} QueueSource;

static size_t QueueSourceRead(QueueSource *self, void *buffer, size_t len);
static void QueueSourceClose(QueueSource *self);

Source *
CreateQueueSource(const char *path, TupleDesc desc)
{
	unsigned	key;
	char		junk[2];

	QueueSource *self = palloc0(sizeof(QueueSource));
	self->base.read = (SourceReadProc) QueueSourceRead;
	self->base.close = (SourceCloseProc) QueueSourceClose;

	if (sscanf(path, ":%u%1s", &key, junk) != 1)
		elog(ERROR, "invalid shmem key format: %s", path);
	self->queue = QueueOpen(key);

	return (Source *) self;
}

static size_t
QueueSourceRead(QueueSource *self, void *buffer, size_t len)
{
	return QueueRead(self->queue, buffer, len);
}

static void
QueueSourceClose(QueueSource *self)
{
	QueueClose(self->queue);
	pfree(self);
}

/* ========================================================================
 * FileSource
 * ========================================================================*/

Source *
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
				 errmsg("could not read from COPY file: %m")));

	return bytesread;
}

static void
FileSourceClose(FileSource *self)
{
	if (self->fd != NULL && FreeFile(self->fd) < 0)
	{
		ereport(WARNING, (errcode_for_file_access(),
			errmsg("could not close source file%m")));
	}
	pfree(self);
}

/* ========================================================================
 * RemoteSource
 * ========================================================================*/

#define IsBinaryCopy()	(false)

Source *
CreateRemoteSource(const char *path, TupleDesc desc)
{
	RemoteSource *self = palloc0(sizeof(RemoteSource));
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
			if (desc->attrs[i]->attisdropped)
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

	bytesread = 0;
	while (len > 0 && bytesread < 1 && !self->eof)
	{
		int			avail;

		while (self->buffer->cursor >= self->buffer->len)
		{
			/* Try to receive another message */
			int			mtype;

readmessage:
			mtype = pq_getbyte();
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
		pq_copymsgbytes(self->buffer, buffer, avail);
		buffer = (void *) ((char *) buffer + avail);
		len -= avail;
		bytesread += avail;
	}

	return bytesread;
}

static size_t
RemoteSourceReadOld(RemoteSource *self, void *buffer, size_t len)
{
	if (pq_getbytes((char *) buffer, 1))
	{
		/* Only a \. terminator is legal EOF in old protocol */
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("unexpected EOF on client connection")));
	}

	return 1;
}

static void
SendResultDescriptionMessage(Oid typid, int16 typlen, int32 typmod)
{
	int			proto = PG_PROTOCOL_MAJOR(FrontendProtocol);
	StringInfoData buf;

	pq_beginmessage(&buf, 'T'); /* tuple descriptor message type */
	pq_sendint(&buf, 1, 2); /* # of attrs in tuples */

	pq_sendstring(&buf, "pg_bulkload");
	/* column ID info appears in protocol 3.0 and up */
	if (proto >= 3)
	{
		pq_sendint(&buf, 0, 4);
		pq_sendint(&buf, 0, 2);
	}
	/* If column is a domain, send the base type and typmod instead */
	pq_sendint(&buf, typid, sizeof(Oid));
	pq_sendint(&buf, typlen, sizeof(int16));
	/* typmod appears in protocol 2.0 and up */
	if (proto >= 2)
		pq_sendint(&buf, typmod, sizeof(int32));
	/* format info appears in protocol 3.0 and up */
	if (proto >= 3)
		pq_sendint(&buf, 0, 2);

	pq_endmessage(&buf);
}

static void
RemoteSourceClose(RemoteSource *self)
{
	SendResultDescriptionMessage(INT8OID, 8, -1);
	pfree(self);
}
