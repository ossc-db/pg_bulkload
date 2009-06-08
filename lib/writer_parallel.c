/*
 * pg_bulkload: lib/writer_parallel.c
 *
 *	  Copyright(C) 2009, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

#include "postgres.h"

#include "libpq-fe.h"

#include "access/heapam.h"
#include "access/xact.h"
#include "commands/dbcommands.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"

#include "writer.h"
#include "pg_strutil.h"
#include "pgut/pgut-ipc.h"

extern PGDLLIMPORT int	PostPortNumber;

#define DEFAULT_BUFFER_SIZE		(16 * 1024 * 1024)	/* 16MB */
#define DEFAULT_TIMEOUT_MSEC	100	/* 100ms */

typedef struct ParallelWriter
{
	Writer	base;

	PGconn	   *conn;
	Queue	   *queue;
} ParallelWriter;

static void	ParallelWriterInsert(ParallelWriter *self, HeapTuple tuple);
static void	ParallelWriterClose(ParallelWriter *self);
static const char *finish_and_get_message(PGconn *conn);
static char *get_relation_name(Oid relid);
static void write_queue(PGconn *conn, Queue *queue, const void *buffer, uint32 len);

/* ========================================================================
 * Implementation
 * ========================================================================*/

Writer *
CreateParallelWriter(Oid relid, ON_DUPLICATE on_duplicate)
{
	unsigned	queryKey;
	char		queueName[MAXPGPATH];
	char		port[32];
	char	   *relname;
	const char *params[3];

	ParallelWriter *self = palloc0(sizeof(ParallelWriter));
	self->base.insert = (WriterInsertProc) ParallelWriterInsert;
	self->base.close = (WriterCloseProc) ParallelWriterClose;
	self->base.context = AllocSetContextCreate(
							CurrentMemoryContext,
							"ParallelWriter",
							ALLOCSET_DEFAULT_MINSIZE,
							ALLOCSET_DEFAULT_INITSIZE,
							ALLOCSET_DEFAULT_MAXSIZE);

	snprintf(port, lengthof(port), "%d", PostPortNumber);
	relname = get_relation_name(relid);

	/* create self */
	self->queue = QueueCreate(&queryKey, DEFAULT_BUFFER_SIZE);
	snprintf(queueName, lengthof(queueName), ":%u", queryKey);

	/* connect to localhost */
	self->conn = PQsetdbLogin(
		"localhost",
		port,
		NULL, NULL,
		get_database_name(MyDatabaseId),
		GetUserNameFromId(GetUserId()),
		NULL);
	if (PQstatus(self->conn) == CONNECTION_BAD)
	{
		ereport(ERROR,
				(errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION),
				 errmsg("could not establish connection"),
				 errdetail("%s", finish_and_get_message(self->conn))));
	}

	/* async query send */
	params[0] = queueName;
	params[1] = relname;
	params[2] = ON_DUPLICATE_NAMES[on_duplicate];
	if (1 != PQsendQueryParams(self->conn,
		"SELECT pg_bulkload(NULL, 'TYPE=TUPLE\nINFILE=' || $1 || '\nTABLE=' || $2 || '\nON_DUPLICATE=' || $3 || '\n')",
		3, NULL, params, NULL, NULL, 0))
	{
		ereport(ERROR,
				(errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION),
				 errmsg("could not send query"),
				 errdetail("%s", finish_and_get_message(self->conn))));
	}

	return (Writer *) self;
}

static void
ParallelWriterInsert(ParallelWriter *self, HeapTuple tuple)
{
	write_queue(self->conn, self->queue, tuple->t_data, tuple->t_len);
}

static void
ParallelWriterClose(ParallelWriter *self)
{
	/* wait for reader */
	if (self->conn)
	{
		/* terminate with zero */
		if (self->queue)
		{
			PGresult *rs;
			write_queue(self->conn, self->queue, NULL, 0);

			while ((rs = PQgetResult(self->conn)) == NULL)
			{
				CHECK_FOR_INTERRUPTS();
				pg_usleep(DEFAULT_TIMEOUT_MSEC);
			}

			if (PQresultStatus(rs) != PGRES_TUPLES_OK)
			{
				PQclear(rs);
				ereport(ERROR,
					(errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION),
					 errmsg("reader failed"),
					 errdetail("%s", finish_and_get_message(self->conn))));
			}

			self->base.count = ParseInt64(PQgetvalue(rs, 0, 0), 0);
			PQclear(rs);
		}

		PQfinish(self->conn);
	}

	/* 
	 * Close self after wait for reader because reader hasn't opened the self
	 * yet. If we close self too early, the reader cannot open the self.
	 */
	if (self->queue)
		QueueClose(self->queue);

	MemoryContextDelete(self->base.context);
	pfree(self);
}

static const char *
finish_and_get_message(PGconn *conn)
{
	const char *msg;
	msg = PQerrorMessage(conn);
	msg = (msg ? pstrdup(msg) : "(no message)");
	PQfinish(conn);
	return msg;
}

static char *
get_relation_name(Oid relid)
{
	return quote_qualified_identifier(
		get_namespace_name(get_rel_namespace(relid)),
		get_rel_name(relid));
}

static void
write_queue(PGconn *conn, Queue *queue, const void *buffer, uint32 len)
{
	struct iovec	iov[2];

	AssertArg(conn != NULL);
	AssertArg(queue != NULL);
	AssertArg(len == 0 || buffer != NULL);

	iov[0].iov_base = &len;
	iov[0].iov_len = sizeof(len);
	iov[1].iov_base = (void *) buffer;
	iov[1].iov_len = len;

	for (;;)
	{
		PGresult *rs;

		if (QueueWrite(queue, iov, 2, DEFAULT_TIMEOUT_MSEC))
			return;

		if ((rs = PQgetResult(conn)) != NULL)
		{
			PQclear(rs);
			/* TODO: free queue's mmap object. */
			ereport(ERROR,
				(errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION),
				 errmsg("unexpected reader termination"),
				 errdetail("%s", finish_and_get_message(conn))));
		}

		/* retry */
	}
}
