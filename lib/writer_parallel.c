/*
 * pg_bulkload: lib/writer_parallel.c
 *
 *	  Copyright (c) 2009-2010, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

#include "postgres.h"

#include "libpq-fe.h"

#include "access/heapam.h"
#include "access/xact.h"
#include "commands/dbcommands.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"

#include "pgut/pgut-be.h"
#include "pgut/pgut-ipc.h"

#include "logger.h"
#include "writer.h"
#include "pg_strutil.h"

#define DEFAULT_BUFFER_SIZE		(16 * 1024 * 1024)	/* 16MB */
#define DEFAULT_TIMEOUT_MSEC	100	/* 100ms */

typedef struct ParallelWriter
{
	Writer	base;

	PGconn	   *conn;
	Queue	   *queue;
} ParallelWriter;

static void	ParallelWriterInsert(ParallelWriter *self, HeapTuple tuple);
static WriterResult	ParallelWriterClose(ParallelWriter *self, bool onError);
static void	ParallelWriterDumpParams(ParallelWriter *self);
static const char *finish_and_get_message(ParallelWriter *self);
static char *get_relation_name(Oid relid);
static void write_queue(ParallelWriter *self, const void *buffer, uint32 len);
static void transfer_message(void *arg, const PGresult *res);
static PGconn *connect_to_localhost(void);

/* ========================================================================
 * Implementation
 * ========================================================================*/
#define MAXINT8LEN		25

Writer *
CreateParallelWriter(Oid relid, const WriterOptions *options)
{
	ParallelWriter *self;
	unsigned	queryKey;
	char		queueName[MAXPGPATH];
	char	   *relname;
	PGresult   *res;
	const char *params[7];
	char		max_dup_errors[MAXINT8LEN + 1];

	self = palloc0(sizeof(ParallelWriter));
	self->base.insert = (WriterInsertProc) ParallelWriterInsert,
	self->base.close = (WriterCloseProc) ParallelWriterClose,
	self->base.dumpParams = (WriterDumpParamsProc) ParallelWriterDumpParams,
	self->base.context = AllocSetContextCreate(
							CurrentMemoryContext,
							"ParallelWriter",
							ALLOCSET_DEFAULT_MINSIZE,
							ALLOCSET_DEFAULT_INITSIZE,
							ALLOCSET_DEFAULT_MAXSIZE);
	self->base.count = 0;

	relname = get_relation_name(relid);

	snprintf(max_dup_errors, MAXINT8LEN, INT64_FORMAT, options->max_dup_errors);

	/* create queue */
	self->queue = QueueCreate(&queryKey, DEFAULT_BUFFER_SIZE);
	snprintf(queueName, lengthof(queueName), ":%u", queryKey);

	/* connect to localhost */
	self->conn = connect_to_localhost();

	/* start transaction */
	res = PQexec(self->conn, "BEGIN");
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		ereport(ERROR,
				(errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION),
				 errmsg("could not start transaction"),
				 errdetail("%s", finish_and_get_message(self))));
	}

	PQclear(res);

	/* async query send */
	params[0] = queueName;
	params[1] = relname;
	params[2] = (options->truncate ? "true" : "no");
	params[3] = ON_DUPLICATE_NAMES[options->on_duplicate];
	params[4] = max_dup_errors;
	params[5] = options->dup_badfile;
	params[6] = "remote";

	if (1 != PQsendQueryParams(self->conn,
			"SELECT * FROM pg_bulkload(ARRAY["
			"'TYPE=TUPLE',"
			"'INFILE=' || $1,"
			"'TABLE=' || $2,"
			"'TRUNCATE=' || $3,"
			"'ON_DUPLICATE_KEEP=' || $4,"
			"'DUPLICATE_ERRORS=' || $5,"
			"'DUPLICATE_BADFILE=' || $6,"
			"'LOGFILE=' || $7])",
		7, NULL, params, NULL, NULL, 0))
	{
		ereport(ERROR,
				(errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION),
				 errmsg("could not send query"),
				 errdetail("%s", finish_and_get_message(self))));
	}

	return (Writer *) self;
}

static void
ParallelWriterInsert(ParallelWriter *self, HeapTuple tuple)
{
	write_queue(self, tuple->t_data, tuple->t_len);
}

static WriterResult
ParallelWriterClose(ParallelWriter *self, bool onError)
{
	WriterResult	ret = { 0 };

	/* wait for reader */
	if (self->conn)
	{
		if (self->queue && !onError)
		{
			PGresult   *res;
			int			sock;
			fd_set		input_mask;

			/* terminate with zero */
			write_queue(self, NULL, 0);

			do
			{
				sock = PQsocket(self->conn);

				FD_ZERO(&input_mask);
				FD_SET(sock, &input_mask);

				while (select(sock + 1, &input_mask, NULL, NULL, NULL) < 0)
				{
					if (errno == EINTR)
					{
						CHECK_FOR_INTERRUPTS();
						continue;
					}
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("select() failed"),
							 errdetail("%s", finish_and_get_message(self))));
				}

				PQconsumeInput(self->conn);
			} while (PQisBusy(self->conn));

			res = PQgetResult(self->conn);

			if (PQresultStatus(res) != PGRES_TUPLES_OK)
			{
				PQfinish(self->conn);
				self->conn = NULL;
				transfer_message(NULL, res);
			}
			else
			{
				self->base.count = ParseInt64(PQgetvalue(res, 0, 1), 0);
				ret.num_dup_new = ParseInt64(PQgetvalue(res, 0, 3), 0);
				ret.num_dup_old = ParseInt64(PQgetvalue(res, 0, 4), 0);
				LoggerLog(WARNING, "%s", PQgetvalue(res, 0, 8));
				PQclear(res);

				/* commit transaction */
				res = PQexec(self->conn, "COMMIT");
				if (PQresultStatus(res) != PGRES_COMMAND_OK)
				{
					ereport(ERROR,
							(errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION),
							 errmsg("could not commit transaction"),
							 errdetail("%s", finish_and_get_message(self))));
				}
			}
			PQclear(res);
		}
		else if (PQisBusy(self->conn))
		{
			char		errbuf[256];
			PGcancel   *cancel = PQgetCancel(self->conn);
			if (cancel)
				PQcancel(cancel, errbuf, lengthof(errbuf));
		}

		if (self->conn)
			PQfinish(self->conn);
		self->conn = NULL;
	}

	/* 
	 * Close self after wait for reader because reader hasn't opened the self
	 * yet. If we close self too early, the reader cannot open the self.
	 */
	if (self->queue)
		QueueClose(self->queue);

	self->queue = NULL;

	if (!onError)
		MemoryContextDelete(self->base.context);

	return ret;
}

static void
ParallelWriterDumpParams(ParallelWriter *self)
{
	LoggerLog(INFO, "WRITER = PARALLEL\n\n");
}

static const char *
finish_and_get_message(ParallelWriter *self)
{
	const char *msg;
	msg = PQerrorMessage(self->conn);
	msg = (msg ? pstrdup(msg) : "(no message)");
	PQfinish(self->conn);
	self->conn = NULL;
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
write_queue(ParallelWriter *self, const void *buffer, uint32 len)
{
	struct iovec	iov[2];

	AssertArg(self->conn != NULL);
	AssertArg(self->queue != NULL);
	AssertArg(len == 0 || buffer != NULL);

	iov[0].iov_base = &len;
	iov[0].iov_len = sizeof(len);
	iov[1].iov_base = (void *) buffer;
	iov[1].iov_len = len;

	for (;;)
	{
		if (QueueWrite(self->queue, iov, 2, DEFAULT_TIMEOUT_MSEC, false))
			return;

		PQconsumeInput(self->conn);
		if (!PQisBusy(self->conn))
		{
			ereport(ERROR,
				(errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION),
				 errmsg("unexpected reader termination"),
				 errdetail("%s", finish_and_get_message(self))));
		}

		/* retry */
	}
}

static PGconn *
connect_to_localhost(void)
{
	PGconn *conn;
	char	sql[1024];

	conn = PQsetdbLogin(
		"localhost",
		GetConfigOption("port", false),
		NULL, NULL,
		get_database_name(MyDatabaseId),
		GetUserNameFromId(GetUserId()),
		NULL);
	if (PQstatus(conn) == CONNECTION_BAD)
	{
		ParallelWriter wr;

		wr.conn = conn;
		ereport(ERROR,
				(errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION),
				 errmsg("could not establish connection"),
				 errdetail("%s", finish_and_get_message(&wr))));
	}

	/* attempt to set client encoding to match server encoding */
	PQsetClientEncoding(conn, GetDatabaseEncodingName());

	/* attempt to set default datestyle */
	snprintf(sql, lengthof(sql), "SET datestyle = '%s'", GetConfigOption("datestyle", false));
	PQexec(conn, sql);

	/* TODO: do we need more settings? (ex. CLIENT_CONN_xxx) */

	/* set message receiver */
	PQsetNoticeReceiver(conn, transfer_message, NULL);

	return conn;
}

static void
transfer_message(void *arg, const PGresult *res)
{
	int	elevel;
	int	code;
	const char *severity = PQresultErrorField(res, PG_DIAG_SEVERITY);
	const char *state = PQresultErrorField(res, PG_DIAG_SQLSTATE);
	const char *message = PQresultErrorField(res, PG_DIAG_MESSAGE_PRIMARY);
	const char *detail = PQresultErrorField(res, PG_DIAG_MESSAGE_DETAIL);
	if (detail && !detail[0])
		detail = NULL;

	switch (severity[0])
	{
		case 'D':
			elevel = DEBUG2;
			break;
		case 'L':
			elevel = LOG;
			break;
		case 'I':
			elevel = INFO;
			break;
		case 'N':
			elevel = NOTICE;
			break;
		case 'E':
		case 'F':
			elevel = ERROR;
			break;
		default:
			elevel = WARNING;
			break;
	}
	code = MAKE_SQLSTATE(state[0], state[1], state[2], state[3], state[4]);

	if (elevel >= ERROR)
	{
		if (message)
			message = pstrdup(message);
		if (detail)
			detail = pstrdup(detail);
		PQclear((PGresult *) res);
	}

	ereport(elevel,
			(errcode(code),
			 errmsg("%s", message),
			 (detail ? errdetail("%s", detail) : 0)));
}
