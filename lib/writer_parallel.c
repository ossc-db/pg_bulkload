/*
 * pg_bulkload: lib/writer_parallel.c
 *
 *	  Copyright (c) 2009-2021, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

#include "postgres.h"

#include "libpq-fe.h"

#include "access/heapam.h"
#include "access/xact.h"
#include "commands/dbcommands.h"
#include "commands/variable.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "postmaster/postmaster.h"
#include "storage/lmgr.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/typcache.h"

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

	PGconn *conn;
	Queue  *queue;
	Writer *writer;
} ParallelWriter;

static void	ParallelWriterInit(ParallelWriter *self);
static void	ParallelWriterInsert(ParallelWriter *self, HeapTuple tuple);
static WriterResult	ParallelWriterClose(ParallelWriter *self, bool onError);
static bool	ParallelWriterParam(ParallelWriter *self, const char *keyword, char *value);
static void	ParallelWriterDumpParams(ParallelWriter *self);
static int	ParallelWriterSendQuery(ParallelWriter *self, PGconn *conn, char *queueName, char *logfile, bool verbose);
static const char *finish_and_get_message(ParallelWriter *self);
static void write_queue(ParallelWriter *self, const void *buffer, uint32 len);
static void transfer_message(void *arg, const PGresult *res);
static char *escape_param_str(const char *str);
static PGconn *connect_to_localhost(void);

/* ========================================================================
 * Implementation
 * ========================================================================*/

Writer *
CreateParallelWriter(void *opt)
{
	ParallelWriter *self;

	self = palloc0(sizeof(ParallelWriter));
	self->base.init = (WriterInitProc) ParallelWriterInit;
	self->base.insert = (WriterInsertProc) ParallelWriterInsert,
	self->base.close = (WriterCloseProc) ParallelWriterClose,
	self->base.param = (WriterParamProc) ParallelWriterParam;
	self->base.dumpParams = (WriterDumpParamsProc) ParallelWriterDumpParams,
	self->base.sendQuery = (WriterSendQueryProc) ParallelWriterSendQuery;
	self->writer = opt;

	return (Writer *) self;
}

/**
 * @brief Initialize a ParallelWriter
 */
static void
ParallelWriterInit(ParallelWriter *self)
{
	unsigned	queryKey;
	char		queueName[MAXPGPATH];
	PGresult   *res;
	Relation	rel = NULL;

	Assert(self->base.truncate == false);

	/* Initialize information needed to check tuples when reading. */
	if (self->base.relid != InvalidOid)
	{
		TupleDesc	resultDesc;

		/* open relation to get the TupleDesc */
		self->base.rel = rel = heap_open(self->base.relid, AccessShareLock);
		self->base.desc = RelationGetDescr(self->base.rel);
		self->base.tchecker = CreateTupleChecker(self->base.desc);
		self->base.tchecker->checker = (CheckerTupleProc) CoercionCheckerTuple;

		/*
		 * If the return value of the filter function or input function is a
		 * target table, lookup_rowtype_tupdesc grab AccessShareLock on the
		 * table in the first call.  We call lookup_rowtype_tupdesc here to
		 * avoid deadlock when lookup_rowtype_tupdesc is called by the internal
		 * routine of the filter function or input function, because a parallel
		 * writer process holds an AccessExclusiveLock.
		 */
		resultDesc = lookup_rowtype_tupdesc(self->base.desc->tdtypeid, -1);
		ReleaseTupleDesc(resultDesc);
	}
	else
	{
		self->writer->init(self->writer);
		self->base.desc = self->writer->desc;
		self->base.tchecker = self->writer->tchecker;
	}

	self->base.context = AllocSetContextCreate(
							CurrentMemoryContext,
							"ParallelWriter",
#if PG_VERSION_NUM >= 90600
									ALLOCSET_DEFAULT_SIZES);
#else
									ALLOCSET_SMALL_MINSIZE,
									ALLOCSET_SMALL_INITSIZE,
									ALLOCSET_DEFAULT_MAXSIZE);
#endif

	/*
	 * Create a queue through which we will send the validated rows to
	 * the actual writer process that we will set up below.
	 */
	self->queue = QueueCreate(&queryKey, DEFAULT_BUFFER_SIZE);
	snprintf(queueName, lengthof(queueName), ":%u", queryKey);

	/*
	 * Connect to a new backend process that will actually perform the writes.
	 * As the new process will take an AccessExclusiveLock on the target
	 * relation, we must relinquish ours.  Note that we don't "close" the
	 * relation though, because we will need to set up a Checker; see
	 * CheckerInit().
	 *
	 * NB: This is quite a hack, because there is a window between our
	 * releasing the lock here and the new process acquiring its own during
	 * which the relation schema might change, but maybe that's something we
	 * have to live with.  Note that that's always been the case, because
	 * we never even took a lock in this process before supporting Postgres
	 * 12.  Warn users through documentation that there is such risk when
	 * using the MULTI_PROCESS=TRUE mode.
	 */
	if (rel)
		UnlockRelation(rel, AccessShareLock);
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

	if (!self->writer->dup_badfile)
		self->writer->dup_badfile = self->base.dup_badfile;

	/*
	 * The following executes pg_bulkload() in the new process with a
	 * DirectWriter writer; see DirectWriterSendQuery() for more details.
	 */
	if (1 != self->writer->sendQuery(self->writer, self->conn, queueName,
									 self->base.logfile,
									 self->base.verbose))
	{
		ereport(ERROR,
			(errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION),
					 errmsg("could not send query"),
					 errdetail("%s", finish_and_get_message(self))));
	}
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

	if (!self->base.rel)
		self->writer->close(self->writer, onError);

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
	{
		MemoryContextDelete(self->base.context);

		if (self->base.rel)
			heap_close(self->base.rel, NoLock);
	}

	return ret;
}

static bool
ParallelWriterParam(ParallelWriter *self, const char *keyword, char *value)
{
	bool	result;

	result = self->writer->param(self->writer, keyword, value);

	/* copy a writer output parameter */
	self->base.output = self->writer->output;
	self->base.relid = self->writer->relid;
	self->base.dup_badfile = self->writer->dup_badfile;

	return result;
}

static void
ParallelWriterDumpParams(ParallelWriter *self)
{
	self->writer->dumpParams(self->writer);
}

static int
ParallelWriterSendQuery(ParallelWriter *self, PGconn *conn, char *queueName, char *logfile, bool verbose)
{
	/* not support */
	Assert(false);

	return 0;
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

/*
 * Escaping libpq connect parameter strings.
 *
 * Replaces "'" with "\'" and "\" with "\\".
 */
static char *
escape_param_str(const char *str)
{
	const char *cp;
	StringInfo	buf = makeStringInfo();

	for (cp = str; *cp; cp++)
	{
		if (*cp == '\\' || *cp == '\'')
			appendStringInfoChar(buf, '\\');
		appendStringInfoChar(buf, *cp);
	}

	return buf->data;
}

static PGconn *
connect_to_localhost(void)
{
	PGconn *conn;
	char	sql[1024];
	char   *host;
	char	dbName[1024];

#ifdef HAVE_UNIX_SOCKETS

#if PG_VERSION_NUM >= 90300
    /* UnixSocketDir exist only 9.2 and before. */
    char *UnixSocketDir;

    /* Use PGHOST if it is set, otherwise use unix_socket_direcotoris */
    UnixSocketDir = getenv("PGHOST");
    if ( UnixSocketDir == NULL ) {
        UnixSocketDir = strtok(Unix_socket_directories, ",");
    }
#endif

	host = (UnixSocketDir == NULL || UnixSocketDir[0] == '\0') ?
				DEFAULT_PGSOCKET_DIR :
				UnixSocketDir;
#else
	host = "localhost";
#endif

	/* Also ensure backend isn't confused by this environment var. */
	setenv("PGCLIENTENCODING", GetDatabaseEncodingName(), 1);

	/* set dbname and disable hostaddr */
	snprintf(dbName, lengthof(dbName), "dbname='%s' hostaddr=''",
			 escape_param_str(get_database_name(MyDatabaseId)));

	conn = PQsetdbLogin(
		host,
		GetConfigOption("port", false, false),
		NULL, NULL,
		dbName,
#if PG_VERSION_NUM >= 90500
		GetUserNameFromId(GetUserId(), false),
#else
		GetUserNameFromId(GetUserId()),
#endif
		NULL);
	if (PQstatus(conn) == CONNECTION_BAD)
	{
		ParallelWriter wr;

		wr.conn = conn;
		ereport(ERROR,
				(errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION),
				 errmsg("could not establish connection to parallel writer"),
				 errdetail("%s", finish_and_get_message(&wr)),
				 errhint("Refer to the following if it is an authentication "
						 "error.  Specifies the authentication method to "
						 "without the need for a password in pg_hba.conf (ex. "
						 "trust or ident), or specify the password to the "
						 "password file of the operating system user who ran "
						 "PostgreSQL server.  If cannot use these solution, "
						 "specify WRITER=DIRECT.")));
	}

	/* attempt to set default datestyle */
	snprintf(sql, lengthof(sql), "SET datestyle = '%s'", GetConfigOption("datestyle", false, false));
	PQexec(conn, sql);

	/* attempt to set default datestyle */
	snprintf(sql, lengthof(sql), "SET timezone = '%s'", show_timezone());
	PQexec(conn, sql);

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
