/*
 * pg_bulkload: lib/writer_buffered.c
 *
 *	  Copyright (c) 2007-2021, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

#include "postgres.h"

#include "access/heapam.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "executor/executor.h"
#include "utils/builtins.h"
#include "utils/rel.h"

#include "logger.h"
#include "reader.h"
#include "writer.h"
#include "pg_btree.h"
#include "pg_strutil.h"
#include "pgut/pgut-be.h"

#if PG_VERSION_NUM >= 100000
#include "utils/regproc.h"
#endif

typedef struct BufferedWriter
{
	Writer			base;

	Spooler			spooler;

	BulkInsertState bistate;	/* use bulk insert storategy */
	CommandId		cid;
} BufferedWriter;

static void	BufferedWriterInit(BufferedWriter *self);
static void	BufferedWriterInsert(BufferedWriter *self, HeapTuple tuple);
static WriterResult	BufferedWriterClose(BufferedWriter *self, bool onError);
static bool	BufferedWriterParam(BufferedWriter *self, const char *keyword, char *value);
static void	BufferedWriterDumpParams(BufferedWriter *self);
static int	BufferedWriterSendQuery(BufferedWriter *self, PGconn *conn, char *queueName, char *logfile, bool verbose);

/* ========================================================================
 * Implementation
 * ========================================================================*/

/**
 * @brief Create a new BufferedWriter
 */
Writer *
CreateBufferedWriter(void *opt)
{
	BufferedWriter *self = palloc0(sizeof(BufferedWriter));
	self->base.init = (WriterInitProc) BufferedWriterInit;
	self->base.insert = (WriterInsertProc) BufferedWriterInsert;
	self->base.close = (WriterCloseProc) BufferedWriterClose;
	self->base.param = (WriterParamProc) BufferedWriterParam;
	self->base.dumpParams = (WriterDumpParamsProc) BufferedWriterDumpParams;
	self->base.sendQuery = (WriterSendQueryProc) BufferedWriterSendQuery;
	self->base.max_dup_errors = -2;

	return (Writer *) self;
}

/**
 * @brief Initialize a BufferedWriter
 */
static void
BufferedWriterInit(BufferedWriter *self)
{
	/*
	 * Set defaults to unspecified parameters.
	 */
	if (self->base.max_dup_errors < -1)
		self->base.max_dup_errors = DEFAULT_MAX_DUP_ERRORS;

	self->base.rel = heap_open(self->base.relid, AccessExclusiveLock);
	VerifyTarget(self->base.rel, self->base.max_dup_errors);

	self->base.desc = RelationGetDescr(self->base.rel);

	SpoolerOpen(&self->spooler, self->base.rel, true, self->base.on_duplicate,
				self->base.max_dup_errors, self->base.dup_badfile);
	self->base.context = GetPerTupleMemoryContext(self->spooler.estate);

	self->bistate = GetBulkInsertState();
	self->cid = GetCurrentCommandId(true);

	self->base.tchecker = CreateTupleChecker(self->base.desc);
	self->base.tchecker->checker = (CheckerTupleProc) CoercionCheckerTuple;
}

/**
 * @brief Store tuples into the heap using shared buffers.
 * @return void
 */
static void
BufferedWriterInsert(BufferedWriter *self, HeapTuple tuple)
{
	heap_insert(self->base.rel, tuple, self->cid, 0, self->bistate);
	SpoolerInsert(&self->spooler, tuple);
}

static WriterResult
BufferedWriterClose(BufferedWriter *self, bool onError)
{
	WriterResult	ret = { 0 };

	if (!onError)
	{
		if (self->bistate)
			FreeBulkInsertState(self->bistate);

		SpoolerClose(&self->spooler);
		ret.num_dup_new = self->spooler.dup_new;
		ret.num_dup_old = self->spooler.dup_old;

		if (self->base.rel)
			heap_close(self->base.rel, AccessExclusiveLock);

		pfree(self);
	}

	return ret;
}

static bool
BufferedWriterParam(BufferedWriter *self, const char *keyword, char *value)
{
	if (CompareKeyword(keyword, "TABLE") ||
		CompareKeyword(keyword, "OUTPUT"))
	{
		ASSERT_ONCE(self->base.output == NULL);

		self->base.relid = RangeVarGetRelid(makeRangeVarFromNameList(
						stringToQualifiedNameList(value)), NoLock, false);
		self->base.output = get_relation_name(self->base.relid);
	}
	else if (CompareKeyword(keyword, "DUPLICATE_BADFILE"))
	{
		ASSERT_ONCE(self->base.dup_badfile == NULL);
		self->base.dup_badfile = pstrdup(value);
	}
	else if (CompareKeyword(keyword, "DUPLICATE_ERRORS"))
	{
		ASSERT_ONCE(self->base.max_dup_errors < -1);
		self->base.max_dup_errors = ParseInt64(value, -1);
		if (self->base.max_dup_errors == -1)
			self->base.max_dup_errors = INT64_MAX;
	}
	else if (CompareKeyword(keyword, "ON_DUPLICATE_KEEP"))
	{
		const ON_DUPLICATE values[] =
		{
			ON_DUPLICATE_KEEP_NEW,
			ON_DUPLICATE_KEEP_OLD
		};

		self->base.on_duplicate = values[choice(keyword, value, ON_DUPLICATE_NAMES, lengthof(values))];
	}
	else if (CompareKeyword(keyword, "TRUNCATE"))
	{
		self->base.truncate = ParseBoolean(value);
	}
	else
		return false;	/* unknown parameter */

	return true;
}

static void
BufferedWriterDumpParams(BufferedWriter *self)
{
	char		   *str;
	StringInfoData	buf;

	initStringInfo(&buf);

	appendStringInfoString(&buf, "WRITER = BUFFERED\n");

	str = QuoteString(self->base.dup_badfile);
	appendStringInfo(&buf, "DUPLICATE_BADFILE = %s\n", str);
	pfree(str);

	if (self->base.max_dup_errors == INT64_MAX)
		appendStringInfo(&buf, "DUPLICATE_ERRORS = INFINITE\n");
	else
		appendStringInfo(&buf, "DUPLICATE_ERRORS = " int64_FMT "\n",
						 self->base.max_dup_errors);

	appendStringInfo(&buf, "ON_DUPLICATE_KEEP = %s\n",
					 ON_DUPLICATE_NAMES[self->base.on_duplicate]);

	appendStringInfo(&buf, "TRUNCATE = %s\n",
					 self->base.truncate ? "YES" : "NO");

	LoggerLog(INFO, buf.data, 0);
	pfree(buf.data);
}

static int
BufferedWriterSendQuery(BufferedWriter *self, PGconn *conn, char *queueName, char *logfile, bool verbose)
{
	const char *params[8];
	char		max_dup_errors[MAXINT8LEN + 1];

	if (self->base.max_dup_errors < -1)
		self->base.max_dup_errors = DEFAULT_MAX_DUP_ERRORS;

	snprintf(max_dup_errors, MAXINT8LEN, INT64_FORMAT,	
			 self->base.max_dup_errors);

	/* async query send */
	params[0] = queueName;
	params[1] = self->base.output;
	params[2] = ON_DUPLICATE_NAMES[self->base.on_duplicate];
	params[3] = max_dup_errors;
	params[4] = self->base.dup_badfile;
	params[5] = logfile;
	params[6] = verbose ? "true" : "no";
	params[7] = (self->base.truncate ? "true" : "no");

	return PQsendQueryParams(conn,
		"SELECT * FROM pgbulkload.pg_bulkload(ARRAY["
		"'TYPE=TUPLE',"
		"'INPUT=' || $1,"
		"'WRITER=BUFFERED',"
		"'OUTPUT=' || $2,"
		"'ON_DUPLICATE_KEEP=' || $3,"
		"'DUPLICATE_ERRORS=' || $4,"
		"'DUPLICATE_BADFILE=' || $5,"
		"'LOGFILE=' || $6,"
		"'VERBOSE=' || $7,"
		"'TRUNCATE=' || $8])",
		8, NULL, params, NULL, NULL, 0);
}
