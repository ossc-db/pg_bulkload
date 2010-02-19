/*
 * pg_bulkload: lib/pg_bulkload.c
 *
 *	  Copyright (c) 2007-2010, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

/**
 * @file
 * @brief Core Modules
 */
#include "postgres.h"

#include "access/heapam.h"
#include "commands/tablecmds.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/datetime.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/pg_rusage.h"

#include "logger.h"
#include "reader.h"
#include "writer.h"
#include "pg_btree.h"
#include "pg_bulkload.h"
#include "pg_profile.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(pg_bulkload);

Datum	pg_bulkload(PG_FUNCTION_ARGS);

const char *PROGRAM_VERSION = "3.0alpha2";

static char *timeval_to_cstring(struct timeval tp);

#ifdef ENABLE_BULKLOAD_PROFILE
static instr_time prof_init;
static instr_time prof_reader;
static instr_time prof_writer;
instr_time prof_flush;
instr_time prof_merge;
instr_time prof_index;
instr_time prof_reindex;

instr_time prof_reader_source;
instr_time prof_reader_parser;

instr_time prof_writer_toast;
instr_time prof_writer_table;
instr_time prof_writer_index;

instr_time prof_merge_unique;
instr_time prof_merge_insert;
instr_time prof_merge_term;

instr_time *prof_top;

static void
print_profiles(const char *title, int n, const char *names[], const double seconds[])
{
	int		i;
	double	sum;

	for (sum = 0, i = 0; i < n; i++)
		sum += seconds[i];
	if (sum == 0)
		sum = 1;	/* avoid division by zero */

	elog(INFO, "<%s>", title);
	for (i = 0; i < n; i++)
		elog(INFO, "  %-8s: %.7f (%6.2f%%)", names[i], seconds[i], seconds[i] / sum * 100.0);
}

/**
 * @brief Output the result of profile check.
 */
static void
BULKLOAD_PROFILE_PRINT()
{
	int		i;
	double	seconds[10];
	const char *GLOBALs[] = { "INIT", "READER", "WRITER", "FLUSH", "MERGE", "INDEX", "REINDEX" };
	const char *READERs[] = { "SOURCE", "PARSER" };
	const char *WRITERs[] = { "TOAST", "TABLE", "INDEX" };
	const char *MERGEs[] = { "UNIQUE", "INSERT", "TERM" };

	/* GLOBAL */
	i = 0;
	seconds[i++] = INSTR_TIME_GET_DOUBLE(prof_init);
	seconds[i++] = INSTR_TIME_GET_DOUBLE(prof_reader);
	seconds[i++] = INSTR_TIME_GET_DOUBLE(prof_writer);
	seconds[i++] = INSTR_TIME_GET_DOUBLE(prof_flush);
	seconds[i++] = INSTR_TIME_GET_DOUBLE(prof_merge);
	seconds[i++] = INSTR_TIME_GET_DOUBLE(prof_index);
	seconds[i++] = INSTR_TIME_GET_DOUBLE(prof_reindex);
	print_profiles("GLOBAL", i, GLOBALs, seconds);

	/* READER */
	i = 0;
	seconds[i++] = INSTR_TIME_GET_DOUBLE(prof_reader_source);
	seconds[i++] = INSTR_TIME_GET_DOUBLE(prof_reader_parser);
	print_profiles("READER", i, READERs, seconds);

	/* WRITER */
	i = 0;
	seconds[i++] = INSTR_TIME_GET_DOUBLE(prof_writer_toast);
	seconds[i++] = INSTR_TIME_GET_DOUBLE(prof_writer_table);
	seconds[i++] = INSTR_TIME_GET_DOUBLE(prof_writer_index);
	print_profiles("WRITER", i, WRITERs, seconds);

	/* MERGE */
	i = 0;
	seconds[i++] = INSTR_TIME_GET_DOUBLE(prof_merge_unique);
	seconds[i++] = INSTR_TIME_GET_DOUBLE(prof_merge_insert);
	seconds[i++] = INSTR_TIME_GET_DOUBLE(prof_merge_term);
	print_profiles("MERGE", i, MERGEs, seconds);
}
#else
#define BULKLOAD_PROFILE_PRINT()	((void) 0)
#endif   /* ENABLE_BULKLOAD_PROFILE */

/* ========================================================================
 * Implementation
 * ========================================================================*/

#define diffTime(t1, t2) \
	(((t1).tv_sec - (t2).tv_sec) * 1.0 + \
	((t1).tv_usec - (t2).tv_usec) / 1000000.0)

/**
 * @brief Entry point of the user-defined function for pg_bulkload.
 * @return Returns number of loaded tuples.  If the case of errors, -1 will be
 * returned.
 */
Datum
pg_bulkload(PG_FUNCTION_ARGS)
{
	Reader		   *rd = NULL;
	Writer		   *wt = NULL;
	Datum			options;
	MemoryContext	ctx;
	MemoryContext	ccxt;
	PGRUsage		ru0;
	PGRUsage		ru1;
	int64			count;
	int64			parse_errors;
	int64			skip;
	WriterResult	ret;
	char		   *start;
	char		   *end;
	float8			system;
	float8			user;
	float8			duration;
	TupleDesc		tupdesc;
	Datum			values[PG_BULKLOAD_COLS];
	bool			nulls[PG_BULKLOAD_COLS];
	char		   *messages;
	HeapTuple		result;

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	BULKLOAD_PROFILE_PUSH();

	pg_rusage_init(&ru0);

	options = PG_GETARG_DATUM(0);

	ccxt = CurrentMemoryContext;

	/*
	 * STEP 1: Initialization
	 */

	/* parse options and create reader */
	rd = ReaderCreate(options, ru0.tv.tv_sec);

	/*
	 * We need to split PG_TRY block because gcc optimizes if-branches with
	 * longjmp codes too much. Local variables initialized in either branch
	 * cannot be handled another branch.
	 */
	PG_TRY();
	{
		/* truncate heap if not parallel writer */
		if (rd->wo.truncate && rd->writer != CreateParallelWriter)
			TruncateTable(rd->relid);

		/* open relation without any relation locks */
		rd->rel = heap_open(rd->relid, NoLock);

		/* initialize parser */
		ParserInit(rd->parser, &rd->checker, rd->infile, RelationGetDescr(rd->rel));

		/* initialize checker */
		CheckerInit(&rd->checker, rd->rel);

		/* create writer */
		wt = rd->writer(rd->relid, &rd->wo);
	}
	PG_CATCH();
	{
		if (rd)
			ReaderClose(rd, true);
		PG_RE_THROW();
	}
	PG_END_TRY();

	/* No throwable codes here! */

	PG_TRY();
	{
		/* create logger */
		CreateLogger(rd->logfile, rd->verbose);

		start = timeval_to_cstring(ru0.tv);
		LoggerLog(INFO, "\npg_bulkload %s on %s\n\n",
				   PROGRAM_VERSION, start);

		ReaderDumpParams(rd);
		WriterDumpParams(wt);

		BULKLOAD_PROFILE(&prof_init);

		/*
		 * STEP 2: Build heap
		 */

		/* Switch into its memory context */
		Assert(wt->context);
		ctx = MemoryContextSwitchTo(wt->context);

		/* Loop for each input file record. */
		while (wt->count < rd->limit)
		{
			HeapTuple	tuple;

			CHECK_FOR_INTERRUPTS();

			/* read tuple */
			BULKLOAD_PROFILE_PUSH();
			tuple = ReaderNext(rd);
			BULKLOAD_PROFILE_POP();
			BULKLOAD_PROFILE(&prof_reader);
			if (tuple == NULL)
				break;

			/* write tuple */
			BULKLOAD_PROFILE_PUSH();
			WriterInsert(wt, tuple);
			wt->count += 1;
			BULKLOAD_PROFILE_POP();
			BULKLOAD_PROFILE(&prof_writer);

			MemoryContextReset(wt->context);
		}

		MemoryContextSwitchTo(ctx);

		/*
		 * STEP 3: Finalize heap and merge indexes
		 */

		count = wt->count;
		parse_errors = rd->parse_errors;

		/*
		 * close writer first and reader second because shmem_exit callback
		 * is managed by a simple stack.
		 */
		ret = WriterClose(wt, false);
		wt = NULL;
		skip = ReaderClose(rd, false);
		rd = NULL;
	}
	PG_CATCH();
	{
		ErrorData	   *errdata;
		MemoryContext	ecxt;

		ecxt = MemoryContextSwitchTo(ccxt);
		errdata = CopyErrorData();
		LoggerLog(INFO, "%s\n", errdata->message);
		FreeErrorData(errdata);

		/* close writer first, and reader second */
		if (wt)
			WriterClose(wt, true);
		if (rd)
			ReaderClose(rd, true);

		MemoryContextSwitchTo(ecxt);
		PG_RE_THROW();
	}
	PG_END_TRY();

	count -= ret.num_dup_new;

	LoggerLog(INFO, "\n"
			  "  " int64_FMT " Rows skipped.\n"
			  "  " int64_FMT " Rows successfully loaded.\n"
			  "  " int64_FMT " Rows not loaded due to parse errors.\n"
			  "  " int64_FMT " Rows not loaded due to duplicate errors.\n"
			  "  " int64_FMT " Rows deleted due to duplicate errors.\n\n",
			  skip, count, parse_errors, ret.num_dup_new, ret.num_dup_old);

	pg_rusage_init(&ru1);
	system = diffTime(ru1.ru.ru_stime, ru0.ru.ru_stime);
	user = diffTime(ru1.ru.ru_utime, ru0.ru.ru_utime);
	duration = diffTime(ru1.tv, ru0.tv);
	end = timeval_to_cstring(ru1.tv);

	memset(nulls, 0, sizeof(nulls));
	values[0] = Int64GetDatum(skip);
	values[1] = Int64GetDatum(count);
	values[2] = Int64GetDatum(parse_errors);
	values[3] = Int64GetDatum(ret.num_dup_new);
	values[4] = Int64GetDatum(ret.num_dup_old);
	values[5] = Float8GetDatumFast(system);
	values[6] = Float8GetDatumFast(user);
	values[7] = Float8GetDatumFast(duration);

	LoggerLog(INFO,
		"Run began on %s\n"
		"Run ended on %s\n\n"
		"CPU %.2fs/%.2fu sec elapsed %.2f sec\n",
		start, end, system, user, duration);

	if ((messages = LoggerClose()) != NULL)
		values[8] = CStringGetTextDatum(messages);
	else
		nulls[8] = true;

	result = heap_form_tuple(tupdesc, values, nulls);

	BULKLOAD_PROFILE_POP();
	BULKLOAD_PROFILE_PRINT();

	PG_RETURN_DATUM(HeapTupleGetDatum(result));
}

/*
 * Check iff the write target is ok
 */
void
VerifyTarget(Relation rel)
{
	AclResult	aclresult;
	if (rel->rd_rel->relkind != RELKIND_RELATION)
	{
		const char *type;
		switch (rel->rd_rel->relkind)
		{
			case RELKIND_VIEW:
				type = "view";
				break;
			case RELKIND_SEQUENCE:
				type = "sequence";
				break;
			default:
				type = "non-table relation";
				break;
		}
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("cannot load to %s \"%s\"",
					type, RelationGetRelationName(rel))));
	}

	aclresult = pg_class_aclcheck(
		RelationGetRelid(rel), GetUserId(), ACL_INSERT);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult, ACL_KIND_CLASS,
					   RelationGetRelationName(rel));
}

/*
 * truncate relation
 */
void
TruncateTable(Oid relid)
{
	TruncateStmt	stmt;
	RangeVar	   *heap;

	Assert(OidIsValid(relid));

	heap = makeRangeVar(get_namespace_name(get_rel_namespace(relid)),
						get_rel_name(relid), -1);

	memset(&stmt, 0, sizeof(stmt));
	stmt.type = T_TruncateStmt;
	stmt.relations = list_make1(heap);
	stmt.behavior = DROP_RESTRICT;
	ExecuteTruncate(&stmt);

	CommandCounterIncrement();
}

static char *
timeval_to_cstring(struct timeval tp)
{
	TimestampTz tz;
	char	   *str;

	tz = (TimestampTz) tp.tv_sec -
		((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY);

#ifdef HAVE_INT64_TIMESTAMP
	tz = (tz * USECS_PER_SEC) + tp.tv_usec;
#else
	tz = tz + (tp.tv_usec / 1000000.0);
#endif

	str = DatumGetCString(DirectFunctionCall1(timestamptz_out,
				TimestampTzGetDatum(tz)));
	return str;
}
