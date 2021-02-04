/*
 * pg_bulkload: lib/pg_bulkload.c
 *
 *	  Copyright (c) 2007-2021, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

/**
 * @file
 * @brief Core Modules
 */
#include "pg_bulkload.h"

#include <unistd.h>
#include <time.h>

#include "access/heapam.h"
#include "access/reloptions.h"
#include "catalog/objectaddress.h"
#if PG_VERSION_NUM >= 120000
#include "catalog/pg_am.h"
#endif
#include "commands/dbcommands.h"
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
#include "utils/rel.h"

#include "common.h"
#include "logger.h"
#include "reader.h"
#include "writer.h"
#include "pg_btree.h"
#include "pg_loadstatus.h"
#include "pg_profile.h"
#include "pg_strutil.h"
#include "pgut/pgut-be.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(pg_bulkload);

Datum	PGUT_EXPORT pg_bulkload(PG_FUNCTION_ARGS);

static char *timeval_to_cstring(struct timeval tp);
static void ParseOptions(Datum options, Reader **rd, Writer **wt, time_t tm);

#ifdef ENABLE_BULKLOAD_PROFILE
static instr_time prof_init;
static instr_time prof_reader;
static instr_time prof_writer;
static instr_time prof_reset;
instr_time prof_flush;
instr_time prof_merge;
instr_time prof_index;
instr_time prof_reindex;
instr_time prof_fini;

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
	const char *GLOBALs[] = { "INIT", "READER", "WRITER", "RESET", "FLUSH", "MERGE", "INDEX", "REINDEX", "FINI" };
	const char *READERs[] = { "SOURCE", "PARSER" };
	const char *WRITERs[] = { "TOAST", "TABLE", "INDEX" };
	const char *MERGEs[] = { "UNIQUE", "INSERT", "TERM" };

	/* GLOBAL */
	i = 0;
	seconds[i++] = INSTR_TIME_GET_DOUBLE(prof_init);
	seconds[i++] = INSTR_TIME_GET_DOUBLE(prof_reader);
	seconds[i++] = INSTR_TIME_GET_DOUBLE(prof_writer);
	seconds[i++] = INSTR_TIME_GET_DOUBLE(prof_reset);
	seconds[i++] = INSTR_TIME_GET_DOUBLE(prof_flush);
	seconds[i++] = INSTR_TIME_GET_DOUBLE(prof_merge);
	seconds[i++] = INSTR_TIME_GET_DOUBLE(prof_index);
	seconds[i++] = INSTR_TIME_GET_DOUBLE(prof_reindex);
	seconds[i++] = INSTR_TIME_GET_DOUBLE(prof_fini);
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
	HeapTuple		result;

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	BULKLOAD_PROFILE_PUSH();

	pg_rusage_init(&ru0);

	/* must be the super user */
	if (!superuser())
		ereport(ERROR,
			(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
			 errmsg("must be superuser to use pg_bulkload")));

	options = PG_GETARG_DATUM(0);

	ccxt = CurrentMemoryContext;

	/*
	 * STEP 1: Initialization
	 */

	/* parse options and create reader and writer */
	ParseOptions(options, &rd, &wt, ru0.tv.tv_sec);

	/* initialize reader */
	ReaderInit(rd);

	/*
	 * We need to split PG_TRY block because gcc optimizes if-branches with
	 * longjmp codes too much. Local variables initialized in either branch
	 * cannot be handled another branch.
	 */
	PG_TRY();
	{
		/* truncate heap */
		if (wt->truncate)
			TruncateTable(wt->relid);

		/* initialize writer */
		WriterInit(wt);

		/* initialize checker */
		CheckerInit(&rd->checker, wt->rel, wt->tchecker);

		/* initialize parser */
		ParserInit(rd->parser, &rd->checker, rd->infile, wt->desc,
				   wt->multi_process, PG_GET_COLLATION());
	}
	PG_CATCH();
	{
		if (rd)
			ReaderClose(rd, true);
		if (wt)
			WriterClose(wt, true);
		PG_RE_THROW();
	}
	PG_END_TRY();

	/* No throwable codes here! */

	PG_TRY();
	{
		/* create logger */
		CreateLogger(rd->logfile, wt->verbose, rd->infile[0] == ':');

		start = timeval_to_cstring(ru0.tv);
		LoggerLog(INFO, "\npg_bulkload %s on %s\n\n",
				   PG_BULKLOAD_VERSION, start);

		ReaderDumpParams(rd);
		WriterDumpParams(wt);
		LoggerLog(INFO, "\n");

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
			BULKLOAD_PROFILE(&prof_reset);
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
			  "  " int64_FMT " Rows replaced with new rows.\n\n",
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

	LoggerClose();

	result = heap_form_tuple(tupdesc, values, nulls);

	BULKLOAD_PROFILE(&prof_fini);
	BULKLOAD_PROFILE_POP();
	BULKLOAD_PROFILE_PRINT();

	PG_RETURN_DATUM(HeapTupleGetDatum(result));
}

/*
 * Check iff the write target is ok
 */
void
VerifyTarget(Relation rel, int64 max_dup_errors)
{
	AclMode	required_access;
	AclMode	aclresult;
	if (rel->rd_rel->relkind != RELKIND_RELATION)
	{
		const char *type;
		switch (rel->rd_rel->relkind)
		{
#if PG_VERSION_NUM >= 100000
			case RELKIND_PARTITIONED_TABLE:
				type = "partitioned table";
				break;
#endif
#if PG_VERSION_NUM >= 90100
			case RELKIND_FOREIGN_TABLE:
				type = "foreign table";
				break;
#endif
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

	required_access = ACL_INSERT |
						(max_dup_errors != 0 ? ACL_DELETE : ACL_NO_RIGHTS);
	aclresult = pg_class_aclmask(
		RelationGetRelid(rel), GetUserId(), required_access, ACLMASK_ALL);
	if (required_access != aclresult)
		aclcheck_error(ACLCHECK_NO_PRIV,
#if PG_VERSION_NUM >= 110000
					   get_relkind_objtype(rel->rd_rel->relkind),
#else
					   ACL_KIND_CLASS,
#endif
					   RelationGetRelationName(rel));

#if PG_VERSION_NUM >= 120000
	if (rel->rd_rel->relam != HEAP_TABLE_AM_OID)
		ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("pg_bulkload only supports tables with \"heap\" access method")));
#endif
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

static void
ParseOptions(Datum options, Reader **rd, Writer **wt, time_t tm)
{
	List		   *defs;
	List		   *rest_defs = NIL;
	ListCell	   *cell;
	DefElem		   *opt;
	char		   *keyword;
	char		   *value;
	char		   *type = NULL;
	char		   *writer = NULL;
	bool			multi_process = false;

	Assert(*rd == NULL);
	Assert(*wt == NULL);

	/* parse for each option */
	defs = untransformRelOptions(options);
	foreach (cell, defs)
	{
		opt = lfirst(cell);
		if (opt->arg == NULL)
			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("option \"%s\" has no value", opt->defname)));

		keyword = opt->defname;
		value = strVal(opt->arg);

		if (CompareKeyword(keyword, "TYPE"))
		{
			ASSERT_ONCE(type == NULL);
			type = value;
		}
		else if (CompareKeyword(keyword, "WRITER") ||
				 CompareKeyword(keyword, "LOADER"))
		{
			ASSERT_ONCE(writer == NULL);
			writer = value;
		}
		else if (CompareKeyword(keyword, "MULTI_PROCESS"))
		{
			multi_process = ParseBoolean(value);
		}
		else
		{
			rest_defs = lappend(rest_defs, opt);
			continue;
		}
	}

	*wt = WriterCreate(writer, multi_process);
	*rd = ReaderCreate(type);

	foreach (cell, rest_defs)
	{
		opt = lfirst(cell);
		keyword = opt->defname;
		value = strVal(opt->arg);

		if (!WriterParam((*wt), keyword, value) &&
			!ReaderParam((*rd), keyword, value))
		{
			ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("invalid keyword \"%s\"", keyword)));
		}
	}

	/*
	 * checking necessary common setting items
	 */
	if ((*rd)->infile == NULL)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("INPUT option required")));
	if ((*wt)->output == NULL)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("OUTPUT option required")));

	/*
	 * If log paths are not specified, generate them with the following rules:
	 *   {basename}        : $PGDATA/pg_bulkload/YYYYMMDDHHMISS_{db}_{nsp}_{tbl}
	 *   LOGFILE           : {basename}.log
	 *   PARSE_BADFILE     : {basename}.prs.{extension-of-infile}
	 *   DUPLICATE_BADFILE : {basename}.dup.csv
	 */
	if ((*rd)->logfile == NULL ||
		(*rd)->parse_badfile == NULL ||
		(*wt)->dup_badfile == NULL)
	{
		char		path[MAXPGPATH] = BULKLOAD_LSF_DIR "/";
		int			len;
		int			elen;
		char	   *dbname;

		if (getcwd(path, MAXPGPATH) == NULL)
			elog(ERROR, "could not get current working directory: %m");
		len = strlen(path);
		len += snprintf(path + len, MAXPGPATH - len, "/%s/", BULKLOAD_LSF_DIR);
		len += strftime(path + len, MAXPGPATH - len, "%Y%m%d%H%M%S_",
						localtime(&tm));

		/* TODO: cleanup characters in object names, ex. ?, *, etc. */
		dbname = get_database_name(MyDatabaseId);
		if ((*wt)->relid != InvalidOid)
		{
			char   *nspname;
			char   *relname;

			nspname = get_namespace_name(get_rel_namespace((*wt)->relid));
			relname = get_rel_name((*wt)->relid);
			len += snprintf(path + len, MAXPGPATH - len, "%s_%s_%s.",
						dbname, nspname, relname);
			pfree(nspname);
			pfree(relname);
		}
		pfree(dbname);

		if (len >= MAXPGPATH)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("default loader output file name is too long")));

		canonicalize_path(path);

		if ((*rd)->logfile == NULL)
		{
			elen = snprintf(path + len, MAXPGPATH - len, "log");
			if (elen + len >= MAXPGPATH)
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("default loader log file name is too long")));

			(*rd)->logfile = pstrdup(path);
		}

		if ((*rd)->parse_badfile == NULL)
		{
			char   *filename;
			char   *extension;

			/* find the extension of infile */
			filename = strrchr((*rd)->infile, '/');
			extension = strrchr((*rd)->infile, '.');
			if (!filename || !extension || filename >= extension)
				extension = "";

			elen = snprintf(path + len, MAXPGPATH - len, "prs%s", extension);
			if (elen + len >= MAXPGPATH)
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("default parse bad file name is too long")));

			(*rd)->parse_badfile = pstrdup(path);
		}

		if ((*wt)->dup_badfile == NULL)
		{
			elen = snprintf(path + len, MAXPGPATH - len, "dup.csv");
			if (elen + len >= MAXPGPATH)
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("default duplicate bad file name is too long")));

			(*wt)->dup_badfile = pstrdup(path);
		}

		/* Verify DataDir/pg_bulkload directory */
		ValidateLSFDirectory(BULKLOAD_LSF_DIR);
	}

	/*
	 * check it whether there is not the same file name.
	 */
	if ((*wt)->relid != InvalidOid &&
		(strcmp((*rd)->infile, (*rd)->logfile) == 0 ||
		 strcmp((*rd)->infile, (*rd)->parse_badfile) == 0 ||
		 strcmp((*rd)->infile, (*wt)->dup_badfile) == 0 ||
		 strcmp((*rd)->logfile, (*rd)->parse_badfile) == 0 ||
		 strcmp((*rd)->logfile, (*wt)->dup_badfile) == 0 ||
		 strcmp((*rd)->parse_badfile, (*wt)->dup_badfile) == 0))
		ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("INPUT, PARSE_BADFILE, DUPLICATE_BADFILE and LOGFILE cannot set the same file name.")));

	if ((*wt)->relid == InvalidOid)
	{
		char	ctlpath[MAXPGPATH];

		snprintf(ctlpath, MAXPGPATH, "%s.ctl", (*wt)->output);

		if (strcmp((*rd)->infile, (*rd)->logfile) == 0 ||
			strcmp((*rd)->infile, (*rd)->parse_badfile) == 0 ||
			strcmp((*rd)->infile, (*wt)->output) == 0 ||
			strcmp((*rd)->infile, ctlpath) == 0 ||
			strcmp((*rd)->logfile, (*rd)->parse_badfile) == 0 ||
			strcmp((*rd)->logfile, (*wt)->output) == 0 ||
			strcmp((*rd)->logfile, ctlpath) == 0 ||
			strcmp((*rd)->parse_badfile, (*wt)->output) == 0 ||
			strcmp((*rd)->parse_badfile, ctlpath) == 0)
		ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("INPUT, PARSE_BADFILE, OUTPUT, LOGFILE and sample control file cannot set the same file name.")));
	}

	(*wt)->logfile = pstrdup((*rd)->logfile);
}
