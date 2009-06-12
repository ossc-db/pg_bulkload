/*
 * pg_bulkload: lib/pg_bulkload.c
 *
 *	  Copyright(C) 2007-2009, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

/**
 * @file
 * @brief Core Modules
 */
#include "postgres.h"

#include "funcapi.h"
#include "miscadmin.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/memutils.h"

#include "reader.h"
#include "writer.h"
#include "pg_btree.h"
#include "pg_profile.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(pg_bulkload);
PG_FUNCTION_INFO_V1(pg_bulkread);
PG_FUNCTION_INFO_V1(pg_bulkwrite_accum);
PG_FUNCTION_INFO_V1(pg_bulkwrite_finish);

Datum	pg_bulkload(PG_FUNCTION_ARGS);
Datum	pg_bulkread(PG_FUNCTION_ARGS);
Datum	pg_bulkwrite_accum(PG_FUNCTION_ARGS);
Datum	pg_bulkwrite_finish(PG_FUNCTION_ARGS);

extern void _PG_init(void);
extern void _PG_fini(void);

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

void
_PG_init(void)
{
	/* Ensure close data file and status file on error */
	RegisterXactCallback(AtEOXact_DirectLoader, 0);
}

void
_PG_fini(void)
{
	UnregisterXactCallback(AtEOXact_DirectLoader, 0);
}

#define GETARG_CSTRING(n) \
	((PG_NARGS() <= (n) || PG_ARGISNULL(n)) \
	? NULL : text_to_cstring(PG_GETARG_TEXT_PP(n)))

/**
 * @brief Entry point of the user-defined function for pg_bulkload.
 * @return Returns number of loaded tuples.  If the case of errors, -1 will be
 * returned.
 */
Datum
pg_bulkload(PG_FUNCTION_ARGS)
{
	Reader			rd;
	Writer		   *wt;
	char		   *path;
	char		   *options;
	MemoryContext	ctx;
	int64			count;

	/*
	 * STEP 1: Initialization
	 */

	BULKLOAD_PROFILE_PUSH();

	path = GETARG_CSTRING(0);
	options = GETARG_CSTRING(1);

	/* open reader - TODO: split reader and controlfile parser. */
	ReaderOpen(&rd, path, options);

	/* open writer - TODO: pass relid and on_duplicate from parser is ugly. */
	wt = rd.writer(rd.relid, rd.on_duplicate);

	BULKLOAD_PROFILE(&prof_init);

	/*
	 * STEP 2: Build heap
	 */

	/* Switch into its memory context */
	Assert(wt->context);
	ctx = MemoryContextSwitchTo(wt->context);

	/* Loop for each input file record. */
	while (wt->count < rd.ci_limit)
	{
		HeapTuple	tuple;

		CHECK_FOR_INTERRUPTS();

		/* read tuple */
		BULKLOAD_PROFILE_PUSH();
		tuple = ReaderNext(&rd);
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

	ReaderClose(&rd);
	count = wt->count;
	WriterClose(wt);

	BULKLOAD_PROFILE_POP();
	BULKLOAD_PROFILE_PRINT();

	PG_RETURN_INT64(count);
}

/*
 * bulk reader
 */
Datum
pg_bulkread(PG_FUNCTION_ARGS)
{
	FuncCallContext	   *funcctx;
	Reader			   *rd;
	HeapTuple			tuple;

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext	ctx;
		TupleDesc		tupdesc;
		char		   *path;
		char		   *options;

		if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
			elog(ERROR, "return and sql tuple descriptions are incompatible");

		funcctx = SRF_FIRSTCALL_INIT();

		path = GETARG_CSTRING(0);
		options = GETARG_CSTRING(1);

		ctx = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		rd = (Reader *) palloc(sizeof(Reader));
		ReaderOpen(rd, path, options);
		funcctx->user_fctx = rd;
		MemoryContextSwitchTo(ctx);
	}
	else
	{
		funcctx = SRF_PERCALL_SETUP();
		rd = funcctx->user_fctx;
	}

	/* read the next tuple and return it, or close reader. */
	if (funcctx->call_cntr < rd->ci_limit && (tuple = ReaderNext(rd)) != NULL)
		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));

	/* done */
	ReaderClose(rd);

	SRF_RETURN_DONE(funcctx);
}

/*
 * Aggregation-based bulk writer - accumulator
 */
Datum
pg_bulkwrite_accum(PG_FUNCTION_ARGS)
{
	Writer		   *wt = (Writer *) PG_GETARG_POINTER(0);
	Oid				relid = PG_GETARG_OID(1);
	HeapTupleHeader htup = PG_GETARG_HEAPTUPLEHEADER(2);
	HeapTupleData	tuple;

	if (wt == NULL)
	{
		MemoryContext	ctx;

		ctx = MemoryContextSwitchTo(fcinfo->flinfo->fn_mcxt);
		wt = CreateBufferedWriter(relid, ON_DUPLICATE_ERROR);
//			 CreateDirectWriter(relid);
		MemoryContextSwitchTo(ctx);
	}
//	else if (RelationGetRelid(wt->rel) != relid)
//		elog(ERROR, "relid cannot be changed");

	/* Build a temporary HeapTuple control structure */
	tuple.t_len = HeapTupleHeaderGetDatumLength(htup);
	ItemPointerSetInvalid(&(tuple.t_self));
	tuple.t_tableOid = InvalidOid;
	tuple.t_data = htup;

	WriterInsert(wt, &tuple);
	wt->count += 1;

	PG_RETURN_POINTER(wt);
}

/*
 * Aggregation-based bulk writer - finalizer
 */
Datum
pg_bulkwrite_finish(PG_FUNCTION_ARGS)
{
	Writer *wt = (Writer *) PG_GETARG_POINTER(0);
	int64	count = 0;

	if (wt)
	{
		count = wt->count;
		WriterClose(wt);
	}

	PG_RETURN_INT64(count);
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
