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
static instr_time prof_reader_close;
static instr_time prof_writer_close;

instr_time prof_reader_read;
instr_time prof_reader_parse;

instr_time prof_writer_toast;
instr_time prof_writer_table;
instr_time prof_writer_index;

instr_time prof_index_merge;
instr_time prof_index_reindex;

instr_time prof_index_merge_flush;
instr_time prof_index_merge_build;

instr_time prof_index_merge_build_init;
instr_time prof_index_merge_build_unique;
instr_time prof_index_merge_build_insert;
instr_time prof_index_merge_build_term;
instr_time prof_index_merge_build_flush;

instr_time *prof_top;

/**
 * @brief Output the result of profile check.
 */
static void
BULKLOAD_PROFILE_PRINT()
{
	elog(INFO, "<GLOBAL>");
	elog(INFO, "  INIT   : %.7f", INSTR_TIME_GET_DOUBLE(prof_init));
	elog(INFO, "  READER : %.7f", INSTR_TIME_GET_DOUBLE(prof_reader));
	elog(INFO, "  WRITER : %.7f", INSTR_TIME_GET_DOUBLE(prof_writer));
	elog(INFO, "  MERGE  : %.7f", INSTR_TIME_GET_DOUBLE(prof_writer_close));
	elog(INFO, "<READER>");
	elog(INFO, "  READ   : %.7f", INSTR_TIME_GET_DOUBLE(prof_reader_read));
	elog(INFO, "  PARSE  : %.7f", INSTR_TIME_GET_DOUBLE(prof_reader_parse));
	elog(INFO, "<WRITER>");
	elog(INFO, "  TOAST  : %.7f", INSTR_TIME_GET_DOUBLE(prof_writer_toast));
	elog(INFO, "  TABLE  : %.7f", INSTR_TIME_GET_DOUBLE(prof_writer_table));
	elog(INFO, "  INDEX  : %.7f", INSTR_TIME_GET_DOUBLE(prof_writer_index));
	elog(INFO, "<MERGE>");
	elog(INFO, "  MERGE      : %.7f", INSTR_TIME_GET_DOUBLE(prof_index_merge));
	elog(INFO, "    FLUSH    : %.7f", INSTR_TIME_GET_DOUBLE(prof_index_merge_flush));
	elog(INFO, "    BUILD    : %.7f", INSTR_TIME_GET_DOUBLE(prof_index_merge_build));
	elog(INFO, "      INIT   : %.7f", INSTR_TIME_GET_DOUBLE(prof_index_merge_build_init));
	elog(INFO, "      UNIQUE : %.7f", INSTR_TIME_GET_DOUBLE(prof_index_merge_build_unique));
	elog(INFO, "      INSERT : %.7f", INSTR_TIME_GET_DOUBLE(prof_index_merge_build_insert));
	elog(INFO, "      TERM   : %.7f", INSTR_TIME_GET_DOUBLE(prof_index_merge_build_term));
	elog(INFO, "      FLUSH  : %.7f", INSTR_TIME_GET_DOUBLE(prof_index_merge_build_flush));
	elog(INFO, "  REINDEX    : %.7f", INSTR_TIME_GET_DOUBLE(prof_index_reindex));
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

	/* open writer - TODO: pass wt and on_duplicate from parser is ugly. */
	wt = rd.writer(rd.relid, rd.on_duplicate);

	/* no contfile errors. start bulkloading */
	elog(NOTICE, "BULK LOAD START");

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

		if ((tuple = ReaderNext(&rd)) == NULL)
			break;
		BULKLOAD_PROFILE(&prof_reader);

		/* Insert the heap tuple and index entries. */
		WriterInsert(wt, tuple);
		wt->count += 1;

		BULKLOAD_PROFILE(&prof_writer);

		MemoryContextReset(wt->context);
	}

	MemoryContextSwitchTo(ctx);

	ReaderClose(&rd);
	BULKLOAD_PROFILE(&prof_reader_close);

	/*
	 * STEP 3: Finalize heap and merge indexes
	 */

	count = wt->count;
	WriterClose(wt);
	BULKLOAD_PROFILE(&prof_writer_close);

	/*
	 * STEP 4: Postprocessing
	 */

	/* Write end log. */
	elog(NOTICE, "BULK LOAD END (" int64_FMT " records)", count);

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
