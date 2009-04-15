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

#include "access/heapam.h"
#include "access/transam.h"
#include "catalog/catalog.h"
#include "miscadmin.h"
#include "utils/builtins.h"

#include "pg_bulkload.h"
#include "pg_btree.h"
#include "pg_controlinfo.h"
#include "pg_profile.h"

PG_MODULE_MAGIC;

#ifdef ENABLE_BULKLOAD_PROFILE
static instr_time prof_init;
static instr_time prof_heap;
static instr_time prof_index;
static instr_time prof_term;

instr_time prof_heap_read;
instr_time prof_heap_toast;
instr_time prof_heap_table;
instr_time prof_heap_index;
instr_time prof_heap_flush;

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
	elog(INFO, "  INIT  : %.7f", INSTR_TIME_GET_DOUBLE(prof_init));
	elog(INFO, "  HEAP  : %.7f", INSTR_TIME_GET_DOUBLE(prof_heap));
	elog(INFO, "  INDEX : %.7f", INSTR_TIME_GET_DOUBLE(prof_index));
	elog(INFO, "  TERM  : %.7f", INSTR_TIME_GET_DOUBLE(prof_term));
	elog(INFO, "<HEAP>");
	elog(INFO, "  READ  : %.7f", INSTR_TIME_GET_DOUBLE(prof_heap_read));
	elog(INFO, "  TOAST : %.7f", INSTR_TIME_GET_DOUBLE(prof_heap_toast));
	elog(INFO, "  TABLE : %.7f", INSTR_TIME_GET_DOUBLE(prof_heap_table));
	elog(INFO, "  INDEX : %.7f", INSTR_TIME_GET_DOUBLE(prof_heap_index));
	elog(INFO, "  FLUSH : %.7f", INSTR_TIME_GET_DOUBLE(prof_heap_flush));
	elog(INFO, "<INDEX>");
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
PG_FUNCTION_INFO_V1(pg_bulkload);
Datum		pg_bulkload(PG_FUNCTION_ARGS);

/**
 * @brief Entry point of the user-defined function for pg_bulkload.
 * @return Returns number of loaded tuples.  If the case of errors, -1 will be
 * returned.
 * @todo It may be better to warn when loaded tuple number is different from
 * the input record number.
 */
Datum
pg_bulkload(PG_FUNCTION_ARGS)
{
	ControlInfo	   *ci = NULL;
	char		   *path = text_to_cstring(PG_GETARG_TEXT_PP(0));
	int32			load_cnt;

	/*
	 * Check user previlege (must be the super user and need INSERT previlege
	 */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to use pg_bulkload()")));

	BULKLOAD_PROFILE_PUSH();

	/*
	 * STEP 0: Read control file
	 */

	ereport(NOTICE, (errmsg("BULK LOAD START")));
	ci = OpenControlInfo(path);

	PG_TRY();
	{
		BTSpool		  **spools = NULL;

		/*
		 * STEP 1: Initialization
		 */

		ParserInitialize(ci->ci_parser, ci);
		spools = IndexSpoolBegin(ci->ci_estate->es_result_relation_info);
		elog(DEBUG1, "pg_bulkload: STEP 1 OK");
		BULKLOAD_PROFILE(&prof_init);

		/*
		 * STEP 2: Build heap
		 */

		BULKLOAD_PROFILE_PUSH();
		ci->ci_loader(ci, spools);
		BULKLOAD_PROFILE_POP();

		/* If an error has been found, abort. */
		if (ci->ci_err_cnt)
		{
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
							errmsg("input file error (error count=%d)",
								   ci->ci_err_cnt)));
		}

		elog(DEBUG1, "pg_bulkload: STEP 2 OK");
		BULKLOAD_PROFILE(&prof_heap);

		/*
		 * STEP 3: Merge indexes
		 */

		if (spools != NULL)
		{
			BULKLOAD_PROFILE_PUSH();
			IndexSpoolEnd(spools, ci->ci_estate->es_result_relation_info,
				true, ci->ci_loader == BufferedHeapLoad);
			BULKLOAD_PROFILE_POP();
		}
		elog(DEBUG1, "pg_bulkload: STEP 3 OK");
		BULKLOAD_PROFILE(&prof_index);

		/*
		 * STEP 4: Postprocessing
		 */

		/* Save the number of loaded tuples and release control info. */
		load_cnt = ci->ci_load_cnt;
		CloseControlInfo(ci);
		ci = NULL;

		elog(DEBUG1, "pg_bulkload: STEP 4 OK");

		/* Write end log. */
		ereport(NOTICE,
				(errmsg("BULK LOAD END  (%d records)", load_cnt)));

		BULKLOAD_PROFILE(&prof_term);
	}
	PG_CATCH();
	{
		CloseControlInfo(ci);
		PG_RE_THROW();
	}
	PG_END_TRY();

	BULKLOAD_PROFILE_POP();
	BULKLOAD_PROFILE_PRINT();

	PG_RETURN_INT32(load_cnt);
}

HeapTuple
ReadTuple(ControlInfo *ci, TransactionId xid, TransactionId cid)
{
	HeapTuple	tuple;

	if (ParserReadLine(ci->ci_parser, ci) == 0)
		return NULL;

	tuple = heap_form_tuple(RelationGetDescr(ci->ci_rel),
							ci->ci_values, ci->ci_isnull);
	if (ci->ci_rel->rd_rel->relhasoids)
		HeapTupleSetOid(tuple, GetNewOid(ci->ci_rel));

	tuple->t_data->t_infomask &= ~(HEAP_XACT_MASK);
#if PG_VERSION_NUM >= 80300
	tuple->t_data->t_infomask2 &= ~(HEAP2_XACT_MASK);
#endif
	tuple->t_data->t_infomask |= HEAP_XMAX_INVALID;
	HeapTupleHeaderSetXmin(tuple->t_data, xid);
	HeapTupleHeaderSetCmin(tuple->t_data, cid);
	HeapTupleHeaderSetXmax(tuple->t_data, 0);
#if PG_VERSION_NUM < 80300
	HeapTupleHeaderSetCmax(tuple->t_data, 0);
#endif
	tuple->t_tableOid = RelationGetRelid(ci->ci_rel);

	return tuple;
}

#ifdef WIN32
#include "../../src/backend/utils/hash/pg_crc.c"
#endif
