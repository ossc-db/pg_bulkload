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
#include "access/tuptoaster.h"
#include "access/xact.h"
#include "executor/executor.h"
#include "catalog/catalog.h"
#include "miscadmin.h"
#include "utils/builtins.h"

#include "pg_bulkload.h"
#include "pg_btree.h"
#include "pg_controlinfo.h"
#include "pg_profile.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(pg_bulkload);
Datum		pg_bulkload(PG_FUNCTION_ARGS);

static HeapTuple ReadTuple(ControlInfo *ci, TransactionId xid, CommandId cid);

#if PG_VERSION_NUM < 80300
#define toast_insert_or_update(rel, newtup, oldtup, options) \
	toast_insert_or_update((rel), (newtup), (oldtup))
#elif PG_VERSION_NUM < 80400
#define toast_insert_or_update(rel, newtup, oldtup, options) \
	toast_insert_or_update((rel), (newtup), (oldtup), true, true)
#endif

#ifdef ENABLE_BULKLOAD_PROFILE
static instr_time prof_init;
static instr_time prof_heap;
static instr_time prof_index;
static instr_time prof_term;

instr_time prof_heap_read;
instr_time prof_heap_toast;
instr_time prof_heap_table;
instr_time prof_heap_index;

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

/**
 * @brief Entry point of the user-defined function for pg_bulkload.
 * @return Returns number of loaded tuples.  If the case of errors, -1 will be
 * returned.
 */
Datum
pg_bulkload(PG_FUNCTION_ARGS)
{
	ControlInfo	   *ci = NULL;
	char		   *path = text_to_cstring(PG_GETARG_TEXT_PP(0));
	ResultRelInfo  *relinfo;
	EState		   *estate;
	TupleTableSlot *slot;
	int64			count;

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

	/* create estate */
	relinfo = makeNode(ResultRelInfo);
	relinfo->ri_RangeTableIndex = 1;	/* dummy */
	relinfo->ri_RelationDesc = ci->ci_rel;
	relinfo->ri_TrigDesc = NULL;	/* TRIGGER is not supported */
	relinfo->ri_TrigInstrument = NULL;
	ExecOpenIndices(relinfo);
	estate = CreateExecutorState();
	estate->es_num_result_relations = 1;
	estate->es_result_relations = relinfo;
	estate->es_result_relation_info = relinfo;

	PG_TRY();
	{
		BTSpool		  **spools;
		bool			use_wal;
		TransactionId	xid;
		CommandId		cid;
		MemoryContext	ctx;

		/*
		 * STEP 1: Initialization
		 */

		/* Obtain transaction ID and command ID. */
		xid = GetCurrentTransactionId();
		cid = GetCurrentCommandId(true);

		ParserInit(ci->ci_parser, ci);
		spools = IndexSpoolBegin(relinfo);
		slot = MakeSingleTupleTableSlot(RelationGetDescr(ci->ci_rel));

		elog(DEBUG1, "pg_bulkload: STEP 1 OK");
		BULKLOAD_PROFILE(&prof_init);

		/*
		 * STEP 2: Build heap
		 */

		BULKLOAD_PROFILE_PUSH();

		LoaderInit(ci->ci_loader, ci->ci_rel);

		/* Switch into its memory context */
		ctx = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));

		/* Loop for each input file record. */
		for (count = 0; count < ci->ci_limit; count++)
		{
			HeapTuple	tuple;

			CHECK_FOR_INTERRUPTS();
			ResetPerTupleExprContext(estate);

			/* Read next tuple */
			if ((tuple = ReadTuple(ci, xid, cid)) == NULL)
				break;
			BULKLOAD_PROFILE(&prof_heap_read);

			/* Compress the tuple data if needed. */
			if (tuple->t_len > TOAST_TUPLE_THRESHOLD)
				tuple = toast_insert_or_update(ci->ci_rel, tuple, NULL, 0);
			BULKLOAD_PROFILE(&prof_heap_toast);

			/* Insert the heap tuple and index entries. */
			LoaderInsert(ci->ci_loader, ci->ci_rel, tuple);
			BULKLOAD_PROFILE(&prof_heap_table);

			/* Spool keys in the tuple */
			ExecStoreTuple(tuple, slot, InvalidBuffer, false);
			IndexSpoolInsert(spools, slot, &(tuple->t_self), estate, true);
			BULKLOAD_PROFILE(&prof_heap_index);
		}

		ResetPerTupleExprContext(estate);
		MemoryContextSwitchTo(ctx);
		use_wal = ci->ci_loader->use_wal;

		/* Terminate loader. Be sure to set ci_loader to NULL. */
		LoaderTerm(ci->ci_loader, false);
		ci->ci_loader = NULL;

		/* Terminate parser. Be sure to set ci_parser to NULL. */
		ParserTerm(ci->ci_parser, false);
		ci->ci_parser = NULL;

		BULKLOAD_PROFILE_POP();

		/* If an error has been found, abort. */
		if (ci->ci_err_cnt > 0)
		{
			if (ci->ci_err_cnt > ci->ci_max_err_cnt)
			{
				ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("%d error(s) found in input file",
							ci->ci_err_cnt)));
			}
			else
			{
				ereport(WARNING,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("skip %d error(s) in input file",
							ci->ci_err_cnt)));
			}
		}

		elog(DEBUG1, "pg_bulkload: STEP 2 OK");
		BULKLOAD_PROFILE(&prof_heap);

		/*
		 * STEP 3: Merge indexes
		 */

		if (spools != NULL)
		{
			BULKLOAD_PROFILE_PUSH();
			IndexSpoolEnd(spools, relinfo, true, use_wal);
			BULKLOAD_PROFILE_POP();
		}
		elog(DEBUG1, "pg_bulkload: STEP 3 OK");
		BULKLOAD_PROFILE(&prof_index);

		/*
		 * STEP 4: Postprocessing
		 */

		/* Terminate spooler. */
		ExecDropSingleTupleTableSlot(slot);
		if (estate->es_result_relation_info)
			ExecCloseIndices(estate->es_result_relation_info);
		FreeExecutorState(estate);

		CloseControlInfo(ci, false);
		ci = NULL;

		elog(DEBUG1, "pg_bulkload: STEP 4 OK");

		/* Write end log. */
		ereport(NOTICE,
				(errmsg("BULK LOAD END (" int64_FMT " records)", count)));

		BULKLOAD_PROFILE(&prof_term);
	}
	PG_CATCH();
	{
		CloseControlInfo(ci, true);
		PG_RE_THROW();
	}
	PG_END_TRY();

	BULKLOAD_PROFILE_POP();
	BULKLOAD_PROFILE_PRINT();

	PG_RETURN_INT32(count);
}

/**
 * @brief Read the next tuple from parser.
 * @param ci  [in/out] control info
 * @param xid [in] current transaction id
 * @param cid [in] current command id
 * @return type
 */
static HeapTuple
ReadTuple(ControlInfo *ci, TransactionId xid, CommandId cid)
{
	HeapTuple		tuple;
	MemoryContext	ccxt;
	bool			eof;

	ccxt = CurrentMemoryContext;

	eof = false;
	do
	{
		tuple = NULL;
		ci->ci_parsing_field = 0;

		PG_TRY();
		{
			if (ParserRead(ci->ci_parser, ci))
				tuple = heap_form_tuple(RelationGetDescr(ci->ci_rel),
										ci->ci_values, ci->ci_isnull);
			else
				eof = true;
		}
		PG_CATCH();
		{
			ErrorData	   *errdata;
			MemoryContext	ecxt;
			char		   *message;

			if (ci->ci_parsing_field <= 0)
				PG_RE_THROW();	/* should not ignore */

			ecxt = MemoryContextSwitchTo(ccxt);
			errdata = CopyErrorData();

			/* We cannot ignore query aborts. */
			switch (errdata->sqlerrcode)
			{
				case ERRCODE_ADMIN_SHUTDOWN:
				case ERRCODE_QUERY_CANCELED:
					MemoryContextSwitchTo(ecxt);
					PG_RE_THROW();
					break;
			}

			/* Absorb general errors. */
			ci->ci_err_cnt++;
			if (errdata->message)
				message = pstrdup(errdata->message);
			else
				message = "<no error message>";
			FlushErrorState();
			FreeErrorData(errdata);

			ereport(WARNING,
				(errmsg("BULK LOAD ERROR (row=" int64_FMT ", col=%d) %s",
					ci->ci_read_cnt, ci->ci_parsing_field, message)));

			/* Terminate if MAX_ERR_CNT has been reached. */
			if (ci->ci_err_cnt > ci->ci_max_err_cnt)
				eof = true;
		}
		PG_END_TRY();

	} while (!eof && !tuple);

	if (!tuple)
		return NULL;	/* EOF */

	if (ci->ci_rel->rd_rel->relhasoids)
		HeapTupleSetOid(tuple, GetNewOid(ci->ci_rel));

	/*
	 * FIXME: the following is only needed for direct loaders because
	 * heap_insert does the same things for buffered loaders.
	 */

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

#if PG_VERSION_NUM < 80400

char *
text_to_cstring(const text *t)
{
	/* must cast away the const, unfortunately */
	text	   *tunpacked = pg_detoast_datum_packed((struct varlena *) t);
	int			len = VARSIZE_ANY_EXHDR(tunpacked);
	char	   *result;

	result = (char *) palloc(len + 1);
	memcpy(result, VARDATA_ANY(tunpacked), len);
	result[len] = '\0';

	if (tunpacked != t)
		pfree(tunpacked);
	
	return result;
}

#endif
