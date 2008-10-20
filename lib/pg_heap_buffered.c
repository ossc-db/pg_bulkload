/*
 * pg_heap_buffered: lib/pg_heap_buffered.c
 *
 *	  Copyright(C) 2007-2008 NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

/**
 * @file
 * @brief Direct heap writer
 */
#include "postgres.h"

#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

#include "access/xact.h"
#include "executor/executor.h"
#include "miscadmin.h"

#include "pg_bulkload.h"
#include "pg_btree.h"
#include "pg_controlinfo.h"

/* ========================================================================
 * Implementation
 * ========================================================================*/

/**
 * @brief Store tuples into the heap using shared buffers.
 * @return void
 */
void
BufferedHeapLoad(ControlInfo *ci, BTSpool **spools)
{
	TransactionId	xid;
	CommandId		cid;
	bool			use_wal = true;
	bool			use_fsm = true;
	MemoryContext	org_ctx;

	/* Obtain transaction ID and command ID. */
	xid = GetCurrentTransactionId();
	cid = GetCurrentCommandId(true);

	/* Switch into its memory context */
	org_ctx = MemoryContextSwitchTo(GetPerTupleMemoryContext(ci->ci_estate));

	/*
	 * Loop for each input file record.
	 * Allow errors upto specified number, so accumulate errors for each record.
	 */
	while (ci->ci_status == 0 && ci->ci_load_cnt < ci->ci_limit)
	{
		PG_TRY();
		{
			HeapTuple	tuple;

			CHECK_FOR_INTERRUPTS();

			/* Reset the per-tuple exprcontext */
			ResetPerTupleExprContext(ci->ci_estate);

			/*
			 * Read record data until EOF is encountered.	Because PG_TRY()
			 * is implemented using "do{} while (...);", goto statement is
			 * used to get out from the loop.	Goto target assumes org_ctx
			 * and so memory context is changed before the goto statement
			 * here.
			 */
			if ((tuple = ReadTuple(ci, xid, cid)) == NULL)
			{
				ci->ci_status = 1;
				goto record_proc_end;
			}

			/* Insert the heap tuple and index entries. */
			heap_insert(ci->ci_estate->es_result_relation_info->ri_RelationDesc,
				tuple, cid, use_wal, use_fsm);

			/*
			 * Loading is complete when a tuple is added to a block buffer.
			 */
			ci->ci_load_cnt++;

			/*
			 * Accumulate info from created heap tuple to the index pool.
			 */
			ExecStoreTuple(tuple, ci->ci_slot, InvalidBuffer, false);
			IndexSpoolInsert(spools, ci->ci_slot, &(tuple->t_self), ci->ci_estate, true);

		  record_proc_end:
			;
		}
		PG_CATCH();
		{
			ErrorData	   *errdata;
			MemoryContext	ecxt;
			char			tplerrmsg[1024];

			ecxt = MemoryContextSwitchTo(GetPerTupleMemoryContext(ci->ci_estate));
			errdata = CopyErrorData();

			/* Clean up files when query is canceled. */
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
			strncpy(tplerrmsg, errdata->message, 1023);
			FlushErrorState();
			FreeErrorData(errdata);

			ereport(WARNING, (errmsg("BULK LOAD ERROR (row=%d, col=%d) %s",
									 ci->ci_read_cnt, ci->ci_field,
									 tplerrmsg)));

			/*
			 * Terminate if MAX_ERR_CNT has been reached.
			 */
			if (ci->ci_max_err_cnt > 0 && ci->ci_err_cnt >= ci->ci_max_err_cnt)
				ci->ci_status = 1;
		}
		PG_END_TRY();
	}

	ResetPerTupleExprContext(ci->ci_estate);
	MemoryContextSwitchTo(org_ctx);
}
