/*
 * writer: lib/writer.c
 *
 *	  Copyright(C) 2009, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

#include "postgres.h"

#include "access/tuptoaster.h"
#include "catalog/catalog.h"
#include "executor/executor.h"
#include "utils/memutils.h"

#include "writer.h"
#include "pg_btree.h"
#include "pg_profile.h"

#if PG_VERSION_NUM < 80300
#define toast_insert_or_update(rel, newtup, oldtup, options) \
	toast_insert_or_update((rel), (newtup), (oldtup))
#elif PG_VERSION_NUM < 80400
#define toast_insert_or_update(rel, newtup, oldtup, options) \
	toast_insert_or_update((rel), (newtup), (oldtup), true, true)
#endif

/* ========================================================================
 * Implementation
 * ========================================================================*/

void
WriterOpen(Writer *wt, Relation rel)
{
	memset(wt, 0, sizeof(Writer));
	wt->rel = rel;

	wt->relinfo = makeNode(ResultRelInfo);
	wt->relinfo->ri_RangeTableIndex = 1;	/* dummy */
	wt->relinfo->ri_RelationDesc = rel;
	wt->relinfo->ri_TrigDesc = NULL;	/* TRIGGER is not supported */
	wt->relinfo->ri_TrigInstrument = NULL;

	ExecOpenIndices(wt->relinfo);

	wt->estate = CreateExecutorState();
	wt->estate->es_num_result_relations = 1;
	wt->estate->es_result_relations = wt->relinfo;
	wt->estate->es_result_relation_info = wt->relinfo;

	wt->slot = MakeSingleTupleTableSlot(RelationGetDescr(rel));

	wt->spools = IndexSpoolBegin(wt->relinfo);
}

void
WriterInsert(Writer *wt, HeapTuple tuple)
{
	Assert(wt->loader != NULL);

	/* Compress the tuple data if needed. */
	if (tuple->t_len > TOAST_TUPLE_THRESHOLD)
		tuple = toast_insert_or_update(wt->rel, tuple, NULL, 0);
	BULKLOAD_PROFILE(&prof_heap_toast);

	/* Insert the heap tuple and index entries. */
	LoaderInsert(wt->loader, wt->rel, tuple);
	BULKLOAD_PROFILE(&prof_heap_table);

	/* Spool keys in the tuple */
	ExecStoreTuple(tuple, wt->slot, InvalidBuffer, false);
	IndexSpoolInsert(wt->spools, wt->slot, &(tuple->t_self), wt->estate, true);
	BULKLOAD_PROFILE(&prof_heap_index);

	ResetPerTupleExprContext(wt->estate);
	wt->count += 1;
}

void
WriterClose(Writer *wt)
{
	bool		use_wal = true;

	/* Terminate loader. Be sure to set loader to NULL. */
	if (wt->loader != NULL)
	{
		use_wal = wt->loader->use_wal;
		LoaderClose(wt->loader);
		wt->loader = NULL;
	}

	/* Merge indexes */
	if (wt->spools != NULL)
	{
		BULKLOAD_PROFILE_PUSH();
		IndexSpoolEnd(wt->spools, wt->relinfo, true, use_wal);
		BULKLOAD_PROFILE_POP();
	}

	/* Terminate spooler. */
	ExecDropSingleTupleTableSlot(wt->slot);
	if (wt->estate->es_result_relation_info)
		ExecCloseIndices(wt->estate->es_result_relation_info);
	FreeExecutorState(wt->estate);
}
