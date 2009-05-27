/*
 * writer: lib/writer.c
 *
 *	  Copyright(C) 2009, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

#include "postgres.h"

#include "access/tuptoaster.h"
#include "catalog/catalog.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "utils/acl.h"
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

static void VerifyTarget(Relation rel);

/* ========================================================================
 * Implementation
 * ========================================================================*/

void
WriterOpen(Writer *wt, Oid relid)
{
	memset(wt, 0, sizeof(Writer));
	wt->rel = heap_open(relid, AccessExclusiveLock);
	VerifyTarget(wt->rel);

	wt->relinfo = makeNode(ResultRelInfo);
	wt->relinfo->ri_RangeTableIndex = 1;	/* dummy */
	wt->relinfo->ri_RelationDesc = wt->rel;
	wt->relinfo->ri_TrigDesc = NULL;	/* TRIGGER is not supported */
	wt->relinfo->ri_TrigInstrument = NULL;

	ExecOpenIndices(wt->relinfo);

	wt->estate = CreateExecutorState();
	wt->estate->es_num_result_relations = 1;
	wt->estate->es_result_relations = wt->relinfo;
	wt->estate->es_result_relation_info = wt->relinfo;

	wt->slot = MakeSingleTupleTableSlot(RelationGetDescr(wt->rel));

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
	bool			use_wal = true;

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
		IndexSpoolEnd(wt->spools, wt->relinfo, true, use_wal, wt->on_duplicate);
		BULKLOAD_PROFILE_POP();
	}

	/* Terminate spooler. */
	ExecDropSingleTupleTableSlot(wt->slot);
	if (wt->estate->es_result_relation_info)
		ExecCloseIndices(wt->estate->es_result_relation_info);
	FreeExecutorState(wt->estate);

	/* Close heap relation. */
	heap_close(wt->rel, AccessExclusiveLock);
}

/*
 * Check iff the write target is ok
 */
static void
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
