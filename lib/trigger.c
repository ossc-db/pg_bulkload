/*
 * pg_bulkload: lib/trigger.c
 *
 *	  Copyright(C) 2007-2008 NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

#include "postgres.h"

#include "access/xact.h"
#include "access/heapam.h"
#include "commands/trigger.h"
#include "funcapi.h"
#include "utils/memutils.h"

#include "pg_bulkload.h"
#include "pg_btree.h"

extern Datum pg_bulkload_trigger_init(PG_FUNCTION_ARGS);
extern Datum pg_bulkload_trigger_main(PG_FUNCTION_ARGS);
extern Datum pg_bulkload_trigger_term(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(pg_bulkload_trigger_init);
PG_FUNCTION_INFO_V1(pg_bulkload_trigger_main);
PG_FUNCTION_INFO_V1(pg_bulkload_trigger_term);

typedef struct Table
{
	ResultRelInfo  *relinfo;	/* Target relation */
	BTSpool		  **spools;		/* Array of index spooler */
	int				n_ins_tup;	/* # of inserted rows */
	bool			use_wal;	/* Use WAL */
	bool			use_fsm;	/* Use FSM */
} Table;

typedef struct Loader
{
	EState		   *estate;
	TupleTableSlot *slot;
	Table			table;		/* Table */
	CommandId		cid;		/* CommandId for inserted tuples */
	bool			trigger;	/* Enable trigger? */
	bool			reindex;	/* Rebuild index? */
} Loader;

static Loader TriggerLoader;

static void LoaderInit(Loader *loader, Relation rel, bool trigger, bool reindex);
static void LoaderTerm(Loader *loader);
static void LoaderInsert(Loader *loader, HeapTuple tuple);
static void TableOpen(Table *table, Relation rel, bool reindex);
static void TableClose(Table *table, EState *estate, bool reindex);

#define TableGetDesc(table) \
	((table)->relinfo->ri_RelationDesc)

/*
 * pg_bulkload_trigger_init - 
 */
Datum
pg_bulkload_trigger_init(PG_FUNCTION_ARGS)
{
	TriggerData	   *data = (TriggerData *) fcinfo->context;
	Relation		rel;
	bool			trigger;
	bool			reindex;

	if (!CALLED_AS_TRIGGER(fcinfo))
		elog(ERROR, "not fired by trigger manager");
	if (!TRIGGER_FIRED_BY_INSERT(data->tg_event))
		elog(ERROR, "must be fired on INSERT events");
	if (TRIGGER_FIRED_AFTER(data->tg_event))
		elog(ERROR, "must be fired on BEFORE events");
	if (TRIGGER_FIRED_FOR_ROW(data->tg_event))
		elog(ERROR, "must be fired on STATEMENT events");

	rel = data->tg_relation;
	trigger = true;
	reindex = false;	/* true is unsupported now */

	elog(DEBUG1, "pg_bulkload: trigger init (reindex=%d)", reindex);

	LoaderInit(&TriggerLoader, rel, trigger, reindex);

	PG_RETURN_POINTER(NULL);
}

/*
 * pg_bulkload_trigger_term - 
 */
Datum
pg_bulkload_trigger_term(PG_FUNCTION_ARGS)
{
	TriggerData	   *data = (TriggerData *) fcinfo->context;

	if (!CALLED_AS_TRIGGER(fcinfo))
		elog(ERROR, "not fired by trigger manager");
	if (!TRIGGER_FIRED_BY_INSERT(data->tg_event))
		elog(ERROR, "must be fired on INSERT events");
	if (TRIGGER_FIRED_BEFORE(data->tg_event))
		elog(ERROR, "must be fired on AFTER events");
	if (TRIGGER_FIRED_FOR_ROW(data->tg_event))
		elog(ERROR, "must be fired on STATEMENT events");

	LoaderTerm(&TriggerLoader);

	elog(DEBUG1, "pg_bulkload: trigger term");

	PG_RETURN_POINTER(NULL);
}

/*
 * pg_bulkload_trigger_main - 
 */
Datum
pg_bulkload_trigger_main(PG_FUNCTION_ARGS)
{
	TriggerData	   *data = (TriggerData *) fcinfo->context;

	if (!CALLED_AS_TRIGGER(fcinfo))
		elog(ERROR, "not fired by trigger manager");
	if (!TRIGGER_FIRED_BY_INSERT(data->tg_event))
		elog(ERROR, "must be fired on INSERT events");
	if (TRIGGER_FIRED_AFTER(data->tg_event))
		elog(ERROR, "must be fired on BEFORE events");
	if (TRIGGER_FIRED_FOR_STATEMENT(data->tg_event))
		elog(ERROR, "must be fired on ROW events");

	LoaderInsert(&TriggerLoader, data->tg_trigtuple);

	PG_RETURN_POINTER(NULL);
}

/*
 * LoaderInit - 
 */
static void
LoaderInit(Loader *loader, Relation rel, bool trigger, bool reindex)
{
	MemoryContext	cxt;

	cxt = MemoryContextSwitchTo(PortalContext);

	loader->estate = CreateExecutorState();
	MemoryContextSwitchTo(loader->estate->es_query_cxt);

	TableOpen(&loader->table, rel, reindex);

	loader->estate->es_num_result_relations = 1;
	loader->estate->es_result_relations = loader->table.relinfo;
	loader->estate->es_result_relation_info = loader->table.relinfo;

	loader->slot = MakeSingleTupleTableSlot(RelationGetDescr(rel));
	loader->cid = GetCurrentCommandId(true);
	loader->trigger = trigger;
	loader->reindex = reindex;

	MemoryContextSwitchTo(cxt);
}

/*
 * LoaderTerm - 
 */
static void
LoaderTerm(Loader *loader)
{
	TableClose(&loader->table, loader->estate, loader->reindex);
	ExecDropSingleTupleTableSlot(loader->slot);
	FreeExecutorState(loader->estate);
}

/*
 * LoaderInsert - 
 */
static void
LoaderInsert(Loader *loader, HeapTuple tuple)
{
	Table	   *target = &loader->table;

	ResetPerTupleExprContext(loader->estate);
	ExecStoreTuple(tuple, loader->slot, InvalidBuffer, false);

	/* Check the constraints of the tuple */
	if (TableGetDesc(target)->rd_att->constr)
		ExecConstraints(target->relinfo, loader->slot, loader->estate);

#if 0
	elog(DEBUG1, "pg_bulkload: trigger insert \"%s\"",
		RelationGetRelationName(TableGetDesc(target)));
#endif

	/* Insert the heap tuple and index entries. */
	heap_insert(TableGetDesc(target), tuple, loader->cid,
				target->use_wal, target->use_fsm);
	IndexSpoolInsert(target->spools, loader->slot, &(tuple->t_self),
					 loader->estate, loader->reindex);

	/*
	 * AFTER INSERT FOR EACH ROW TRIGGER
	 * We need to call by ourselves because we cancel original INSERT.
	 */
	if (loader->trigger)
		ExecARInsertTriggers(loader->estate, target->relinfo, tuple);

	target->n_ins_tup++;
}

/*
 * TableOpen - Open heap relation.
 */
void
TableOpen(Table *table, Relation rel, bool reindex)
{
	ResultRelInfo	   *relinfo;
	bool				isNew;

	Assert(table != NULL);

#if PG_VERSION_NUM >= 80300
	isNew = (rel->rd_createSubid != InvalidSubTransactionId ||
		rel->rd_newRelfilenodeSubid != InvalidSubTransactionId);
#else
	isNew = false;
#endif

	relinfo = makeNode(ResultRelInfo);
	relinfo->ri_RangeTableIndex = 1;	/* dummy */
	relinfo->ri_RelationDesc = rel;
	relinfo->ri_TrigDesc = CopyTriggerDesc(rel->trigdesc);
	if (relinfo->ri_TrigDesc)
		relinfo->ri_TrigFunctions = palloc0(
			relinfo->ri_TrigDesc->numtriggers * sizeof(FmgrInfo));
	relinfo->ri_TrigInstrument = NULL;

	table->relinfo = relinfo;
	table->spools = NULL;
	table->n_ins_tup = 0;
	table->use_wal = (!isNew || XLogArchivingActive());
	table->use_fsm = (!isNew);

	ExecOpenIndices(table->relinfo);
	if (reindex && table->relinfo->ri_NumIndices > 0)
		table->spools = IndexSpoolBegin(table->relinfo);

	elog(DEBUG1, "pg_bulkload: open \"%s\" wal=%d, fsm=%d",
		RelationGetRelationName(rel), table->use_wal, table->use_fsm);
}

/*
 * TableClose - Flush index spooler and relations.
 */
void
TableClose(Table *table, EState *estate, bool reindex)
{
	if (table == NULL || table->relinfo == NULL)
		return;

	elog(DEBUG1, "pg_bulkload: close \"%s\" n_ins_tup=%d",
		RelationGetRelationName(TableGetDesc(table)), table->n_ins_tup);

	/* Flush index spoolers and close indexes. */
	if (table->spools != NULL)
		IndexSpoolEnd(table->spools, table->relinfo, reindex, table->use_wal);
	ExecCloseIndices(table->relinfo);

	/* Flush table buffers and close it. */
	if (table->n_ins_tup > 0 && !table->use_wal)
		heap_sync(TableGetDesc(table));

	/* free memory */
	if (table->relinfo->ri_TrigFunctions)
		pfree(table->relinfo->ri_TrigFunctions);
	pfree(table->relinfo);
}
