/*
 * pg_bulkload: lib/pg_btree.c
 *
 *	  Copyright (c) 2007-2010, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

/**
 * @file
 * @brief implementation of B-Tree index processing module
 */
#include "pg_bulkload.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/nbtree.h"
#include "access/xact.h"
#include "catalog/index.h"
#include "executor/executor.h"
#include "storage/fd.h"
#include "storage/lmgr.h"
#include "storage/smgr.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/tqual.h"

#if PG_VERSION_NUM >= 80400
#include "utils/snapmgr.h"
#endif

#include "logger.h"

static BTSpool *unused_bt_spoolinit(Relation, bool, bool);
static void unused_bt_spooldestroy(BTSpool *);
static void unused_bt_spool(IndexTuple, BTSpool *);
static void unused_bt_leafbuild(BTSpool *, BTSpool *);

#define _bt_spoolinit		unused_bt_spoolinit
#define _bt_spooldestroy	unused_bt_spooldestroy
#define _bt_spool			unused_bt_spool
#define _bt_leafbuild		unused_bt_leafbuild

#if PG_VERSION_NUM >= 90200
#error unsupported PostgreSQL version
#elif PG_VERSION_NUM >= 90100
#include "nbtree/nbtsort-9.1.c"
#elif PG_VERSION_NUM >= 90000
#include "nbtree/nbtsort-9.0.c"
#elif PG_VERSION_NUM >= 80400
#include "nbtree/nbtsort-8.4.c"
#elif PG_VERSION_NUM >= 80300
#include "nbtree/nbtsort-8.3.c"
#elif PG_VERSION_NUM >= 80200
#include "nbtree/nbtsort-8.2.c"
#else
#error unsupported PostgreSQL version
#endif

#undef _bt_spoolinit
#undef _bt_spooldestroy
#undef _bt_spool
#undef _bt_leafbuild

#include "pg_btree.h"
#include "pg_profile.h"
#include "pgut/pgut-be.h"

/**
 * @brief Reader for existing B-Tree index
 *
 * The 'page' field should be allocate with palloc(BLCKSZ) to
 * avoid bus error.
 */
typedef struct BTReader
{
	SMgrRelationData	smgr;	/**< Index file */
	BlockNumber			blkno;	/**< Current block number */
	OffsetNumber		offnum;	/**< Current item offset */
	char			   *page;	/**< Cached page */
} BTReader;

static BTSpool **IndexSpoolBegin(ResultRelInfo *relinfo, bool enforceUnique);
static void IndexSpoolEnd(Spooler *self , bool reindex);
static void IndexSpoolInsert(BTSpool **spools, TupleTableSlot *slot, ItemPointer tupleid, EState *estate, bool reindex);

static IndexTuple BTSpoolGetNextItem(BTSpool *spool, IndexTuple itup, bool *should_free);
static bool BTReaderInit(BTReader *reader, Relation rel);
static void BTReaderTerm(BTReader *reader);
static void BTReaderReadPage(BTReader *reader, BlockNumber blkno);
static IndexTuple BTReaderGetNextItem(BTReader *reader);

static void _bt_mergebuild(Spooler *self, BTSpool *btspool);
static void _bt_mergeload(Spooler *self, BTWriteState *wstate, BTSpool *btspool,
						  BTReader *btspool2, Relation heapRel);
static int compare_indextuple(const IndexTuple itup1, const IndexTuple itup2,
	ScanKey entry, int keysz, TupleDesc tupdes, bool *hasnull);
static bool heap_is_visible(Relation heapRel, ItemPointer htid);
static void report_unique_violation(Relation rel, IndexTuple itup);
static void remove_duplicate(Spooler *self, Relation heap, IndexTuple itup, const char *relname);


void
SpoolerOpen(Spooler *self,
			Relation rel,
			bool use_wal,
			const WriterOptions *options)
{
	memset(self, 0, sizeof(Spooler));

	self->on_duplicate = options->on_duplicate;
	self->use_wal = use_wal;
	self->max_dup_errors = options->max_dup_errors;
	self->dup_old = 0;
	self->dup_new = 0;
	self->dup_badfile = pstrdup(options->dup_badfile);
	self->dup_fp = NULL;

	self->relinfo = makeNode(ResultRelInfo);
	self->relinfo->ri_RangeTableIndex = 1;	/* dummy */
	self->relinfo->ri_RelationDesc = rel;
	self->relinfo->ri_TrigDesc = NULL;	/* TRIGGER is not supported */
	self->relinfo->ri_TrigInstrument = NULL;

	ExecOpenIndices(self->relinfo);

	self->estate = CreateExecutorState();
	self->estate->es_num_result_relations = 1;
	self->estate->es_result_relations = self->relinfo;
	self->estate->es_result_relation_info = self->relinfo;

	self->slot = MakeSingleTupleTableSlot(RelationGetDescr(rel));

	self->spools = IndexSpoolBegin(self->relinfo,
								   options->max_dup_errors == 0);
}

void
SpoolerClose(Spooler *self)
{
	/* Merge indexes */
	if (self->spools != NULL)
		IndexSpoolEnd(self, true);

	/* Terminate spooler. */
	ExecDropSingleTupleTableSlot(self->slot);
	if (self->estate->es_result_relation_info)
		ExecCloseIndices(self->estate->es_result_relation_info);
	FreeExecutorState(self->estate);

	/* Close and release members. */
	if (self->dup_fp != NULL && FreeFile(self->dup_fp) < 0)
		ereport(WARNING,
				(errcode_for_file_access(),
				 errmsg("could not close duplicate bad file \"%s\": %m",
						self->dup_badfile)));
	if (self->dup_badfile != NULL)
		pfree(self->dup_badfile);
}

void
SpoolerInsert(Spooler *self, HeapTuple tuple)
{
	/* Spool keys in the tuple */
	ExecStoreTuple(tuple, self->slot, InvalidBuffer, false);
	IndexSpoolInsert(self->spools, self->slot, &(tuple->t_self), self->estate, true);
	BULKLOAD_PROFILE(&prof_writer_index);
}

/*
 * IndexSpoolBegin - Initialize spools.
 */
static BTSpool **
IndexSpoolBegin(ResultRelInfo *relinfo, bool enforceUnique)
{
	int				i;
	int				numIndices = relinfo->ri_NumIndices;
	RelationPtr		indices = relinfo->ri_IndexRelationDescs;
	BTSpool		  **spools;

	spools = palloc(numIndices * sizeof(BTSpool *));
	for (i = 0; i < numIndices; i++)
	{
		/* TODO: Support hash, gist and gin. */
		if (indices[i]->rd_index->indisvalid && 
			indices[i]->rd_rel->relam == BTREE_AM_OID)
		{
			elog(DEBUG1, "pg_bulkload: spool \"%s\"",
				RelationGetRelationName(indices[i]));
			spools[i] = _bt_spoolinit(indices[i],
					enforceUnique ? indices[i]->rd_index->indisunique: false,
					false);
			spools[i]->isunique = indices[i]->rd_index->indisunique;
		}
		else
			spools[i] = NULL;
	}

	return spools;
}

/*
 * IndexSpoolEnd - Flush and delete spools.
 */
void
IndexSpoolEnd(Spooler *self, bool reindex)
{
	BTSpool **spools = self->spools;
	int				i;
	RelationPtr		indices = self->relinfo->ri_IndexRelationDescs;

	Assert(spools != NULL);
	Assert(self->relinfo != NULL);

	for (i = 0; i < self->relinfo->ri_NumIndices; i++)
	{
		if (spools[i] != NULL)
		{
			_bt_mergebuild(self, spools[i]);
			_bt_spooldestroy(spools[i]);
		}
		else if (reindex)
		{
			Oid		indexOid = RelationGetRelid(indices[i]);

			/* Close index before reindex to pass CheckTableNotInUse. */
			relation_close(indices[i], NoLock);
			indices[i] = NULL;
			reindex_index(indexOid, false);
			CommandCounterIncrement();
			BULKLOAD_PROFILE(&prof_reindex);
		}
		else
		{
			/* We already done using index_insert. */
		}
	}

	pfree(spools);
}

/*
 * IndexSpoolInsert - 
 *
 *	Copy from ExecInsertIndexTuples.
 */
static void
IndexSpoolInsert(BTSpool **spools, TupleTableSlot *slot, ItemPointer tupleid, EState *estate, bool reindex)
{
	ResultRelInfo  *relinfo;
	int				i;
	int				numIndices;
	RelationPtr		indices;
	IndexInfo	  **indexInfoArray;
	Relation		heapRelation;
	ExprContext    *econtext;

	/*
	 * Get information from the result relation relinfo structure.
	 */
	relinfo = estate->es_result_relation_info;
	numIndices = relinfo->ri_NumIndices;
	indices = relinfo->ri_IndexRelationDescs;
	indexInfoArray = relinfo->ri_IndexRelationInfo;
	heapRelation = relinfo->ri_RelationDesc;

	/*
	 * We will use the EState's per-tuple context for evaluating predicates
	 * and index expressions (creating it if it's not already there).
	 */
	econtext = GetPerTupleExprContext(estate);

	/* Arrange for econtext's scan tuple to be the tuple under test */
	econtext->ecxt_scantuple = slot;

	for (i = 0; i < numIndices; i++)
	{
		Datum		values[INDEX_MAX_KEYS];
		bool		isnull[INDEX_MAX_KEYS];
		IndexInfo  *indexInfo;

		if (indices[i] == NULL)
			continue;

		/* Skip non-btree indexes on reindex mode. */
		if (reindex && spools != NULL && spools[i] == NULL)
			continue;

		indexInfo = indexInfoArray[i];

#if PG_VERSION_NUM >= 80300
		/* If the index is marked as read-only, ignore it */
		if (!indexInfo->ii_ReadyForInserts)
			continue;
#endif

		/* Check for partial index */
		if (indexInfo->ii_Predicate != NIL)
		{
			List		   *predicate;

			/*
			 * If predicate state not set up yet, create it (in the estate's
			 * per-query context)
			 */
			predicate = indexInfo->ii_PredicateState;
			if (predicate == NIL)
			{
				predicate = (List *) ExecPrepareExpr((Expr *) indexInfo->ii_Predicate, estate);
				indexInfo->ii_PredicateState = predicate;
			}

			/* Skip this index-update if the predicate isn'loader satisfied */
			if (!ExecQual(predicate, econtext, false))
				continue;
		}

		FormIndexDatum(indexInfo, slot, estate, values, isnull);

		/*
		 * Insert or spool the tuple.
		 */
		if (spools != NULL && spools[i] != NULL)
		{
			IndexTuple itup = index_form_tuple(RelationGetDescr(indices[i]), values, isnull);
			itup->t_tid = *tupleid;
			_bt_spool(itup, spools[i]);
			pfree(itup);
		}
		else
		{
			/* Insert one by one */
			index_insert(indices[i], values, isnull, tupleid, heapRelation, indices[i]->rd_index->indisunique);
		}
	}
}


static void
_bt_mergebuild(Spooler *self, BTSpool *btspool)
{
	Relation heapRel = self->relinfo->ri_RelationDesc;
	BTWriteState	wstate;
	BTReader		reader;
	bool			merge;

	Assert(btspool->index->rd_index->indisvalid);

	tuplesort_performsort(btspool->sortstate);

	wstate.index = btspool->index;

	/*
	 * We need to log index creation in WAL iff WAL archiving is enabled AND
	 * it's not a temp index.
	 */
	wstate.btws_use_wal = self->use_wal &&
		XLogArchivingActive() && !wstate.index->rd_istemp;

	/* reserve the metapage */
	wstate.btws_pages_alloced = BTREE_METAPAGE + 1;
	wstate.btws_pages_written = 0;
	wstate.btws_zeropage = NULL;	/* until needed */

	/*
	 * Flush dirty buffers so that we will read the index files directly
	 * in order to get pre-existing data. We must acquire AccessExclusiveLock
	 * for the target table for calling FlushRelationBuffer().
	 */
	LockRelation(wstate.index, AccessExclusiveLock);
	FlushRelationBuffers(wstate.index);
	BULKLOAD_PROFILE(&prof_flush);

	merge = BTReaderInit(&reader, wstate.index);

	elog(DEBUG1, "pg_bulkload: build \"%s\" %s merge (%s wal)",
		RelationGetRelationName(wstate.index),
		merge ? "with" : "without",
		wstate.btws_use_wal ? "with" : "without");

	if (merge || (btspool->isunique && self->max_dup_errors > 0))
	{
		/* Assign a new file node and merge two streams into it. */
		RelationSetNewRelfilenode(wstate.index, RecentXmin);
		BULKLOAD_PROFILE_PUSH();
		_bt_mergeload(self, &wstate, btspool, &reader, heapRel);
		BULKLOAD_PROFILE_POP();
		BULKLOAD_PROFILE(&prof_merge);
	}
	else
	{
		/* Fast path for newly created index. */
		_bt_load(&wstate, btspool, NULL);
		BULKLOAD_PROFILE(&prof_index);
	}

	BTReaderTerm(&reader);
}

/*
 * _bt_mergeload - Merge two streams of index tuples into new index files.
 */
static void
_bt_mergeload(Spooler *self, BTWriteState *wstate, BTSpool *btspool, BTReader *btspool2, Relation heapRel)
{
	BTPageState	   *state = NULL;
	IndexTuple		itup,
					itup2;
	bool			should_free = false;
	TupleDesc		tupdes = RelationGetDescr(wstate->index);
	int				keysz = RelationGetNumberOfAttributes(wstate->index);
	ScanKey			indexScanKey;
	ON_DUPLICATE	on_duplicate = self->on_duplicate;

	Assert(btspool != NULL);
	Assert(btspool2 != NULL);

	/* the preparation of merge */
	itup = BTSpoolGetNextItem(btspool, NULL, &should_free);
	itup2 = BTReaderGetNextItem(btspool2);
	indexScanKey = _bt_mkscankey_nodata(wstate->index);

	for (;;)
	{
		bool	check_unique = false;
		bool	load1 = true;		/* load BTSpool next ? */

		if (itup2 == NULL)
		{
			if (itup == NULL)
				break;
		}
		else if (itup != NULL)
		{
			int		i;

			check_unique = btspool->isunique;

			for (i = 1; i <= keysz; i++)
			{
				ScanKey		entry;
				Datum		attrDatum1,
							attrDatum2;
				bool		isNull1,
							isNull2;
				int32		compare;

				entry = indexScanKey + i - 1;
				attrDatum1 = index_getattr(itup, i, tupdes, &isNull1);
				attrDatum2 = index_getattr(itup2, i, tupdes, &isNull2);
				if (isNull1)
				{
					check_unique = false;
					if (isNull2)
						compare = 0;		/* NULL "=" NULL */
					else if (entry->sk_flags & SK_BT_NULLS_FIRST)
						compare = -1;		/* NULL "<" NOT_NULL */
					else
						compare = 1;		/* NULL ">" NOT_NULL */
				}
				else if (isNull2)
				{
					check_unique = false;
					if (entry->sk_flags & SK_BT_NULLS_FIRST)
						compare = 1;		/* NOT_NULL ">" NULL */
					else
						compare = -1;		/* NOT_NULL "<" NULL */
				}
				else
				{
					compare = DatumGetInt32(FunctionCall2(&entry->sk_func,
														  attrDatum1,
														  attrDatum2));

					if (entry->sk_flags & SK_BT_DESC)
						compare = -compare;
				}
				if (compare > 0)
				{
					load1 = false;
					check_unique = false;
					break;
				}
				else if (compare < 0)
				{
					check_unique = false;
					break;
				}
			}
		}
		else
			load1 = false;

		if (check_unique)
		{
			Assert(load1);

			/* The tuple pointed by the old index should not be visible. */
			if (heap_is_visible(heapRel, &itup2->t_tid))
			{
				if (self->dup_old + self->dup_new >= self->max_dup_errors)
				{
					ereport(WARNING,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Maximum duplicate error count exceeded")));
					report_unique_violation(wstate->index, itup);
				}

				if (on_duplicate == ON_DUPLICATE_KEEP_NEW)
				{
					self->dup_old++;
					remove_duplicate(self, heapRel, itup2,
						RelationGetRelationName(wstate->index));
					itup2 = BTReaderGetNextItem(btspool2);
				}
				else
				{
					self->dup_new++;
					remove_duplicate(self, heapRel, itup,
						RelationGetRelationName(wstate->index));
					itup = BTSpoolGetNextItem(btspool, itup, &should_free);
				}
				continue;
			}
			else
			{
				/* Discard itup2 and read next */
				itup2 = BTReaderGetNextItem(btspool2);
			}
		}

		BULKLOAD_PROFILE(&prof_merge_unique);

		/* When we see first tuple, create first index page */
		if (state == NULL)
			state = _bt_pagestate(wstate, 0);

		if (load1)
		{
			_bt_buildadd(wstate, state, itup);

			if (btspool->isunique)
			{
				IndexTuple	next_itup = NULL;
				bool		next_should_free = false;

				for (;;)
				{
					next_itup = BTSpoolGetNextItem(btspool, next_itup,
												   &next_should_free);
					if (next_itup == NULL)
						break;
					else
					{
						int32	compare;
						bool	hasnull;

						compare = compare_indextuple(itup, next_itup,
									indexScanKey, keysz, tupdes, &hasnull);
						if (compare < 0 || hasnull)
							break;

						if (compare > 0)
						{
							/* shouldn't happen */
							elog(ERROR, "faild in tuplesort_performsort");
						}

						self->dup_new++;
						remove_duplicate(self, heapRel, next_itup,
							RelationGetRelationName(wstate->index));

						if (self->dup_old + self->dup_new > self->max_dup_errors)
							ereport(ERROR,
								(errcode(ERRCODE_INTERNAL_ERROR),
								 errmsg("Maximum duplicate error count exceeded")));
					}
				}
				if (should_free)
					pfree(itup);

				itup = next_itup;
				should_free = next_should_free;
			}
			else
				itup = BTSpoolGetNextItem(btspool, itup, &should_free);
		}
		else
		{
			_bt_buildadd(wstate, state, itup2);
			itup2 = BTReaderGetNextItem(btspool2);
		}
		BULKLOAD_PROFILE(&prof_merge_insert);
	}
	_bt_freeskey(indexScanKey);

	/* Close down final pages and write the metapage */
	_bt_uppershutdown(wstate, state);

	/*
	 * If the index isn't temp, we must fsync it down to disk before it's safe
	 * to commit the transaction.  (For a temp index we don't care since the
	 * index will be uninteresting after a crash anyway.)
	 *
	 * It's obvious that we must do this when not WAL-logging the build. It's
	 * less obvious that we have to do it even if we did WAL-log the index
	 * pages.  The reason is that since we're building outside shared buffers,
	 * a CHECKPOINT occurring during the build has no way to flush the
	 * previously written data to disk (indeed it won't know the index even
	 * exists).  A crash later on would replay WAL from the checkpoint,
	 * therefore it wouldn't replay our earlier WAL entries. If we do not
	 * fsync those pages here, they might still not be on disk when the crash
	 * occurs.
	 */
	if (!wstate->index->rd_istemp)
	{
		RelationOpenSmgr(wstate->index);
		smgrimmedsync(wstate->index->rd_smgr, MAIN_FORKNUM);
	}
	BULKLOAD_PROFILE(&prof_merge_term);
}

static IndexTuple
BTSpoolGetNextItem(BTSpool *spool, IndexTuple itup, bool *should_free)
{
	if (*should_free)
		pfree(itup);
	return tuplesort_getindextuple(spool->sortstate, true, should_free);
}

/**
 * @brief Read the left-most leaf page by walking down on index tree structure
 * from root node.
 *
 * Process flow
 * -# Open index file and read meta page
 * -# Get block number of root page
 * -# Read "fast root" page
 * -# Read left child page until reaching left-most leaf page
 *
 * After calling this function, the members of BTReader are the following:
 * - smgr : Smgr relation of the existing index file.
 * - blkno : block number of left-most leaf page. If there is no leaf page,
 *		   InvalidBlockNumber is set.
 * - offnum : InvalidOffsetNumber is set.
 * - page : Left-most leaf page, or undefined if no leaf page.
 *
 * @param reader [in/out] B-Tree index reader
 * @return true iff there are some tuples
 */
static bool
BTReaderInit(BTReader *reader, Relation rel)
{
	BTPageOpaque	metaopaque;
	BTMetaPageData *metad;
	BTPageOpaque	opaque;
	BlockNumber		blkno;

	/*
	 * HACK: We cannot use smgropen because smgrs returned from it
	 * will be closed automatically when we assign a new file node.
	 *
	 * XXX: It might be better to open the previous relfilenode with
	 * smgropen *after* RelationSetNewRelfilenode.
	 */
	memset(&reader->smgr, 0, sizeof(reader->smgr));
#if PG_VERSION_NUM >= 90100
	reader->smgr.smgr_rnode.node = rel->rd_node;
	reader->smgr.smgr_rnode.backend =
		rel->rd_istemp ? MyBackendId : InvalidBackendId;
#else
	reader->smgr.smgr_rnode = rel->rd_node;
#endif
	reader->smgr.smgr_which = 0;	/* md.c */

	reader->blkno = InvalidBlockNumber;
	reader->offnum = InvalidOffsetNumber;
	reader->page = palloc(BLCKSZ);

	/*
	 * Read meta page and check sanity of it.
	 * 
	 * XXX: It might be better to do REINDEX against corrupted indexes
	 * instead of raising errors because we've spent long time for data
	 * loading...
	 */
	BTReaderReadPage(reader, BTREE_METAPAGE);
	metaopaque = (BTPageOpaque) PageGetSpecialPointer(reader->page);
	metad = BTPageGetMeta(reader->page);

	if (!(metaopaque->btpo_flags & BTP_META) ||
		metad->btm_magic != BTREE_MAGIC)
		ereport(ERROR,
				(errcode(ERRCODE_INDEX_CORRUPTED),
				 errmsg("index \"%s\" is not a reader",
						RelationGetRelationName(rel))));

	if (metad->btm_version != BTREE_VERSION)
		ereport(ERROR,
				(errcode(ERRCODE_INDEX_CORRUPTED),
				 errmsg("version mismatch in index \"%s\": file version %d,"
						" code version %d",
						RelationGetRelationName(rel),
						metad->btm_version, BTREE_VERSION)));

	if (metad->btm_root == P_NONE)
	{
		/* No root page; We ignore the index in the subsequent build. */
		reader->blkno = InvalidBlockNumber;
		return false;
	}

	/* Go to the fast root page. */
	blkno = metad->btm_fastroot;
	BTReaderReadPage(reader, blkno);
	opaque = (BTPageOpaque) PageGetSpecialPointer(reader->page);

	/* Walk down to the left-most leaf page */
	while (!P_ISLEAF(opaque))
	{
		ItemId		firstid;
		IndexTuple	itup;

		/* Get the block number of the left child */
		firstid = PageGetItemId(reader->page, P_FIRSTDATAKEY(opaque));
		itup = (IndexTuple) PageGetItem(reader->page, firstid);
		blkno = ItemPointerGetBlockNumber(&(itup->t_tid));

		/* Go down to children */
		for (;;)
		{
			BTReaderReadPage(reader, blkno);
			opaque = (BTPageOpaque) PageGetSpecialPointer(reader->page);

			if (!P_IGNORE(opaque))
				break;

			if (P_RIGHTMOST(opaque))
			{
				/* We reach end of the index without any valid leaves. */
				reader->blkno = InvalidBlockNumber;
				return false;
			}
			blkno = opaque->btpo_next;
		}
	}
	
	return true;
}

/**
 * @brief Release resources used in the reader
 */
static void
BTReaderTerm(BTReader *reader)
{
	/* FIXME: We should use smgrclose, but it is not managed in smgr. */
	Assert(reader->smgr.smgr_which == 0);
	mdclose(&reader->smgr, MAIN_FORKNUM);
	pfree(reader->page);
}

/**
 * @brief Read the indicated block into BTReader structure
 */
static void
BTReaderReadPage(BTReader *reader, BlockNumber blkno)
{
	smgrread(&reader->smgr, MAIN_FORKNUM, blkno, reader->page);
	reader->blkno = blkno;
	reader->offnum = InvalidOffsetNumber;
}

/**
 * @brief Get the next smaller item from the old index
 *
 * Process flow
 * -# Examine the max offset position in the page
 * -# Search the next item
 * -# If the item has deleted flag, seearch the next one
 * -# If we can't find items any more, read the leaf page on the right side
 *	  and search the next again
 *
 * These members are updated:
 *	 - page : page which includes picked-up item
 *	 - offnum : item offset number of the picked-up item
 *
 * @param reader [in/out] BTReader structure
 * @return next index tuple, or null if no more tuples
 */
static IndexTuple
BTReaderGetNextItem(BTReader *reader)
{
	OffsetNumber	maxoff;
	ItemId			itemid;
	BTPageOpaque	opaque;

	/*
	 * If any leaf page isn't read, the state is treated like as EOF 
	 */
	if (reader->blkno == InvalidBlockNumber)
		return NULL;

	maxoff = PageGetMaxOffsetNumber(reader->page);

	for (;;)
	{
		/*
		 * If no one items are picked up, offnum is set to InvalidOffsetNumber.
		 */
		if (reader->offnum == InvalidOffsetNumber)
		{
			opaque = (BTPageOpaque) PageGetSpecialPointer(reader->page);
			reader->offnum = P_FIRSTDATAKEY(opaque);
		}
		else
			reader->offnum = OffsetNumberNext(reader->offnum);

		if (reader->offnum <= maxoff)
		{
			itemid = PageGetItemId(reader->page, reader->offnum);

			/* Ignore dead items */
			if (ItemIdIsDead(itemid))
				continue;

			return (IndexTuple) PageGetItem(reader->page, itemid);
		}
		else
		{
			/* The end of the leaf page. Go right. */
			opaque = (BTPageOpaque) PageGetSpecialPointer(reader->page);

			if (P_RIGHTMOST(opaque))
				return NULL;	/* No more index tuples */

			BTReaderReadPage(reader, opaque->btpo_next);
			maxoff = PageGetMaxOffsetNumber(reader->page);
		}
	}
}

static int
compare_indextuple(const IndexTuple itup1, const IndexTuple itup2,
	ScanKey entry, int keysz, TupleDesc tupdes, bool *hasnull)
{
	int		i;
	int32	compare;

	*hasnull = false;
	for (i = 1; i <= keysz; i++, entry++)
	{
		Datum		attrDatum1,
					attrDatum2;
		bool		isNull1,
					isNull2;

		attrDatum1 = index_getattr(itup1, i, tupdes, &isNull1);
		attrDatum2 = index_getattr(itup2, i, tupdes, &isNull2);
		if (isNull1)
		{
			*hasnull = true;
			if (isNull2)
				compare = 0;		/* NULL "=" NULL */
			else if (entry->sk_flags & SK_BT_NULLS_FIRST)
				compare = -1;		/* NULL "<" NOT_NULL */
			else
				compare = 1;		/* NULL ">" NOT_NULL */
		}
		else if (isNull2)
		{
			*hasnull = true;
			if (entry->sk_flags & SK_BT_NULLS_FIRST)
				compare = 1;		/* NOT_NULL ">" NULL */
			else
				compare = -1;		/* NOT_NULL "<" NULL */
		}
		else
		{
			compare = DatumGetInt32(FunctionCall2(&entry->sk_func,
												  attrDatum1,
												  attrDatum2));

			if (entry->sk_flags & SK_BT_DESC)
				compare = -compare;
		}
		if (compare != 0)
			return compare;
	}

	return 0;
}

/*
 * heap_is_visible
 */
static bool
heap_is_visible(Relation heapRel, ItemPointer htid)
{
#if PG_VERSION_NUM >= 80300
	SnapshotData	SnapshotDirty;

	InitDirtySnapshot(SnapshotDirty);

	/*
	 * Visiblilty checking is simplifed comapred with _bt_check_unique
	 * because we have exclusive lock on the relation. (XXX: Is it true?)
	 */
	return heap_hot_search(htid, heapRel, &SnapshotDirty, NULL);
#else
	bool			visible;
	HeapTupleData	htup;
	Buffer			hbuffer;

	htup.t_self = *htid;
	visible = heap_fetch(heapRel, SnapshotDirty, &htup, &hbuffer, true, NULL);
	ReleaseBuffer(hbuffer);

	return visible;
#endif
}

static void
remove_duplicate(Spooler *self, Relation heap, IndexTuple itup, const char *relname)
{
	HeapTupleData	tuple;
	BlockNumber		blknum;
	BlockNumber		offnum;
	Buffer			buffer;
	Page			page;
	ItemId			itemid;

	blknum = ItemPointerGetBlockNumber(&itup->t_tid);
	offnum = ItemPointerGetOffsetNumber(&itup->t_tid);
	buffer = ReadBuffer(heap, blknum);

	LockBuffer(buffer, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buffer);
	itemid = PageGetItemId(page, offnum);
	tuple.t_data = ItemIdIsNormal(itemid)
		? (HeapTupleHeader) PageGetItem(page, itemid)
		: NULL;
	LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

	if (tuple.t_data != NULL)
	{
		char		   *str;
		TupleDesc		tupdesc;

		simple_heap_delete(heap, &itup->t_tid);

		/* output duplicate bad file. */
		if (self->dup_fp == NULL)
			if ((self->dup_fp = AllocateFile(self->dup_badfile, "w")) == NULL)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not open duplicate bad file \"%s\": %m",
								self->dup_badfile)));

		tupdesc = RelationGetDescr(heap);
		tuple.t_len = ItemIdGetLength(itemid);
		tuple.t_self = itup->t_tid;

		str = tuple_to_cstring(RelationGetDescr(heap), &tuple);
		if (fprintf(self->dup_fp, "%s\n", str) < 0 || fflush(self->dup_fp))
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not write parse badfile \"%s\": %m",
							self->dup_badfile)));

		pfree(str);
	}

	ReleaseBuffer(buffer);

	LoggerLog(WARNING, "Duplidate error Record " int64_FMT
		": Rejected - duplicate key value violates unique constraint \"%s\"\n",
		self->dup_old + self->dup_new, relname);
}

char *
tuple_to_cstring(TupleDesc tupdesc, HeapTuple tuple)
{
	bool		needComma = false;
	int			ncolumns;
	int			i;
	Datum	   *values;
	bool	   *nulls;
	StringInfoData buf;

	ncolumns = tupdesc->natts;

	values = (Datum *) palloc(ncolumns * sizeof(Datum));
	nulls = (bool *) palloc(ncolumns * sizeof(bool));

	/* Break down the tuple into fields */
	heap_deform_tuple(tuple, tupdesc, values, nulls);

	/* And build the result string */
	initStringInfo(&buf);

	for (i = 0; i < ncolumns; i++)
	{
		char	   *value;
		char	   *tmp;
		bool		nq;

		/* Ignore dropped columns in datatype */
		if (tupdesc->attrs[i]->attisdropped)
			continue;

		if (needComma)
			appendStringInfoChar(&buf, ',');
		needComma = true;

		if (nulls[i])
		{
			/* emit nothing... */
			continue;
		}
		else
		{
			Oid			foutoid;
			bool		typisvarlena;

			getTypeOutputInfo(tupdesc->attrs[i]->atttypid,
							  &foutoid, &typisvarlena);
			value = OidOutputFunctionCall(foutoid, values[i]);
		}

		/* Detect whether we need double quotes for this value */
		nq = (value[0] == '\0');	/* force quotes for empty string */
		for (tmp = value; *tmp; tmp++)
		{
			char		ch = *tmp;

			if (ch == '"' || ch == '\\' ||
				ch == '(' || ch == ')' || ch == ',' ||
				isspace((unsigned char) ch))
			{
				nq = true;
				break;
			}
		}

		/* And emit the string */
		if (nq)
			appendStringInfoChar(&buf, '"');
		for (tmp = value; *tmp; tmp++)
		{
			char		ch = *tmp;

			if (ch == '"' || ch == '\\')
				appendStringInfoChar(&buf, ch);
			appendStringInfoChar(&buf, ch);
		}
		if (nq)
			appendStringInfoChar(&buf, '"');
	}

	pfree(values);
	pfree(nulls);

	return buf.data;
}

static void
report_unique_violation(Relation rel, IndexTuple itup)
{
#if PG_VERSION_NUM >= 90000
	Datum	values[INDEX_MAX_KEYS];
	bool	isnull[INDEX_MAX_KEYS];

	index_deform_tuple(itup, RelationGetDescr(rel), values, isnull);
	ereport(ERROR,
			(errcode(ERRCODE_UNIQUE_VIOLATION),
			 errmsg("duplicate key value violates unique constraint \"%s\"",
					RelationGetRelationName(rel)),
			 errdetail("Key %s already exists.",
					   BuildIndexValueDescription(rel, values, isnull))));
#else
#define BUFLENGTH	512
	char		key_names[BUFLENGTH];
	char		key_values[BUFLENGTH];
	char	   *name_ptr = key_names;
	char	   *val_ptr = key_values;
	int			i;

	TupleDesc	tupdesc = rel->rd_att;

	for (i = 0; i < tupdesc->natts; i++)
	{
		Datum		value;
		bool		isnull;
		char	   *name,
				   *val;

		name = NameStr(tupdesc->attrs[i]->attname);
		value = index_getattr(itup, i + 1, tupdesc, &isnull);
		if (isnull)
			val = "null";
		else
		{
			Oid			foutoid;
			bool		typisvarlena;

			getTypeOutputInfo(tupdesc->attrs[i]->atttypid,
							  &foutoid, &typisvarlena);
			val = OidOutputFunctionCall(foutoid, value);
		}

		/*
		 * Go to "..." if name or value doesn't fit in buffer.  We reserve 5
		 * bytes to ensure we can add comma, "...", null.
		 */
		if (strlen(name) >= (key_names + BUFLENGTH - 5) - name_ptr ||
			strlen(val) >= (key_values + BUFLENGTH - 5) - val_ptr)
		{
			sprintf(name_ptr, "...");
			sprintf(val_ptr, "...");
			break;
		}

		name_ptr += sprintf(name_ptr, "%s%s", i > 0 ? ", " : "", name);
		val_ptr += sprintf(val_ptr, "%s%s", i > 0 ? ", " : "", val);
	}

	ereport(ERROR,
			(errcode(ERRCODE_UNIQUE_VIOLATION),
			 errmsg("duplicate key value violates unique constraint \"%s\"",
					RelationGetRelationName(rel)),
		errdetail("Key (%s)=(%s) already exists.",
				  key_names, key_values)));
#endif
}
