/*
 * pg_bulkload: lib/pg_btree.c
 *
 *	  Copyright(C) 2007-2009, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

/**
 * @file
 * @brief implementation of B-Tree index processing module
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/nbtree.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/index.h"
#include "executor/executor.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "storage/smgr.h"
#include "utils/tqual.h"

#include "pg_bulkload_win32.h"
#include "pg_btree.h"
#include "pg_profile.h"

static BTSpool *unused_bt_spoolinit(Relation, bool, bool);
static void unused_bt_spooldestroy(BTSpool *);
static void unused_bt_spool(IndexTuple, BTSpool *);
static void unused_bt_leafbuild(BTSpool *, BTSpool *);

#define _bt_spoolinit		unused_bt_spoolinit
#define _bt_spooldestroy	unused_bt_spooldestroy
#define _bt_spool			unused_bt_spool
#define _bt_leafbuild		unused_bt_leafbuild

#include "../../src/backend/access/nbtree/nbtsort.c"

#undef _bt_spoolinit
#undef _bt_spooldestroy
#undef _bt_spool
#undef _bt_leafbuild

#include "pg_bulkload.h"

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

static bool BTReaderInit(BTReader *reader, Relation rel);
static void BTReaderTerm(BTReader *reader);
static void BTReaderReadPage(BTReader *reader, BlockNumber blkno);
static IndexTuple BTReaderGetNextItem(BTReader *reader);

static void _bt_mergebuild(BTSpool *btspool, Relation heapRel, bool use_wal);
static void _bt_mergeload(BTWriteState *wstate, BTSpool *btspool,
						  BTReader *btspool2, Relation heapRel);
static bool heap_is_visible(Relation heapRel, ItemPointer htid);

/*
 * IndexSpoolBegin - Initialize spools.
 */
BTSpool **
IndexSpoolBegin(ResultRelInfo *relinfo)
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
			spools[i] = _bt_spoolinit(indices[i], indices[i]->rd_index->indisunique, false);
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
IndexSpoolEnd(BTSpool **spools,
			  ResultRelInfo *relinfo,
			  bool reindex,
			  bool use_wal)
{
	int				i;
	RelationPtr		indices = relinfo->ri_IndexRelationDescs;

	Assert(spools != NULL);
	Assert(relinfo != NULL);

	for (i = 0; i < relinfo->ri_NumIndices; i++)
	{
		if (spools[i] != NULL)
		{
			BULKLOAD_PROFILE_PUSH();
			_bt_mergebuild(spools[i], relinfo->ri_RelationDesc, use_wal);
			BULKLOAD_PROFILE_POP();
			_bt_spooldestroy(spools[i]);
			BULKLOAD_PROFILE(&prof_index_merge);
		}
		else if (reindex)
		{
			Oid		indexOid = RelationGetRelid(indices[i]);

			/* Close index before reindex to pass CheckTableNotInUse. */
			relation_close(indices[i], NoLock);
			indices[i] = NULL;
			reindex_index(indexOid);
			CommandCounterIncrement();
			BULKLOAD_PROFILE(&prof_index_reindex);
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
void
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
_bt_mergebuild(BTSpool *btspool, Relation heapRel, bool use_wal)
{
	BTWriteState	wstate;
	BTReader		reader;

	Assert(btspool->index->rd_index->indisvalid);

	tuplesort_performsort(btspool->sortstate);

	wstate.index = btspool->index;

	/*
	 * We need to log index creation in WAL iff WAL archiving is enabled AND
	 * it's not a temp index.
	 */
	wstate.btws_use_wal = use_wal &&
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
	BULKLOAD_PROFILE(&prof_index_merge_flush);

	PG_TRY();
	{
		bool	merge = BTReaderInit(&reader, wstate.index);

		elog(DEBUG1, "pg_bulkload: build \"%s\" %s merge (%s wal)",
			RelationGetRelationName(wstate.index),
			merge ? "with" : "without",
			wstate.btws_use_wal ? "with" : "without");

		if (merge)
		{
			/* Assign a new file node and merge two streams into it. */
			setNewRelfilenode(wstate.index, RecentXmin);
			BULKLOAD_PROFILE_PUSH();
			_bt_mergeload(&wstate, btspool, &reader, heapRel);
			BULKLOAD_PROFILE_POP();
		}
		else
		{
			/* Fast path for newly created index. */
			_bt_load(&wstate, btspool, NULL);
		}

		BTReaderTerm(&reader);
	}
	PG_CATCH();
	{
		BTReaderTerm(&reader);
		PG_RE_THROW();
	}
	PG_END_TRY();

	BULKLOAD_PROFILE(&prof_index_merge_build);
}

/*
 * _bt_mergeload - Merge two streams of index tuples into new index files.
 */
static void
_bt_mergeload(BTWriteState *wstate, BTSpool *btspool, BTReader *btspool2,
			  Relation heapRel)
{
	BTPageState	   *state = NULL;
	IndexTuple		itup,
					itup2;
	bool			should_free;
	TupleDesc		tupdes = RelationGetDescr(wstate->index);
	int				keysz = RelationGetNumberOfAttributes(wstate->index);
	ScanKey			indexScanKey;

	Assert(btspool != NULL);
	Assert(btspool2 != NULL);

	/* the preparation of merge */
	itup = tuplesort_getindextuple(btspool->sortstate, true, &should_free);
	itup2 = BTReaderGetNextItem(btspool2);
	indexScanKey = _bt_mkscankey_nodata(wstate->index);

	BULKLOAD_PROFILE(&prof_index_merge_build_init);

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
				ereport(ERROR,
						(errcode(ERRCODE_UNIQUE_VIOLATION),
						 errmsg("could not create unique index"),
						 errdetail("Table contains duplicated values.")));

			/* Discard itup2 and read next */
			itup2 = BTReaderGetNextItem(btspool2);
		}
		BULKLOAD_PROFILE(&prof_index_merge_build_unique);

		/* When we see first tuple, create first index page */
		if (state == NULL)
			state = _bt_pagestate(wstate, 0);

		if (load1)
		{
			_bt_buildadd(wstate, state, itup);
			if (should_free)
				pfree(itup);
			itup = tuplesort_getindextuple(btspool->sortstate,
										   true, &should_free);
		}
		else
		{
			_bt_buildadd(wstate, state, itup2);
			itup2 = BTReaderGetNextItem(btspool2);
		}
		BULKLOAD_PROFILE(&prof_index_merge_build_insert);
	}
	_bt_freeskey(indexScanKey);

	/* Close down final pages and write the metapage */
	_bt_uppershutdown(wstate, state);
	BULKLOAD_PROFILE(&prof_index_merge_build_term);

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
	BULKLOAD_PROFILE(&prof_index_merge_build_flush);
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
	 * smgropen *after* setNewRelfilenode.
	 */
	memset(&reader->smgr, 0, sizeof(reader->smgr));
	reader->smgr.smgr_rnode = rel->rd_node;
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
	Assert(reader->blkno != InvalidBlockNumber);

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
