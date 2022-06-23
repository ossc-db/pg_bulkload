/*
 * pg_bulkload: lib/nbtree/nbtsort-common.c
 *
 * NOTES
 *
 * Although nbtsort-XX.c is the copy of postgresql core's src/backend/access/nbtree/nbtsort.c,
 * this file has functions which related to nbtsort, but is not implemented core's code.
 *
 *	  Copyright (c) 2022, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */


#if PG_VERSION_NUM >= 150000
/*
 * create and initialize a spool structure
 */
static BTSpool *
_bt_spoolinit(Relation heap, Relation index, bool isunique, bool nulls_not_distinct, bool isdead)
{
	BTSpool    *btspool = (BTSpool *) palloc0(sizeof(BTSpool));
	int			btKbytes;

	btspool->heap = heap;
	btspool->index = index;
	btspool->isunique = isunique;
	btspool->nulls_not_distinct = nulls_not_distinct;

	/*
	 * We size the sort area as maintenance_work_mem rather than work_mem to
	 * speed index creation.  This should be OK since a single backend can't
	 * run multiple index creations in parallel.  Note that creation of a
	 * unique index actually requires two BTSpool objects.  We expect that the
	 * second one (for dead tuples) won't get very full, so we give it only
	 * work_mem.
	 */


	btKbytes = isdead ? work_mem : maintenance_work_mem;
	btspool->sortstate = tuplesort_begin_index_btree(heap, index, isunique,
													nulls_not_distinct,
													btKbytes, NULL, false);

	return btspool;
}

#elif PG_VERSION_NUM >= 140000
/*
 * create and initialize a spool structure
 */
static BTSpool *
_bt_spoolinit(Relation heap, Relation index, bool isunique, bool isdead)
{
	BTSpool    *btspool = (BTSpool *) palloc0(sizeof(BTSpool));
	int			btKbytes;

	btspool->heap = heap;
	btspool->index = index;
	btspool->isunique = isunique;

	/*
	 * We size the sort area as maintenance_work_mem rather than work_mem to
	 * speed index creation.  This should be OK since a single backend can't
	 * run multiple index creations in parallel.  Note that creation of a
	 * unique index actually requires two BTSpool objects.  We expect that the
	 * second one (for dead tuples) won't get very full, so we give it only
	 * work_mem.
	 */
	btKbytes = isdead ? work_mem : maintenance_work_mem;
	btspool->sortstate = tuplesort_begin_index_btree(heap, index, isunique,
													 btKbytes, NULL, false);

	return btspool;
}
#endif
