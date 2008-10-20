/*
 * pg_bulkload: include/pg_btree.h
 *
 *	  Copyright(C) 2007-2008 NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

/**
 * @file
 * @brief Declaration of B-Tree index processing module.
 *
 */
#ifndef BTREE_H
#define BTREE_H

#include "postgres.h"
#include "access/nbtree.h"
#include "nodes/execnodes.h"

/* External declarations */
extern BTSpool **IndexSpoolBegin(ResultRelInfo *relinfo);
extern void	IndexSpoolEnd(BTSpool **spools, ResultRelInfo *relinfo, bool reindex, bool use_wal);
extern void	IndexSpoolInsert(BTSpool **spools, TupleTableSlot *slot, ItemPointer tupleid, EState *estate, bool reindex);

#endif   /* BTREE_H */
