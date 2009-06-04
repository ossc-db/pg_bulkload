/*
 * pg_bulkload: include/pg_btree.h
 *
 *	  Copyright(C) 2007-2009, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
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

typedef struct Spooler
{
	BTSpool		  **spools;		/**< index spool */
	ResultRelInfo  *relinfo;	/**<  */
	EState		   *estate;		/**<  */
	TupleTableSlot *slot;		/**<  */
	ON_DUPLICATE	on_duplicate;
	bool			use_wal;
} Spooler;

/* External declarations */
extern void SpoolerOpen(Spooler *self, Relation rel, ON_DUPLICATE on_duplicate, bool use_wal);
extern void SpoolerClose(Spooler *self);
extern void SpoolerInsert(Spooler *self, HeapTuple tuple);

#endif   /* BTREE_H */
