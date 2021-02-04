/*
 * pg_bulkload: include/pg_btree.h
 *
 *	  Copyright (c) 2007-2021, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
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
	struct BTSpool **spools;		/**< index spool */
	ResultRelInfo  *relinfo;	/**<  */
	EState		   *estate;		/**<  */
	TupleTableSlot *slot;		/**<  */
	ON_DUPLICATE	on_duplicate;
	bool			use_wal;
	int64			max_dup_errors;	/**< max error admissible number by duplicate */
	int64			dup_old;	/**< number of deleted by duplicate error */
	int64			dup_new;	/**< number of not loaded by duplicate error */
	char		   *dup_badfile;
	FILE		   *dup_fp;
} Spooler;

/* External declarations */
extern void SpoolerOpen(Spooler *self,
						Relation rel,
						bool use_wal,
						ON_DUPLICATE on_duplicate,
						int64 max_dup_errors,
						const char *dup_badfile);
extern void SpoolerClose(Spooler *self);
extern void SpoolerInsert(Spooler *self, HeapTuple tuple);

#endif   /* BTREE_H */
