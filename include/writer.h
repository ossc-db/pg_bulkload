/*
 * pg_bulkload: include/writer.h
 *
 *	  Copyright(C) 2009, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

/**
 * @file
 * @brief Declaration of writer module
 *
 */
#ifndef WRITER_H_INCLUDED
#define WRITER_H_INCLUDED

#include "pg_bulkload.h"

#include "access/xact.h"
#include "access/nbtree.h"
#include "nodes/execnodes.h"

/*
 * Loader
 */

typedef bool (*LoaderInsertProc)(Loader *self, Relation rel, HeapTuple tuple);
typedef void (*LoaderCloseProc)(Loader *self);

struct Loader
{
	LoaderInsertProc	insert;	/**< insert one tuple */
	LoaderCloseProc		close;	/**< clean up */
	bool				use_wal;
};

extern Loader *CreateDirectLoader(Relation rel);
extern Loader *CreateBufferedLoader(Relation rel);
extern void AtEOXact_DirectLoader(XactEvent event, void *arg);

#define LoaderInsert(self, rel, tuple)	((self)->insert((self), (rel), (tuple)))
#define LoaderClose(self)				((self)->close((self)))

/*
 * Writer
 */

typedef struct Writer
{
	Loader		   *loader;		/**< loader object */
	Relation		rel;		/**< dest relation */
	BTSpool		  **spools;		/**< index spool */
	ResultRelInfo  *relinfo;	/**<  */
	EState		   *estate;		/**<  */
	TupleTableSlot *slot;		/**<  */
	int64			count;		/**< number of inserted tuples */
	ON_DUPLICATE	on_duplicate;
} Writer;

extern void WriterOpen(Writer *wt, Oid relid);
extern void WriterInsert(Writer *wt, HeapTuple tuple);
extern void WriterClose(Writer *wt);

#endif   /* WRITER_H_INCLUDED */
