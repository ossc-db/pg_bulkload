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
 * Writer
 */

typedef bool (*WriterInsertProc)(Writer *self, HeapTuple tuple);
typedef void (*WriterCloseProc)(Writer *self);

struct Writer
{
	WriterInsertProc	insert;	/**< insert one tuple */
	WriterCloseProc		close;	/**< clean up */

	MemoryContext		context;
	int64				count;
};

extern Writer *CreateDirectWriter(Oid relid, ON_DUPLICATE on_duplicate);
extern Writer *CreateBufferedWriter(Oid relid, ON_DUPLICATE on_duplicate);
extern Writer *CreateParallelWriter(Oid relid, ON_DUPLICATE on_duplicate);
extern void AtEOXact_DirectLoader(XactEvent event, void *arg);

#define WriterInsert(self, tuple)	((self)->insert((self), (tuple)))
#define WriterClose(self)			((self)->close((self)))

/*
 * Utilitiy functions
 */

extern void VerifyTarget(Relation rel);

#endif   /* WRITER_H_INCLUDED */
