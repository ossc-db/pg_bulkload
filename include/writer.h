/*
 * pg_bulkload: include/writer.h
 *
 *	  Copyright (c) 2009-2010, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
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

typedef struct WriterResult
{
	int64		num_dup_new;
	int64		num_dup_old;
} WriterResult;

typedef bool (*WriterInsertProc)(Writer *self, HeapTuple tuple);
typedef WriterResult (*WriterCloseProc)(Writer *self, bool onError);
typedef void (*WriterDumpParamsProc)(Writer *self);

struct Writer
{
	WriterInsertProc		insert;		/**< insert one tuple */
	WriterCloseProc			close;		/**< clean up */
	WriterDumpParamsProc	dumpParams;	/**< dump parameters */

	MemoryContext		context;
	int64				count;
};

extern Writer *CreateDirectWriter(Oid relid, const WriterOptions *options);
extern Writer *CreateBufferedWriter(Oid relid, const WriterOptions *options);
extern Writer *CreateParallelWriter(Oid relid, const WriterOptions *options);

#define WriterInsert(self, tuple)	((self)->insert((self), (tuple)))
#define WriterClose(self, onError)	((self)->close((self), (onError)))
#define WriterDumpParams(self)		((self)->dumpParams((self)))

/*
 * Utilitiy functions
 */

extern void VerifyTarget(Relation rel);
extern void TruncateTable(Oid relid);

#endif   /* WRITER_H_INCLUDED */
