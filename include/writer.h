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

extern Writer *CreateDirectWriter(Oid relid, ON_DUPLICATE on_duplicate, int64 max_dup_errors, char *dup_badfile);
extern Writer *CreateBufferedWriter(Oid relid, ON_DUPLICATE on_duplicate, int64 max_dup_errors, char *dup_badfile);
extern Writer *CreateParallelWriter(Oid relid, ON_DUPLICATE on_duplicate, int64 max_dup_errors, char *dup_badfile);

#define WriterInsert(self, tuple)	((self)->insert((self), (tuple)))
#define WriterClose(self, onError)	((self)->close((self), (onError)))
#define WriterDumpParams(self)		((self)->dumpParams((self)))

/*
 * Utilitiy functions
 */

extern void VerifyTarget(Relation rel);

#endif   /* WRITER_H_INCLUDED */
