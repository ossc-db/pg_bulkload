/*
 * pg_bulkload: lib/writer_buffered.c
 *
 *	  Copyright (c) 2007-2010, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

#include "postgres.h"

#include "access/heapam.h"
#include "access/xact.h"
#include "executor/executor.h"

#include "logger.h"
#include "writer.h"
#include "pg_btree.h"

typedef struct BufferedWriter
{
	Writer	base;

	Relation		rel;
	Spooler			spooler;

	BulkInsertState bistate;	/* use bulk insert storategy */
	CommandId		cid;
} BufferedWriter;

static void	BufferedWriterInsert(BufferedWriter *self, HeapTuple tuple);
static WriterResult	BufferedWriterClose(BufferedWriter *self, bool onError);
static void	BufferedWriterDumpParams(BufferedWriter *self);

/* ========================================================================
 * Implementation
 * ========================================================================*/

/**
 * @brief Create a new BufferedWriter
 */
Writer *
CreateBufferedWriter(Oid relid, ON_DUPLICATE on_duplicate, int64 max_dup_errors, char *dup_badfile)
{
	BufferedWriter *self = palloc0(sizeof(BufferedWriter));
	self->base.insert = (WriterInsertProc) BufferedWriterInsert;
	self->base.close = (WriterCloseProc) BufferedWriterClose;
	self->base.dumpParams = (WriterDumpParamsProc) BufferedWriterDumpParams;

	self->rel = heap_open(relid, AccessExclusiveLock);
	VerifyTarget(self->rel);

	SpoolerOpen(&self->spooler, self->rel, on_duplicate, true, max_dup_errors,
				dup_badfile);
	self->base.context = GetPerTupleMemoryContext(self->spooler.estate);

	self->bistate = GetBulkInsertState();
	self->cid = GetCurrentCommandId(true);

	return (Writer *) self;
}

/**
 * @brief Store tuples into the heap using shared buffers.
 * @return void
 */
static void
BufferedWriterInsert(BufferedWriter *self, HeapTuple tuple)
{
	heap_insert(self->rel, tuple, self->cid, 0, self->bistate);
	SpoolerInsert(&self->spooler, tuple);
}

static WriterResult
BufferedWriterClose(BufferedWriter *self, bool onError)
{
	WriterResult	ret = { 0 };

	if (!onError)
	{
		if (self->bistate)
			FreeBulkInsertState(self->bistate);

		SpoolerClose(&self->spooler);
		ret.num_dup_new = self->spooler.dup_new;
		ret.num_dup_old = self->spooler.dup_old;

		heap_close(self->rel, AccessExclusiveLock);

		pfree(self);
	}

	return ret;
}

static void
BufferedWriterDumpParams(BufferedWriter *self)
{
	LoggerLog(INFO, "WRITER = BUFFERED\n\n");
}
