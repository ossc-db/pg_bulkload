/*
 * pg_heap_buffered: lib/pg_heap_buffered.c
 *
 *	  Copyright(C) 2007-2009, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

/**
 * @file
 * @brief Direct heap writer
 */
#include "postgres.h"

#include "pg_bulkload.h"
#include "pg_controlinfo.h"

typedef struct BufferedLoader
{
	Loader	base;

	BulkInsertState bistate;	/* use bulk insert storategy */
} BufferedLoader;

/*
 * Prototype declaration for local functions.
 */

static void	BufferedLoaderInit(BufferedLoader *self, Relation rel);
static void	BufferedLoaderInsert(BufferedLoader *self, Relation rel, HeapTuple tuple);
static void	BufferedLoaderTerm(BufferedLoader *self, bool inError);

/* ========================================================================
 * Implementation
 * ========================================================================*/

/**
 * @brief Create a new BufferedLoader
 */
Loader *
CreateBufferedLoader(void)
{
	BufferedLoader* self = palloc0(sizeof(BufferedLoader));
	self->base.init = (LoaderInitProc) BufferedLoaderInit;
	self->base.insert = (LoaderInsertProc) BufferedLoaderInsert;
	self->base.term = (LoaderTermProc) BufferedLoaderTerm;
	self->base.use_wal = true;
	return (Loader *) self;
}

static void
BufferedLoaderInit(BufferedLoader *self, Relation rel)
{
	self->bistate = GetBulkInsertState();
}

/**
 * @brief Store tuples into the heap using shared buffers.
 * @return void
 */
static void
BufferedLoaderInsert(BufferedLoader *self, Relation rel, HeapTuple tuple)
{
	CommandId		cid = HeapTupleHeaderGetCmin(tuple->t_data);

	/* Insert the heap tuple and index entries. */
	heap_insert(rel, tuple, cid, 0, self->bistate);
}

static void
BufferedLoaderTerm(BufferedLoader *self, bool inError)
{
	if (self->bistate)
		FreeBulkInsertState(self->bistate);
	pfree(self);
}
