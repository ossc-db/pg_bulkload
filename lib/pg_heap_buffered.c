/*
 * pg_heap_buffered: lib/pg_heap_buffered.c
 *
 *	  Copyright(C) 2007-2009, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

/**
 * @file
 * @brief Buffered heap writer
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/xact.h"

#include "writer.h"

typedef struct BufferedLoader
{
	Loader	base;

	BulkInsertState bistate;	/* use bulk insert storategy */
	CommandId		cid;
} BufferedLoader;

/*
 * Prototype declaration for local functions.
 */

static void	BufferedLoaderInsert(BufferedLoader *self, Relation rel, HeapTuple tuple);
static void	BufferedLoaderClose(BufferedLoader *self);

/* ========================================================================
 * Implementation
 * ========================================================================*/

/**
 * @brief Create a new BufferedLoader
 */
Loader *
CreateBufferedLoader(Relation rel)
{
	BufferedLoader* self = palloc0(sizeof(BufferedLoader));
	self->base.insert = (LoaderInsertProc) BufferedLoaderInsert;
	self->base.close = (LoaderCloseProc) BufferedLoaderClose;
	self->base.use_wal = true;
	self->bistate = GetBulkInsertState();
	self->cid = GetCurrentCommandId(true);
	return (Loader *) self;
}

/**
 * @brief Store tuples into the heap using shared buffers.
 * @return void
 */
static void
BufferedLoaderInsert(BufferedLoader *self, Relation rel, HeapTuple tuple)
{
	/* Insert the heap tuple and index entries. */
	heap_insert(rel, tuple, self->cid, 0, self->bistate);
}

static void
BufferedLoaderClose(BufferedLoader *self)
{
	if (self->bistate)
		FreeBulkInsertState(self->bistate);
	pfree(self);
}
