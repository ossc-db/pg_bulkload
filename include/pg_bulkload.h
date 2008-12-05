/*
 * pg_bulkload: include/pg_bulkload.h
 *
 *	  Copyright(C) 2007-2008 NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */
#ifndef BULKLOAD_H_INCLUDED
#define BULKLOAD_H_INCLUDED

#include "postgres.h"
#include "access/xlogdefs.h"

/**
 * @file
 * @brief General definition in pg_bulkload.
 */

typedef struct ControlInfo	ControlInfo;
typedef struct Parser		Parser;

/*
 * Version compatibility issues.
 */
#if PG_VERSION_NUM < 80400

#define MAIN_FORKNUM
#define relpath(rnode, forknum)			relpath((rnode))
#define smgrimmedsync(reln, forknum)	smgrimmedsync((reln))
#define smgrread(reln, forknum, blocknum, buffer) \
	smgrread((reln), (blocknum), (buffer))
#define mdclose(reln, forknum)			mdclose((reln))
#define heap_insert(relation, tup, cid, options, bistate) \
	heap_insert((relation), (tup), (cid), true, true)
#define HEAP_INSERT_SKIP_WAL	0x0001
#define HEAP_INSERT_SKIP_FSM	0x0002
typedef void *BulkInsertState;
#define GetBulkInsertState()			(NULL)
#define FreeBulkInsertState(bistate)	((void)0)

#endif

#if PG_VERSION_NUM < 80300

#define SK_BT_DESC					0	/* Always ASC */
#define SK_BT_NULLS_FIRST			0	/* Always NULLS LAST */
#define MaxHeapTupleSize			MaxTupleSize
#define heap_sync(rel)				((void)0)
#define ItemIdIsDead(itemId)		ItemIdDeleted(itemId)
#define GetCurrentCommandId(used)	GetCurrentCommandId()
#define stringToQualifiedNameList(str) \
    stringToQualifiedNameList((str), "pg_bulkload")
#define setNewRelfilenode(rel, xid) \
	setNewRelfilenode((rel))

#endif

#include "pg_bulkload_win32.h"

#endif   /* BULKLOAD_H_INCLUDED */
