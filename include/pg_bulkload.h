/*
 * pg_bulkload: include/pg_bulkload.h
 *
 *	  Copyright(C) 2007-2009, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
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
typedef struct Loader		Loader;

/*
 * 64bit integer utils
 */

#ifndef INT64_MAX
#ifdef LLONG_MAX
#define INT64_MAX	LLONG_MAX
#else
#define INT64_MAX	INT64CONST(0x7FFFFFFFFFFFFFFF)
#endif
#endif

#ifdef HAVE_LONG_INT_64
#define int64_FMT		"%ld"
#else
#define int64_FMT		"%lld"
#endif

/*
 * Version compatibility issues.
 */
#if PG_VERSION_NUM < 80400

#define MAIN_FORKNUM					0
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
extern char *text_to_cstring(const text *t);

#if PG_VERSION_NUM >= 80300
#define log_newpage(rnode, forknum, blk, page) \
	log_newpage((rnode), (blk), (page))
#endif

#endif

#if PG_VERSION_NUM < 80300

#define PG_GETARG_TEXT_PP(n)		PG_GETARG_TEXT_P(n)
#define VARSIZE_ANY_EXHDR(v)		(VARSIZE(v) - VARHDRSZ)
#define VARDATA_ANY(v)				VARDATA(v)
#define SET_VARSIZE(v, sz)			(VARATT_SIZEP(v) = (sz))
#define pg_detoast_datum_packed(v)	pg_detoast_datum(v)
#define DatumGetTextPP(v)			DatumGetTextP(v)
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
#define PageAddItem(page, item, size, offnum, overwrite, is_heap) \
	PageAddItem((page), (item), (size), (offnum), LP_USED)

#endif

#include "pg_bulkload_win32.h"

#endif   /* BULKLOAD_H_INCLUDED */
