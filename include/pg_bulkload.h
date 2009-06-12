/*
 * pg_bulkload: include/pg_bulkload.h
 *
 *	  Copyright(C) 2007-2009, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */
#ifndef BULKLOAD_H_INCLUDED
#define BULKLOAD_H_INCLUDED

#include "postgres.h"
#include "pgut/pgut-be.h"

/**
 * @file
 * @brief General definition in pg_bulkload.
 */

/*-------------------------------------------------------------------------
 *
 * Source -> Parser -> Writer
 * |              |
 * +--------------+
 *      Reader
 * |                        |
 * +------------------------+
 *           Loader
 *
 * Source:
 *  - FileSource   : from local file
 *  - RemoteSource : from remote client using copy protocol
 *  - QueueSource  : from another process
 *
 * Parser:
 *  - BinaryParser : known as FixedParser before
 *  - CSVParser    : csv file
 *  - TupleParser  : almost noop
 *
 * Writer:
 *  - BufferedWriter : to file using shared buffers
 *  - DirectLoader   : to file using local buffers
 *  - ParallelWriter : to another process
 *
 *-------------------------------------------------------------------------
 */
typedef struct Source	Source;
typedef struct Parser	Parser;
typedef struct Writer	Writer;
typedef struct Reader	Reader;

typedef enum ON_DUPLICATE
{
	ON_DUPLICATE_ERROR,
	ON_DUPLICATE_REMOVE_NEW,
	ON_DUPLICATE_REMOVE_OLD
} ON_DUPLICATE;

extern const char *ON_DUPLICATE_NAMES[3];

typedef Source *(*SourceCreate)(const char *path, TupleDesc desc);
typedef Parser *(*ParserCreate)(void);
typedef Writer *(*WriterCreate)(Oid relid, ON_DUPLICATE on_duplicate);

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

#include "pg_bulkload_win32.h"

#endif   /* BULKLOAD_H_INCLUDED */
