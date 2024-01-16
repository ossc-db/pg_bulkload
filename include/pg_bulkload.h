/*
 * pg_bulkload: include/pg_bulkload.h
 *
 *	  Copyright (c) 2007-2024, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */
#ifndef BULKLOAD_H_INCLUDED
#define BULKLOAD_H_INCLUDED

#include "postgres.h"

#undef ENABLE_GSS
#undef USE_SSL

#if PG_VERSION_NUM < 80300
#error pg_bulkload does not support PostgreSQL 8.2 or earlier versions.
#endif

#include "access/tupdesc.h"

/**
 * @file
 * @brief General definition in pg_bulkload.
 */

/*-------------------------------------------------------------------------
 *
 * Parser -> Writer
 * |              |
 * +--------------+
 *      Loader
 *
 * Source:
 *  - FileSource   : from local file
 *  - RemoteSource : from remote client using copy protocol
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
	ON_DUPLICATE_KEEP_NEW,
	ON_DUPLICATE_KEEP_OLD
} ON_DUPLICATE;

extern const char *ON_DUPLICATE_NAMES[2];

typedef Parser *(*ParserCreate)(void);

#define PG_BULKLOAD_COLS	8

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

#if !defined(__GNUC__) || (__GNUC__ == 2 && __GNUC_MINOR__ < 96)
#define __builtin_expect(x, expected_value) (x)
#endif

#ifndef likely
#define likely(x)   __builtin_expect((x),1)
#endif

#ifndef unlikely
#define unlikely(x) __builtin_expect((x),0)
#endif

#endif   /* BULKLOAD_H_INCLUDED */
