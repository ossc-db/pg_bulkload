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

typedef struct Reader	Reader;
typedef struct Parser	Parser;
typedef struct Loader	Loader;

typedef enum ON_DUPLICATE
{
	ON_DUPLICATE_ERROR,
	ON_DUPLICATE_REMOVE_NEW,
	ON_DUPLICATE_REMOVE_OLD
} ON_DUPLICATE;

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
