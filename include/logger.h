/*
 * pg_bulkload: include/logger.h
 *
 *	  Copyright (c) 2009-2021, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

/**
 * @file
 * @brief Declaration of logger module
 *
 */

#include "common.h"

#ifndef LOGGER_H_INCLUDED
#define LOGGER_H_INCLUDED

#include "access/xact.h"
#include "access/tupdesc.h"
#include "access/htup.h"

typedef struct Logger	Logger;

extern void CreateLogger(const char *path, bool verbose, bool writer);
extern void LoggerLog(int elevel, const char *fmt,...)
__attribute__((format(PG_BULKLOAD_PRINTF_ATTRIBUTE, 2, 3)));
extern void LoggerClose(void);

/*
 * Utilitiy functions
 */

extern char *tuple_to_cstring(TupleDesc tupdesc, HeapTuple tuple);

#endif   /* LOGGER_H_INCLUDED */
