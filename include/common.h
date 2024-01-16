/*
 * pg_bulkload: include/common.h
 *
 *	  Copyright (c) 2010-2024, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

/**
 * @file
 * @brief Common definition in pg_bulkload.
 *
 */
#ifndef COMMON_H_INCLUDED
#define COMMON_H_INCLUDED

#define PG_BULKLOAD_VERSION "3.1.21"


#ifndef PG_BULKLOAD_PRINTF_ATTRIBUTE
#ifdef WIN32
#define PG_BULKLOAD_PRINTF_ATTRIBUTE gnu_printf
#else
#define PG_BULKLOAD_PRINTF_ATTRIBUTE printf
#endif
#endif

#endif   /* COMMON_H_INCLUDED */
