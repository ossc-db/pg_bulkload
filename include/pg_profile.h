/*
 * pg_bulkload: include/pg_bulkload.h
 *
 *	  Copyright(C) 2007-2009, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

/**
 * @file
 * @brief Macro definition for profiling
 */
#ifndef PROFILE_H_INCLUDED
#define PROFILE_H_INCLUDED

/*
#define ENABLE_BULKLOAD_PROFILE
*/

/* Profiling routine */
#ifdef ENABLE_BULKLOAD_PROFILE
#include "portability/instr_time.h"

/**
 * @brief Keep timestamp at the last BULKLOAD_PROFILE() finishing point
 */
extern instr_time *prof_top;

extern instr_time prof_heap_read;
extern instr_time prof_heap_toast;
extern instr_time prof_heap_table;
extern instr_time prof_heap_index;
extern instr_time prof_heap_flush;

extern instr_time prof_index_merge;
extern instr_time prof_index_reindex;

extern instr_time prof_index_merge_flush;
extern instr_time prof_index_merge_build;

extern instr_time prof_index_merge_build_init;
extern instr_time prof_index_merge_build_unique;
extern instr_time prof_index_merge_build_insert;
extern instr_time prof_index_merge_build_term;
extern instr_time prof_index_merge_build_flush;

/**
 * @brief Record profile information
 */
#define BULKLOAD_PROFILE(total) \
	do { \
		instr_time now; \
		INSTR_TIME_SET_CURRENT(now); \
		INSTR_TIME_ACCUM_DIFF(*(total), now, *prof_top); \
		*prof_top = now; \
	} while (0);
#define BULKLOAD_PROFILE_PUSH() \
	do { \
		instr_time		_prof; \
		instr_time	   *_prof_save; \
		INSTR_TIME_SET_CURRENT(_prof); \
		_prof_save = prof_top; \
		prof_top = &_prof;

#define BULKLOAD_PROFILE_POP() \
		prof_top = _prof_save; \
	} while (0)
#else
#define BULKLOAD_PROFILE(x)		((void) 0)
#define BULKLOAD_PROFILE_PUSH()	((void) 0)
#define BULKLOAD_PROFILE_POP()	((void) 0)
#endif

#endif   /* PROFILE_H_INCLUDED */
