/*
 * pg_bulkload: include/pg_profile.h
 *
 *	  Copyright (c) 2007-2021, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

/**
 * @file
 * @brief Macro definition for profiling
 * Usage: make USE_PROFILE=1
 */
#ifndef PROFILE_H_INCLUDED
#define PROFILE_H_INCLUDED

/* Profiling routine */
#ifdef ENABLE_BULKLOAD_PROFILE
#include "portability/instr_time.h"

extern instr_time *prof_top;

extern instr_time prof_merge;
extern instr_time prof_index;
extern instr_time prof_reindex;

extern instr_time prof_reader_source;
extern instr_time prof_reader_parser;

extern instr_time prof_writer_toast;
extern instr_time prof_writer_table;
extern instr_time prof_writer_index;

extern instr_time prof_flush;
extern instr_time prof_merge_unique;
extern instr_time prof_merge_insert;
extern instr_time prof_merge_term;

/**
 * @brief Record profile information
 */
#define BULKLOAD_PROFILE(name) \
	do { \
		instr_time now; \
		INSTR_TIME_SET_CURRENT(now); \
		INSTR_TIME_ACCUM_DIFF(*(name), now, *prof_top); \
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
