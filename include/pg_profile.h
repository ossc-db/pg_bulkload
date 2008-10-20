/*
 * pg_bulkload: include/pg_bulkload.h
 *
 *	  Copyright(C) 2007-2008 NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

/**
 * @file
 * @brief Macro definition for profiling
 */
#ifndef PROFILE_H_INCLUDED
#define PROFILE_H_INCLUDED

/* Profiling routine */
#ifdef PROFILE
#include <sys/time.h>

/**
 * @brief Keep timestamp at the last add_prof() finishing point
 */
static struct timeval tv_last;

/**
 * @brief Compute the difference of timevals
 * @param tv1 left parameter of subtraction
 * @param tv2 right parameter of subtraction
 * @return tv2 - tv1
 */
static struct timeval
time_diff(struct timeval tv1, struct timeval tv2)
{
	struct timeval ret;

	ret.tv_sec = tv2.tv_sec - tv1.tv_sec;
	ret.tv_usec = tv2.tv_usec - tv1.tv_usec;
	if (ret.tv_usec < 0)
	{
		ret.tv_sec--;
		ret.tv_usec += 1000000;
	}
	return ret;
}

/**
 * @brief Record profile information
 */
#define add_prof(total) \
do { \
	struct timeval tv_tmp; \
	struct timeval tv_diff; \
 \
	gettimeofday(&tv_tmp, NULL); \
 \
	if (total) \
	{ \
		tv_diff = time_diff(tv_last, tv_tmp); \
		(total)->tv_sec += tv_diff.tv_sec; \
		(total)->tv_usec += tv_diff.tv_usec; \
		if ((total)->tv_usec > 999999) \
		{ \
			(total)->tv_sec++; \
			(total)->tv_usec -= 1000000; \
		} \
	} \
	tv_last = tv_tmp; \
} while (0);
#else
#define add_prof(x)
#endif

#endif   /* PROFILE_H_INCLUDED */
