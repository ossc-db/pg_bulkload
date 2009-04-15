/*
 * pg_bulkload: include/pg_loadstatus.h
 *
 *	  Copyright(C) 2007-2009, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

/**
 * @file
 * @brief Declaration of loading status proccesing module
 *
 */
#ifndef LOADSTATUS_H
#define LOADSTATUS_H

#include <sys/param.h>
#include "postgres.h"
#include "storage/block.h"
#include "utils/rel.h"

/**
 * @brief Loading status information
 */
typedef struct LoadStatus
{
	/**
	 * @name Written every time
	 */
	pg_crc32	ls_crc;					/**< For file invalidation check */
	BlockNumber ls_create_cnt;			/**< The number of blocks pg_bulkload creates */

	/**
	 * @name  Written only first one time
	 */
	BlockNumber ls_exist_cnt;			/**< The number of blocks already existing */
	char		ls_datafname[MAXPATHLEN + 1];
										/**< Data file name of the first segment */

	/**
	 * @name Not written for file
	 */
	int			ls_fd;		/**< File descriptor of load status file */
	char		ls_lsfname[MAXPATHLEN + 1];
										/**< Load status file name */

} LoadStatus;

#endif   /* LOADSTATUS_H */
