/*
 * pg_bulkload: include/pg_loadstatus.h
 *
 *	  Copyright (c) 2007-2021, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

/**
 * @file
 * @brief Declaration of loading status proccesing module
 *
 */
#ifndef LOADSTATUS_H
#define LOADSTATUS_H

#include "storage/block.h"
#include "storage/relfilenode.h"

#ifndef MAXPGPATH
#define MAXPGPATH		1024
#endif

#define BULKLOAD_LSF_DIR		"pg_bulkload"

/* typical sector size is 512 byte */
#define BULKLOAD_LSF_BLCKSZ		512

#define BULKLOAD_LSF_PATH(buffer, ls) \
	snprintf((buffer), MAXPGPATH, \
			 BULKLOAD_LSF_DIR "/%d.%d.loadstatus", \
			 (ls)->ls.rnode.dbNode, (ls)->ls.relid)

/**
 * @brief Loading status information
 */
typedef union LoadStatus
{
	struct
	{
		Oid			relid;		/**< target relation oid */
		RelFileNode	rnode;		/**< target relation node */
		BlockNumber exist_cnt;	/**< number of blocks already existing */
		BlockNumber create_cnt;	/**< number of blocks pg_bulkload creates */
	} ls;
	char	padding[BULKLOAD_LSF_BLCKSZ];
} LoadStatus;

#endif   /* LOADSTATUS_H */
