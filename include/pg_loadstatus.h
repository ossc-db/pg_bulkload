/*
 * pg_bulkload: include/pg_loadstatus.h
 *
 *	  Copyright (c) 2007-2024, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

/**
 * @file
 * @brief Declaration of loading status proccesing module
 *
 */
#ifndef LOADSTATUS_H
#define LOADSTATUS_H

#include "storage/block.h"
#if PG_VERSION_NUM >= 160000
#include "storage/relfilelocator.h"
#else
#include "storage/relfilenode.h"
#endif

#ifndef MAXPGPATH
#define MAXPGPATH		1024
#endif

#define BULKLOAD_LSF_DIR		"pg_bulkload"

/* typical sector size is 512 byte */
#define BULKLOAD_LSF_BLCKSZ		512

#if PG_VERSION_NUM >= 160000
#define BULKLOAD_LSF_PATH(buffer, ls) \
	snprintf((buffer), MAXPGPATH, \
			 BULKLOAD_LSF_DIR "/%d.%d.loadstatus", \
			 (ls)->ls.rLocator.dbOid, (ls)->ls.relid)
#else
#define BULKLOAD_LSF_PATH(buffer, ls) \
	snprintf((buffer), MAXPGPATH, \
			 BULKLOAD_LSF_DIR "/%d.%d.loadstatus", \
			 (ls)->ls.rnode.dbNode, (ls)->ls.relid)
#endif
/**
 * @brief Loading status information
 */
typedef union LoadStatus
{
	struct
	{
		Oid			relid;		/**< target relation oid */
#if PG_VERSION_NUM >= 160000
		RelFileLocator rLocator;
#else
		RelFileNode	rnode;		/**< target relation node */
#endif
		BlockNumber exist_cnt;	/**< number of blocks already existing */
		BlockNumber create_cnt;	/**< number of blocks pg_bulkload creates */
	} ls;
	char	padding[BULKLOAD_LSF_BLCKSZ];
} LoadStatus;

#endif   /* LOADSTATUS_H */
