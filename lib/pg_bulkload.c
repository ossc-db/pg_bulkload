/*
 * pg_bulkload: lib/pg_bulkload.c
 *
 *	  Copyright(C) 2007-2008 NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

/**
 * @file
 * @brief Core Modules
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/transam.h"
#include "catalog/catalog.h"
#include "miscadmin.h"

#include "pg_bulkload.h"
#include "pg_btree.h"
#include "pg_controlinfo.h"
#include "pg_profile.h"

PG_MODULE_MAGIC;

/* Signature of static functions */
static char *get_text_arg(FunctionCallInfo fcinfo, int index);

/* Obtaining core-modules' profile */
#ifdef PROFILE
#include <sys/time.h>
struct timeval tv_prepare;
struct timeval tv_read;
struct timeval tv_create_tup;
struct timeval tv_compress;
struct timeval tv_flush;
struct timeval tv_add;
struct timeval tv_cleanup;
struct timeval tv_write_data;
struct timeval tv_write_lsf;

/**
 * @brief Output the result of profile check.
 */
static void
print_profile()
{
	elog(INFO, "prepare: %d.%07d",
		 (int) tv_prepare.tv_sec, (int) tv_prepare.tv_usec);
	elog(INFO, "read: %d.%07d", (int) tv_read.tv_sec, (int) tv_read.tv_usec);
	elog(INFO, "create_tup: %d.%07d",
		 (int) tv_create_tup.tv_sec, (int) tv_create_tup.tv_usec);
	elog(INFO, "compress: %d.%07d",
		 (int) tv_compress.tv_sec, (int) tv_compress.tv_usec);
	elog(INFO, "flush: %d.%07d",
		 (int) tv_flush.tv_sec, (int) tv_flush.tv_usec);
	elog(INFO, "	write data: %d.%07d",
		 (int) tv_write_data.tv_sec, (int) tv_write_data.tv_usec);
	elog(INFO, "	write LSF: %d.%07d",
		 (int) tv_write_lsf.tv_sec, (int) tv_write_lsf.tv_usec);
	elog(INFO, "add: %d.%07d", (int) tv_add.tv_sec, (int) tv_add.tv_usec);
	elog(INFO, "cleanup: %d.%07d",
		 (int) tv_cleanup.tv_sec, (int) tv_cleanup.tv_usec);
}
#else
#define print_profile()
#endif   /* PROFILE */

/* ========================================================================
 * Implementation
 * ========================================================================*/
PG_FUNCTION_INFO_V1(pg_bulkload);
Datum		pg_bulkload(PG_FUNCTION_ARGS);

/**
 * @brief Entry point of the user-defined function for pg_bulkload.
 * @return Returns number of loaded tuples.  If the case of errors, -1 will be
 * returned.
 * @todo It may be better to warn when loaded tuple number is different from
 * the input record number.
 */
Datum
pg_bulkload(PG_FUNCTION_ARGS)
{
	ControlInfo	   *ci = NULL;

	add_prof((struct timeval *) NULL);

	/*
	 * Check user previlege (must be the super user and need INSERT previlege
	 */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to use pg_bulkload()")));

	/*
	 * STEP 0: Read control file
	 */

	ereport(NOTICE, (errmsg("BULK LOAD START")));
	ci = OpenControlInfo(get_text_arg(fcinfo, 0));

	PG_TRY();
	{
		int32			load_cnt;
		BTSpool		  **spools = NULL;

		/*
		 * STEP 1: Initialization
		 */

		ParserInitialize(ci->ci_parser, ci);
		spools = IndexSpoolBegin(ci->ci_estate->es_result_relation_info);
		ereport(DEBUG1, (errmsg("pg_bulkload: STEP 1 OK")));
		add_prof(&tv_prepare);

		/*
		 * STEP 2: Build heap
		 */

		ci->ci_loader(ci, spools);

		/* If an error has been found, abort. */
		if (ci->ci_err_cnt)
		{
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
							errmsg("input file error (error count=%d)",
								   ci->ci_err_cnt)));
		}

		ereport(DEBUG1, (errmsg("pg_bulkload: STEP 2 OK")));

		/*
		 * STEP 3: Merge indexes
		 */

		if (spools != NULL)
		{
			IndexSpoolEnd(spools, ci->ci_estate->es_result_relation_info,
				true, ci->ci_loader == BufferedHeapLoad);
		}
		ereport(DEBUG1, (errmsg("pg_bulkload: STEP 3 OK")));

		/*
		 * STEP 4: Postprocessing
		 */

		/* Save the number of loaded tuples and release control info. */
		load_cnt = ci->ci_load_cnt;
		CloseControlInfo(ci);
		ci = NULL;

		ereport(DEBUG1, (errmsg("pg_bulkload: STEP 4 OK")));

		/* Write end log. */
		ereport(NOTICE,
				(errmsg("BULK LOAD END  (%d records)", load_cnt)));

		add_prof(&tv_cleanup);
		print_profile();

		PG_RETURN_INT32(load_cnt);
	}
	PG_CATCH();
	{
		CloseControlInfo(ci);
		PG_RE_THROW();
	}
	PG_END_TRY();
}

HeapTuple
ReadTuple(ControlInfo *ci, TransactionId xid, TransactionId cid)
{
	HeapTuple	tuple;

	if (ParserReadLine(ci->ci_parser, ci) == 0)
		return NULL;
	add_prof(&tv_read);

	tuple = heap_form_tuple(RelationGetDescr(ci->ci_rel),
							ci->ci_values, ci->ci_isnull);
	if (ci->ci_rel->rd_rel->relhasoids)
		HeapTupleSetOid(tuple, GetNewOid(ci->ci_rel));

	tuple->t_data->t_infomask &= ~(HEAP_XACT_MASK);
#if PG_VERSION_NUM >= 80300
	tuple->t_data->t_infomask2 &= ~(HEAP2_XACT_MASK);
#endif
	tuple->t_data->t_infomask |= HEAP_XMAX_INVALID;
	HeapTupleHeaderSetXmin(tuple->t_data, xid);
	HeapTupleHeaderSetCmin(tuple->t_data, cid);
	HeapTupleHeaderSetXmax(tuple->t_data, 0);
#if PG_VERSION_NUM < 80300
	HeapTupleHeaderSetCmax(tuple->t_data, 0);
#endif
	tuple->t_tableOid = ci->ci_rel->rd_id;
	add_prof(&tv_create_tup);

	return tuple;
}

/**
 * @brief Obtain data length from varlena.
 */
#define VARLEN(x)	(VARSIZE(x) - VARHDRSZ)

/**
 * @brief Obtain TEXT type augument as a character string of C-language.
 * @param fcinfo [in] Argument info (specify 'fcinfo')
 * @param index [in] Index of the argument
 * @return Returns argument character string.	Returns NULL if error occurs.
 */
static char *
get_text_arg(FunctionCallInfo fcinfo, int index)
{
	text	   *text_arg = PG_GETARG_TEXT_P(0);
	char	   *arg;

	arg = (char *) palloc(VARLEN(text_arg) + 1);
	memmove(arg, VARDATA(text_arg), VARLEN(text_arg));
	arg[VARLEN(text_arg)] = '\0';

	return arg;
}

#ifdef WIN32
#include "../../src/backend/utils/hash/pg_crc.c"
#endif
