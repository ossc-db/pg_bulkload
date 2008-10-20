/*
 * pg_heap_direct: lib/pg_heap_direct.c
 *
 *	  Copyright(C) 2007-2008 NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

/**
 * @file
 * @brief Direct heap writer
 */
#include "postgres.h"

#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

#include "access/tuptoaster.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/fd.h"

#include "pg_bulkload.h"
#include "pg_btree.h"
#include "pg_controlinfo.h"
#include "pg_loadstatus.h"
#include "pg_profile.h"

#if PG_VERSION_NUM < 80300
#define PageAddItem(page, item, size, offnum, overwrite, is_heap) \
    PageAddItem((page), (item), (size), (offnum), LP_USED)
#define toast_insert_or_update(rel, newtup, oldtup, use_wal, use_fsm) \
	toast_insert_or_update((rel), (newtup), (oldtup))
#endif


/**
 * @brief Total number of blocks at the time
 */
#define LS_TOTAL_CNT(ls)	((ls)->ls_exist_cnt + (ls)->ls_create_cnt)

 /**
 * @brief Number of the block buffer
 */
#define BLOCK_BUF_NUM		1024

/* Signature of static functions */
static void direct_load(ControlInfo *ci, LoadStatus *ls, BTSpool **spools);
static int	open_data_file(ControlInfo *ci, LoadStatus *ls);
static int	flush_pages(int fd, ControlInfo *ci, char *blocks, int num, LoadStatus *ls);
static void	close_data_file(ControlInfo *ci, int fd);
static void	CreateLSF(LoadStatus *ls, Relation rel);
static void	UpdateLSF(LoadStatus *ls, BlockNumber num);
static void	CleanupLSF(LoadStatus *ls);

/* ========================================================================
 * Implementation
 * ========================================================================*/

/**
 * @brief Create LoadStatus file and load heap tuples directly.
 * @return void
 */
void
DirectHeapLoad(ControlInfo *ci, BTSpool **spools)
{
	LoadStatus ls = { 0 };

	PG_TRY();
	{
		CreateLSF(&ls, ci->ci_rel);
		direct_load(ci, &ls, spools);
		CleanupLSF(&ls);
	}
	PG_CATCH();
	{
		/* Remove the load status file if there is no fatal erros. */
		if (ci->ci_status >= 0)
			CleanupLSF(&ls);
		PG_RE_THROW();
	}
	PG_END_TRY();
}

/**
 * @brief Store tuples into the heap using local buffers.
 * @return void
 */
static void
direct_load(ControlInfo *ci, LoadStatus *ls, BTSpool **spools)
{
	int				i;
	TransactionId	xid;
	CommandId		cid;
	Size			saveFreeSpace;
	int				cur_block = 0;	/* Index of the current block buffer */
	OffsetNumber	cur_offset = FirstOffsetNumber;
	char		   *blocks = NULL;	/* Pointer to the local block buffer */
	int				datafd = -1;
	MemoryContext	org_ctx;

	/* Initialize block buffers. */
	blocks = (char *) palloc(BLCKSZ * BLOCK_BUF_NUM);
	for (i = 0; i < BLOCK_BUF_NUM; i++)
	{
		PageInit(blocks + BLCKSZ * i, BLCKSZ, 0);
		PageSetTLI(blocks + BLCKSZ * i, ThisTimeLineID);
	}

	/* Obtain transaction ID and command ID. */
	xid = GetCurrentTransactionId();
	cid = GetCurrentCommandId(true);
	saveFreeSpace = RelationGetTargetPageFreeSpace(ci->ci_rel,
												   HEAP_DEFAULT_FILLFACTOR);

	/* Switch into its memory context */
	org_ctx = MemoryContextSwitchTo(GetPerTupleMemoryContext(ci->ci_estate));

	/*
	 * Loop for each input file record.
	 * Allow errors upto specified number, so accumulate errors for each record.
	 */
	while (ci->ci_status == 0 && ci->ci_load_cnt < ci->ci_limit)
	{
		PG_TRY();
		{
			HeapTuple		tuple;
			OffsetNumber	off;
			ItemId			itemId;
			Item			item;

			CHECK_FOR_INTERRUPTS();

			/* Reset the per-tuple exprcontext */
			ResetPerTupleExprContext(ci->ci_estate);

			/*
			 * Read record data until EOF is encountered.	Because PG_TRY()
			 * is implemented using "do{} while (...);", goto statement is
			 * used to get out from the loop.	Goto target assumes org_ctx
			 * and so memory context is changed before the goto statement
			 * here.
			 */
			if ((tuple = ReadTuple(ci, xid, cid)) == NULL)
			{
				ci->ci_status = 1;
				goto record_proc_end;
			}

			/*
			 * Compress the tuple data as needed.	TOAST_TUPLE_THRESHOLD
			 * is one fourth of the block size.
			 */
			if (tuple->t_len > TOAST_TUPLE_THRESHOLD)
			{
				/* XXX: Better parameter for use_wal and use_fsm */
				tuple = toast_insert_or_update(ci->ci_rel, tuple, NULL, true, true);
				ereport(DEBUG1,
						(errmsg("tup compressed to %d", tuple->t_len)));
			}
			add_prof(&tv_compress);

			/*
			 * If a tuple does not fit to single block, it's an error. 
			 */
			if (MAXALIGN(tuple->t_len) > MaxHeapTupleSize)
				ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
								errmsg("tuple too long (%d byte)",
									   tuple->t_len)));

			/*
			 * Tuples are added from the end of each page, we have to determine
			 * the insersion point regarding alignment.   For this, we have to
			 * compare tuple size and the free area of a give page considering
			 * this alignment.
			 *
			 * We don't have to consider this alignment if free area of pages
			 * decreases in the unit of MAXIMUM_ALIGNOF.
			 *
			 * In the other cases, we have to consider this alignment to prevent
			 * incorrect page update.
			 *
			 * Here, we use MAXALIGN() macro to obtain tuple size with alignment
			 * and compare this with free are in this page.   We flush the buffer
			 * and obtain new block if sufficient free area is not found in the page.
			 */
			if (PageGetFreeSpace(blocks + BLCKSZ * cur_block) <
				MAXALIGN(tuple->t_len) + saveFreeSpace)
			{
				if (cur_block < BLOCK_BUF_NUM - 1)
					cur_block++;
				else
				{
					datafd = flush_pages(datafd, ci, blocks, cur_block + 1, ls);
					cur_block = 0;
				}

				cur_offset = FirstOffsetNumber;
				ereport(DEBUG2, (errmsg("current buffer# is %d", cur_block)));
			}
			add_prof(&tv_flush);

			/*
			 * We have obtained shared lock of the table.  We can increment
			 * the point to insert a tuple one by one.
			 */
			off = PageAddItem(blocks + BLCKSZ * cur_block,
					(Item) tuple->t_data, tuple->t_len, cur_offset, false, true);
			cur_offset = OffsetNumberNext(off);

			ItemPointerSet(&(tuple->t_self), LS_TOTAL_CNT(ls) + cur_block, off);
			itemId = PageGetItemId(blocks + BLCKSZ * cur_block, off);
			item = PageGetItem(blocks + BLCKSZ * cur_block, itemId);
			((HeapTupleHeader) item)->t_ctid = tuple->t_self;
			add_prof(&tv_add);

			/*
			 * Loading is complete when a tuple is added to a block buffer.
			 */
			ci->ci_load_cnt++;

			/*
			 * Accumulate info from created heap tuple to the index pool.
			 */
			ExecStoreTuple(tuple, ci->ci_slot, InvalidBuffer, false);
			IndexSpoolInsert(spools, ci->ci_slot, &(tuple->t_self), ci->ci_estate, true);

		  record_proc_end:
			;
		}
		PG_CATCH();
		{
			ErrorData	   *errdata;
			MemoryContext	ecxt;
			char			tplerrmsg[1024];

			ecxt = MemoryContextSwitchTo(GetPerTupleMemoryContext(ci->ci_estate));
			errdata = CopyErrorData();

			/* Clean up files when query is canceled. */
			switch (errdata->sqlerrcode)
			{
				case ERRCODE_ADMIN_SHUTDOWN:
				case ERRCODE_QUERY_CANCELED:
					if (datafd != -1)
					{
						close_data_file(ci, datafd);
						datafd = -1;
					}
					MemoryContextSwitchTo(ecxt);
					PG_RE_THROW();
					break;
			}

			/* Absorb general errors. */
			ci->ci_err_cnt++;
			strncpy(tplerrmsg, errdata->message, 1023);
			FlushErrorState();
			FreeErrorData(errdata);

			ereport(WARNING, (errmsg("BULK LOAD ERROR (row=%d, col=%d) %s",
									 ci->ci_read_cnt, ci->ci_field,
									 tplerrmsg)));

			/*
			 * Terminate if MAX_ERR_CNT has been reached.
			 */
			if (ci->ci_max_err_cnt > 0 && ci->ci_err_cnt >= ci->ci_max_err_cnt)
				ci->ci_status = 1;
		}
		PG_END_TRY();
	}

	ResetPerTupleExprContext(ci->ci_estate);
	MemoryContextSwitchTo(org_ctx);

	/* Flush unflushed block buffer and close the heap file. */
	if (cur_block > 0 || !PageIsEmpty(blocks + BLCKSZ * cur_block))
		datafd = flush_pages(datafd, ci, blocks, cur_block + 1, ls);

	if (datafd != -1)
	{
		close_data_file(ci, datafd);
		datafd = -1;
	}
	add_prof(&tv_flush);

	/*
	 * Release the block buffer
	 */
	pfree(blocks);
}

/**
 * @brief Write block buffer contents.	Number of block buffer to be
 * written is specified by num argument.
 *
 * Flow:
 * <ol>
 *	 <li>If no more space is available in the data file, switch to a new one.</li>
 *	 <li>Compute block number which can be written to the current file.</li>
 *	 <li>Save the last block number in the load status file.</li>
 *	 <li>Write to the current file.</li>
 *	 <li>Initialize the flushed block buffers.</li>
 *	 <li>If there are other data, write them too.</li>
 * </ol>
 *
 * @param fd [in] File descripter of the data file.
 * @param ci [in/out] Control Info.
 * @param blocks [in/out] Pointer to the block buffer.
 * @param num [in] Number of buffers to be written.
 * @return File descriptor for the current data file.
 */
static int
flush_pages(int fd, ControlInfo *ci, char *blocks, int num, LoadStatus *ls)
{
	int			i;
	int			ret;
	int			flush_num;		/* Number of blocks to be added to the current file. */

	/*
	 * Switch to the next file if the current file has been filled up.
	 */
	if ((LS_TOTAL_CNT(ls)) % RELSEG_SIZE == 0 && fd != -1)
	{
		close_data_file(ci, fd);
		fd = -1;
	}
	if (fd == -1)
		fd = open_data_file(ci, ls);

	/*
	 * Add WAL entry (only the first page).
	 * See backend/access/nbtree/nbtsort.c:278.
	 */
	if (ls->ls_create_cnt == 0)
	{
		/*
		 * We use the heap NEWPAGE record type for this
		 */
		xl_heap_newpage xlrec;
		XLogRecPtr	recptr;
		XLogRecData rdata[2];

		/*
		 * NO ELOG(ERROR) will belogged from here untill newpage op.
		 */
		START_CRIT_SECTION();

		xlrec.node = ci->ci_rel->rd_node;
		xlrec.blkno = ls->ls_exist_cnt; /* 0 origin */

		rdata[0].buffer = InvalidBuffer;
		rdata[0].data = (char *) &xlrec;
		rdata[0].len = SizeOfHeapNewpage;
		rdata[0].next = &(rdata[1]);

		rdata[1].buffer = InvalidBuffer;
		rdata[1].data = (char *) blocks;
		rdata[1].len = BLCKSZ;
		rdata[1].next = NULL;

		recptr = XLogInsert(RM_HEAP_ID, XLOG_HEAP_NEWPAGE, rdata);

		/*
		 * In nbtsort.c, the code looks as follows.   Here, because we need WAL
		 * pointer fo the page header in the recovery, this pointer is not upadted.
		 * TimeLineID has been set at block buffer initialization.	 It is not
		 * updated here.
		 */
		/*
		 * PageSetLSN(blocks, recptr);
		 * PageSetTLI(blocks, ThisTimeLineID);
		 */

		END_CRIT_SECTION();

		/*
		 * If postgres process, such as loader and COPY, is killed by "kill -9",
		 * database will be rewound to the last checkpoint and recovery will
		 * be performed using WAL.
		 *
		 * After the recovery, if there are xid's which have not been recorded
		 * to WAL, such xid's will be reused.
		 *
		 * However, in the loader and COPY, data file is actually updated and
		 * xid must not be reused.
		 *
		 * WAL entry with such xid can be added using XLogInsert().  However,
		 * such entries are not really written to the disk immediately.
		 * WAL entries are flushed to the disk by XLogFlush(), typically
		 * when a transaction is commited.	COPY prevents xid reuse by
		 * this method.
		 *
		 * In the case of the loader, xid reuse is avoided by calling
		 * XLogFlush() right after adding a WAL entry, before flushing
		 * data block to follow the WAL protocol.
		 */
		XLogFlush(recptr);
	}

	/*
	 * Obtain number of blocks can be written to the current file.
	 */
	flush_num = Min(num, RELSEG_SIZE - LS_TOTAL_CNT(ls) % RELSEG_SIZE);

	/*
	 * Write the last block number to the load status file.
	 */
	UpdateLSF(ls, flush_num);
	add_prof(&tv_write_lsf);

	/*
	 * Flush flush_num data block to the current file.
	 * Then the current file size becomes RELSEG_SIZE blocks.
	 */
	{
		int			write_len = 0;

		do
		{
			ret =
				write(fd, blocks + write_len, BLCKSZ * flush_num - write_len);
			if (ret == -1)
			{
				ci->ci_status = -1;
				ereport(ERROR, (errcode_for_file_access(),
								errmsg("could not write to data file: %m")));
			}
			write_len += ret;
		}
		while (write_len < BLCKSZ * flush_num);
	}

	/*
	 * Initialize block buffers which have been flushed to the data file.
	 */
	for (i = 0; i < flush_num; i++)
	{
		PageInit(blocks + BLCKSZ * i, BLCKSZ, 0);
		PageSetTLI(blocks + BLCKSZ * i, ThisTimeLineID);
	}

	add_prof(&tv_write_data);

	/*
	 * Flush block buffers if any buffers remain unflushed.
	 */
	if (flush_num < num)
		fd = flush_pages(fd, ci, blocks + BLCKSZ * flush_num, num - flush_num, ls);

	return fd;
}

/**
 * @brief Open the next data file and returns its descriptor.
 * @param ci [in] Control Info.
 * @return File descriptor of the last data file.
 */
static int
open_data_file(ControlInfo *ci, LoadStatus *ls)
{
	int			fd = -1;
	int			ret;
	BlockNumber segno;
	char	   *fname = NULL;

	fname = relpath(ci->ci_rel->rd_node, MAIN_FORKNUM);
	segno = LS_TOTAL_CNT(ls) / RELSEG_SIZE;
	if (segno > 0)
	{
		/*
		 * The length `+ 12' is taken from _mdfd_openmesg() in backend/storage/smgr/md.c.
		 */
		char	   *tmp = palloc(strlen(fname) + 12);

		sprintf(tmp, "%s.%u", fname, segno);
		pfree(fname);
		fname = tmp;
	}
	fd = BasicOpenFile(fname, O_CREAT | O_WRONLY | PG_BINARY, S_IRUSR | S_IWUSR);
	if (fd == -1)
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not open data file: %m")));
	ret = lseek(fd, BLCKSZ * ((LS_TOTAL_CNT(ls)) % RELSEG_SIZE), SEEK_SET);
	if (ret == -1)
	{
		close(fd);
		ereport(ERROR, (errcode_for_file_access(),
						errmsg
						("could not seek the end of the data file: %m")));
	}

	pfree(fname);

	return fd;
}

/**
 * @brief Flush and close the data file.
 * @param ci [in] Control Info.
 * @param fd [in] Data file.
 * @return void
 */
static void
close_data_file(ControlInfo *ci, int fd)
{
	if (pg_fsync(fd) != 0)
	{
		ci->ci_status = -1;
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not sync data file: %m")));
	}
	if (close(fd) < 0)
	{
		ci->ci_status = -1;
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not close data file: %m")));
	}
}

/*
 * The target of computing CRC is from ls_create_cnt to ls_datafname
 * (This includes the end of NUL character.)
 * The total size of target area is as following:
 *	- ls_create_cnt
 *	- ls_exist_cnt
 *	- length of ls_datafname + 1(for NUL character)
 */
#define CALC_CRC32(ls) \
do { \
	INIT_CRC32((ls)->ls_crc); \
	COMP_CRC32((ls)->ls_crc, (char *)&(ls)->ls_create_cnt, \
			(sizeof(BlockNumber) * 2) + \
			strlen((ls)->ls_datafname) + 1 \
	); \
	FIN_CRC32((ls)->ls_crc); \
} while (0);

/**
 * @brief Create load status file
 * @param rel [in] Relation for loading
 * @return Load status information
 */
static void
CreateLSF(LoadStatus *ls, Relation rel)
{
	int			ret;
	int			len;
	char	   *datafname;

	/*
	 * Initialize load status information
	 */
	ls->ls_exist_cnt = 0;
	ls->ls_create_cnt = 0;
	ls->ls_lsfname[0] = '\0';
	ls->ls_datafname[0] = '\0';
	ls->ls_fd = -1;

	/*
	 * Create load statul file name based on database OID and table OID.
	 */
	snprintf(ls->ls_lsfname, MAXPATHLEN,
			 "%s/pg_bulkload/%d.%d.loadstatus", DataDir, MyDatabaseId,
			 rel->rd_id);

	/*
	 * Get the first data file segment name.
	 * Note: We must release the area of file name returned by relpath() because
	 * it was palloc()'ed.
	 */
	datafname = relpath(rel->rd_node, MAIN_FORKNUM);
	strncpy(ls->ls_datafname, datafname, MAXPATHLEN - 1);
	pfree(datafname);

	/*
	 * Acquire the number of existing blocks.
	 */
	ls->ls_exist_cnt = RelationGetNumberOfBlocks(rel);

	/*
	 * Now that all the contents for computing CRC are decided, so do it.
	 */
	CALC_CRC32(ls);

	/*
	 * Create a load status file and write the initial status for it.
	 * At the time, if we find any existing load status files, exit with error
	 * because recovery process haven't been executed after failing load to the
	 * same table.
	 */
	ls->ls_fd = BasicOpenFile(ls->ls_lsfname,
					 O_CREAT | O_EXCL | O_RDWR, S_IRUSR | S_IWUSR);
	if (ls->ls_fd == -1)
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not create loadstatus file \"%s\": %m", ls->ls_lsfname),
						errhint("You might need to create directry \"%s/pg_bulkload\" in advance.", DataDir)));

	len = offsetof(LoadStatus, ls_datafname) + strlen(ls->ls_datafname) + 1;
	ret = write(ls->ls_fd, ls, len);
	if (ret != len)
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not write loadstatus file \"%s\": %m",
							   ls->ls_lsfname)));
	if (pg_fsync(ls->ls_fd) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not fsync loadstatus file \"%s\": %m", ls->ls_lsfname)));
}

/**
 * @brief Update load status file.
 * @param ls [in/out] Load status information
 * @param num [in] the number of blocks already written
 * @return void
 */
static void
UpdateLSF(LoadStatus *ls, BlockNumber num)
{
	int			ret;

	ls->ls_create_cnt += num;

	/*
	 * Computing CRC
	 */
	CALC_CRC32(ls);

	/*
	 * Write from CRC to data file name.
	 * We BasicOpenFile()'ed it with O_SYNC flag, so don't sync it.
	 */
	lseek(ls->ls_fd, 0, SEEK_SET);
	ret = write(ls->ls_fd, ls, offsetof(LoadStatus, ls_datafname));
	if (ret != offsetof(LoadStatus, ls_datafname))
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not write to \"%s\": %m",
							   ls->ls_lsfname)));
	if (pg_fsync(ls->ls_fd) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not fsync file \"%s\": %m", ls->ls_lsfname)));
}

/**
 * @brief Clean up load status information
 *
 * @param ls [in/out] Load status information
 * @return void
 */
static void
CleanupLSF(LoadStatus *ls)
{
	if (ls->ls_fd != -1)
	{
		close(ls->ls_fd);
		ls->ls_fd = -1;
		if (unlink(ls->ls_lsfname) == -1)
			ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not unlink load status file: %m")));
	}
}
