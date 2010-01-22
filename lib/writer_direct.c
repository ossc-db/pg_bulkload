/*
 * pg_bulkload: lib/writer_direct.c
 *
 *	  Copyright (c) 2007-2010, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

#include "postgres.h"

#include <fcntl.h>
#include <unistd.h>

#include "access/heapam.h"
#include "access/transam.h"
#include "access/tuptoaster.h"
#include "catalog/catalog.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/fd.h"
#include "utils/rel.h"

#include "logger.h"
#include "pg_loadstatus.h"
#include "writer.h"
#include "pg_btree.h"
#include "pg_profile.h"

#if PG_VERSION_NUM < 80300

#define toast_insert_or_update(rel, newtup, oldtup, options) \
	toast_insert_or_update((rel), (newtup), (oldtup))

static XLogRecPtr
log_newpage(RelFileNode *rnode, int fork, BlockNumber blkno, Page page)
{
	xl_heap_newpage xlrec;
	XLogRecPtr	recptr;
	XLogRecData rdata[2];

	/* NO ELOG(ERROR) from here till newpage op is logged */
	START_CRIT_SECTION();

	xlrec.node = *rnode;
	xlrec.blkno = blkno;

	rdata[0].data = (char *) &xlrec;
	rdata[0].len = SizeOfHeapNewpage;
	rdata[0].buffer = InvalidBuffer;
	rdata[0].next = &(rdata[1]);

	rdata[1].data = (char *) page;
	rdata[1].len = BLCKSZ;
	rdata[1].buffer = InvalidBuffer;
	rdata[1].next = NULL;

	recptr = XLogInsert(RM_HEAP_ID, XLOG_HEAP_NEWPAGE, rdata);

	PageSetLSN(page, recptr);
	PageSetTLI(page, ThisTimeLineID);

	END_CRIT_SECTION();

	return recptr;
}

#elif PG_VERSION_NUM < 80400

#define toast_insert_or_update(rel, newtup, oldtup, options) \
	toast_insert_or_update((rel), (newtup), (oldtup), true, true)

#define log_newpage(rnode, forknum, blk, page) \
	log_newpage((rnode), (blk), (page))

#endif

/**
 * @brief Heap loader using direct path
 */
typedef struct DirectWriter
{
	Writer			base;

	Relation		rel;
	Spooler			spooler;

	LoadStatus		ls;
	int				lsf_fd;		/**< File descriptor of load status file */
	char			lsf_path[MAXPGPATH];	/**< Load status file path */

	TransactionId	xid;
	CommandId		cid;

	int				datafd;		/**< File descriptor of data file */

	char		   *blocks;		/**< Local heap block buffer */
	int				curblk;		/**< Index of the current block buffer */
} DirectWriter;

/**
 * @brief Number of the block buffer
 */
#define BLOCK_BUF_NUM		1024

static void	DirectWriterInsert(DirectWriter *self, HeapTuple tuple);
static WriterResult	DirectWriterClose(DirectWriter *self, bool onError);
static void	DirectWriterDumpParams(DirectWriter *self);

#define GetCurrentPage(self)	((Page) ((self)->blocks + BLCKSZ * (self)->curblk))

/**
 * @brief Total number of blocks at the time
 */
#define LS_TOTAL_CNT(ls)	((ls)->ls.exist_cnt + (ls)->ls.create_cnt)

/* Signature of static functions */
static int	open_data_file(RelFileNode rnode, BlockNumber blknum);
static void	flush_pages(DirectWriter *loader);
static void	close_data_file(DirectWriter *loader);
static void	UpdateLSF(DirectWriter *loader, BlockNumber num);
static void UnlinkLSF(DirectWriter *loader);
static void ValidateLSFDirectory(const char *path);

/* ========================================================================
 * Implementation
 * ========================================================================*/

/**
 * @brief Initialize a new DirectWriter
 * Create load status file
 * @param rel [in] Relation for loading
 */
Writer *
CreateDirectWriter(Oid relid, ON_DUPLICATE on_duplicate, int64 max_dup_errors, char *dup_badfile)
{
	DirectWriter	   *self;
	LoadStatus		   *ls;

	self = palloc0(sizeof(DirectWriter));
	self->base.insert = (WriterInsertProc) DirectWriterInsert,
	self->base.close = (WriterCloseProc) DirectWriterClose,
	self->base.dumpParams = (WriterDumpParamsProc) DirectWriterDumpParams,
	self->base.count = 0;
	self->lsf_fd = -1;
	self->datafd = -1;
	self->blocks = palloc(BLCKSZ * BLOCK_BUF_NUM);
	self->curblk = 0;

	self->rel = heap_open(relid, AccessExclusiveLock);
	VerifyTarget(self->rel);

	SpoolerOpen(&self->spooler, self->rel, on_duplicate, false, max_dup_errors,
				dup_badfile);
	self->base.context = GetPerTupleMemoryContext(self->spooler.estate);

	/* Verify DataDir/pg_bulkload directory */
	ValidateLSFDirectory(BULKLOAD_LSF_DIR);

	/* Initialize first block */
	PageInit(GetCurrentPage(self), BLCKSZ, 0);
	PageSetTLI(GetCurrentPage(self), ThisTimeLineID);

	/* Obtain transaction ID and command ID. */
	self->xid = GetCurrentTransactionId();
	self->cid = GetCurrentCommandId(true);

	/*
	 * Initialize load status information
	 */
	ls = &self->ls;
	ls->ls.relid = relid;
	ls->ls.rnode = self->rel->rd_node;
	ls->ls.exist_cnt = RelationGetNumberOfBlocks(self->rel);
	ls->ls.create_cnt = 0;

	/*
	 * Create a load status file and write the initial status for it.
	 * At the time, if we find any existing load status files, exit with
	 * error because recovery process haven't been executed after failing
	 * load to the same table.
	 */
	BULKLOAD_LSF_PATH(self->lsf_path, ls);
	self->lsf_fd = BasicOpenFile(self->lsf_path,
		O_CREAT | O_EXCL | O_RDWR | PG_BINARY, S_IRUSR | S_IWUSR);
	if (self->lsf_fd == -1)
		ereport(ERROR, (errcode_for_file_access(),
			errmsg("could not create loadstatus file \"%s\": %m", self->lsf_path)));

	if (write(self->lsf_fd, ls, sizeof(LoadStatus)) != sizeof(LoadStatus) ||
		pg_fsync(self->lsf_fd) != 0)
	{
		UnlinkLSF(self);
		ereport(ERROR, (errcode_for_file_access(),
			errmsg("could not write loadstatus file \"%s\": %m", self->lsf_path)));
	}

	return (Writer *) self;
}

/**
 * @brief Create LoadStatus file and load heap tuples directly.
 * @return void
 */
static void
DirectWriterInsert(DirectWriter *self, HeapTuple tuple)
{
	Page			page;
	OffsetNumber	offnum;
	ItemId			itemId;
	Item			item;
	LoadStatus	   *ls = &self->ls;

	/* Compress the tuple data if needed. */
	if (tuple->t_len > TOAST_TUPLE_THRESHOLD)
		tuple = toast_insert_or_update(self->rel, tuple, NULL, 0);
	BULKLOAD_PROFILE(&prof_writer_toast);

	/* Assign oids if needed. */
	if (self->rel->rd_rel->relhasoids)
	{
		Assert(!OidIsValid(HeapTupleGetOid(tuple)));
		HeapTupleSetOid(tuple, GetNewOid(self->rel));
	}

	/* Assume the tuple has been toasted already. */
	if (MAXALIGN(tuple->t_len) > MaxHeapTupleSize)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("row is too big: size %lu, maximum size %lu",
						(unsigned long) tuple->t_len,
						(unsigned long) MaxHeapTupleSize)));

	/* Fill current page, or go to next page if the page is full. */
	page = GetCurrentPage(self);
	if (PageGetFreeSpace(page) < MAXALIGN(tuple->t_len) +
		RelationGetTargetPageFreeSpace(self->rel, HEAP_DEFAULT_FILLFACTOR))
	{
		if (self->curblk < BLOCK_BUF_NUM - 1)
			self->curblk++;
		else
		{
			flush_pages(self);
			self->curblk = 0;	/* recycle from first block */
		}

		page = GetCurrentPage(self);

		/* Initialize current block */
		PageInit(page, BLCKSZ, 0);
		PageSetTLI(page, ThisTimeLineID);
	}

	tuple->t_data->t_infomask &= ~(HEAP_XACT_MASK);
#if PG_VERSION_NUM >= 80300
	tuple->t_data->t_infomask2 &= ~(HEAP2_XACT_MASK);
#endif
	tuple->t_data->t_infomask |= HEAP_XMAX_INVALID;
	HeapTupleHeaderSetXmin(tuple->t_data, self->xid);
	HeapTupleHeaderSetCmin(tuple->t_data, self->cid);
	HeapTupleHeaderSetXmax(tuple->t_data, 0);
#if PG_VERSION_NUM < 80300
	HeapTupleHeaderSetCmax(tuple->t_data, 0);
#endif

	/* put the tuple on local page. */
	offnum = PageAddItem(page, (Item) tuple->t_data,
		tuple->t_len, InvalidOffsetNumber, false, true);

	ItemPointerSet(&(tuple->t_self), LS_TOTAL_CNT(ls) + self->curblk, offnum);
	itemId = PageGetItemId(page, offnum);
	item = PageGetItem(page, itemId);
	((HeapTupleHeader) item)->t_ctid = tuple->t_self;

	SpoolerInsert(&self->spooler, tuple);
}

/**
 * @brief Clean up load status information
 *
 * @param self [in/out] Load status information
 * @return void
 */
static WriterResult
DirectWriterClose(DirectWriter *self, bool onError)
{
	WriterResult	ret = { 0 };

	Assert(self != NULL);

	/* Flush unflushed block buffer and close the heap file. */
	if (!onError)
		flush_pages(self);

	close_data_file(self);
	UnlinkLSF(self);

	if (!onError)
	{
		SpoolerClose(&self->spooler);
		ret.num_dup_new = self->spooler.dup_new;
		ret.num_dup_old = self->spooler.dup_old;

		if (self->rel)
			heap_close(self->rel, AccessExclusiveLock);

		if (self->blocks)
			pfree(self->blocks);

		pfree(self);
	}

	return ret;
}

static void
DirectWriterDumpParams(DirectWriter *self)
{
	LoggerLog(INFO, "WRITER = DIRECT\n\n");
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
 *	 <li>If there are other data, write them too.</li>
 * </ol>
 *
 * @param loader [in] Direct Writer.
 * @return File descriptor for the current data file.
 */
static void
flush_pages(DirectWriter *loader)
{
	int			i;
	int			num;
	LoadStatus *ls = &loader->ls;

	num = loader->curblk;
	if (!PageIsEmpty(GetCurrentPage(loader)))
		num += 1;

	if (num <= 0)
		return;		/* no work */

	/*
	 * Add WAL entry (only the first page) to ensure the current xid will
	 * be recorded in xlog. We must flush some xlog records with XLogFlush()
	 * before write any data blocks to follow the WAL protocol.
	 *
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
	 */
	if (ls->ls.create_cnt == 0 && !loader->rel->rd_istemp)
	{
		XLogRecPtr	recptr;

#if PG_VERSION_NUM >= 80500
		char		reason[NAMEDATALEN + 30];

		snprintf(reason, sizeof(reason), "pg_bulkload on \"%s\"",
				 RelationGetRelationName(loader->rel));
		XLogReportUnloggedStatement(reason);
#endif

		recptr = log_newpage(&ls->ls.rnode, MAIN_FORKNUM,
			ls->ls.exist_cnt, loader->blocks);
		XLogFlush(recptr);
	}

	/*
	 * Write blocks. We might need to write multiple files on boundary of
	 * relation segments.
	 */
	for (i = 0; i < num;)
	{
		char	   *buffer;
		int			total;
		int			written;
		int			flush_num;
		BlockNumber	relblks = LS_TOTAL_CNT(ls);

		/* Switch to the next file if the current file has been filled up. */
		if (relblks % RELSEG_SIZE == 0)
			close_data_file(loader);
		if (loader->datafd == -1)
			loader->datafd = open_data_file(ls->ls.rnode, relblks);

		/* Number of blocks to be added to the current file. */
		flush_num = Min(num - i, RELSEG_SIZE - relblks % RELSEG_SIZE);
		Assert(flush_num > 0);

		/* Write the last block number to the load status file. */
		UpdateLSF(loader, flush_num);

		/*
		 * Flush flush_num data block to the current file.
		 * Then the current file size becomes RELSEG_SIZE self->blocks.
		 */
		buffer = loader->blocks + BLCKSZ * i;
		total = BLCKSZ * flush_num;
		written = 0;
		while (total > 0)
		{
			int	len = write(loader->datafd, buffer + written, total);
			if (len == -1)
			{
				/* fatal error, do not want to write blocks anymore */
				ereport(ERROR, (errcode_for_file_access(),
								errmsg("could not write to data file: %m")));
			}
			written += len;
			total -= len;
		}

		i += flush_num;
	}

	/*
	 * NOTICE: Be sure reset curblk to 0 and reinitialize recycled page
	 * if you will continue to use blocks.
	 */
}

/**
 * @brief Open the next data file and returns its descriptor.
 * @param rnode  [in] RelFileNode of target relation.
 * @param blknum [in] Block number to seek.
 * @return File descriptor of the last data file.
 */
static int
open_data_file(RelFileNode rnode, BlockNumber blknum)
{
	int			fd = -1;
	int			ret;
	BlockNumber segno;
	char	   *fname = NULL;

	fname = relpath(rnode, MAIN_FORKNUM);
	segno = blknum / RELSEG_SIZE;
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
	ret = lseek(fd, BLCKSZ * (blknum % RELSEG_SIZE), SEEK_SET);
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
 * @param loader [in] Direct Writer.
 * @return void
 */
static void
close_data_file(DirectWriter *loader)
{
	if (loader->datafd != -1)
	{
		if (pg_fsync(loader->datafd) != 0)
			ereport(WARNING, (errcode_for_file_access(),
						errmsg("could not sync data file: %m")));
		if (close(loader->datafd) < 0)
			ereport(WARNING, (errcode_for_file_access(),
						errmsg("could not close data file: %m")));
		loader->datafd = -1;
	}
}

/**
 * @brief Update load status file.
 * @param loader [in/out] Load status information
 * @param num [in] the number of blocks already written
 * @return void
 */
static void
UpdateLSF(DirectWriter *loader, BlockNumber num)
{
	int			ret;
	LoadStatus *ls = &loader->ls;

	ls->ls.create_cnt += num;

	lseek(loader->lsf_fd, 0, SEEK_SET);
	ret = write(loader->lsf_fd, ls, sizeof(LoadStatus));
	if (ret != sizeof(LoadStatus))
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not write to \"%s\": %m",
							   loader->lsf_path)));
	if (pg_fsync(loader->lsf_fd) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not fsync file \"%s\": %m", loader->lsf_path)));
}

static void
UnlinkLSF(DirectWriter *loader)
{
	if (loader->lsf_fd != -1)
	{
		close(loader->lsf_fd);
		loader->lsf_fd = -1;
		if (unlink(loader->lsf_path) < 0 && errno != ENOENT)
			ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not unlink load status file: %m")));
	}
}

/*
 * Check for LSF directory. If not exists, create it.
 */
static void
ValidateLSFDirectory(const char *path)
{
	struct stat	stat_buf;

	if (stat(path, &stat_buf) == 0)
	{
		/* Check for weird cases where it exists but isn't a directory */
		if (!S_ISDIR(stat_buf.st_mode))
			ereport(ERROR,
			(errmsg("pg_bulkload: required LSF directory \"%s\" does not exist",
							path)));
	}
	else
	{
		ereport(LOG,
				(errmsg("pg_bulkload: creating missing LSF directory \"%s\"", path)));
		if (mkdir(path, 0700) < 0)
			ereport(ERROR,
					(errmsg("could not create missing directory \"%s\": %m",
							path)));
	}
}
