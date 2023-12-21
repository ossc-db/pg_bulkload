/*
 * pg_bulkload: lib/writer_direct.c
 *
 *	  Copyright (c) 2007-2023, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

#include "pg_bulkload.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "access/heapam.h"
#include "access/transam.h"
#if PG_VERSION_NUM >= 130000
#include "access/heaptoast.h"
#else
#include "access/tuptoaster.h"
#endif
#include "access/xlog.h"
#include "catalog/catalog.h"
#include "catalog/namespace.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/fd.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "storage/bufpage.h"

#include "logger.h"
#include "pg_loadstatus.h"
#include "reader.h"
#include "writer.h"
#include "pg_btree.h"
#include "pg_profile.h"
#include "pg_strutil.h"
#include "pgut/pgut-be.h"

#if PG_VERSION_NUM >= 90300
#include "common/relpath.h"
#include "access/heapam_xlog.h"
#include "storage/checksum.h"
#include "storage/checksum_impl.h"
#endif

#if PG_VERSION_NUM >= 90500
#include "access/xloginsert.h"
#endif

#if PG_VERSION_NUM >= 100000
#include "utils/regproc.h"
#endif

#if PG_VERSION_NUM >= 90400

#define log_newpage(rnode, forknum, blk, page) \
	log_newpage(rnode, forknum, blk, page, true)

#elif PG_VERSION_NUM < 80400

#define toast_insert_or_update(rel, newtup, oldtup, options) \
	toast_insert_or_update((rel), (newtup), (oldtup), true, true)

#define log_newpage(rnode, forknum, blk, page) \
	log_newpage((rnode), (blk), (page))

#endif

/**
 *  * pg_tli is removed in 9.3 and added pg_checksum instead
 *   */
#if PG_VERSION_NUM >= 90300
#define PageSetTLI(page, tli) \
	(((PageHeader) (page))->pd_checksum = (uint16) (0))
#endif

/**
 * @brief Heap loader using direct path
 */
typedef struct DirectWriter
{
	Writer			base;

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

static void	DirectWriterInit(DirectWriter *self);
static void	DirectWriterInsert(DirectWriter *self, HeapTuple tuple);
static WriterResult	DirectWriterClose(DirectWriter *self, bool onError);
static bool	DirectWriterParam(DirectWriter *self, const char *keyword, char *value);
static void	DirectWriterDumpParams(DirectWriter *self);
static int	DirectWriterSendQuery(DirectWriter *self, PGconn *conn, char *queueName, char *logfile, bool verbose);

#define GetCurrentPage(self) \
			((Page) ((self)->blocks + BLCKSZ * (self)->curblk))
#define GetTargetPage(self, blk_offset) \
		((Page) ((self)->blocks + BLCKSZ * (blk_offset)))

/**
 * @brief Total number of blocks at the time
 */
#define LS_TOTAL_CNT(ls)	((ls)->ls.exist_cnt + (ls)->ls.create_cnt)

/* Signature of static functions */
static int	open_data_file(
#if PG_VERSION_NUM >= 160000
			RelFileLocator relNumber, 
#else
			RelFileNode rnode, 
#endif
			bool istemp, BlockNumber blknum);
static void	flush_pages(DirectWriter *loader);
static void	close_data_file(DirectWriter *loader);
static void	UpdateLSF(DirectWriter *loader, BlockNumber num);
static void UnlinkLSF(DirectWriter *loader);

/* ========================================================================
 * Implementation
 * ========================================================================*/

/**
 * @brief Create a new DirectWriter
 */
Writer *
CreateDirectWriter(void *opt)
{
	DirectWriter	   *self;

	self = palloc0(sizeof(DirectWriter));
	self->base.init = (WriterInitProc) DirectWriterInit;
	self->base.insert = (WriterInsertProc) DirectWriterInsert,
	self->base.close = (WriterCloseProc) DirectWriterClose,
	self->base.param = (WriterParamProc) DirectWriterParam;
	self->base.dumpParams = (WriterDumpParamsProc) DirectWriterDumpParams,
	self->base.sendQuery = (WriterSendQueryProc) DirectWriterSendQuery;
	self->base.max_dup_errors = -2;
	self->lsf_fd = -1;
	self->datafd = -1;
	self->blocks = palloc(BLCKSZ * BLOCK_BUF_NUM);
	self->curblk = 0;

	return (Writer *) self;
}

/**
 * @brief Initialize a DirectWriter
 */
static void
DirectWriterInit(DirectWriter *self)
{
	LoadStatus		   *ls;

	/*
	 * Set defaults to unspecified parameters.
	 */
	if (self->base.max_dup_errors < -1)
		self->base.max_dup_errors = DEFAULT_MAX_DUP_ERRORS;
#if PG_VERSION_NUM >= 130000
	self->base.rel = table_open(self->base.relid, AccessExclusiveLock);
#else
	self->base.rel = heap_open(self->base.relid, AccessExclusiveLock);
#endif
	VerifyTarget(self->base.rel, self->base.max_dup_errors);

	self->base.desc = RelationGetDescr(self->base.rel);

	SpoolerOpen(&self->spooler, self->base.rel, false, self->base.on_duplicate,
				self->base.max_dup_errors, self->base.dup_badfile);
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
	ls->ls.relid = self->base.relid;
#if PG_VERSION_NUM >= 160000
	ls->ls.relNumber = self->base.rel->rd_locator;
#else
	ls->ls.rnode = self->base.rel->rd_node;
#endif
	ls->ls.exist_cnt = RelationGetNumberOfBlocks(self->base.rel);
	ls->ls.create_cnt = 0;

	/*
	 * Create a load status file and write the initial status for it.
	 * At the time, if we find any existing load status files, exit with
	 * error because recovery process haven't been executed after failing
	 * load to the same table.
	 */
	BULKLOAD_LSF_PATH(self->lsf_path, ls);
#if PG_VERSION_NUM >= 110000
	self->lsf_fd = BasicOpenFilePerm(self->lsf_path,
		O_CREAT | O_EXCL | O_RDWR | PG_BINARY, S_IRUSR | S_IWUSR);
#else
	self->lsf_fd = BasicOpenFile(self->lsf_path,
		O_CREAT | O_EXCL | O_RDWR | PG_BINARY, S_IRUSR | S_IWUSR);
#endif
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

	self->base.tchecker = CreateTupleChecker(self->base.desc);
	self->base.tchecker->checker = (CheckerTupleProc) CoercionCheckerTuple;
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
#if PG_VERSION_NUM >= 130000
		tuple = heap_toast_insert_or_update(self->base.rel, tuple, NULL, 0);
#else
		tuple = toast_insert_or_update(self->base.rel, tuple, NULL, 0);
#endif
	BULKLOAD_PROFILE(&prof_writer_toast);

#if PG_VERSION_NUM < 120000
	/* Assign oids if needed. */
	if (self->base.rel->rd_rel->relhasoids)
	{
		Assert(!OidIsValid(HeapTupleGetOid(tuple)));
		HeapTupleSetOid(tuple, GetNewOid(self->base.rel));
	}
#endif

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
		RelationGetTargetPageFreeSpace(self->base.rel, HEAP_DEFAULT_FILLFACTOR))
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
	tuple->t_data->t_infomask2 &= ~(HEAP2_XACT_MASK);
	tuple->t_data->t_infomask |= HEAP_XMAX_INVALID;
	HeapTupleHeaderSetXmin(tuple->t_data, self->xid);
	HeapTupleHeaderSetCmin(tuple->t_data, self->cid);
	HeapTupleHeaderSetXmax(tuple->t_data, 0);

	/* put the tuple on local page. */
	offnum = PageAddItem(page, (Item) tuple->t_data,
		tuple->t_len, InvalidOffsetNumber, false, true);

	ItemPointerSet(&(tuple->t_self), LS_TOTAL_CNT(ls) + self->curblk, offnum);
	itemId = PageGetItemId(page, offnum);
	item = PageGetItem(page, itemId);
	((HeapTupleHeader) item)->t_ctid = tuple->t_self;

	BULKLOAD_PROFILE(&prof_writer_table);
	SpoolerInsert(&self->spooler, tuple);
	BULKLOAD_PROFILE(&prof_writer_index);
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

		if (self->base.rel)
#if PG_VERSION_NUM >= 130000
			table_close(self->base.rel, AccessExclusiveLock);
#else
			heap_close(self->base.rel, AccessExclusiveLock);
#endif

		if (self->blocks)
			pfree(self->blocks);

		pfree(self);
	}

	return ret;
}

static bool
DirectWriterParam(DirectWriter *self, const char *keyword, char *value)
{
	if (CompareKeyword(keyword, "TABLE") ||
		CompareKeyword(keyword, "OUTPUT"))
	{
		ASSERT_ONCE(self->base.output == NULL);

		self->base.relid = RangeVarGetRelid(makeRangeVarFromNameList(
#if PG_VERSION_NUM >= 160000
						stringToQualifiedNameList(value, NULL)), NoLock, false);
#else
						stringToQualifiedNameList(value), NoLock, false);
#endif
		self->base.output = get_relation_name(self->base.relid);
	}
	else if (CompareKeyword(keyword, "DUPLICATE_BADFILE"))
	{
		ASSERT_ONCE(self->base.dup_badfile == NULL);
		self->base.dup_badfile = pstrdup(value);
	}
	else if (CompareKeyword(keyword, "DUPLICATE_ERRORS"))
	{
		ASSERT_ONCE(self->base.max_dup_errors < -1);
		self->base.max_dup_errors = ParseInt64(value, -1);
		if (self->base.max_dup_errors == -1)
			self->base.max_dup_errors = INT64_MAX;
	}
	else if (CompareKeyword(keyword, "ON_DUPLICATE_KEEP"))
	{
		const ON_DUPLICATE values[] =
		{
			ON_DUPLICATE_KEEP_NEW,
			ON_DUPLICATE_KEEP_OLD
		};

		self->base.on_duplicate = values[choice(keyword, value, ON_DUPLICATE_NAMES, lengthof(values))];
	}
	else if (CompareKeyword(keyword, "TRUNCATE"))
	{
		self->base.truncate = ParseBoolean(value);
	}
	else
		return false;	/* unknown parameter */

	return true;
}

static void
DirectWriterDumpParams(DirectWriter *self)
{
	char		   *str;
	StringInfoData	buf;

	initStringInfo(&buf);

	appendStringInfoString(&buf, "WRITER = DIRECT\n");

	str = QuoteString(self->base.dup_badfile);
	appendStringInfo(&buf, "DUPLICATE_BADFILE = %s\n", str);
	pfree(str);

	if (self->base.max_dup_errors == INT64_MAX)
		appendStringInfo(&buf, "DUPLICATE_ERRORS = INFINITE\n");
	else
		appendStringInfo(&buf, "DUPLICATE_ERRORS = " int64_FMT "\n",
						 self->base.max_dup_errors);

	appendStringInfo(&buf, "ON_DUPLICATE_KEEP = %s\n",
					 ON_DUPLICATE_NAMES[self->base.on_duplicate]);

	appendStringInfo(&buf, "TRUNCATE = %s\n",
					 self->base.truncate ? "YES" : "NO");

	LoggerLog(INFO, buf.data, 0);
	pfree(buf.data);
}

static int
DirectWriterSendQuery(DirectWriter *self, PGconn *conn, char *queueName, char *logfile, bool verbose)
{
	const char *params[8];
	char		max_dup_errors[MAXINT8LEN + 1];

	if (self->base.max_dup_errors < -1)
		self->base.max_dup_errors = DEFAULT_MAX_DUP_ERRORS;

	snprintf(max_dup_errors, MAXINT8LEN, INT64_FORMAT,	
			 self->base.max_dup_errors);

	/* async query send */
	params[0] = queueName;
	params[1] = self->base.output;
	params[2] = ON_DUPLICATE_NAMES[self->base.on_duplicate];
	params[3] = max_dup_errors;
	params[4] = self->base.dup_badfile;
	params[5] = logfile;
	params[6] = verbose ? "true" : "no";
	params[7] = (self->base.truncate ? "true" : "no");

	return PQsendQueryParams(conn,
		"SELECT * FROM pgbulkload.pg_bulkload(ARRAY["
		"'TYPE=TUPLE',"
		"'INPUT=' || $1,"
		"'WRITER=DIRECT',"
		"'OUTPUT=' || $2,"
		"'ON_DUPLICATE_KEEP=' || $3,"
		"'DUPLICATE_ERRORS=' || $4,"
		"'DUPLICATE_BADFILE=' || $5,"
		"'LOGFILE=' || $6,"
		"'VERBOSE=' || $7,"
		"'TRUNCATE=' || $8])",
		8, NULL, params, NULL, NULL, 0);
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
	 * Log the first page that pg_bulkload adds to WAL to ensure the current
	 * XID will be recorded in xlog.
	 *
	 * In recovery mode, PostgreSQL recognizes the current XID which was
	 * already assigned by reading through the xlog.
	 *
	 * As for pg_bulkload, if the first page WAL entry were not recorded,
	 * PostgreSQL would not remember the XID being used for this loading.
	 * This may cause an inconsistent database state after recovery.
	 *
	 * For example,
	 * 1. pg_bulkload is started in XID=1111.
	 * 2. PostgreSQL process crashes during the loading.
	 * 3. PostgreSQL drops all existing connections and begins crash recovery
	 *    with xlog. If pg_bulkload had not logged the first page, PostgreSQL
	 *    would (wrongly) fail to recognize that 1111 has been used.
	 * 4. After recovery, a new transaction would get 1111 as XID. If that
	 *    transaction commits eventually, the data insufficiently loaded by
	 *    pg_bulkload would be incorrectly visible because the loaded data
	 *    would have the same XID.
	 *
	 * In order to prevent that, we arrange that the first page added by
	 * pg_bulkload is logged to WAL.
	 */
#if PG_VERSION_NUM >= 90100
	if (ls->ls.create_cnt == 0 && !RELATION_IS_LOCAL(loader->base.rel)
			&& !(loader->base.rel->rd_rel->relpersistence == RELPERSISTENCE_UNLOGGED) )
	{
		XLogRecPtr	recptr;

		recptr = log_newpage(
#if PG_VERSION_NUM >= 160000
				&ls->ls.relNumber, 
#else
				&ls->ls.rnode, 
#endif
				MAIN_FORKNUM,
			ls->ls.exist_cnt, loader->blocks);
		XLogFlush(recptr);
	}
#else
	if (ls->ls.create_cnt == 0 && !RELATION_IS_LOCAL(loader->base.rel) )
	{
		XLogRecPtr	recptr;

		recptr = log_newpage(&ls->ls.rnode, MAIN_FORKNUM,
			ls->ls.exist_cnt, loader->blocks);
		XLogFlush(recptr);
	}
#endif
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
			loader->datafd = open_data_file(
#if PG_VERSION_NUM >= 160000
											ls->ls.relNumber,
#else
											ls->ls.rnode,
#endif
											RELATION_IS_LOCAL(loader->base.rel),
											relblks);

		/* Number of blocks to be added to the current file. */
		flush_num = Min(num - i, RELSEG_SIZE - relblks % RELSEG_SIZE);
		Assert(flush_num > 0);

#if PG_VERSION_NUM >= 90300
		if (DataChecksumsEnabled())
		{
			Page	contained_page;
			int		j;

			/*
			 * Write checksum for pages that are going to be written to the
			 * current file.  We will be writing flush_num pages from the
			 * block buffer starting at block offset i.
			 */
			for (j = 0; j < flush_num; j++)
			{
				contained_page = GetTargetPage(loader, i + j);
				PageSetChecksumInplace(contained_page, LS_TOTAL_CNT(ls) + j);
			}
		}	
#endif

		/* Write the last block number to the load status file. */
		UpdateLSF(loader, flush_num);

		/*
		 * Write flush_num blocks to the current file starting at block
		 * offset i.  The current file might get full, ie, RELSEG_SIZE blocks
		 * full, after writing that much (see how flush_num is calculated
		 * above to understand why) .  We write the remaining content of the
		 * block buffer (ie, loader->blocks) in the new file during the next
		 * iteration.
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
open_data_file(
#if PG_VERSION_NUM >= 160000
				RelFileLocator relNumber, 
#else
				RelFileNode rnode, 
#endif
				bool istemp, BlockNumber blknum)
{
	int			fd = -1;
	int			ret;
	BlockNumber segno;
	char	   *fname = NULL;

#if PG_VERSION_NUM >= 90100
#if PG_VERSION_NUM >= 160000
	RelFileLocatorBackend	bknode;
	bknode.locator = relNumber;
#else
	RelFileNodeBackend	bknode;
	bknode.node = rnode;
#endif
	bknode.backend = istemp ? MyBackendId : InvalidBackendId;
	fname = relpath(bknode, MAIN_FORKNUM);
#else
	fname = relpath(rnode, MAIN_FORKNUM);
#endif
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
#if PG_VERSION_NUM >= 110000
	fd = BasicOpenFilePerm(fname, O_CREAT | O_WRONLY | PG_BINARY, S_IRUSR | S_IWUSR);
#else
	fd = BasicOpenFile(fname, O_CREAT | O_WRONLY | PG_BINARY, S_IRUSR | S_IWUSR);
#endif
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
void
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
