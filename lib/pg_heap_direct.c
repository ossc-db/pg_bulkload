/*
 * pg_heap_direct: lib/pg_heap_direct.c
 *
 *	  Copyright(C) 2007-2009, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

/**
 * @file
 * @brief Direct heap writer
 */
#include "postgres.h"

#include "access/heapam.h"
#include "catalog/catalog.h"
#include "miscadmin.h"
#include "storage/fd.h"

#include "pg_bulkload.h"
#include "pg_controlinfo.h"
#include "pg_loadstatus.h"

#if PG_VERSION_NUM < 80300
#define PageAddItem(page, item, size, offnum, overwrite, is_heap) \
	PageAddItem((page), (item), (size), (offnum), LP_USED)
#define toast_insert_or_update(rel, newtup, oldtup, options) \
	toast_insert_or_update((rel), (newtup), (oldtup))
#elif PG_VERSION_NUM < 80400
#define toast_insert_or_update(rel, newtup, oldtup, options) \
	toast_insert_or_update((rel), (newtup), (oldtup), true, true)
#endif

/**
 * @brief Heap loader using direct path
 */
typedef struct DirectLoader
{
	Loader			base;

	LoadStatus		ls;
	int				lsf_fd;		/**< File descriptor of load status file */
	char			lsf_path[MAXPGPATH];	/**< Load status file path */

	int				datafd;		/**< File descriptor of data file */

	char		   *blocks;		/**< Local heap block buffer */
	int				curblk;		/**< Index of the current block buffer */
} DirectLoader;

/**
 * @brief Number of the block buffer
 */
#define BLOCK_BUF_NUM		1024

/*
 * Prototype declaration for local functions.
 */

static void	DirectLoaderInit(DirectLoader *self, Relation rel);
static void	DirectLoaderInsert(DirectLoader *self, Relation rel, HeapTuple tuple);
static void	DirectLoaderTerm(DirectLoader *self, bool inError);

#define GetCurrentPage(self)	((Page) ((self)->blocks + BLCKSZ * (self)->curblk))

/**
 * @brief Total number of blocks at the time
 */
#define LS_TOTAL_CNT(ls)	((ls)->ls.exist_cnt + (ls)->ls.create_cnt)

/* Signature of static functions */
static int	open_data_file(RelFileNode rnode, LoadStatus *ls);
static void	flush_pages(DirectLoader *loader);
static void	close_data_file(DirectLoader *loader);
static void	UpdateLSF(DirectLoader *loader, BlockNumber num);
static void ValidateLSFDirectory(const char *path);

/* ========================================================================
 * Implementation
 * ========================================================================*/

/**
 * @brief Create a new DirectLoader
 */
Loader *
CreateDirectLoader(void)
{
	DirectLoader* self = palloc0(sizeof(DirectLoader));
	self->base.init = (LoaderInitProc) DirectLoaderInit;
	self->base.insert = (LoaderInsertProc) DirectLoaderInsert;
	self->base.term = (LoaderTermProc) DirectLoaderTerm;
	self->base.use_wal = false;
	self->datafd = -1;
	self->blocks = palloc(BLCKSZ * BLOCK_BUF_NUM);
	return (Loader *) self;
}

/**
 * @brief Create load status file
 * @param self [in/out] Load status information
 * @param rel [in] Relation for loading
 */
static void
DirectLoaderInit(DirectLoader *self, Relation rel)
{
	int			ret;
	LoadStatus *ls = &self->ls;

	/* Initialize first block */
	PageInit(GetCurrentPage(self), BLCKSZ, 0);
	PageSetTLI(GetCurrentPage(self), ThisTimeLineID);

	ValidateLSFDirectory(BULKLOAD_LSF_DIR);

	/*
	 * Initialize load status information
	 */
	ls->ls.relid = RelationGetRelid(rel);
	ls->ls.rnode = rel->rd_node;
	ls->ls.exist_cnt = RelationGetNumberOfBlocks(rel);
	ls->ls.create_cnt = 0;
	self->lsf_fd = -1;

	BULKLOAD_LSF_PATH(self->lsf_path, ls);

	/*
	 * Create a load status file and write the initial status for it.
	 * At the time, if we find any existing load status files, exit with error
	 * because recovery process haven't been executed after failing load to the
	 * same table.
	 */
	self->lsf_fd = BasicOpenFile(self->lsf_path,
					 O_CREAT | O_EXCL | O_RDWR, S_IRUSR | S_IWUSR);
	if (self->lsf_fd == -1)
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not create loadstatus file \"%s\": %m", self->lsf_path)));

	ret = write(self->lsf_fd, ls, sizeof(LoadStatus));
	if (ret != sizeof(LoadStatus))
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not write loadstatus file \"%s\": %m",
							   self->lsf_path)));
	if (pg_fsync(self->lsf_fd) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not fsync loadstatus file \"%s\": %m", self->lsf_path)));
}

/**
 * @brief Create LoadStatus file and load heap tuples directly.
 * @return void
 */
static void
DirectLoaderInsert(DirectLoader *self, Relation rel, HeapTuple tuple)
{
	Page			page;
	OffsetNumber	offnum;
	ItemId			itemId;
	Item			item;
	LoadStatus	   *ls = &self->ls;

	/*
	 * If we're gonna fail for oversize tuple, do it right away
	 */
	if (MAXALIGN(tuple->t_len) > MaxHeapTupleSize)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("row is too big: size %lu, maximum size %lu",
						(unsigned long) tuple->t_len,
						(unsigned long) MaxHeapTupleSize)));

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
	page = GetCurrentPage(self);
	if (PageGetFreeSpace(page) < MAXALIGN(tuple->t_len) +
		RelationGetTargetPageFreeSpace(rel, HEAP_DEFAULT_FILLFACTOR))
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

	/* put the tuple on local page. */
	offnum = PageAddItem(page, (Item) tuple->t_data,
		tuple->t_len, InvalidOffsetNumber, false, true);

	ItemPointerSet(&(tuple->t_self), LS_TOTAL_CNT(ls) + self->curblk, offnum);
	itemId = PageGetItemId(page, offnum);
	item = PageGetItem(page, itemId);
	((HeapTupleHeader) item)->t_ctid = tuple->t_self;
}

/**
 * @brief Clean up load status information
 *
 * @param self [in/out] Load status information
 * @param 
 * @return void
 */
static void
DirectLoaderTerm(DirectLoader *self, bool inError)
{
	/* Flush unflushed block buffer and close the heap file. */
	if (!inError)
		flush_pages(self);
	close_data_file(self);

	if (self->lsf_fd != -1)
	{
		close(self->lsf_fd);
		self->lsf_fd = -1;
		if (unlink(self->lsf_path) == -1)
			ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not unlink load status file: %m")));
	}

	if (self->blocks)
		pfree(self->blocks);
	pfree(self);
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
 * @param loader [in] Direct Loader.
 * @return File descriptor for the current data file.
 */
static void
flush_pages(DirectLoader *loader)
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
	 * Add WAL entry (only the first page).
	 * See backend/access/nbtree/nbtsort.c : _bt_blwritepage()
	 */
	if (ls->ls.create_cnt == 0)
	{
		XLogRecPtr	recptr;

		recptr = log_newpage(&ls->ls.rnode, MAIN_FORKNUM,
			ls->ls.exist_cnt, loader->blocks);

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
			loader->datafd = open_data_file(ls->ls.rnode, ls);

		/* Number of blocks to be added to the current file. */
		flush_num = Min(num, RELSEG_SIZE - relblks % RELSEG_SIZE);
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
				/*
				 * TODO: retry if recoverable error
				 */

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
 * @param rnode [in] RelFileNode of target relation.
 * @return File descriptor of the last data file.
 */
static int
open_data_file(RelFileNode rnode, LoadStatus *ls)
{
	int			fd = -1;
	int			ret;
	BlockNumber segno;
	char	   *fname = NULL;

	fname = relpath(rnode, MAIN_FORKNUM);
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
 * @param loader [in] Direct Loader.
 * @return void
 */
static void
close_data_file(DirectLoader *loader)
{
	if (loader->datafd != -1)
	{
		if (pg_fsync(loader->datafd) != 0)
			ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not sync data file: %m")));
		if (close(loader->datafd) < 0)
			ereport(ERROR, (errcode_for_file_access(),
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
UpdateLSF(DirectLoader *loader, BlockNumber num)
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
