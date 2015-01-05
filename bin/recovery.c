/*
 * pg_bulkload: bin/recovery.c
 *
 *	  Copyright (c) 2007-2015, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

/**
 *	@file
 *	@brief Recovery routine for pg_bulkload.
 */
#include "postgres_fe.h"

#include <dirent.h>
#include <fcntl.h>
#include <signal.h>
#include <time.h>
#include <sys/stat.h>
#ifndef WIN32
#include <sys/shm.h>
#endif

#include "pg_loadstatus.h"
#include "pgut/pgut.h"

#include "catalog/pg_control.h"
#include "catalog/pg_tablespace.h"
#include "nodes/pg_list.h"
#include "storage/bufpage.h"
#include "storage/pg_shmem.h"

/**
 * @brief length of ".loadstatus" file
 * (Used to search files whose names end with ".loadStatus".)
 */
#define LSFEXT 11

/*
 * Global variables
 */

extern char  *DataDir;

/*
 * Prototypes
 */

/* Entry point for the recovery. */
extern int	LoaderRecoveryMain(void);

static void GetSegmentPath(char path[MAXPGPATH], RelFileNode rnode, int segno);

/* Determins if the recovery is necessary, and then overwrites data file pages with the vacant one if needed. */
static void StartLoaderRecovery(void);

/* Test the existence of LSF and if exists, add its name to List. */
static List *GetLSFList(void);

/* Tests DBCluster status. */
static DBState GetDBClusterState(const char *fname);

/* Obtains load start block, end block, and the first data file name. */
static void GetLoadStatusInfo(const char *lsfpath, LoadStatus * ls);

/* Overwrite data pages with a vacant page. */
static void ClearLoadedPage(RelFileNode rnode,
							BlockNumber blkbeg,
							BlockNumber blkend);

/* Tests if the data block is constructed by this loader. */
static bool IsPageCreatedByLoader(Page page);

/* Deletes a lock file. */
static void LoaderUnlinkLockFile(const char *filename);

/* Creates a lock file. */
static void LoaderCreateLockFile(const char *filename,
								 bool amPostmaster,
								 bool isDDLock, const char *refName);

/* Check that the header fields of a page appear valid. */
bool PageHeaderIsValid(PageHeader page);


/**
 * @brief Entry point for recovery process
 *
 * @param  none
 * @return exitcode
 */
int
LoaderRecoveryMain(void)
{
	if (chdir(DataDir) < 0)
		elog(ERROR, "could not change directory to \"%s\"", DataDir);

	LoaderCreateLockFile("postmaster.pid", true, true, DataDir);
	StartLoaderRecovery();
	LoaderUnlinkLockFile("postmaster.pid");
	return 0;
}


/**
 * @brief judge necessisty of recovery and if necessary overwrite by blank pages.
 *
 * The folloing conditions must be satisfied when this function is called:
 *	 - postmaster/postgres process is not running.
 *	 - other recovery process is not running.
 * So when this function is called, LoaderCreateLockFile() must have been called previously and
 * a lock file has already created.
 *
 * After you call this funcation, you must call LoaderUnlinkLockFile()
 * for lock file deletion process.
 *
 * Processing flow
 * <ol>
 *	 <li> verify existence of load status file(LSF). </li>
 *	 <li> while LSF exists, the followings are looped. </li>
 *	   <ul>
 *		 <li> judge the status of database cluster. </li>
 *		 <li> if the database cluster has abnormally shutdowned
 *			  overwrite a range recorded in LSF by blank pages. </li>
 *		 <li> delete LSF.  </li>
 *	   </ul>
 *	 </li>
 * </ol>
 *
 * @param none
 * @return void
 *
 */
static void
StartLoaderRecovery(void)
{
	List	   *lsflist = NULL;
	ListCell   *cur;
	LoadStatus	ls;
	bool		need_recovery;

	/*
	 * verify DataDir
	 */
	Assert(DataDir != NULL);

	/*
	 * verify existence of load status file.
	 * need to free lsflist later.
	 */
	lsflist = GetLSFList();

	/*
	 * if lsflist is empty, need not to recovery by loader
	 */
	if (list_length(lsflist) == 0)
		return;

	need_recovery = GetDBClusterState("global/pg_control") != DB_SHUTDOWNED;

	/*
	 * while there are load status files, process recovery.
	 */
	foreach(cur, lsflist)
	{
		char	   *lsfname;
		char		lsfpath[MAXPGPATH];

		lsfname = (char *) lfirst(cur);

		snprintf(lsfpath, MAXPGPATH, BULKLOAD_LSF_DIR "/%s", lsfname);

		/*
		 * if database cluster has abnormally shutdown,
		 * start recovery of overwriting blank pages.
		 */
		if (need_recovery)
		{
			/*
			 * get contents of load status file.
			 */
			GetLoadStatusInfo(lsfpath, &ls);

			/*
			 * XXX :need to store relaion name?
			 */
			elog(NOTICE,
				 "Starting pg_bulkload recovery for file \"%s\"",
				 lsfname);

			/*
			 * overwrite pages created by the loader by blank pages
			 */
			ClearLoadedPage(ls.ls.rnode,
							ls.ls.exist_cnt,
							ls.ls.exist_cnt + ls.ls.create_cnt);

			elog(NOTICE,
				 "Ended pg_bulkload recovery for file \"%s\"",
				 lsfname);
		}

		/*
		 * delete load status file.
		 */
		if (unlink(lsfpath) != 0)
			elog(ERROR,
				 "could not delete loadstatus file \"%s\": %s",
				 lsfpath, strerror(errno));

		elog(NOTICE, "delete loadstatus file \"%s\"", lsfname);
	}

	list_free_deep(lsflist);

	/* revocery process succeeded */
	elog(NOTICE, "recovered all relations");
}


/**
 * @brief check existence of load status file.
 *
 * Processing flow
 * <ol>
 *	<li> initialize List which stores LSF. </li>
 *	<li> get path which stores LSF. </li>
 *	<li> loop the following processes.
 *	 <ul>
 *	  <li> check directory and find ".loadstatus" file. </li>
 *	  <li> if LSF exists, add the file name to List. </li>
 *	 </ul>
 *	</li>
 *	<li> return List. </li>
 * </ol>
 *
 * @param none
 * @return list structure which includes".loadstatus" file name
 */
static List *
GetLSFList(void)
{
	char	   *tmp;
	int			i,
				filelen;
	struct dirent *dp;
	List	   *list = NIL;
	DIR		   *dir;

	/*
	 * verify path of $PGDATA is not NULL
	 */
	Assert(DataDir != NULL);

	/*
	 * If neither $PGDATA/pg_bulkload directory nor ".loadstatus"
	 * file in it exists, we return the empty list and skip doing
	 * recovery. Otherwise, we add the name of found LSF into
	 * the list, then return it.
	 */
	if ((dir = opendir(BULKLOAD_LSF_DIR)) == NULL)
		return NIL;

	while ((dp = readdir(dir)) != NULL)
	{
		tmp = dp->d_name;
		filelen = strlen(dp->d_name);

		if (filelen > LSFEXT)
		{
			for (i = 0; i < (filelen - LSFEXT); i++)
				tmp++;

			if ((strcmp(tmp, ".loadstatus") == 0))
				list = lappend(list, strdup(dp->d_name));
		}
	}

	if (closedir(dir) == -1)
		elog(ERROR,
			 "could not close LSF Directory \"%s\": %s",
			 BULKLOAD_LSF_DIR, strerror(errno));

	return list;
}


/**
 * @brief check status of database cluster.
 *
 * Processing flow
 * <ol>
 *	<li> open pg_control file, and get path which stores pg_control file
 *		 to check status of database cluster.  </li>
 *	<li> open pg_control file. </li>
 *	<li> read pg_control file information,and store them to ControlFile structure. </li>
 *	<li> return status of database cluster(State) of ControlFile. </li>
 * </ol>
 *
 * @param  fname	control file path from $PGDATA.
 * @return status of database cluster
 */
static DBState
GetDBClusterState(const char *fname)
{
	int				fd;
	ControlFileData ControlFile;

	/*
	 * confirm path of $PGDATA is not NULL
	 */
	Assert(DataDir != NULL);

	/*
	 * open, read, and close ControlFileData
	 */
	if ((fd = open(fname, O_RDONLY | PG_BINARY, 0)) == -1)
		elog(ERROR,
			 "could not open control file \"%s\": %s",
			 fname, strerror(errno));

	if (read(fd, &ControlFile, sizeof(ControlFile)) != sizeof(ControlFile))
		elog(ERROR,
			 "could not read control file \"%s\": %s",
			 fname, strerror(errno));

	/* TODO: check CRC of the control file here. */

	if (close(fd) == -1)
		elog(ERROR,
			 "could not close control file \"%s\": %s",
			 fname, strerror(errno));

	return ControlFile.state;
}


/**
 * @brief Get number of blocks at loading start time,
 *	number of blocks made by pg_bulkload, and first data file name.
 *
 * Processing flow
 * <ol>
 *	<li> open LSF by checking path of LSG. </li>
 *	<li> read LSF, and store them to LoadStatus structure. </li>
 * </ol>
 *
 * @param lsfpath [in]	path of load status file
 * @param ls  [out] LoadStatus structure
 * @return void
 */
static void
GetLoadStatusInfo(const char *lsfpath, LoadStatus * ls)
{
	int			fd;
	int			read_len;

	Assert(lsfpath != NULL);

	/*
	 * open and read LSF
	 */
	if ((fd = open(lsfpath, O_RDONLY | PG_BINARY, 0)) == -1)
		elog(ERROR,
			 "could not open LoadStatusFile \"%s\": %s",
			 lsfpath, strerror(errno));

	read_len = read(fd, ls, sizeof(LoadStatus));
	if (read_len != sizeof(LoadStatus))
		elog(ERROR,
			 "could not read LoadStatusFile \"%s\": %s",
			 lsfpath, strerror(errno));

	if (close(fd) == -1)
		elog(ERROR,
			 "could not close LoadStatusFile \"%s\": %s",
			 lsfpath, strerror(errno));
}

/**
 * @brief Overwrite pages created by pg_bulkload by blank pages.
 *
 * Processing flow
 * <ol>
 *	<li> create blank page images. </li>
 *	<li> compute first and last block for recovery. </li>
 *	<li> get file name of load status file,
 *		 and open the data file. </li>
 *	<li> overwrite this area by blank pages.  </li>
 * </ol>
 * @param rnode  [in] Target relation
 * @param blkbeg [in] Where to begin zerofill (included)
 * @param blkend [in] Where to end zerofill (excluded)
 * @return void
 */
static void
ClearLoadedPage(RelFileNode rnode, BlockNumber blkbeg, BlockNumber blkend)
{
	BlockNumber segno;				/* data file segment no */
	char		segpath[MAXPGPATH];	/* data file name to open */
	char	   *page;				/* area to read blocks */
	Page		zeropage;			/* blank page */
	BlockNumber	blknum;				/* block no currently procesing */
	int			fd;					/* file descriptor */
	off_t		seekpos;			/* position of block to recovery */
	ssize_t		ret;				/* return value of read()  */
	ssize_t		readlen;			/* size of data read by read()	*/

	/* if no block is created by pg_bulkload, no work needed. */
	if (blkbeg <= blkend)
		return;

	/*
	 * Allocate buffer page and blank pages with malloc so that the buffers
	 * will be well-aligned.
	 */
	page = malloc(BLCKSZ);
	zeropage = (Page) malloc(BLCKSZ);
	PageInit(zeropage, BLCKSZ, 0);

	/*
	 * get file name of first file name from ls.
	 *	   open the file.
	 *	   if size of the file is over than 1 file segment size(default 1GB),
	 *	   set	proper extension.
	 */
	segno = blkbeg / RELSEG_SIZE;
	GetSegmentPath(segpath, rnode, segno);

	/*
	 * TODO: consider to use truncate instead of zero-fill to end of file.
	 */

	fd = open(segpath, O_RDWR | PG_BINARY, S_IRUSR | S_IWUSR);
	if (fd == -1)
		elog(ERROR,
			 "could not open data file \"%s\": %s",
			 segpath, strerror(errno));

	seekpos = lseek(fd, (blkbeg % RELSEG_SIZE) * BLCKSZ, SEEK_SET);

	if (seekpos == -1)
		elog(ERROR,
			 "could not seek the target position in the data file \"%s\": %s",
			 segpath, strerror(errno));

	blknum = blkbeg;

	/*
	 * pages created by pg_bulklod, overwrite them by blank pages.
	 */
	for (;;)
	{
		readlen = 0;
		ret = 0;

		/*
		 * to judge the page is created by pg_bulkload or not,
		 * read target blocks.
		 */
		do
		{
			ret = read(fd, page + readlen, BLCKSZ - readlen);
			if (ret == -1)
			{
				if (errno == EAGAIN || errno == EINTR)
					continue;
				else
					elog(ERROR,
						 "could not read data file \"%s\": %s",
						 segpath, strerror(errno));
			}
			else if (ret == 0)
			{
				/*
				 * case of partially writing, refill 0.
				 */
				memset(page + readlen, 0, BLCKSZ - readlen);
				ret = BLCKSZ - readlen;
			}
			readlen += ret;
		}
		while (readlen < BLCKSZ);


		/*
		 * if page is created by pg_bulkload, overwrite it by blank page.
		 */
		if (IsPageCreatedByLoader((Page) page))
		{
			seekpos = lseek(fd, (blknum % RELSEG_SIZE) * BLCKSZ, SEEK_SET);

			if (write(fd, zeropage, BLCKSZ) == -1)
				elog(ERROR,
					 "could not write correct empty page : %s",
					 strerror(errno));
		}

		blknum++;

		if (blknum >= blkend)
			break;

		/*
		 * if current block reach to the end of file, and need to process continuously,
		 * open next segment file.
		 */
		if (blknum % RELSEG_SIZE == 0)
		{
			if (fsync(fd) != 0)
				elog(ERROR,
					 "could not sync data file \"%s\": %s",
					 segpath, strerror(errno));

			if (close(fd) == -1)
				elog(ERROR,
					 "could not close data file \"%s\": %s",
					 segpath, strerror(errno));

			++segno;
			GetSegmentPath(segpath, rnode, segno);

			fd = open(segpath, O_RDWR | PG_BINARY, S_IRUSR | S_IWUSR);
			if (fd == -1)
				elog(ERROR,
					 "could not open data file \"%s\": %s",
					 segpath, strerror(errno));
		}
	}

	/*
	 * post process
	 */
	if (fsync(fd) != 0)
		elog(ERROR,
			 "could not sync data file \"%s\": %s",
			 segpath, strerror(errno));

	if (close(fd) == -1)
		elog(ERROR,
			 "could not close data file \"%s\": %s",
			 segpath, strerror(errno));
}

/**
 * @brief Is the page created by bulk loader?
 *
 * Processing flow
 * <ol>
 *	 <li> Validate the page header </li>
 *	 <li> Then check XLogReqPtr, the page with 0 is created by bulk loader. </li>
 * </ol>
 *
 * @param page [in] Page to be recovered
 * @return Return true if the page created by bulk loader else return false .
 */
static bool
IsPageCreatedByLoader(Page page)
{
	PageHeader	targetBlock = (PageHeader) page;

	if (!PageHeaderIsValid(targetBlock))
		return true;

	if (targetBlock->pd_lsn.xlogid == 0 && targetBlock->pd_lsn.xrecoff == 0)
		return true;
	else
		return false;
}

/*------------------------------------------------------------------------
 *	 The following codes are copied from PostgreSQL source code with some changes.
 *	   - UnlinkLockFile()		 Change the argument. Remove free(fname)
 *		  + backend/utils/init/miscinit.c:466
 *	   - CreateLockFile()		 Replace ereport with the message report function of bulk loader.
 *		  + backend/utils/init/miscinit.c:488
 *	   - PGSharedMemoryIsInUse() No change
 *		  + backend/port/pg_shmem.c:196
 *	   - PageInit()				 No change
 *		  + backend/storage/page/bufpage.c:30
 *	   - PageHeaderIsValid		 No change
 *		  + backend/storage/page/bufpage.c:68
 *
 *------------------------------------------------------------------------*/

/*-------------------------------------------------------------------------
 *				Interlock-file support
 *
 * These routines are used to create both a data-directory lockfile
 * ($DATADIR/postmaster.pid) and a Unix-socket-file lockfile ($SOCKFILE.lock).
 * Both kinds of files contain the same info:
 *
 *		Owning process' PID
 *		Data directory path
 *
 * By convention, the owning process' PID is negated if it is a standalone
 * backend rather than a postmaster.  This is just for informational purposes.
 * The path is also just for informational purposes (so that a socket lockfile
 * can be more easily traced to the associated postmaster).
 *
 * A data-directory lockfile can optionally contain a third line, containing
 * the key and ID for the shared memory block used by this postmaster.
 *
 * On successful lockfile creation, a proc_exit callback to remove the
 * lockfile is automatically created.
 *-------------------------------------------------------------------------
 */


/**
 * @brief proc_exit callback to remove a lockfile.
 *
 * Changes with the original source code.
 * <ol>
 *	 <li> Simplify the argument of the function. </li>
 *	 <li> Remove free(fname). </li>
 * </ol>
 */

/*
 * proc_exit callback to remove a lockfile.
 */
static void
LoaderUnlinkLockFile(const char *fname)
{
	if (fname != NULL)
	{
		if (unlink(fname) != 0)
		{
			/*
			 * Should we complain if the unlink fails?
			 */
		}
	}
}


/**
 * @brief  Create a lockfile.
 *
 * Changes from the original source code.
 * <ol>
 *	 <li> Use elog() to replace ereport(). </li>
 * </ol>
 */

/*
 * Create a lockfile.
 *
 * filename is the name of the lockfile to create.
 * amPostmaster is used to determine how to encode the output PID.
 * isDDLock and refName are used to determine what error message to produce.
 */
static void
LoaderCreateLockFile(const char *filename, bool amPostmaster,
					 bool isDDLock, const char *refName)
{
	int			fd;
	char		buffer[MAXPGPATH + 100];
	int			ntries;
	int			len;
	int			encoded_pid;
	pid_t		other_pid;
	pid_t		my_pid = getpid();

	/*
	 * We need a loop here because of race conditions.	But don't loop forever
	 * (for example, a non-writable $PGDATA directory might cause a failure
	 * that won't go away).  100 tries seems like plenty.
	 */
	for (ntries = 0;; ntries++)
	{
		/*
		 * Try to create the lock file --- O_EXCL makes this atomic.
		 *
		 * Think not to make the file protection weaker than 0600.	See
		 * comments below.
		 */
		fd = open(filename, O_RDWR | O_CREAT | O_EXCL | PG_BINARY, 0600);
		if (fd >= 0)
			break;				/* Success; exit the retry loop */

		/*
		 * Couldn't create the pid file. Probably it already exists.
		 */
		if ((errno != EEXIST && errno != EACCES) || ntries > 100)
			elog(FATAL,
				 "could not create lock file \"%s\": %s",
				 filename, strerror(errno));

		/*
		 * Read the file to get the old owner's PID.  Note race condition
		 * here: file might have been deleted since we tried to create it.
		 */
		fd = open(filename, O_RDONLY | PG_BINARY, 0600);
		if (fd < 0)
		{
			if (errno == ENOENT)
				continue;		/* race condition; try again */
			elog(FATAL,
				 "could not open lock file \"%s\": %s",
				 filename, strerror(errno));
		}
		if ((len = read(fd, buffer, sizeof(buffer) - 1)) < 0)
			elog(FATAL,
				 "could not read lock file \"%s\": %s",
				 filename, strerror(errno));
		close(fd);

		buffer[len] = '\0';
		encoded_pid = atoi(buffer);

		/*
		 * if pid < 0, the pid is for postgres, not postmaster
		 */
		other_pid = (pid_t) (encoded_pid < 0 ? -encoded_pid : encoded_pid);

		if (other_pid <= 0)
			elog(FATAL, "bogus data in lock file \"%s\": %s", filename, buffer);

		/*
		 * Check to see if the other process still exists
		 *
		 * If the PID in the lockfile is our own PID or our parent's PID, then
		 * the file must be stale (probably left over from a previous system
		 * boot cycle).  We need this test because of the likelihood that a
		 * reboot will assign exactly the same PID as we had in the previous
		 * reboot.	Also, if there is just one more process launch in this
		 * reboot than in the previous one, the lockfile might mention our
		 * parent's PID.  We can reject that since we'd never be launched
		 * directly by a competing postmaster.	We can't detect grandparent
		 * processes unfortunately, but if the init script is written
		 * carefully then all but the immediate parent shell will be
		 * root-owned processes and so the kill test will fail with EPERM.
		 *
		 * We can treat the EPERM-error case as okay because that error
		 * implies that the existing process has a different userid than we
		 * do, which means it cannot be a competing postmaster.  A postmaster
		 * cannot successfully attach to a data directory owned by a userid
		 * other than its own.	(This is now checked directly in
		 * checkDataDir(), but has been true for a long time because of the
		 * restriction that the data directory isn't group- or
		 * world-accessible.)  Also, since we create the lockfiles mode 600,
		 * we'd have failed above if the lockfile belonged to another userid
		 * --- which means that whatever process kill() is reporting about
		 * isn't the one that made the lockfile.  (NOTE: this last
		 * consideration is the only one that keeps us from blowing away a
		 * Unix socket file belonging to an instance of Postgres being run by
		 * someone else, at least on machines where /tmp hasn't got a
		 * stickybit.)
		 *
		 * Windows hasn't got getppid(), but doesn't need it since it's not
		 * using real kill() either...
		 *
		 * Normally kill() will fail with ESRCH if the given PID doesn't
		 * exist. BeOS returns EINVAL for some silly reason, however.
		 */
		if (other_pid != my_pid
#ifndef WIN32
			&& other_pid != getppid()
#endif
			)
		{
			if (kill(other_pid, 0) == 0 || (errno != ESRCH && errno != EPERM))
				/*
				 * lockfile belongs to a live process
				 */
				elog(FATAL, "lock file \"%s\" already exists.\n"
					"Is another postmaster (PID %d) running in data directory \"%s\"?",
					filename, (int) other_pid, refName);
		}

		/*
		 * No, the creating process did not exist.	However, it could be that
		 * the postmaster crashed (or more likely was kill -9'd by a clueless
		 * admin) but has left orphan backends behind.	Check for this by
		 * looking to see if there is an associated shmem segment that is
		 * still in use.
		 */
		if (isDDLock)
		{
			char	   *ptr;
			unsigned long id1,
						id2;

			ptr = strchr(buffer, '\n');
			if (ptr != NULL && (ptr = strchr(ptr + 1, '\n')) != NULL)
			{
				ptr++;
				if (sscanf(ptr, "%lu %lu", &id1, &id2) == 2)
				{
					if (PGSharedMemoryIsInUse(id1, id2))
						elog(FATAL,
							 "pre-existing shared memory block "
							 "(key %lu, ID %lu) is still in use "
							 "If you're sure there are no old "
							 "server processes still running, remove "
							 "the shared memory block with the command "
							 "\"ipcrm\", or just delete the file \"%s\".",
							 id1, id2, filename);
				}
			}
		}

		/*
		 * Looks like nobody's home.  Unlink the file and try again to create
		 * it.	Need a loop because of possible race condition against other
		 * would-be creators.
		 */
		if (unlink(filename) < 0)
			elog(FATAL,
				 "could not remove old lock file \"%s\": %s "
				 "The file seems accidentally left over, but "
				 "it could not be removed. Please remove the file "
				 "by hand and try again.",
				 filename, strerror(errno));
	}

	/*
	 * Successfully created the file, now fill it.
	 */
	snprintf(buffer, sizeof(buffer), "%d\n%s\n",
			 amPostmaster ? (int) my_pid : -((int) my_pid), DataDir);
	errno = 0;
	if (write(fd, buffer, strlen(buffer)) != strlen(buffer))
	{
		int			save_errno = errno;

		close(fd);
		unlink(filename);
		/*
		 * if write didn't set errno, assume problem is no disk space 
		 */
		errno = save_errno ? save_errno : ENOSPC;
		elog(FATAL,
			 "could not write lock file \"%s\": %s",
			 filename, strerror(errno));
	}
	if (close(fd))
	{
		int			save_errno = errno;

		unlink(filename);
		errno = save_errno;
		elog(FATAL,
			 "could not write lock file \"%s\": %s",
			 filename, strerror(errno));
	}
}

/**
 * @brief Initializes the contents of a pagee.
 *
 * Changes with the original source code.
 * <ol>
 *	 <li> None </li>
 * </ol>
 */

/*
 * PageInit
 *		Initializes the contents of a page.
 */
void
PageInit(Page page, Size pageSize, Size specialSize)
{
	PageHeader	p = (PageHeader) page;

	specialSize = MAXALIGN(specialSize);

	Assert(pageSize == BLCKSZ);
	Assert(pageSize > specialSize + SizeOfPageHeaderData);

	/*
	 * Make sure all fields of page are zero, as well as unused space
	 */
	MemSet(p, 0, pageSize);

	p->pd_lower = SizeOfPageHeaderData;
	p->pd_upper = pageSize - specialSize;
	p->pd_special = pageSize - specialSize;
	PageSetPageSizeAndVersion(page, pageSize, PG_PAGE_LAYOUT_VERSION);
}



 /**
 * @brief Page header validation.
 *
 * Changes with the original source code.
 * <ol>
 *	 <li> None </li>
 * </ol>
 */

/*
 * PageHeaderIsValid
 *		Check that the header fields of a page appear valid.
 *
 * This is called when a page has just been read in from disk.	The idea is
 * to cheaply detect trashed pages before we go nuts following bogus item
 * pointers, testing invalid transaction identifiers, etc.
 *
 * It turns out to be necessary to allow zeroed pages here too.  Even though
 * this routine is *not* called when deliberately adding a page to a relation,
 * there are scenarios in which a zeroed page might be found in a table.
 * (Example: a backend extends a relation, then crashes before it can write
 * any WAL entry about the new page.  The kernel will already have the
 * zeroed page in the file, and it will stay that way after restart.)  So we
 * allow zeroed pages here, and are careful that the page access macros
 * treat such a page as empty and without free space.  Eventually, VACUUM
 * will clean up such a page and make it usable.
 */
bool
PageHeaderIsValid(PageHeader page)
{
	char	   *pagebytes;
	int			i;

	/*
	 * Check normal case
	 */
	if (PageGetPageSize(page) == BLCKSZ &&
		PageGetPageLayoutVersion(page) == PG_PAGE_LAYOUT_VERSION &&
		page->pd_lower >= SizeOfPageHeaderData &&
		page->pd_lower <= page->pd_upper &&
		page->pd_upper <= page->pd_special &&
		page->pd_special <= BLCKSZ &&
		page->pd_special == MAXALIGN(page->pd_special))
		return true;

	/*
	 * Check all-zeroes case
	 */
	pagebytes = (char *) page;
	for (i = 0; i < BLCKSZ; i++)
	{
		if (pagebytes[i] != 0)
			return false;
	}
	return true;
}

static void
GetSegmentPath(char path[MAXPGPATH], RelFileNode rnode, int segno)
{
	if (rnode.spcNode == GLOBALTABLESPACE_OID)
	{
		/* Shared system relations live in {datadir}/global */
		snprintf(path, MAXPGPATH, "global/%u", rnode.relNode);
	}
	else if (rnode.spcNode == DEFAULTTABLESPACE_OID)
	{
		/* The default tablespace is {datadir}/base */
		snprintf(path, MAXPGPATH, "base/%u/%u",
					 rnode.dbNode, rnode.relNode);
	}
	else
	{
		/* All other tablespaces are accessed via symlinks */
		snprintf(path, MAXPGPATH, "pg_tblspc/%u/%u/%u",
					 rnode.spcNode, rnode.dbNode, rnode.relNode);
	}

	if (segno > 0)
	{
		size_t	len = strlen(path);
		snprintf(path + len, MAXPGPATH - len, ".%u", segno);
	}
}


/*
 * PGSharedMemoryIsInUse
 *
 * Is a previously-existing shmem segment still existing and in use?
 *
 * The point of this exercise is to detect the case where a prior postmaster
 * crashed, but it left child backends that are still running.	Therefore
 * we only care about shmem segments that are associated with the intended
 * DataDir.  This is an important consideration since accidental matches of
 * shmem segment IDs are reasonably common.
 */
#ifndef WIN32

/**
 * @brief Shared memory ID returned by shmget(2).
 * See the line 40, backend/port/pg_shmem.c.
 */
typedef int IpcMemoryId;

#ifdef SHM_SHARE_MMU
#define PG_SHMAT_FLAGS SHM_SHARE_MMU
#else
#define PG_SHMAT_FLAGS 0
#endif

bool
PGSharedMemoryIsInUse(unsigned long id1, unsigned long id2)
{
	IpcMemoryId shmId = (IpcMemoryId) id2;
	struct shmid_ds shmStat;

	struct stat statbuf;
	PGShmemHeader *hdr;

	/*
	 * We detect whether a shared memory segment is in use by seeing whether
	 * it (a) exists and (b) has any processes are attached to it.
	 */
	if (shmctl(shmId, IPC_STAT, &shmStat) < 0)
	{
		/*
		 * EINVAL actually has multiple possible causes documented in the
		 * shmctl man page, but we assume it must mean the segment no longer
		 * exists.
		 */
		if (errno == EINVAL)
			return false;

		/*
		 * EACCES implies that the segment belongs to some other userid, which
		 * means it is not a Postgres shmem segment (or at least, not one that
		 * is relevant to our data directory).
		 */
		if (errno == EACCES)
			return false;

		/*
		 * Otherwise, we had better assume that the segment is in use. The
		 * only likely case is EIDRM, which implies that the segment has been
		 * IPC_RMID'd but there are still processes attached to it.
		 */
		return true;
	}

	/*
	 * If it has no attached processes, it's not in use 
	 */
	if (shmStat.shm_nattch == 0)
		return false;

	/*
	 * Try to attach to the segment and see if it matches our data directory.
	 * This avoids shmid-conflict problems on machines that are running
	 * several postmasters under the same userid.  On Windows, which doesn't
	 * have useful inode numbers, we can't do this so we punt and assume there
	 * is a conflict.
	 */
	if (stat(DataDir, &statbuf) < 0)
		return true;			/* if can't stat, be conservative */

	hdr = (PGShmemHeader *) shmat(shmId, NULL, PG_SHMAT_FLAGS);

	if (hdr == (PGShmemHeader *) -1)
		return true;			/* if can't attach, be conservative */

	if (hdr->magic != PGShmemMagic ||
		hdr->device != statbuf.st_dev || hdr->inode != statbuf.st_ino)
	{
		/*
		 * It's either not a Postgres segment, or not one for my data
		 * directory.  In either case it poses no threat.
		 */
		shmdt((void *) hdr);
		return false;
	}

	/*
	 * Trouble --- looks a lot like there's still live backends 
	 */
	shmdt((void *) hdr);

	return true;
}

#else	/* WIN32 */
#include "pg_bulkload_win32.c"
#endif
