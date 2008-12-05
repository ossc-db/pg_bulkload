/*
 * pg_bulkload: bin/pg_bulkload.c
 *
 *	  Copyright(C) 2007-2008 NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

/**
 *	@file
 *	@brief Initiator and recovery routine for the PostgreSQL high-speed loader.
 *
 *	Calls pg_bulkload() as a user-defined function and performs loading.
 *
 *	If -r option is specified, performs recovery to cancel inconveniences caused
 *	by errors in the previous loading.
 */
#include "postgres.h"

#undef PGDLLIMPORT
#define PGDLLIMPORT

#include <assert.h>
#include <dirent.h>
#include <fcntl.h>
#include <signal.h>
#include <time.h>
#include <unistd.h>
#include <sys/stat.h>
#ifndef WIN32
#include <sys/shm.h>
#endif

#include "catalog/pg_control.h"
#include "libpq-fe.h"
#include "storage/bufpage.h"
#include "storage/pg_shmem.h"

#include "pg_loadstatus.h"

/**
 * @brief My version string
 */
#define LOADER_VERSION "2.3.0"

/**
 * @brief shmat() flag used in PGSharedMemoryIsInUse.
 * Refer to the line 44, backend/port/pg_shmem.c.
 */
#ifdef SHM_SHARE_MMU
#define PG_SHMAT_FLAGS SHM_SHARE_MMU
#else
#define PG_SHMAT_FLAGS 0
#endif

/**
 * @brief Definition of Assert() macros as done in PosgreSQL.
 */
#ifdef Assert
#undef Assert
#endif
#ifdef USE_ASSERT_CHECKING
#define Assert(x) assert(x)
#else
#define Assert(x)
#endif

/**
 * @brief dirent struction for directory entry.
 */
typedef struct dirent Dirent;

/**
 * @brief length of ".loadstatus" file
 * (Used to search files whose names end with ".loadStatus".)
 */
#define LSFEXT 11

/*
 * Global variables
 */

/**
 * @brief Database cluster directory.
 */
char	   *DataDir;

/**
 * @brief argument obtained by getopt().
 */
extern char *optarg;

/**
 * @brief Index of the argument being handled by getopt().
 */
extern int	optind;

/**
 * @brief Error number being handled by bgetopt().
 */
extern int	opterr;

/**
 * @brief Variable to store an option which getopt() did not recognize.
 */
extern int	optopt;

/**
 * @brief Flag indicating interrupt.
 */
bool		intrflag;

/**
 * @brief Shared memory ID returned by shmget(2).
 * See the line 40, backend/port/pg_shmem.c.
 */
typedef int IpcMemoryId;

/**
 * @brief Area to read a block.
 */
char		page[BLCKSZ];

/**
 * @brief Vacant page.
 */
char		zeropage[BLCKSZ];

/**
 * @Flag to indicate that the recovery should be performed in the silent mode.
 */
bool		isSilentRecovery;

/*
 * Prototypes
 */

/* Show the useage of the high-speed loader. */
static void PrintUsage(void);

/* Show the version of the high-speed loader. */
static void PrintVersion(void);

/* Performs data loading. */
static int	LoaderLoadMain(const char *dbname,
						   const char *dbport,
						   const char *dbuser,
						   const char *dbpass, const char *ctlpath);

/* Set the flag to indicate that SIGINT or SIGTERM has been notofied. */
static void LoadInterrupt(void);

/* Entry point for the recovery. */
static void LoaderRecoveryMain(void);

/* Determins if the recovery is necessary, and then overwrites data file pages with the vacant one if needed. */
static void StartLoaderRecovery(void);

/* Test the existence of LSF and if exists, add its name to List. */
static List *GetLSFList(void);

/* Tests DBCluster status. */
static DBState GetDBClusterState(void);

/* Obtains load start block, end block, and the first data file name. */
static void GetLoadStatusInfo(const char *lsfpath, LoadStatus * lsinfo);

/* Initialize the List structure. */
static List *InitializeList(void);

/* Adds the load status file name to the List. */
static void AddListLSFName(List *list, const char *filename);

/* Overwrite data pages with a vacant page. */
static void ClearLoadedPage(LoadStatus * lsinfo);

/* Write a log message. */
static void LoaderLogMessage(bool cont, int elevel, const char *format, ...);

/* Tests if the data block is constructed by this loader. */
static bool IsPageCreatedByLoader(Page page);

/* Release all the resources in the List structure. */
static void CleanUpList(List *list);

/* Deletes a lock file. */
static void LoaderUnlinkLockFile(const char *filename);

/* Creates a lock file. */
static void LoaderCreateLockFile(const char *filename,
								 bool amPostmaster,
								 bool isDDLock, const char *refName);

/* Initializes the specified page. */
void		PageInit(Page page, Size pageSize, Size specialSize);

/* Tests the page header is valid. */
bool		PageHeaderIsValid(PageHeader page);

/* Tests if the shared memory is in use. */
bool		PGSharedMemoryIsInUse(unsigned long id1, unsigned long id2);

static const char *get_user_name(const char *progname);


/**
 * @brief Entry point for pg_bulkload command.
 *
 * Flow:
 * <ol>
 *	 <li> Parses command arguments. </li>
 *	 <li> Without -r option: Starts the loading. </li>
 *	 <li> With -r option: Starts the recovery. </li>
 * </ol>
 *
 * @param argc [in] Number of arguments.
 * @param argv [in] Argument list.
 * @return Returns zero if successful, 1 otherwise.
 */
int
main(int argc, char *argv[])
{
	int			exitcode;
	int			opt;
	const char *dbname = NULL;
	const char *dbport = NULL;
	const char *dbuser = NULL;
	const char *dbpass = NULL;
	char		ctlpath[MAXPGPATH];
	char		curpath[MAXPGPATH];
	bool		isDataLoad = true;
	const char *login_user;

	DataDir = NULL;
	isSilentRecovery = false;

	/*
	 * Surpress error message output by getopt().
	 */
	opterr = 0;

	/*
	 * Help message and version are handled at first.
	 */
	if (argc > 1)
	{
		if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0)
		{
			PrintUsage();
			return 0;
		}
		if (strcmp(argv[1], "--version") == 0 || strcmp(argv[1], "-V") == 0)
		{
			PrintVersion();
			return 0;
		}
	}

	/*
	 * Then, other options are handled by getopt().
	 */
	while ((opt = getopt(argc, argv, "rD:d:p:sU:W:")) != -1)
	{
		switch (opt)
		{
			case 'r':
				isDataLoad = false;
				break;

			case 'D':
				if ((DataDir = strdup(optarg)) == NULL)
				{
					fprintf(stderr, "can't duplicate input string '-D' : %s",
							strerror(errno));
					return 1;
				}
				break;

			case 'd':
				if ((dbname = strdup(optarg)) == NULL)
				{
					fprintf(stderr, "can't duplicate input string '-d' : %s",
							strerror(errno));
					return 1;
				}
				break;

			case 'p':
				if ((dbport = strdup(optarg)) == NULL)
				{
					fprintf(stderr, "can't duplicate input string '-p' : %s",
							strerror(errno));
					return 1;
				}
				break;

			case 's':
				isSilentRecovery = true;
				break;

			case 'U':
				if ((dbuser = strdup(optarg)) == NULL)
				{
					fprintf(stderr, "can't duplicate input string '-U' : %s",
							strerror(errno));
					return 1;
				}
				break;

			case 'W':
				if ((dbpass = strdup(optarg)) == NULL)
				{
					fprintf(stderr, "can't duplicate input string '-W' : %s",
							strerror(errno));
					return 1;
				}
				break;

			case '?':
				PrintUsage();
				return 1;

			case '-':
				PrintUsage();
				return 1;

			default:
				/*
				 * never reached
				 */
				PrintUsage();
				return 1;
		}
	}

	if (optind < argc)
	{
		int			pathlen;

		if (argv[optind][0] != '/')
		{
			if (getcwd(curpath, MAXPGPATH) == NULL)
			{
				fprintf(stderr, "too long control file path\n");
				return 1;
			}

			pathlen = snprintf(ctlpath, MAXPGPATH,
							   "%s/%s", curpath, argv[optind]);
		}
		else
			pathlen = snprintf(ctlpath, MAXPGPATH, "%s", argv[optind]);

		if (pathlen >= MAXPGPATH)
		{
			fprintf(stderr, "too long control file path\n");
			return 1;
		}

		optind++;
	}
	else
		ctlpath[0] = '\0';


	if (optind != argc)
	{
		fprintf(stderr, "illegal arguments : try pg_bulkload --help\n");
		return 1;
	}


	/*
	 * Determines data loading or recovery.
	 */
	if (isDataLoad)
	{
		/*
		 * No database cluster specification is needed for data loading.
		 */
		if (DataDir)
		{
			fprintf(stderr, "invalid option '-D' for data load\n");
			return 1;
		}
		if (isSilentRecovery)
		{
			fprintf(stderr, "invalid option '-s' for data load\n");
			return 1;
		}

		login_user = get_user_name("pg_bulkload");

		/*
		 * Sets default values.
		 */
		if (!dbname)
			dbname = getenv("PGDATABASE");
		if (!dbname)
			dbname = login_user;

		if (!dbport)
			dbport = getenv("PGPORT");
		if (!dbport)
			dbport = DEF_PGPORT_STR;	/* include/pg_config.h:38 */

		if (!dbuser)
			dbuser = getenv("PGUSER");
		if (!dbuser)
			dbuser = login_user;

		if (!dbpass)
			dbpass = getenv("PGPASSWORD");

		if (ctlpath[0] == '\0')
		{
			fprintf(stderr, "no control file path specified\n");
			return 1;
		}

		exitcode = LoaderLoadMain(dbname, dbport, dbuser, dbpass, ctlpath);
	}
	else
	{

		/**
		 * Determines database cluster directory.
		 */
		if (!DataDir)
		{
			if ((DataDir = getenv("PGDATA")) == NULL)
			{
				fprintf(stderr, "no $PGDATA specified\n");
				return 1;
			}
		}

		/*
		 * The length of the database cluster directory name should be short enough so that
		 * the length of LSF (load status file) full path name is not longer than MAXPGPATH,
		 * including the trailing '\0'.
		 *
		 * Considering the length for OID (max value of OID = max value of unsigned int =
		 * 4294967295), the max length of the load status file should be file name +
		 * 13chars("/pg_bulkload/") + 10chars (OID) + 1char (".") + 10chars(OID) +
		 * 11chars(".loadstatus").
		 */
		if (strlen(DataDir) + 45 + 1 > MAXPGPATH)
		{
			fprintf(stderr, "too long $PGDATA path length\n");
			return 1;
		}

		if (dbname)
		{
			fprintf(stderr, "invalid option '-d' for recovery\n");
			return 1;
		}

		if (dbport)
		{
			fprintf(stderr, "invalid option '-p' for recovery\n");
			return 1;
		}

		if (dbuser)
		{
			fprintf(stderr, "invalid option '-U' for recovery\n");
			return 1;
		}

		if (dbpass)
		{
			fprintf(stderr, "invalid option '-W' for recovery\n");
			return 1;
		}

		if (ctlpath[0] != '\0')
		{
			fprintf(stderr, "invalid argument 'control file' for recovery\n");
			return 1;
		}

		/*
		 * Performs recovery.
		 */
		LoaderRecoveryMain();
		exitcode = 0;
	}

	return exitcode;
}


/**
 * @brief Set flag indicating SIGINT and SIGTERM has been received.
 *
 * Flow
 * <ol>
 *	 <li>Set intrflag value to "true".	</li>
 * </ol>
 *
 * @param  None
 * @return None
 */
static void
LoadInterrupt(void)
{
	intrflag = true;
}

/**
 * @brief Show pg_bulkload usage.
 *
 * @param  None
 * @return None
 */
static void
PrintUsage(void)
{
	fprintf(stderr, "pg_bulkload is a bulk data loading tool for PostgreSQL\n"
			"\n"
			"Usage:\n"
			"Data Load : pg_bulkload [-d DBNAME] [-p PORT] [-U USER]"
			" [-W PASSWORD] control_file_path\n"
			"Recovery  : pg_bulkload -r [-D DATADIR]\n"
			"\n"
			"Options for data load:\n"
			"  -d DBNAME       database name\n"
			"  -p PORT         port number to listen on\n"
			"  -U USER         user name\n"
			"  -W PASSWORD     password\n"
			"If these options are omitted, "
			"the environment variables are used.\n"
			"And if the environment variables are omitted too, "
			"internal default values are used.\n"
			"\n"
			"Options for recovery:\n"
			"  -r              execute recovery\n"
			"  -D DATADIR      database directory\n\n"
			"If the -D option is omitted, "
			"the environment variable PGDATA is used.\n"
			"\n"
			"Other options:\n"
			"  --help, -?      show this help, then exit\n"
			"  --version, -V   output version information, then exit\n");
}


/**
 * @brief Show the version of pg_bulkload.
 *
 * @param  None
 * @return None
 */
static void
PrintVersion(void)
{
	fprintf(stderr, "pg_bulkload " LOADER_VERSION "\n");
}


/**
 * @brief Performs data loading.
 *
 * Invokes pg_bulkload() user-defined function giving the control file name
 * as specified in the command parameter, and performs data loading.
 *
 * Libpq protocol is used as an interface to PostgreSQL server.  Here, because
 * it is assumed that PostgreSQL server is running in the same host, local
 * connection (UNIX domain socket) is used.
 *
 * In pg_bulkload command, the whole loading is treated as a single transaction.
 * Because transaction control is not available within user-defined functions,
 * transaction control is done within pg_bulkload command.	Each pg_bulkload
 * command invokation includes only one pg_bulkload() function call.   THerefore,
 * one loading includes only one transaction.	We cannot commit the transaction
 * until all the data is loaded.
 *
 * Flow
 * <ol>
 *	 <li> Connect to PostgreSQL server using PQsetdbLogin. </li>
 *	 <li> Obtain file descriptor using PQsocket(). </li>
 *	 <li> Setup a signal handler for SIGINT and SIGTERM. </li>
 *	 <li> Perform pg_bulkload() user-defined function in asynchronoous mode so
 *		  that signal can be handled during the loading. </li>
 *	 <li> Performs the following in a loop. </li>
 *	   <ul>
 *		 <li> Wait for incoming data by select().	If SIGINT or SIGTERM is
 *			  received, tell the server to stop the loading by PGcancel(). </li>
 *		 <li> Call PQconsumeInput() and PQisBusy.  If the loading is complete,
 *			  break from the loop. </li>
 *	   </ul>
 *	 <li> Obtain the result by PQgetResult(). </li>
 *	 <li> Confirm the result by PQresultStatus(). </li>
 *	   <ul>
 *		 <li> If loading is not successful, write an error message and exit. </li>
 *	   </ul>
 *	 <li> Write the number of loaded records to stdout. </li>
 *	 <li> Disconnect from the server using PQfinish(). </li>
 * </ol>
 *
 * @param dbname  [in] Database name,
 * @param dbport  [in] Port number,
 * @param dbuser  [in] User name,
 * @param dbpass  [in] Password,
 * @param ctlpath [in] Control file path name.
 * @return None (number of the loaded records will be written to stdout).
 */
static int
LoaderLoadMain(const char *dbname,
			   const char *dbport, const char *dbuser,
			   const char *dbpass, const char *ctlpath)
{
	PGconn	   *conn = NULL;
	const char *paramValues[1];
	PGresult   *res = NULL;
	int			sock;
	int			sendres;
	fd_set		input_mask;

	intrflag = false;

	/*
	 * Connect to the server
	 */
	conn = PQsetdbLogin(NULL, dbport, NULL, NULL, dbname, dbuser, dbpass);

	if (!conn)
	{
		fprintf(stderr, "could not establish connection\n");
		exit(1);
	}

	if (PQstatus(conn) != CONNECTION_OK)
		goto err_pqfinish;


	sock = PQsocket(conn);
	if (sock < 0)
		goto err_pqfinish;

	FD_ZERO(&input_mask);
	FD_SET(sock, &input_mask);

	if (!FD_ISSET(sock, &input_mask))
	{
		fprintf(stderr, "could not monitor file discriptor: %s\n",
				strerror(errno));
		goto err_pqfinish;
	}

	/*
	 * Setup signal handers.
	 */
	signal(SIGINT, (void *) LoadInterrupt);
	signal(SIGTERM, (void *) LoadInterrupt);

	res = PQexec(conn, "BEGIN");
	if (!res || PQresultStatus(res) != PGRES_COMMAND_OK)
		goto err_pqfinish;

	PQclear(res);
	res = NULL;

	/*
	 * Performs pg_bulkload() user-defined function.
	 * Use PGsendQueryParams(), not PQexec(), to perform asynchronously.
	 */
	paramValues[0] = ctlpath;
	sendres = PQsendQueryParams(conn,
								"SELECT pg_bulkload($1)",
								1, NULL, paramValues, NULL, NULL, 0);
	if (!sendres)
		goto err_rollback;

	/*
	 * Loop until the loading is complete.
	 * We can receive signals during this loop.
	 */
	do
	{
		if (select(sock + 1, &input_mask, NULL, NULL, NULL) < 0)
		{
			if (errno == EINTR)
			{
				if (intrflag == true)
				{
					PGcancel   *cancel;

					/*
					 * Recomended value of errbuf size is 256 bytes.
					 * (See interfaces/libpq:2381)
					 */
					char		errbuf[256];

					cancel = PQgetCancel(conn);
					if (!cancel)
						goto err_rollback;

					if (!PQcancel(cancel, errbuf, 256))
					{
						PQfreeCancel(cancel);
						goto err_rollback;
					}

					PQfreeCancel(cancel);

					intrflag = false;
				}
				continue;
			}
			else
			{
				/*
				 * error number other tha EINTR is not the normal status.	Error exit.
				 */
				fprintf(stderr, "unexpected error : %s\n", strerror(errno));
				goto err_rollback;
			}
		}

		/* Receive an input from the server. */
		if (!PQconsumeInput(conn))
			goto err_rollback;

	} while (PQisBusy(conn));

	/* Receive asynchronous results */
	res = PQgetResult(conn);
	if (!res || PQresultStatus(res) != PGRES_TUPLES_OK)
		goto err_rollback;
	PQclear(res);

	res = PQexec(conn, "COMMIT");
	if (!res || PQresultStatus(res) != PGRES_COMMAND_OK)
		goto err_rollback;
	PQclear(res);

	/*
	 * Successful
	 */
	PQfinish(conn);
	return 0;

	/*
	 * Error handling
	 */
	/*
	 * err_rollback : If error occurd between BEGIN and COMMIT, rollback the transaction and terminate.  :
	 */
err_rollback:
	fprintf(stderr, "%s", PQerrorMessage(conn));
	PQclear(res);

	if (PQstatus(conn) == CONNECTION_OK)
	{
		res = PQexec(conn, "ROLLBACK");
		PQfinish(conn);
	}
	return 1;

	/*
	 * If error occured other than err_pqfinish, here's the error exit.
	 */
err_pqfinish:
	fprintf(stderr, "%s", PQerrorMessage(conn));
	PQclear(res);

	PQfinish(conn);
	return 1;
}

/**
 * @brief Entry point for recovery process
 *
 * Flow of recovery process
 * <ol>
 *	 <li> create a lock file not to execute postmaster/postgres or other recovery processes. </li>
 *	 <li> call a recovery process main routine. </li>
 *	 <li> delete the lock file. </li>
 * </ol>
 *
 * @param  none
 * @return void
 */
static void
LoaderRecoveryMain(void)
{
	/*
	 * change working directory to DataDir
	 */
	if (chdir(DataDir) < 0)
		LoaderLogMessage(false, ERROR,
						 "could not change directory to \"DataDir\"");

	/*
	 * create lock file.
	 */
	LoaderCreateLockFile("postmaster.pid", true, true, DataDir);

	/*
	 * recovery process.
	 */
	StartLoaderRecovery();

	/*
	 * delete lock file
	 */
	LoaderUnlinkLockFile("postmaster.pid");
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
	LoadStatus	lsinfo;
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
	if (lsflist->length == 0)
		return;

	need_recovery = GetDBClusterState() != DB_SHUTDOWNED;

	/*
	 * while there are load status files, process recovery.
	 */
	foreach(cur, lsflist)
	{
		char	   *lsfname;
		char		lsfpath[MAXPGPATH];

		lsfname = (char *) lfirst(cur);

		snprintf(lsfpath, MAXPGPATH, "pg_bulkload/%s", (char *) lfirst(cur));
		lsfpath[MAXPGPATH - 1] = '\0';


		/*
		 * if database cluster has abnormally shutdown,
		 * start recovery of overwriting blank pages.
		 */
		if (need_recovery)
		{
			/*
			 * get contents of load status file.
			 */
			GetLoadStatusInfo(lsfpath, &lsinfo);

			/*
			 * XXX :need to store relaion name?
			 */
			if (!isSilentRecovery)
				LoaderLogMessage(true, NOTICE,
								 "Starting pg_bulkload recovery for file \"%s\"",
								 lsfname);

			/*
			 * overwrite pages created by the loader by blank pages
			 */
			ClearLoadedPage(&lsinfo);

			if (!isSilentRecovery)
				LoaderLogMessage(true, NOTICE,
								 "Ended pg_bulkload recovery for file \"%s\"",
								 lsfname);
		}

		/*
		 * delete load status file.
		 */
		if (unlink(lsfpath) != 0)
			LoaderLogMessage(false, ERROR,
							 "could not delete loadstatus file \"%s\" : %s",
							 lsfpath, strerror(errno));

		if (!isSilentRecovery)
			LoaderLogMessage(true, LOG,
							 "delete loadstatus file \"%s\"", lsfname);
	}

	CleanUpList(lsflist);

	if (!isSilentRecovery)
		LoaderLogMessage(true, LOG, "recovered all relations");
	return;						/* revocery process successfully terminated, */
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
	Dirent	   *dp;
	List	   *list;
	DIR		   *dir;

	/*
	 * initialize List structore
	 */
	list = InitializeList();

	/*
	 * verify path of $PGDATA is not NULL
	 */
	Assert(DataDir != NULL);

	/*
	 * check $PGDATA/pg_bulkload/ directory
	 *	   and find files whose name end with ".loadstatus".
	 *	   if exists, add file name to List.
	 */
	if ((dir = opendir("pg_bulkload")) == NULL)
		LoaderLogMessage(false, ERROR,
						 "could not open LSF Directory \"pg_bulkload/\" : %s",
						 strerror(errno));

	while ((dp = readdir(dir)) != NULL)
	{
		tmp = dp->d_name;
		filelen = strlen(dp->d_name);

		if (filelen > LSFEXT)
		{
			for (i = 0; i < (filelen - LSFEXT); i++)
				tmp++;

			if ((strcmp(tmp, ".loadstatus") == 0))
				AddListLSFName(list, dp->d_name);
		}
	}

	if (closedir(dir) == -1)
		LoaderLogMessage(false, ERROR,
						 "could not close LSF Directory \"pg_bulkload/\" : %s",
						 strerror(errno));

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
 *	<li> check CRC to verify pg_control file. </li>
 *	<li> return status of database cluster(State) of ControlFile. </li>
 * </ol>
 *
 * @param  none
 * @return status of database cluster
 */
static DBState
GetDBClusterState(void)
{
	int			fd;
	pg_crc32	crc;
	ControlFileData ControlFile;

	/*
	 * confirm path of $PGDATA is not NULL
	 */
	Assert(DataDir != NULL);

	/*
	 * open, read, and close ControlFileData
	 */
	if ((fd = open("global/pg_control", O_RDONLY, 0)) == -1)
		LoaderLogMessage(false, ERROR,
						 "could not open Control File \"global/pg_control\" : %s",
						 strerror(errno));

	if ((read(fd, &ControlFile,
			  sizeof(ControlFileData))) != sizeof(ControlFileData))
		LoaderLogMessage(false, ERROR,
						 "could not read Control File \"global/pg_control\" : %s",
						 strerror(errno));

	if (close(fd) == -1)
		LoaderLogMessage(false, ERROR,
						 "could not close Control File \"global/pg_control\" : %s",
						 strerror(errno));

	/*
	 * CRC Check
	 */
	INIT_CRC32(crc);
	COMP_CRC32(crc, &ControlFile, offsetof(ControlFileData, crc));

	FIN_CRC32(crc);

	if (!EQ_CRC32(crc, ControlFile.crc))
		LoaderLogMessage(false, ERROR,
						 "Control File : CRC Check Error. "
						 "Calculated CRC checksum does not match value stored in file. "
						 "Either the file is corrupt, or it has a different layout "
						 "than this program is expecting.");

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
 *	<li> check CRC to verify LSF. </li>
 * </ol>
 *
 * @param lsfpath [in]	path of load status file
 * @param lsinfo  [out] LoadStatus structure
 * @return void
 */
static void
GetLoadStatusInfo(const char *lsfpath, LoadStatus * lsinfo)
{
	int			fd;
	pg_crc32	crc;
	int			read_len;

	Assert(lsfpath != NULL);

	/*
	 * open and read LSF
	 */
	if ((fd = open(lsfpath, O_RDONLY, 0)) == -1)
		LoaderLogMessage(false, ERROR,
						 "could not open LoadStatusFile \"%s\" : %s",
						 lsfpath, strerror(errno));

	read_len = read(fd, lsinfo, sizeof(LoadStatus));
	if (read_len < sizeof(pg_crc32))
	{
		LoaderLogMessage(false, ERROR,
						 "could not read LoadStatusFile \"%s\" : %s",
						 lsfpath, strerror(errno));
	}

	if (close(fd) == -1)
		LoaderLogMessage(false, ERROR,
						 "could not close LoadStatusFile \"%s\" : %s",
						 lsfpath, strerror(errno));

	/*
	 * CRC Check
	 */
	INIT_CRC32(crc);
	COMP_CRC32(crc, ((char *) lsinfo) + sizeof(pg_crc32),
			   read_len - sizeof(pg_crc32));
	FIN_CRC32(crc);

	/*
	 * CRC Check Error
	 */
	if (!EQ_CRC32(crc, lsinfo->ls_crc))
		LoaderLogMessage(false, ERROR,
						 "LoadStatusFile : CRC Check Error. "
						 "Calculated CRC checksum does not match value stored in file. "
						 "Either the file is corrupt, or it has a different layout "
						 "than this program is expecting.");
}

/**
 * @brief Allocate List structure and initialize members to use List.
 *
 * caller must release memory area allocated in this function.
 *
 * Processing flow
 * <ol>
 *	<li> allocate memory area to create List. </li>
 *	<li> set initial value to members of the List. </li>
 *	<li> return List structure. </li>
 * </ol>
 *
 * @param none
 * @return List structure
 */
static List *
InitializeList(void)
{
	List	   *new_list;

	new_list = (List *) malloc(sizeof(List));
	if (new_list == NULL)
		LoaderLogMessage(false, ERROR,
						 "not enough memory available to proceed");

	new_list->type = T_Invalid;
	new_list->length = 0;
	new_list->head = NULL;
	new_list->tail = NULL;

	return new_list;
}


/**
 * @brief add load status file name to List(stored in ListCell).
 *
 * Caller must release allocated memory area.
 *
 * Processing flow
 * <ol>
 *	<li> Allocate memoery area to create ListCell which is added to List. </li>
 *	<li> Add LSF name to ListCell. </li>
 *	<li> If first addition of LSF name, set type of List(Type).</li>
 *	<li> If LSF has been in the List, chain it to the List.
 * </ol>
 * @param list [in/out] list structure to add file name
 * @param filename [in] file name to add
 * @return nonw
 */
static void
AddListLSFName(List *list, const char *filename)
{
	ListCell   *new_tail;

	/*
	 * create ListCell
	 */
	new_tail = (ListCell *) malloc(sizeof(ListCell));
	if (new_tail == NULL)
		LoaderLogMessage(false, ERROR,
						 "not enough memory available to proceed");

	/*
	 * add file name
	 */
	new_tail->data.ptr_value = (void *) strdup(filename);
	if (new_tail->data.ptr_value == NULL)
		LoaderLogMessage(false, ERROR,
						 "could not duplicate string : %s", strerror(errno));

	new_tail->next = NULL;

	/*
	 * Case: first adddtion of Cell
	 */
	if (list->length == 0)
	{
		list->head = new_tail;
		list->tail = new_tail;
		list->type = T_List;
	}
	/*
	 * Case: addition of several Cells
	 */
	else
	{
		list->tail->next = new_tail;
		list->tail = new_tail;
	}

	/*
	 * increment length of List
	 */
	list->length++;
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
 * @param lsinfo [in] information stored in load status file
 * @return void
 */
static void
ClearLoadedPage(LoadStatus * lsinfo)
{
	BlockNumber rec_start;		/* recovery start point */
	BlockNumber rec_end;		/* recovery end point */
	BlockNumber segno;			/* data file segment no */
	char		tmpfname[MAXPGPATH];	/* data file name to open */
	Page		p = (Page) page;		/* area to read blocks */
	Page		z = (Page) zeropage;	/* blank page */
	int			blockcur;		/* block no currently procesing */
	int			fd;				/* file descriptor */
	off_t		seekpos;		/* position of block to recovery */
	ssize_t		ret;			/* return value of read()  */
	ssize_t		readlen;		/* size of data read by read()	*/


	/*
	 * if no block is created by pg_bulkload, program terminates
	 */
	if (lsinfo->ls_create_cnt == 0)
		return;

	/*
	 * create blank pages
	 */
	PageInit(z, BLCKSZ, 0);


	/*
	 * compute first and last blocks of areas for recovery.
	 * (first block) = (blocks at load start time: ls_exist_cnt)
	 * (last block) = (blocks at load start time ls_exist_cnt)
	 *						+ (blocks created by pg_bulkload ls_create_cnt)
	 */
	rec_start = lsinfo->ls_exist_cnt;
	rec_end = lsinfo->ls_exist_cnt + lsinfo->ls_create_cnt;

	/*
	 * get file name of first file name from lsinfo.
	 *	   open the file.
	 *	   if size of the file is over than 1 file segment size(default 1GB),
	 *	   set	proper extension.
	 */
	segno = rec_start / RELSEG_SIZE;
	if (segno > 0)
		sprintf(tmpfname, "%s.%u", lsinfo->ls_datafname, segno);
	else
		strcpy(tmpfname, lsinfo->ls_datafname);


	fd = open(tmpfname, O_RDWR, S_IRUSR | S_IWUSR);
	if (fd == -1)
		LoaderLogMessage(false, ERROR,
						 "could not open data file \"%s\" : %s",
						 tmpfname, strerror(errno));

	seekpos = lseek(fd, (rec_start % RELSEG_SIZE) * BLCKSZ, SEEK_SET);

	if (seekpos == -1)
		LoaderLogMessage(false, ERROR,
						 "could not seek the target position "
						 "in the data file \"%s\" : %s", tmpfname,
						 strerror(errno));

	blockcur = rec_start;

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
			ret = read(fd, p + readlen, BLCKSZ - readlen);
			if (ret == -1)
			{
				if (errno == EAGAIN || errno == EINTR)
					continue;
				else
					LoaderLogMessage(false, ERROR,
									 "could not read data file \"%s\" : %s",
									 tmpfname, strerror(errno));
			}
			else if (ret == 0)
			{
				/*
				 * case of partially writing, refill 0.
				 */
				memset(p + readlen, 0, BLCKSZ - readlen);
				ret = BLCKSZ - readlen;
			}
			readlen += ret;
		}
		while (readlen < BLCKSZ);


		/*
		 * if page is created by pg_bulkload, overwrite it by blank page.
		 */
		if (IsPageCreatedByLoader(p))
		{
			seekpos = lseek(fd, (blockcur % RELSEG_SIZE) * BLCKSZ, SEEK_SET);

			if (write(fd, zeropage, BLCKSZ) == -1)
				LoaderLogMessage(false, ERROR,
								 "could not write correct empty page : %s",
								 strerror(errno));
		}

		blockcur++;

		if (blockcur >= rec_end)
			break;

		/*
		 * if current block reach to the end of file, and need to process continuously,
		 * open next segment file.
		 */
		if (blockcur % RELSEG_SIZE == 0)
		{
			if (fsync(fd) != 0)
				LoaderLogMessage(false, ERROR,
								 "could not sync data file \"%s\" : %s",
								 tmpfname, strerror(errno));

			if (close(fd) == -1)
				LoaderLogMessage(false, ERROR,
								 "could not close data file \"%s\" : %s",
								 tmpfname, strerror(errno));

			sprintf(tmpfname, "%s.%u", lsinfo->ls_datafname, ++segno);

			fd = open(tmpfname, O_RDWR, S_IRUSR | S_IWUSR);
			if (fd == -1)
				LoaderLogMessage(false, ERROR,
								 "could not open data file \"%s\" : %s",
								 tmpfname, strerror(errno));
		}

	}

	/*
	 * post process
	 */
	if (fsync(fd) != 0)
		LoaderLogMessage(false, ERROR,
						 "could not sync data file \"%s\" : %s",
						 tmpfname, strerror(errno));

	if (close(fd) == -1)
		LoaderLogMessage(false, ERROR,
						 "could not close data file \"%s\" : %s",
						 tmpfname, strerror(errno));
}


/**
 * @brief Output the log message of bulkloader.
 *
 * @param cont		  [in] continue to process or not after output the log message.
 *						  true for continue processing, false for not
 * @param elevel	  [in] error message level
 * @param format, ... [in] error message(variable parameters)
 * @return void
 */

static void
LoaderLogMessage(bool cont, int elevel, const char *format, ...)
{
	va_list		argpt;
	time_t		etime;
	char		etimebuf[128];

	va_start(argpt, format);

	etime = time(NULL);
	strftime(etimebuf, sizeof(etimebuf),
			 "%Y-%m-%d %H:%M:%S %Z", localtime(&etime));

	fprintf(stderr, "%s BULK LOAD ", etimebuf);

	switch (elevel)
	{
		case LOG:
			fprintf(stderr, "LOG ");
			break;

		case NOTICE:
			fprintf(stderr, "NOTICE ");
			break;

		case ERROR:
			fprintf(stderr, "ERROR ");
			break;

		case FATAL:
			fprintf(stderr, "FATAL ");
			break;
		default:
			/*
			 * never reached
			 */
			fprintf(stderr, "UNKNOWN ");

	}

	vfprintf(stderr, format, argpt);
	fprintf(stderr, "\n");

	va_end(argpt);

	if (cont == false)
		exit(1);
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


/**
 * @brief Release the resources hold in List structure.
 *
 * Processing flow
 * <ol>
 *	 <li> Free all of the List and ListCell </li>
 * </ol>
 *
 * @param list [in] The List structure of cleanup object
 * @return None
 */
static void
CleanUpList(List *list)
{
	ListCell   *cell;

	if (list)
	{
		cell = list->head;

		while (cell)
		{
			ListCell   *tmp = cell;

			cell = lnext(cell);

			free(lfirst(tmp));
			free(tmp);
		}
	}

	free(list);
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
 * Changes with the original source code.
 * <ol>
 *	 <li> Use LoaderLogMessage() to replace ereport(). </li>
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
		fd = open(filename, O_RDWR | O_CREAT | O_EXCL, 0600);
		if (fd >= 0)
			break;				/* Success; exit the retry loop */

		/*
		 * Couldn't create the pid file. Probably it already exists.
		 */
		if ((errno != EEXIST && errno != EACCES) || ntries > 100)
			LoaderLogMessage(false, FATAL,
							 "could not create lock file \"%s\": %s",
							 filename, strerror(errno));

		/*
		 * Read the file to get the old owner's PID.  Note race condition
		 * here: file might have been deleted since we tried to create it.
		 */
		fd = open(filename, O_RDONLY, 0600);
		if (fd < 0)
		{
			if (errno == ENOENT)
				continue;		/* race condition; try again */
			LoaderLogMessage(false, FATAL,
							 "could not open lock file \"%s\": %s",
							 filename, strerror(errno));
		}
		if ((len = read(fd, buffer, sizeof(buffer) - 1)) < 0)
			LoaderLogMessage(false, FATAL,
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
			LoaderLogMessage(false, FATAL,
							 "bogus data in lock file \"%s\": %s",
							 filename, buffer);

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
				LoaderLogMessage(false, FATAL,
								 "lock file \"%s\" already exists. "
								 "Is another postmaster (PID %d) "
								 "running in data directory \"%s\"?",
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
						LoaderLogMessage(false, FATAL,
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
			LoaderLogMessage(false, FATAL,
							 "could not remove old lock file \"%s\": %s ",
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
		LoaderLogMessage(false, FATAL,
						 "could not write lock file \"%s\": %s",
						 filename, strerror(errno));
	}
	if (close(fd))
	{
		int			save_errno = errno;

		unlink(filename);
		errno = save_errno;
		LoaderLogMessage(false, FATAL,
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

/*
 * Returns the current user name.
 */
static const char *
get_user_name(const char *progname)
{
#ifndef WIN32
	struct passwd *pw;

	pw = getpwuid(geteuid());
	if (!pw)
	{
		fprintf(stderr, _("%s: could not obtain information about current user: %s\n"),
				progname, strerror(errno));
		exit(1);
	}
	return pw->pw_name;
#else
	static char username[128];	/* remains after function exit */
	DWORD		len = sizeof(username) - 1;

	if (!GetUserName(username, &len))
	{
		fprintf(stderr, _("%s: could not get current user name: %s\n"),
				progname, strerror(errno));
		exit(1);
	}
	return username;
#endif
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
