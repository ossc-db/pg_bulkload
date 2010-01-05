/*
 * pg_bulkload: bin/pg_bulkload.c
 *
 *	  Copyright (c) 2007-2010, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

/**
 *	@file
 *	@brief Initiator and loader routine for the PostgreSQL high-speed loader.
 *
 *	Calls pg_bulkload() as a user-defined function and performs loading.
 *
 *	If -r option is specified, performs recovery to cancel inconveniences caused
 *	by errors in the previous loading.
 */
#include "postgres_fe.h"
#include <unistd.h>
#include "pgut/pgut.h"

const char *PROGRAM_VERSION	= "3.0alpha2";
const char *PROGRAM_URL		= "http://pgbulkload.projects.postgresql.org/";
const char *PROGRAM_EMAIL	= "pgbulkload-general@pgfoundry.org";

/*
 * Global variables
 */

/** @brief Database cluster directory. */
const char *DataDir = NULL;

/** @Flag do recovery, or bulkload */
static bool		recovery = false;

static char	   *additional_options = NULL;

/*
 * The length of the database cluster directory name should be short enough
 * so that the length of LSF (load status file) full path name is not longer
 * than MAXPGPATH, including the trailing '\0'. Since names of load status
 * files are "/pg_bulkload/(oid).(oid).loadstatus" and the max value of oid
 * is 4294967295 (10 chars), so we reserve 45 characters.
 */
#define MAX_LOADSTATUS_NAME		45

/*
 * Prototypes
 */

static int LoaderLoadMain(const char *control_file);
extern int LoaderRecoveryMain(void);
static void make_absolute_path(char dst[], const char *key, const char *relpath);
static void add_option(const char *option);
static PGresult *RemoteLoad(PGconn *conn, FILE *copystream, bool isbinary);

static void
parse_option(pgut_option *opt, const char *arg)
{
	char	path[MAXPGPATH];

	switch (opt->sname)
	{
		case 'i':
			if (pg_strcasecmp(arg, "stdin") == 0)
				strlcpy(path, "INFILE = stdin", lengthof(path));
			else
				make_absolute_path(path, "INFILE = ", arg);
			add_option(path);
			break;
		case 'l':
			make_absolute_path(path, "LOGFILE = ", arg);
			add_option(path);
			break;
		case 'P':
			make_absolute_path(path, "PARSE_BADFILE = ", arg);
			add_option(path);
			break;
		case 'u':
			make_absolute_path(path, "DUPLICATE_BADFILE = ", arg);
			add_option(path);
			break;
		case 'o':
			opt->source = SOURCE_DEFAULT;	/* -o can be specified many times */
			add_option(arg);
			break;
	}
}

static pgut_option options[] =
{
	/* Dataload options */
	{ 'f', 'i', "infile"			, parse_option },
	{ 'f', 'l', "logfile"			, parse_option },
	{ 'f', 'P', "parse_badfile"		, parse_option },
	{ 'f', 'u', "duplicate_badfile"	, parse_option },
	{ 'f', 'o', "option"			, parse_option },
	/* Recovery options */
	{ 's', 'D', "pgdata"	, &DataDir },
	{ 'b', 'r', "recovery"	, &recovery },
	/* Common options */
	{ 'b', 's', "silent", &quiet },	/* same as 'q'.*/
	{ 0 }
};

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
	char	control_file[MAXPGPATH] = "";
	int		i;

	i = pgut_getopt(argc, argv, options);

	for (; i < argc; i++)
	{
		if (control_file[0])
			elog(ERROR_ARGS, "too many arguments");
		make_absolute_path(control_file, NULL, argv[i]);
	}

	/*
	 * Determines data loading or recovery.
	 */
	if (recovery)
	{
		/* verify arguments */
		if (!DataDir && (DataDir = getenv("PGDATA")) == NULL)
			elog(ERROR, "no $PGDATA specified");
		if (strlen(DataDir) + MAX_LOADSTATUS_NAME >= MAXPGPATH)
			elog(ERROR, "too long $PGDATA path length");
		if (control_file[0] != '\0')
			elog(ERROR, "invalid argument 'control file' for recovery");

		return LoaderRecoveryMain();
	}
	else
	{
		/* verify arguments */
		if (DataDir)
			elog(ERROR, "invalid option '-D' for data load");

		return LoaderLoadMain(control_file);
	}
}

void
pgut_help(bool details)
{
	printf("%s is a bulk data loading tool for PostgreSQL\n", PROGRAM_NAME);
	printf("\nUsage:\n");
	printf("  Dataload: %s [dataload options] control_file_path\n", PROGRAM_NAME);
	printf("  Recovery: %s -r [-D DATADIR]\n", PROGRAM_NAME);

	if (!details)
		return;

	printf("\nDataload options:\n");
	printf("  -i, --infile=INFILE       INFILE path\n");
	printf("  -l, --logfile=LOGFILE     LOGFILE path\n");
	printf("  -P, --parse_badfile=*     PARSE_BADFILE path\n");
	printf("  -u, --duplicate_badfile=* DUPLICATE_BADFILE path\n");
	printf("  -o, --option=\"key=val\"    additional option\n");
	printf("\nRecovery options:\n");
	printf("  -r, --recovery            execute recovery\n");
	printf("  -D, --pgdata=DATADIR      database directory\n");
}

/**
 * @brief Performs data loading.
 *
 * Invokes pg_bulkload() user-defined function with given parameters
 * in single transaction.
 *
 * @return exitcode (always 0).
 */
static int
LoaderLoadMain(const char *control_file)
{
	PGresult	   *res;
	const char	   *params[2];

	params[0] = control_file;
	params[1] = additional_options;

	reconnect();

	elog(NOTICE, "BULK LOAD START");

	command("BEGIN", 0, NULL);
	res = execute("SELECT * FROM pg_bulkload($1, $2)", 2, params);
	if (PQresultStatus(res) == PGRES_COPY_IN)
	{
		PQclear(res);
		res = RemoteLoad(connection, stdin, false);
		if (PQresultStatus(res) != PGRES_TUPLES_OK)
			elog(ERROR, "copy failed: %s", PQerrorMessage(connection));
	}
	command("COMMIT", 0, NULL);

	elog(NOTICE, "BULK LOAD END\n"
				 "\t%s Rows skipped.\n"
				 "\t%s Rows successfully loaded.\n"
				 "\t%s Rows not loaded due to parse errors.\n"
				 "\t%s Rows not loaded due to duplicate errors.\n"
				 "\t%s Rows deleted due to duplicate errors.",
				 PQgetvalue(res, 0, 0), PQgetvalue(res, 0, 1),
				 PQgetvalue(res, 0, 2), PQgetvalue(res, 0, 3),
				 PQgetvalue(res, 0, 4));
	PQclear(res);

	disconnect();

	return 0;
}

/*
 * Add current working directory if path is relative.
 */
static void
make_absolute_path(char dst[], const char *key, const char *relpath)
{
	char	cwd[MAXPGPATH];
	size_t	keylen;

	if (key)
	{
		keylen = strlen(key);
		memcpy(dst, key, keylen);
	}
	else
		keylen = 0;

	if (is_absolute_path(relpath))
		strlcpy(dst + keylen, relpath, MAXPGPATH - keylen);
	else
	{
		if (getcwd(cwd, MAXPGPATH) == NULL)
			elog(ERROR_SYSTEM, "cannot read current directory");
		snprintf(dst + keylen, MAXPGPATH - keylen, "%s/%s", cwd, relpath);
	}
}

/*
 * Additional option is an array of "KEY=VALUE\n".
 */
static void
add_option(const char *option)
{
	size_t	len;
	size_t	addlen;

	if (!option || !option[0])
		return;

	len = (additional_options ? strlen(additional_options) : 0);
	addlen = strlen(option);

	additional_options = realloc(additional_options, len + addlen + 2);
	memcpy(&additional_options[len], option, addlen);
	additional_options[len + addlen] = '\n';
	additional_options[len + addlen + 1] = '\0';
}

/*
 * RemoteLoad : modified handleCopyIn() in bin/psql/copy.c
 * sends data to complete a COPY ... FROM STDIN command
 *
 * conn should be a database connection that you just issued COPY FROM on
 * and got back a PGRES_COPY_IN result.
 * copystream is the file stream to read the data from.
 * isbinary can be set from PQbinaryTuples().
 */

/* read chunk size for COPY IN - size is not critical */
#define COPYBUFSIZ 8192

static PGresult *
RemoteLoad(PGconn *conn, FILE *copystream, bool isbinary)
{
	bool		OK;
	char		buf[COPYBUFSIZ];

	OK = true;

	if (isbinary)
	{
		while (!interrupted)
		{
			int			buflen;

			buflen = fread(buf, 1, COPYBUFSIZ, copystream);

			if (buflen <= 0)
				break;

			if (PQputCopyData(conn, buf, buflen) <= 0)
			{
				OK = false;
				break;
			}
		}
	}
	else
	{
		bool		copydone = false;

		while (!interrupted && !copydone)
		{						/* for each input line ... */
			bool		firstload;
			bool		linedone;

			firstload = true;
			linedone = false;

			while (!linedone)
			{					/* for each bufferload in line ... */
				int			linelen;
				char	   *fgresult;

				fgresult = fgets(buf, sizeof(buf), copystream);

				if (!fgresult)
				{
					copydone = true;
					break;
				}

				linelen = strlen(buf);

				/* current line is done? */
				if (linelen > 0 && buf[linelen - 1] == '\n')
					linedone = true;

				/* check for EOF marker, but not on a partial line */
				if (firstload)
				{
					if (strcmp(buf, "\\.\n") == 0 ||
						strcmp(buf, "\\.\r\n") == 0)
					{
						copydone = true;
						break;
					}

					firstload = false;
				}

				if (PQputCopyData(conn, buf, linelen) <= 0)
				{
					OK = false;
					copydone = true;
					break;
				}
			}
		}
	}

	if (interrupted)
	{
		PQputCopyEnd(conn, "canceled by user");
		return PQgetResult(conn);
	}

	/* Check for read error */
	if (ferror(copystream))
		OK = false;

	/* Terminate data transfer */
	if (PQputCopyEnd(conn, OK ? NULL : "aborted because of read failure") <= 0)
		OK = false;

	/* Check command status and return to normal libpq state */
	if (!OK)
		return NULL;

	return PQgetResult(conn);
}
