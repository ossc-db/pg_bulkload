/*
 * pg_bulkload: bin/pg_bulkload.c
 *
 *	  Copyright (c) 2007-2024, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
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
#include "common.h"
#include "pgut/pgut-fe.h"
#include "pgut/pgut-list.h"

const char *PROGRAM_VERSION	= PG_BULKLOAD_VERSION;
const char *PROGRAM_URL		= "http://github.com/ossc-db/pg_bulkload";
const char *PROGRAM_ISSUES	= "http://github.com/ossc-db/pg_bulkload/issues";

/*
 * Global variables
 */

/** @brief Database cluster directory. */
char *DataDir = NULL;

/** @Flag do recovery, or bulkload */
static bool		recovery = false;

static char *infile = NULL;				/* INFILE */
static char *input = NULL;				/* INPUT */
static char *output = NULL;				/* OUTPUT */
static char *logfile = NULL;			/* LOGFILE */
static char *parse_badfile = NULL;		/* PARSE_BADFILE */
static char *duplicate_badfile = NULL;	/* DUPLICATE_BADFILE */
static List *bulkload_options = NIL;
static bool	type_function = false;
static bool	type_binary = false;
static bool	writer_binary = false;

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

static int LoaderLoadMain(List *options);
static List *ParseControlFile(const char *path);
extern int LoaderRecoveryMain(void);
static PGresult *RemoteLoad(PGconn *conn, FILE *copystream, bool isbinary);
static bool ParseControlFileLine(char buf[], char **outKeyword, char **outValue);
static char *TrimSpaces(char *str);
static char *UnquoteString(char *str, char quote, char escape);
static char *FindUnquotedChar(char *str, char target, char quote, char escape);

static void
parse_option(pgut_option *opt, char *arg)
{
	opt->source = SOURCE_DEFAULT;	/* -o can be specified many times */

	if (arg && arg[0])
		bulkload_options = lappend(bulkload_options, arg);

	if (pg_strcasecmp(arg, "TYPE=FUNCTION") == 0)
		type_function = true;

	if (pg_strcasecmp(arg, "TYPE=BINARY") == 0 ||
		pg_strcasecmp(arg, "TYPE=FIXED") == 0)
		type_binary = true;

	if (pg_strcasecmp(arg, "WRITER=BINARY") == 0)
		writer_binary = true;
}

static pgut_option options[] =
{
	/* Dataload options */
	{ 's', 'i', "infile"			, &infile },
	{ 's', 'i', "input"				, &input },
	{ 's', 'O', "output"			, &output },
	{ 's', 'l', "logfile"			, &logfile },
	{ 's', 'P', "parse-badfile"		, &parse_badfile },
	{ 's', 'u', "duplicate-badfile"	, &duplicate_badfile },
	{ 'f', 'o', "option"			, parse_option },
	/* Recovery options */
	{ 's', 'D', "pgdata"			, &DataDir },
	{ 'b', 'r', "recovery"			, &recovery },
	{ 0 }
};

#define NUM_PATH_OPTIONS		6

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
	char	cwd[MAXPGPATH];
	char	control_file[MAXPGPATH] = "";
	int		i;

	pgut_init(argc, argv);
	if (argc < 2)
	{
		help(false);
		return E_PG_OTHER;
	}

	if (getcwd(cwd, MAXPGPATH) == NULL)
		ereport(ERROR,
			(errcode(EXIT_FAILURE),
			 errmsg("cannot read current directory: ")));

	i = pgut_getopt(argc, argv, options);

	for (; i < argc; i++)
	{
		if (control_file[0])
			ereport(ERROR,
				(errcode(EXIT_FAILURE),
				 errmsg("too many arguments")));

		/* make absolute control file path */
		if (is_absolute_path(argv[i]))
			strlcpy(control_file, argv[i], MAXPGPATH);
		else
			join_path_components(control_file, cwd, argv[i]);
		canonicalize_path(control_file);
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

		if (control_file[0])
			bulkload_options = list_concat(
				ParseControlFile(control_file), bulkload_options);

		/* chdir control_file to the parent directory */
		get_parent_directory(control_file);

		/* add path options */
		for (i = 0; i < NUM_PATH_OPTIONS; i++)
		{
			const pgut_option  *opt = &options[i];
			const char		   *path = *(const char **) opt->var;
			char				abspath[MAXPGPATH];
			char				item[MAXPGPATH + 32];

			if (path == NULL)
				continue;

			if ((i == 0 || i == 1) &&
				(pg_strcasecmp(path, "stdin") == 0 || type_function))
			{
				/* special case for stdin and input from function */
				strlcpy(abspath, path, lengthof(abspath));
			}
			else if (is_absolute_path(path) || (i == 2 && !writer_binary))
			{
				/* absolute path */
				strlcpy(abspath, path, lengthof(abspath));
			}
			else if (opt->source == SOURCE_FILE)
			{
				/* control file relative path */
				join_path_components(abspath, control_file, path);
			}
			else
			{
				/* current working directory relative path */
				join_path_components(abspath, cwd, path);
			}

			canonicalize_path(abspath);
			snprintf(item, lengthof(item), "%s=%s", opt->lname, abspath);
			bulkload_options = lappend(bulkload_options, pgut_strdup(item));
		}

		return LoaderLoadMain(bulkload_options);
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
	printf("  -i, --input=INPUT         INPUT path or function\n");
	printf("  -O, --output=OUTPUT       OUTPUT path or table\n");
	printf("  -l, --logfile=LOGFILE     LOGFILE path\n");
	printf("  -P, --parse-badfile=*     PARSE_BADFILE path\n");
	printf("  -u, --duplicate-badfile=* DUPLICATE_BADFILE path\n");
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
LoaderLoadMain(List *options)
{
	PGresult	   *res;
	const char	   *params[1];
	StringInfoData	buf;
	int				encoding;
	int				errors;
	ListCell	   *cell;

	if (options == NIL)
		ereport(ERROR,
			(errcode(EXIT_FAILURE),
			 errmsg("requires control file or command line options")));

	initStringInfo(&buf);
	reconnect(ERROR);
	encoding = PQclientEncoding(connection);

	elog(NOTICE, "BULK LOAD START");

	/* form options as text[] */
	appendStringInfoString(&buf, "{\"");
	foreach (cell, options)
	{
		const char *item = lfirst(cell);

		if (buf.len > 2)
			appendStringInfoString(&buf, "\",\"");

		/* escape " and \ */
		while (*item)
		{
			if (*item == '"' || *item == '\\')
			{
				appendStringInfoChar(&buf, '\\');
				appendStringInfoChar(&buf, *item);
				item++;
			}
			else if (!IS_HIGHBIT_SET(*item))
			{
				appendStringInfoChar(&buf, *item);
				item++;
			}
			else
			{
				int	n = PQmblen(item, encoding);
				appendBinaryStringInfo(&buf, item, n);
				item += n;
			}
		}
	}
	appendStringInfoString(&buf, "\"}");

	command("BEGIN", 0, NULL);
	params[0] = buf.data;
	res = execute("SELECT * FROM pgbulkload.pg_bulkload($1)", 1, params);
	if (PQresultStatus(res) == PGRES_COPY_IN)
	{
		PQclear(res);
		res = RemoteLoad(connection, stdin, type_binary);
		if (PQresultStatus(res) != PGRES_TUPLES_OK)
			elog(ERROR, "copy failed: %s", PQerrorMessage(connection));
	}
	command("COMMIT", 0, NULL);

	errors = atoi(PQgetvalue(res, 0, 2)) +	/* parse errors */
			 atoi(PQgetvalue(res, 0, 3));	/* duplicate errors */

	elog(NOTICE, "BULK LOAD END\n"
				 "\t%s Rows skipped.\n"
				 "\t%s Rows successfully loaded.\n"
				 "\t%s Rows not loaded due to parse errors.\n"
				 "\t%s Rows not loaded due to duplicate errors.\n"
				 "\t%s Rows replaced with new rows.",
				 PQgetvalue(res, 0, 0), PQgetvalue(res, 0, 1),
				 PQgetvalue(res, 0, 2), PQgetvalue(res, 0, 3),
				 PQgetvalue(res, 0, 4));
	PQclear(res);

	disconnect();
	termStringInfo(&buf);

	if (errors > 0)
	{
		elog(WARNING, "some rows were not loaded due to errors.");
		return E_PG_USER;
	}
	else
		return 0;	/* succeeded without errors */
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

static List *
ParseControlFile(const char *path)
{
#define LINEBUF 1024
	char	buf[LINEBUF];
	int		lineno;
	FILE   *file;
	List   *items = NIL;

	file = pgut_fopen(path, "rt");

	for (lineno = 1; fgets(buf, LINEBUF, file); lineno++)
	{
		char   *keyword;
		char   *value;
		int		i;

		if (!ParseControlFileLine(buf, &keyword, &value))
			continue;

		/* PATH_OPTIONS */
		for (i = 0; i < NUM_PATH_OPTIONS; i++)
		{
			pgut_option *opt = &options[i];

			if (pgut_keyeq(keyword, opt->lname))
			{
				pgut_setopt(opt, value, SOURCE_FILE);
				break;
			}
		}

		/* Other options */
		if (i >= NUM_PATH_OPTIONS)
		{
			size_t	len;
			char   *item;

			len = strlen(keyword) + strlen(value) + 2;
			item = pgut_malloc(len);
			snprintf(item, len, "%s=%s", keyword, value);
			items = lappend(items, item);

			if (pg_strcasecmp(item, "TYPE=FUNCTION") == 0)
				type_function = true;

			if (pg_strcasecmp(item, "TYPE=BINARY") == 0 ||
				pg_strcasecmp(item, "TYPE=FIXED") == 0)
				type_binary = true;

			if (pg_strcasecmp(item, "WRITER=BINARY") == 0)
				writer_binary = true;
		}
	}

	fclose(file);

	return items;
}

/**
 * @brief Parse a line in control file.
 */
static bool
ParseControlFileLine(char buf[], char **outKeyword, char **outValue)
{
	char	   *keyword = NULL;
	char	   *value = NULL;
	char	   *p;
	char	   *q;

	*outKeyword = NULL;
	*outValue = NULL;

	if (buf[strlen(buf) - 1] != '\n')
		ereport(ERROR,
			(errcode(EXIT_FAILURE),
			 errmsg("too long line \"%s\"", buf)));

	p = buf;				/* pointer to keyword */

	/*
	 * replace '\n' to '\0'
	 */
	q = strchr(buf, '\n');
	if (q != NULL)
		*q = '\0';

	/*
	 * delete strings after a comment letter outside quotations
	 */
	q = FindUnquotedChar(buf, '#', '"', '\\');
	if (q != NULL)
		*q = '\0';

	/*
	 * if result of trimming is a null string, it is treated as an empty line
	 */
	p = TrimSpaces(buf);
	if (*p == '\0')
		return false;

	/*
	 * devide after '='
	 */
	q = FindUnquotedChar(buf, '=', '"', '\\');
	if (q != NULL)
		*q = '\0';
	else
		ereport(ERROR,
			(errcode(EXIT_FAILURE),
			 errmsg("invalid input \"%s\"", buf)));

	q++;					/* pointer to input value */

	/*
	 * return a value trimmed space
	 */
	keyword = TrimSpaces(p);
	value = TrimSpaces(q);

	if (!keyword[0] || !value[0])
		ereport(ERROR,
			(errcode(EXIT_FAILURE),
			 errmsg("invalid input \"%s\"", buf)));

	value = UnquoteString(value, '"', '\\');
	if (!value)
		ereport(ERROR,
			(errcode(EXIT_FAILURE),
			 errmsg("unterminated quoted field")));

	*outKeyword = keyword;
	*outValue = value;
	return true;
}

/**
 * @brief Trim white spaces before and after input value.
 *
 * Flow
 * <ol>
 *	 <li>Trim spaces after input value. </li>
 *	 <li>Search the first non-space character, and return the pointer. </li>
 * </ol>
 * @param input		[in/out] Input character string
 * @return The pointer for the head of the character string after triming spaces
 * @note Input string is over written.
 * @note The returned value points the middle of input string.
 */
static char *
TrimSpaces(char *input)
{
	char	   *beg;
	char	   *end;

	/* trim spaces at head */
	for (beg = input; IsSpace(*beg); beg++);

	/* trim spaces at tail */
	for (end = beg + strlen(beg); end > beg && IsSpace(end[-1]); end--);
	*end = '\0';

	return beg;
}

/**
 * @brief Trim quotes surrounding string
 *
 * Quoting character(i.e. quote and escape character) is transformed as follows.
 * <ul>
 *	 <li>abc -> abc</li>
 *	 <li>"abc" -> abc</li>
 *	 <li>"abc\"123" -> abc"123</li>
 *	 <li>"abc\\123" -> abc\123</li>
 *	 <li>"abc\123" -> abc\123</li>
 *	 <li>"abc"123 -> abc123</li>
 *	 <li>"abc""123" -> abc123</li>
 *	 <li>"abc -> NG(error occuring) </li>
 * </ul>
 * @param str [in/out] Proccessed string
 * @param quote [in] Quote mark character
 * @param escape [in] Escape character
 * @retval !NULL String not surrounding quote mark character
 * @retval NULL  Error(not closed by quote mark)
 */
static char *
UnquoteString(char *str, char quote, char escape)
{
	int			i;				/* Read position */
	int			j;				/* Write position */
	int			in_quote = 0;


	for (i = 0, j = 0; str[i]; i++)
	{
		/*
		 * Find an opened quote mark.
		 */
		if (!in_quote && str[i] == quote)
		{
			in_quote = 1;
			continue;
		}

		/*
		 * Find an closing quote mark.
		 */
		if (in_quote && str[i] == quote)
		{
			in_quote = 0;
			continue;
		}

		/*
		 * Find an escape character.
		 * Process if the next is meta character.
		 */
		if (in_quote && str[i] == escape)
		{
			if (str[i + 1] == quote)
			{
				str[j++] = quote;
				i++;
				continue;
			}
			else if (str[i + 1] == escape)
			{
				str[j++] = escape;
				i++;
				continue;
			}
		}

		/*
		 * If it is ordinal character, copy it without modification.
		 */
		str[j++] = str[i];
	}
	str[j] = '\0';

	/*
	 * Quote mark is not closed
	 */
	if (in_quote)
		return NULL;

	return str;
}

/**
 * @brief Find the first specified character outside of quote mark
 * @param str [in] Searched string
 * @param target [in] Searched character
 * @param quote [in] Quote mark
 * @param escape [in] Escape character
 * @return If the specified character is found outside quoted string, return the
 * pointer. If it is not found, return NULL.
 */
static char *
FindUnquotedChar(char *str, char target, char quote, char escape)
{
	int			i;
	bool		in_quote = false;

	for (i = 0; str[i]; i++)
	{
		if (str[i] == escape)
		{
			/*
			 * Treat it as escape character if it is before meta character
			 */
			if (str[i + 1] == escape || str[i + 1] == quote)
				i++;
		}
		else if (str[i] == quote)
			in_quote = !in_quote;
		else if (!in_quote && str[i] == target)
			return str + i;
	}

	return NULL;
}
