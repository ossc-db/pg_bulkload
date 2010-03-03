/*-------------------------------------------------------------------------
 *
 * pgut.c
 *
 * Copyright (c) 2009-2010, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 *
 *-------------------------------------------------------------------------
 */

#define FRONTEND
#include "postgres_fe.h"
#include "libpq/pqsignal.h"

#include <limits.h>
#include <sys/stat.h>
#include <time.h>

#include "pgut.h"

#ifdef PGUT_MULTI_THREADED
#include "pgut-pthread.h"
static pthread_mutex_t				pgut_connections_lock;
#define pgut_connections_init()		pthread_mutex_init(&pgut_connections_lock, NULL)
#define pgut_connections_lock()		pthread_mutex_lock(&pgut_connections_lock)
#define pgut_connections_unlock()	pthread_mutex_unlock(&pgut_connections_lock)
#else
#define pgut_connections_init()		((void) 0)
#define pgut_connections_lock()		((void) 0)
#define pgut_connections_unlock()	((void) 0)
#endif

/* old gcc doesn't have LLONG_MAX. */
#ifndef LLONG_MAX
#if defined(HAVE_LONG_INT_64) || !defined(HAVE_LONG_LONG_INT_64)
#define LLONG_MAX		LONG_MAX
#else
#define LLONG_MAX		INT64CONST(0x7FFFFFFFFFFFFFFF)
#endif
#endif

const char *PROGRAM_NAME = NULL;

bool			debug = false;
bool			quiet = false;

/* Interrupted by SIGINT (Ctrl+C) ? */
bool			interrupted = false;
static bool		in_cleanup = false;

/* Database connections */
typedef struct pgutConn	pgutConn;
struct pgutConn
{
	PGconn	   *conn;
	PGcancel   *cancel;
	pgutConn   *next;
};

static pgutConn *pgut_connections;

/* Connection routines */
static void init_cancel_handler(void);
static void on_before_exec(pgutConn *conn);
static void on_after_exec(pgutConn *conn);
static void on_interrupt(void);
static void on_cleanup(void);
static void exit_or_abort(int exitcode);

void
pgut_init(int argc, char **argv)
{
	if (PROGRAM_NAME == NULL)
	{
		PROGRAM_NAME = get_progname(argv[0]);
		set_pglocale_pgservice(argv[0], "pgscripts");

		pgut_connections_init();
		init_cancel_handler();
		atexit(on_cleanup);
	}
}

/*
 * Try to interpret value as boolean value.  Valid values are: true,
 * false, yes, no, on, off, 1, 0; as well as unique prefixes thereof.
 * If the string parses okay, return true, else false.
 * If okay and result is not NULL, return the value in *result.
 */
bool
parse_bool(const char *value, bool *result)
{
	return parse_bool_with_len(value, strlen(value), result);
}

bool
parse_bool_with_len(const char *value, size_t len, bool *result)
{
	switch (*value)
	{
		case 't':
		case 'T':
			if (pg_strncasecmp(value, "true", len) == 0)
			{
				if (result)
					*result = true;
				return true;
			}
			break;
		case 'f':
		case 'F':
			if (pg_strncasecmp(value, "false", len) == 0)
			{
				if (result)
					*result = false;
				return true;
			}
			break;
		case 'y':
		case 'Y':
			if (pg_strncasecmp(value, "yes", len) == 0)
			{
				if (result)
					*result = true;
				return true;
			}
			break;
		case 'n':
		case 'N':
			if (pg_strncasecmp(value, "no", len) == 0)
			{
				if (result)
					*result = false;
				return true;
			}
			break;
		case 'o':
		case 'O':
			/* 'o' is not unique enough */
			if (pg_strncasecmp(value, "on", (len > 2 ? len : 2)) == 0)
			{
				if (result)
					*result = true;
				return true;
			}
			else if (pg_strncasecmp(value, "off", (len > 2 ? len : 2)) == 0)
			{
				if (result)
					*result = false;
				return true;
			}
			break;
		case '1':
			if (len == 1)
			{
				if (result)
					*result = true;
				return true;
			}
			break;
		case '0':
			if (len == 1)
			{
				if (result)
					*result = false;
				return true;
			}
			break;
		default:
			break;
	}

	if (result)
		*result = false;		/* suppress compiler warning */
	return false;
}

/*
 * Parse string as 32bit signed int.
 * valid range: -2147483648 ~ 2147483647
 */
bool
parse_int32(const char *value, int32 *result)
{
	int64	val;
	char   *endptr;

	if (strcmp(value, INFINITE_STR) == 0)
	{
		*result = INT_MAX;
		return true;
	}

	errno = 0;
	val = strtol(value, &endptr, 0);
	if (endptr == value || *endptr)
		return false;

	if (errno == ERANGE || val != (int64) ((int32) val))
		return false;

	*result = (int32) val;

	return true;
}

/*
 * Parse string as 32bit unsigned int.
 * valid range: 0 ~ 4294967295 (2^32-1)
 */
bool
parse_uint32(const char *value, uint32 *result)
{
	uint64	val;
	char   *endptr;

	if (strcmp(value, INFINITE_STR) == 0)
	{
		*result = UINT_MAX;
		return true;
	}

	errno = 0;
	val = strtoul(value, &endptr, 0);
	if (endptr == value || *endptr)
		return false;

	if (errno == ERANGE || val != (uint64) ((uint32) val))
		return false;

	*result = (uint32) val;

	return true;
}

/*
 * Parse string as int64
 * valid range: -9223372036854775808 ~ 9223372036854775807
 */
bool
parse_int64(const char *value, int64 *result)
{
	int64	val;
	char   *endptr;

	if (strcmp(value, INFINITE_STR) == 0)
	{
		*result = LLONG_MAX;
		return true;
	}

	errno = 0;
#ifdef WIN32
	val = _strtoi64(value, &endptr, 0);
#elif defined(HAVE_LONG_INT_64)
	val = strtol(value, &endptr, 0);
#elif defined(HAVE_LONG_LONG_INT_64)
#else
	val = strtol(value, &endptr, 0);
#endif
	if (endptr == value || *endptr)
		return false;

	if (errno == ERANGE)
		return false;

	*result = val;

	return true;
}

/*
 * Parse string as uint64
 * valid range: 0 ~ (2^64-1)
 */
bool
parse_uint64(const char *value, uint64 *result)
{
	uint64	val;
	char   *endptr;

	if (strcmp(value, INFINITE_STR) == 0)
	{
#if defined(HAVE_LONG_INT_64)
		*result = ULONG_MAX;
#elif defined(HAVE_LONG_LONG_INT_64)
		*result = ULLONG_MAX;
#else
		*result = ULONG_MAX;
#endif
		return true;
	}

	errno = 0;
#ifdef WIN32
	val = _strtoui64(value, &endptr, 0);
#elif defined(HAVE_LONG_INT_64)
	val = strtoul(value, &endptr, 0);
#elif defined(HAVE_LONG_LONG_INT_64)
	val = strtoull(value, &endptr, 0);
#else
	val = strtoul(value, &endptr, 0);
#endif
	if (endptr == value || *endptr)
		return false;

	if (errno == ERANGE)
		return false;

	*result = val;

	return true;
}

/*
 * Convert ISO-8601 format string to time_t value.
 */
bool
parse_time(const char *value, time_t *time)
{
	size_t		len;
	char	   *tmp;
	int			i;
	struct tm	tm;
	char		junk[2];

	/* tmp = replace( value, !isalnum, ' ' ) */
	tmp = pgut_malloc(strlen(value) + + 1);
	len = 0;
	for (i = 0; value[i]; i++)
		tmp[len++] = (IsAlnum(value[i]) ? value[i] : ' ');
	tmp[len] = '\0';

	/* parse for "YYYY-MM-DD HH:MI:SS" */
	tm.tm_year = 0;		/* tm_year is year - 1900 */
	tm.tm_mon = 0;		/* tm_mon is 0 - 11 */
	tm.tm_mday = 1;		/* tm_mday is 1 - 31 */
	tm.tm_hour = 0;
	tm.tm_min = 0;
	tm.tm_sec = 0;
	i = sscanf(tmp, "%04d %02d %02d %02d %02d %02d%1s",
		&tm.tm_year, &tm.tm_mon, &tm.tm_mday,
		&tm.tm_hour, &tm.tm_min, &tm.tm_sec, junk);
	free(tmp);

	if (i < 1 || 6 < i)
		return false;

	/* adjust year */
	if (tm.tm_year < 100)
		tm.tm_year += 2000 - 1900;
	else if (tm.tm_year >= 1900)
		tm.tm_year -= 1900;

	/* adjust month */
	if (i > 1)
		tm.tm_mon -= 1;

	*time = mktime(&tm);

	return true;
}

static char *
prompt_for_password(void)
{
	return simple_prompt("Password: ", 100, false);
}

#if PG_VERSION_NUM < 80300
static bool
PQconnectionNeedsPassword(PGconn *conn)
{
	return strcmp(PQerrorMessage(conn), PQnoPasswordSupplied) == 0 && !feof(stdin);
}
#endif

PGconn *
pgut_connect(const char *info, YesNo prompt, int elevel)
{
	char	   *passwd;

	CHECK_FOR_INTERRUPTS();

	if (prompt == YES)
		passwd = prompt_for_password();
	else
		passwd = NULL;

	/* Start the connection. Loop until we have a password if requested by backend. */
	for (;;)
	{
		PGconn	   *conn;

		conn = PQconnectdb(info);

		if (PQstatus(conn) == CONNECTION_OK)
		{
			pgutConn *c;

			free(passwd);

			c = pgut_new(pgutConn);
			c->conn = conn;
			c->cancel = NULL;

			pgut_connections_lock();
			c->next = pgut_connections;
			pgut_connections = c;
			pgut_connections_unlock();

			return conn;
		}

		if (conn && PQconnectionNeedsPassword(conn) && prompt != NO)
		{
			PQfinish(conn);
			free(passwd);
			passwd = prompt_for_password();
			continue;
		}
		elog(elevel, "could not connect to database with %s: %s",
			 info, PQerrorMessage(conn));
		PQfinish(conn);
		return NULL;
	}
}

void
pgut_disconnect(PGconn *conn)
{
	if (conn)
	{
		pgutConn	   *c;
		pgutConn	  **prev;

		pgut_connections_lock();
		prev = &pgut_connections;
		for (c = pgut_connections; c; c = c->next)
		{
			if (c->conn == conn)
			{
				*prev = c->next;
				break;
			}
			prev = &c->next;
		}
		pgut_connections_unlock();

		PQfinish(conn);
	}
}

void
pgut_disconnect_all(void)
{
	pgut_connections_lock();
	while (pgut_connections)
	{
		PQfinish(pgut_connections->conn);
		pgut_connections = pgut_connections->next;
	}
	pgut_connections_unlock();
}

PGresult *
pgut_execute(PGconn* conn, const char *query, int nParams, const char **params, int elevel)
{
	PGresult   *res;
	pgutConn	   *c;

	CHECK_FOR_INTERRUPTS();

	/* write query to elog if debug */
	if (debug)
	{
		int		i;

		if (strchr(query, '\n'))
			elog(LOG, "(query)\n%s", query);
		else
			elog(LOG, "(query) %s", query);
		for (i = 0; i < nParams; i++)
			elog(LOG, "\t(param:%d) = %s", i, params[i] ? params[i] : "(null)");
	}

	if (conn == NULL)
	{
		elog(elevel, "not connected");
		return NULL;
	}

	/* find connection */
	pgut_connections_lock();
	for (c = pgut_connections; c; c = c->next)
		if (c->conn == conn)
			break;
	pgut_connections_unlock();

	if (c)
		on_before_exec(c);
	if (nParams == 0)
		res = PQexec(conn, query);
	else
		res = PQexecParams(conn, query, nParams, NULL, params, NULL, NULL, 0);
	if (c)
		on_after_exec(c);

	switch (PQresultStatus(res))
	{
		case PGRES_TUPLES_OK:
		case PGRES_COMMAND_OK:
		case PGRES_COPY_IN:
			break;
		default:
			elog(elevel, "query failed: %squery was: %s",
				PQerrorMessage(conn), query);
			break;
	}

	return res;
}

ExecStatusType
pgut_command(PGconn* conn, const char *query, int nParams, const char **params, int elevel)
{
	PGresult	   *res;
	ExecStatusType	code;
	
	res = pgut_execute(conn, query, nParams, params, elevel);
	code = PQresultStatus(res);
	PQclear(res);

	return code;
}

bool
pgut_send(PGconn* conn, const char *query, int nParams, const char **params, int elevel)
{
	int			res;

	CHECK_FOR_INTERRUPTS();

	/* write query to elog if debug */
	if (debug)
	{
		int		i;

		if (strchr(query, '\n'))
			elog(LOG, "(query)\n%s", query);
		else
			elog(LOG, "(query) %s", query);
		for (i = 0; i < nParams; i++)
			elog(LOG, "\t(param:%d) = %s", i, params[i] ? params[i] : "(null)");
	}

	if (conn == NULL)
	{
		elog(elevel, "not connected");
		return false;
	}

	if (nParams == 0)
		res = PQsendQuery(conn, query);
	else
		res = PQsendQueryParams(conn, query, nParams, NULL, params, NULL, NULL, 0);

	if (res != 1)
	{
		elog(elevel, "query failed: %squery was: %s",
			PQerrorMessage(conn), query);
		return false;
	}

	return true;
}

int
pgut_wait(int num, PGconn *connections[], struct timeval *timeout)
{
	/* all connections are busy. wait for finish */
	while (!interrupted)
	{
		int		i;
		fd_set	mask;
		int		maxsock;

		FD_ZERO(&mask);

		maxsock = -1;
		for (i = 0; i < num; i++)
		{
			int	sock;

			if (connections[i] == NULL)
				continue;
			sock = PQsocket(connections[i]);
			if (sock >= 0)
			{
				FD_SET(sock, &mask);
				if (maxsock < sock)
					maxsock = sock;
			}
		}

		if (maxsock == -1)
		{
			errno = ENOENT;
			return -1;
		}

		i = wait_for_sockets(maxsock + 1, &mask, timeout);
		if (i == 0)
			break;	/* timeout */

		for (i = 0; i < num; i++)
		{
			if (connections[i] && FD_ISSET(PQsocket(connections[i]), &mask))
			{
				PQconsumeInput(connections[i]);
				if (PQisBusy(connections[i]))
					continue;
				return i;
			}
		}
	}

	errno = EINTR;
	return -1;
}

/*
 * CHECK_FOR_INTERRUPTS - Ctrl+C pressed?
 */
void
CHECK_FOR_INTERRUPTS(void)
{
	if (interrupted && !in_cleanup)
		elog(ERROR_INTERRUPTED, "interrupted");
}

/*
 * elog - log to stderr and exit if ERROR or FATAL
 */
void
elog(int elevel, const char *fmt, ...)
{
	va_list		args;

	if (!debug && elevel <= LOG)
		return;
	if (quiet && elevel < WARNING)
		return;

	switch (elevel)
	{
	case LOG:
		fputs("LOG: ", stderr);
		break;
	case INFO:
		fputs("INFO: ", stderr);
		break;
	case NOTICE:
		fputs("NOTICE: ", stderr);
		break;
	case WARNING:
		fputs("WARNING: ", stderr);
		break;
	case ALERT:
		fputs("ALERT: ", stderr);
		break;
	case FATAL:
		fputs("FATAL: ", stderr);
		break;
	case PANIC:
		fputs("PANIC: ", stderr);
		break;
	default:
		if (elevel >= ERROR)
			fputs("ERROR: ", stderr);
		break;
	}

	va_start(args, fmt);
	vfprintf(stderr, fmt, args);
	fputc('\n', stderr);
	fflush(stderr);
	va_end(args);

	if (elevel > 0)
		exit_or_abort(elevel);
}

#ifdef WIN32
static CRITICAL_SECTION cancelConnLock;
#endif

/*
 * on_before_exec
 *
 * Set cancel to point to the current database connection.
 */
static void
on_before_exec(pgutConn *conn)
{
	PGcancel   *old;

	if (in_cleanup)
		return;	/* forbid cancel during cleanup */

#ifdef WIN32
	EnterCriticalSection(&cancelConnLock);
#endif

	/* Free the old one if we have one */
	old = conn->cancel;

	/* be sure handle_sigint doesn't use pointer while freeing */
	conn->cancel = NULL;

	if (old != NULL)
		PQfreeCancel(old);

	conn->cancel = PQgetCancel(conn->conn);

#ifdef WIN32
	LeaveCriticalSection(&cancelConnLock);
#endif
}

/*
 * on_after_exec
 *
 * Free the current cancel connection, if any, and set to NULL.
 */
static void
on_after_exec(pgutConn *conn)
{
	PGcancel   *old;

	if (in_cleanup)
		return;	/* forbid cancel during cleanup */

#ifdef WIN32
	EnterCriticalSection(&cancelConnLock);
#endif

	old = conn->cancel;

	/* be sure handle_sigint doesn't use pointer while freeing */
	conn->cancel = NULL;

	if (old != NULL)
		PQfreeCancel(old);

#ifdef WIN32
	LeaveCriticalSection(&cancelConnLock);
#endif
}

/*
 * Handle interrupt signals by cancelling the current command.
 */
static void
on_interrupt(void)
{
	pgutConn	   *c;
	int			save_errno = errno;

	/* Set interruped flag */
	interrupted = true;

	if (in_cleanup)
		return;

	/* Send QueryCancel if we are processing a database query */
	pgut_connections_lock();
	for (c = pgut_connections; c; c = c->next)
	{
		char		buf[256];

		if (c->cancel != NULL && PQcancel(c->cancel, buf, sizeof(buf)))
			elog(WARNING, "Cancel request sent");
	}
	pgut_connections_unlock();

	errno = save_errno;			/* just in case the write changed it */
}

typedef struct pgut_atexit_item pgut_atexit_item;
struct pgut_atexit_item
{
	pgut_atexit_callback	callback;
	void				   *userdata;
	pgut_atexit_item	   *next;
};

static pgut_atexit_item *pgut_atexit_stack = NULL;

void
pgut_atexit_push(pgut_atexit_callback callback, void *userdata)
{
	pgut_atexit_item *item;

	AssertArg(callback != NULL);

	item = pgut_new(pgut_atexit_item);
	item->callback = callback;
	item->userdata = userdata;
	item->next = pgut_atexit_stack;

	pgut_atexit_stack = item;
}

void
pgut_atexit_pop(pgut_atexit_callback callback, void *userdata)
{
	pgut_atexit_item  *item;
	pgut_atexit_item **prev;

	for (item = pgut_atexit_stack, prev = &pgut_atexit_stack;
		 item;
		 prev = &item->next, item = item->next)
	{
		if (item->callback == callback && item->userdata == userdata)
		{
			*prev = item->next;
			free(item);
			break;
		}
	}
}

static void
call_atexit_callbacks(bool fatal)
{
	pgut_atexit_item  *item;

	for (item = pgut_atexit_stack; item; item = item->next)
		item->callback(fatal, item->userdata);
}

static void
on_cleanup(void)
{
	in_cleanup = true;
	interrupted = false;
	call_atexit_callbacks(false);
	pgut_disconnect_all();
}

static void
exit_or_abort(int exitcode)
{
	if (in_cleanup)
	{
		/* oops, error in cleanup*/
		call_atexit_callbacks(true);
		abort();
	}
	else
	{
		/* normal exit */
		exit(exitcode);
	}
}

int
appendStringInfoFile(StringInfo str, FILE *fp)
{
	AssertArg(str != NULL);
	AssertArg(fp != NULL);

	for (;;)
	{
		int		rc;

		if (str->maxlen - str->len < 2 && enlargeStringInfo(str, 1024) == 0)
			return errno = ENOMEM;

		rc = fread(str->data + str->len, 1, str->maxlen - str->len - 1, fp);
		if (rc == 0)
			break;
		else if (rc > 0)
		{
			str->len += rc;
			str->data[str->len] = '\0';
		}
		else if (ferror(fp) && errno != EINTR)
			return errno;
	}
	return 0;
}

int
appendStringInfoFd(StringInfo str, int fd)
{
	AssertArg(str != NULL);
	AssertArg(fd != -1);

	for (;;)
	{
		int		rc;

		if (str->maxlen - str->len < 2 && enlargeStringInfo(str, 1024) == 0)
			return errno = ENOMEM;

		rc = read(fd, str->data + str->len, str->maxlen - str->len - 1);
		if (rc == 0)
			break;
		else if (rc > 0)
		{
			str->len += rc;
			str->data[str->len] = '\0';
		}
		else if (errno != EINTR)
			return errno;
	}
	return 0;
}

void *
pgut_malloc(size_t size)
{
	char *ret;

	if ((ret = malloc(size)) == NULL)
		elog(ERROR_NOMEM, "could not allocate memory (%lu bytes): %s",
			(unsigned long) size, strerror(errno));
	return ret;
}

void *
pgut_realloc(void *p, size_t size)
{
	char *ret;

	if ((ret = realloc(p, size)) == NULL)
		elog(ERROR_NOMEM, "could not re-allocate memory (%lu bytes): %s",
			(unsigned long) size, strerror(errno));
	return ret;
}

char *
pgut_strdup(const char *str)
{
	char *ret;

	if (str == NULL)
		return NULL;

	if ((ret = strdup(str)) == NULL)
		elog(ERROR_NOMEM, "could not duplicate string \"%s\": %s",
			str, strerror(errno));
	return ret;
}

char *
strdup_with_len(const char *str, size_t len)
{
	char *r;

	if (str == NULL)
		return NULL;

	r = pgut_malloc(len + 1);
	memcpy(r, str, len);
	r[len] = '\0';
	return r;
}

/* strdup but trim whitespaces at head and tail */
char *
strdup_trim(const char *str)
{
	size_t	len;

	if (str == NULL)
		return NULL;

	while (IsSpace(str[0])) { str++; }
	len = strlen(str);
	while (len > 0 && IsSpace(str[len - 1])) { len--; }

	return strdup_with_len(str, len);
}

/* Try open file. Also create parent directries if open for writes. */
FILE *
pgut_fopen(const char *path, const char *mode, bool missing_ok)
{
	FILE *fp;

retry:
	if ((fp = fopen(path, mode)) == NULL)
	{
		if (errno == ENOENT)
		{
			if (missing_ok)
				return NULL;
			if (mode[0] == 'w' || mode[0] == 'a')
			{
				char	dir[MAXPGPATH];

				strlcpy(dir, path, MAXPGPATH);
				get_parent_directory(dir);
				pgut_mkdir(dir);
				goto retry;
			}
		}

		elog(ERROR_SYSTEM, "could not open file \"%s\": %s",
			path, strerror(errno));
	}

	return fp;
}

/*
 * this tries to build all the elements of a path to a directory a la mkdir -p
 * we assume the path is in canonical form, i.e. uses / as the separator.
 */
void
pgut_mkdir(const char *dirpath)
{
	struct stat sb;
	int			first,
				last,
				retval;
	char	   *path;
	char	   *p;

	Assert(dirpath != NULL);

	p = path = pgut_strdup(dirpath);
	retval = 0;

#ifdef WIN32
	/* skip network and drive specifiers for win32 */
	if (strlen(p) >= 2)
	{
		if (p[0] == '/' && p[1] == '/')
		{
			/* network drive */
			p = strstr(p + 2, "/");
			if (p == NULL)
				elog(ERROR_ARGS, "invalid path \"%s\"", dirpath);
		}
		else if (p[1] == ':' &&
				 ((p[0] >= 'a' && p[0] <= 'z') ||
				  (p[0] >= 'A' && p[0] <= 'Z')))
		{
			/* local drive */
			p += 2;
		}
	}
#endif

	if (p[0] == '/')			/* Skip leading '/'. */
		++p;
	for (first = 1, last = 0; !last; ++p)
	{
		if (p[0] == '\0')
			last = 1;
		else if (p[0] != '/')
			continue;
		*p = '\0';
		if (!last && p[1] == '\0')
			last = 1;
		if (first)
			first = 0;

retry:
		/* check for pre-existing directory; ok if it's a parent */
		if (stat(path, &sb) == 0)
		{
			if (!S_ISDIR(sb.st_mode))
			{
				if (last)
					errno = EEXIST;
				else
					errno = ENOTDIR;
				retval = 1;
				break;
			}
		}
		else if (mkdir(path, S_IRWXU) < 0)
		{
			if (errno == EEXIST)
				goto retry;	/* another thread might create the directory. */
			retval = 1;
			break;
		}
		if (!last)
			*p = '/';
	}

	if (retval != 0)
		elog(ERROR_SYSTEM, "could not create directory \"%s\": %s",
					 dirpath, strerror(errno));

	free(path);
}

#ifdef WIN32
static int select_win32(int nfds, fd_set *readfds, fd_set *writefds, fd_set *exceptfds, const struct timeval * timeout);
#define select		select_win32
#endif

int
wait_for_socket(int sock, struct timeval *timeout)
{
	fd_set		fds;

	FD_ZERO(&fds);
	FD_SET(sock, &fds);
	return wait_for_sockets(sock + 1, &fds, timeout);
}

int
wait_for_sockets(int nfds, fd_set *fds, struct timeval *timeout)
{
	int		i;

	for (;;)
	{
		i = select(nfds, fds, NULL, NULL, timeout);
		if (i < 0)
		{
			CHECK_FOR_INTERRUPTS();
			if (errno != EINTR)
				elog(ERROR_SYSTEM, "select failed: %s", strerror(errno));
		}
		else
			return i;
	}
}

#ifndef WIN32
static void
handle_sigint(SIGNAL_ARGS)
{
	on_interrupt();
}

static void
init_cancel_handler(void)
{
	pqsignal(SIGINT, handle_sigint);
}
#else							/* WIN32 */

/*
 * Console control handler for Win32. Note that the control handler will
 * execute on a *different thread* than the main one, so we need to do
 * proper locking around those structures.
 */
static BOOL WINAPI
consoleHandler(DWORD dwCtrlType)
{
	if (dwCtrlType == CTRL_C_EVENT ||
		dwCtrlType == CTRL_BREAK_EVENT)
	{
		EnterCriticalSection(&cancelConnLock);
		on_interrupt();
		LeaveCriticalSection(&cancelConnLock);
		return TRUE;
	}
	else
		/* Return FALSE for any signals not being handled */
		return FALSE;
}

static void
init_cancel_handler(void)
{
	InitializeCriticalSection(&cancelConnLock);

	SetConsoleCtrlHandler(consoleHandler, TRUE);
}

int
sleep(unsigned int seconds)
{
	Sleep(seconds * 1000);
	return 0;
}

int
usleep(unsigned int usec)
{
	Sleep((usec + 999) / 1000);	/* rounded up */
	return 0;
}

#undef select
static int
select_win32(int nfds, fd_set *readfds, fd_set *writefds, fd_set *exceptfds, const struct timeval * timeout)
{
	struct timeval	remain;

	if (timeout != NULL)
		remain = *timeout;
	else
	{
		remain.tv_usec = 0;
		remain.tv_sec = LONG_MAX;	/* infinite */
	}

	/* sleep only one second because Ctrl+C doesn't interrupt select. */
	while (remain.tv_sec > 0 || remain.tv_usec > 0)
	{
		int				ret;
		struct timeval	onesec;
		fd_set			save_readfds;
		fd_set			save_writefds;
		fd_set			save_exceptfds;

		if (remain.tv_sec > 0)
		{
			onesec.tv_sec = 1;
			onesec.tv_usec = 0;
			remain.tv_sec -= 1;
		}
		else
		{
			onesec.tv_sec = 0;
			onesec.tv_usec = remain.tv_usec;
			remain.tv_usec = 0;
		}

		/* save fds */
		if (readfds)
			memcpy(&save_readfds, readfds, sizeof(fd_set));
		if (writefds)
			memcpy(&save_writefds, writefds, sizeof(fd_set));
		if (exceptfds)
			memcpy(&save_exceptfds, exceptfds, sizeof(fd_set));

		ret = select(nfds, readfds, writefds, exceptfds, &onesec);
		if (ret > 0)
			return ret;	/* succeeded */
		else if (ret < 0)
		{
			/* error */
			_dosmaperr(WSAGetLastError());
			return ret;
		}
		else if (interrupted)
		{
			errno = EINTR;
			return -1;
		}

		/* restore fds */
		if (readfds)
			memcpy(readfds, &save_readfds, sizeof(fd_set));
		if (writefds)
			memcpy(writefds, &save_writefds, sizeof(fd_set));
		if (exceptfds)
			memcpy(exceptfds, &save_exceptfds, sizeof(fd_set));
	}

	return 0;	/* timeout */
}

#endif   /* WIN32 */
