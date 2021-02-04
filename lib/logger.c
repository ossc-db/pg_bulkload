/*
 * pg_bulkload: lib/logger.c
 *
 *	  Copyright (c) 2007-2021, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

#include "pg_bulkload.h"

#include <sys/file.h>

#include "storage/fd.h"

#include "logger.h"

#if defined(LOCK_EX) && defined(LOCK_UN)
#define HAVE_FLOCK		/* no flock in win32 */
#endif

struct Logger
{
	bool	verbose;
	bool	writer;
	char   *logfile;
	FILE   *fp;
};

static Logger logger;

/* ========================================================================
 * Implementation
 * ========================================================================*/

void
CreateLogger(const char *path, bool verbose, bool writer)
{
	memset(&logger, 0, sizeof(logger));

	logger.verbose = verbose;
	logger.writer = writer;

	if (!is_absolute_path(path))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("relative path not allowed for LOGFILE: %s", path)));

	logger.logfile = pstrdup(path);
	logger.fp = AllocateFile(logger.logfile, "at");
	if (logger.fp == NULL)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open loader log file \"%s\": %m",
						logger.logfile)));
}

void
LoggerLog(int elevel, const char *fmt,...)
{
#ifdef HAVE_FLOCK
	int			fd;
#endif
	int			len;
	va_list		args;

	if (logger.writer && elevel <= INFO)
		return;

	if (!logger.fp)
		return;		/* logger is not ready */

#ifdef HAVE_FLOCK
	if ((fd = fileno(logger.fp)) == -1 || flock(fd, LOCK_EX) == -1)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not lock loader log file \"%s\": %m",
						logger.logfile)));
#endif

	if (fseek(logger.fp, 0, SEEK_END) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not seek loader log file \"%s\": %m",
						logger.logfile)));

	va_start(args, fmt);
	len = vfprintf(logger.fp, fmt, args);
	va_end(args);

	if (fflush(logger.fp))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write loader log file \"%s\": %m",
						logger.logfile)));

#ifdef HAVE_FLOCK
	if (flock(fd, LOCK_UN) == -1)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not lock loader log file \"%s\": %m",
						logger.logfile)));
#endif

	if (elevel >= ERROR || (logger.verbose && elevel >= WARNING))
	{
		char   *buf;

		buf = palloc(len + 1);

		va_start(args, fmt);
		vsnprintf(buf, len + 1, fmt, args);
		va_end(args);

		while (len > 0 && isspace((unsigned char) buf[len - 1]))
			len--;
		buf[len] = '\0';

		ereport(elevel, (errmsg("%s", buf)));
		pfree(buf);
	}
}

void
LoggerClose(void)
{
	if (logger.fp != NULL && FreeFile(logger.fp) < 0)
		ereport(WARNING,
				(errcode_for_file_access(),
				 errmsg("could not close loader log file \"%s\": %m",
						logger.logfile)));

	if (logger.logfile != NULL)
		pfree(logger.logfile);

	memset(&logger, 0, sizeof(logger));
}
