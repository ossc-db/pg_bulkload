/*
 * pg_bulkload: lib/logger.c
 *
 *	  Copyright(C) 2007-2009, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

#include "postgres.h"

#include "storage/fd.h"

#include "logger.h"
#include "pg_bulkload.h"

struct Logger
{
	bool	remote;
	bool	verbose;
	char   *logfile;
	FILE   *fp;
	StringInfoData	buf;
};

static Logger logger;

/* ========================================================================
 * Implementation
 * ========================================================================*/

void
CreateLogger(const char *path, bool verbose)
{
	initStringInfo(&logger.buf);
	logger.verbose = verbose;

	if (pg_strcasecmp(path, "remote") == 0)
	{
		logger.remote = true;
		return;
	}

	logger.remote = false;

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
	int			len;
	va_list		args;

	if (logger.remote)
	{
		if (elevel <= INFO)
			return;

		for (;;)
		{
			bool		success;

			len = logger.buf.len;

			/* Try to format the data. */
			va_start(args, fmt);
			success = appendStringInfoVA(&logger.buf, fmt, args);
			va_end(args);

			if (success)
			{
				len = logger.buf.len - len;
				break;
			}
			/* Double the buffer size and try again. */
			enlargeStringInfo(&logger.buf, logger.buf.maxlen);
		}
	}
	else
	{
		va_start(args, fmt);
		len = vfprintf(logger.fp, fmt, args);
		va_end(args);

		if (fflush(logger.fp))
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not write loader log file \"%s\": %m",
							logger.logfile)));
	}

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

char *
LoggerClose(void)
{
	if (logger.fp != NULL && FreeFile(logger.fp) < 0)
		ereport(WARNING,
				(errcode_for_file_access(),
				 errmsg("could not close loader log file \"%s\": %m",
						logger.logfile)));

	logger.remote = false;
	logger.fp = NULL;
	logger.verbose = false;
	if (logger.logfile != NULL)
	{
		pfree(logger.logfile);
		logger.logfile = NULL;
	}

	return logger.buf.data;
}
