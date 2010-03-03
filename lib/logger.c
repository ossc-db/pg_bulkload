/*
 * pg_bulkload: lib/logger.c
 *
 *	  Copyright (c) 2007-2010, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

#include "pg_bulkload.h"

#include "storage/fd.h"

#include "logger.h"

struct Logger
{
	bool			verbose;
	char		   *logfile;
	FILE		   *fp;
	StringInfoData	buf;
};

static Logger logger;

/* ========================================================================
 * Implementation
 * ========================================================================*/

void
CreateLogger(const char *path, bool verbose)
{
	memset(&logger, 0, sizeof(logger));

	logger.verbose = verbose;

	if (pg_strcasecmp(path, "remote") == 0)
	{
		initStringInfo(&logger.buf);
	}
	else
	{
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
}

void
LoggerLog(int elevel, const char *fmt,...)
{
	int			len;
	va_list		args;

	if (logger.fp)
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
	else if (logger.buf.data)
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
		return;		/* logger is not ready */
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
	char *messages;

	if (logger.fp != NULL && FreeFile(logger.fp) < 0)
		ereport(WARNING,
				(errcode_for_file_access(),
				 errmsg("could not close loader log file \"%s\": %m",
						logger.logfile)));

	if (logger.logfile != NULL)
		pfree(logger.logfile);

	messages = logger.buf.data;

	memset(&logger, 0, sizeof(logger));

	return messages;
}
