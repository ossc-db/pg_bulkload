/*
 * pg_bulkload: lib/reader.c
 *
 *	  Copyright(C) 2007-2009, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

/**
 * @file
 * @brief Implementation of reader module
 */
#include "postgres.h"

#include <fcntl.h>
#include <unistd.h>

#include "access/heapam.h"
#include "catalog/catalog.h"
#include "catalog/namespace.h"
#include "executor/executor.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "tcop/tcopprot.h"

#include "pg_strutil.h"
#include "reader.h"
#include "writer.h"

extern PGDLLIMPORT CommandDest whereToSendOutput;
extern PGDLLIMPORT ProtocolVersion FrontendProtocol;

/**
 * @brief  length of a line in control file
 */
#define LINEBUF 1024

typedef struct ControlFileLine
{
	const char *keyword;
	const char *value;
	int			line;
} ControlFileLine;

/*
 * prototype declaration of internal function
 */
static void	ParseControlFile(Reader *rd, const char *fname, const char *options);
static void ParseControlFileLine(Reader *rd, ControlFileLine *line, char *buf);
static void ParseErrorCallback(void *arg);
static void ReaderCopyBegin(Reader *rd);

/**
 * @brief Initialize Reader
 *
 * Processing flow
 *	 -# open relation
 *	 -# open input file
 *	 -# open data file
 *	 -# get function information for type information and type transformation.
 *
 * @param rd      [in] reader
 * @param fname   [in] path of the control file (absolute path)
 * @param options [in] additonal options
 * @return None.
 */
void
ReaderOpen(Reader *rd, const char *fname, const char *options)
{
	Form_pg_attribute  *attr;
	int					attnum;
	Oid					in_func_oid;
	AttrNumber			natts;

	memset(rd, 0, sizeof(Reader));
	rd->ci_max_err_cnt = -1;
	rd->ci_offset = -1;
	rd->ci_limit = INT64_MAX;

	ParseControlFile(rd, fname, options);

	/*
	 * open relation and do a sanity check
	 */
	rd->ci_rel = heap_openrv(rd->ci_rv, AccessShareLock);

	/*
	 * allocate buffer to store columns
	 */
	natts = RelationGetDescr(rd->ci_rel)->natts;
	rd->ci_values = palloc(sizeof(Datum) * natts);
	rd->ci_isnull = palloc(sizeof(bool) * natts);
	MemSet(rd->ci_isnull, true, sizeof(bool) * natts);

	/*
	 * get column information of the target relation
	 */
	attr = RelationGetDescr(rd->ci_rel)->attrs;
	rd->ci_typeioparams = (Oid *) palloc(natts * sizeof(Oid));
	rd->ci_in_functions = (FmgrInfo *) palloc(natts * sizeof(FmgrInfo));
	rd->ci_attnumlist = palloc(natts * sizeof(int));
	rd->ci_attnumcnt = 0;
	for (attnum = 1; attnum <= natts; attnum++)
	{
		/* ignore dropped columns */
		if (attr[attnum - 1]->attisdropped)
			continue;

		/* get type information and input function */
		getTypeInputInfo(attr[attnum - 1]->atttypid,
					 &in_func_oid, &rd->ci_typeioparams[attnum - 1]);
		fmgr_info(in_func_oid, &rd->ci_in_functions[attnum - 1]);

		/* update valid column information */
		rd->ci_attnumlist[rd->ci_attnumcnt] = attnum - 1;
		rd->ci_attnumcnt++;
	}

	/*
	 * open input data file
	 */
	if (pg_strcasecmp(rd->ci_infname, "stdin") == 0)
	{
		if (whereToSendOutput == DestRemote)
			ReaderCopyBegin(rd);
		else
			rd->ci_infd = stdin;
	}
	else
	{
		/* must be the super user if load from a file */
		if (!superuser())
			ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to use pg_bulkload()")));

		rd->ci_infd = AllocateFile(rd->ci_infname, "r");
		if (rd->ci_infd == NULL)
			ereport(ERROR, (errcode_for_file_access(),
				errmsg("could not open \"%s\" %m", rd->ci_infname)));
	}

	ParserInit(rd->ci_parser, rd);
}

/**
 * @brief Parse a line in control file.
 * @param rd   [in] reader
 * @param line [in] current line
 * @param buf  [in] line buffer
 * @return None
 */
static void
ParseControlFileLine(Reader *rd, ControlFileLine *line, char *buf)
{
	char	   *keyword = NULL;
	char	   *target = NULL;
	char	   *p;
	char	   *q;

	line->line++;
	line->keyword = NULL;
	line->value = NULL;

	if (buf[strlen(buf) - 1] != '\n')
		ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
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
	p = TrimSpace(buf);
	if (*p == '\0')
		return;

	/*
	 * devide after '='
	 */
	q = FindUnquotedChar(buf, '=', '"', '\\');
	if (q != NULL)
		*q = '\0';
	else
		ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
						errmsg("invalid input \"%s\"", buf)));

	q++;					/* pointer to input value */

	/*
	 * return a value trimmed space
	 */
	keyword = TrimSpace(p);
	target = TrimSpace(q);
	if (target)
	{
		target = UnquoteString(target, '"', '\\');
		if (!target)
			ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
							errmsg("unterminated quoted field")));
	}

	line->keyword = keyword;
	line->value = target;

	/*
	 * result
	 */
	if (pg_strcasecmp(keyword, "TABLE") == 0)
	{
		ASSERT_ONCE(rd->ci_rv == NULL);

		rd->ci_rv = makeRangeVarFromNameList(
						stringToQualifiedNameList(target));
	}
	else if (pg_strcasecmp(keyword, "INFILE") == 0)
	{
		ASSERT_ONCE(rd->ci_infname == NULL);

		if (!is_absolute_path(target))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("relative path not allowed for INFILE: %s", target)));

		rd->ci_infname = pstrdup(target);
	}
	else if (pg_strcasecmp(keyword, "TYPE") == 0)
	{
		ASSERT_ONCE(rd->ci_parser == NULL);

		if (pg_strcasecmp(target, "FIXED") == 0)
			rd->ci_parser = CreateFixedParser();
		else if (pg_strcasecmp(target, "CSV") == 0)
			rd->ci_parser = CreateCSVParser();
		else
			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							errmsg("invalid file type \"%s\"", target)));
	}
	else if (pg_strcasecmp(keyword, "LOADER") == 0)
	{
		ASSERT_ONCE(rd->ci_loader == NULL);
		
		if (pg_strcasecmp(target, "DIRECT") == 0)
			rd->ci_loader = CreateDirectLoader;
		else if (pg_strcasecmp(target, "BUFFERED") == 0)
			rd->ci_loader = CreateBufferedLoader;
		else
			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							errmsg("invalid loader \"%s\"", target)));
	}
	else if (pg_strcasecmp(keyword, "MAX_ERR_CNT") == 0)
	{
		ASSERT_ONCE(rd->ci_max_err_cnt < 0);
		rd->ci_max_err_cnt = ParseInt32(target, 0);
	}
	else if (pg_strcasecmp(keyword, "OFFSET") == 0)
	{
		ASSERT_ONCE(rd->ci_offset < 0);
		rd->ci_offset = ParseInt64(target, 0);
	}
	else if (pg_strcasecmp(keyword, "LIMIT") == 0)
	{
		ASSERT_ONCE(rd->ci_limit == INT64_MAX);
		rd->ci_limit = ParseInt64(target, 0);
	}
	else if (pg_strcasecmp(keyword, "ON_DUPLICATE") == 0)
	{
		if (pg_strcasecmp(target, "ERROR") == 0)
			rd->on_duplicate = ON_DUPLICATE_ERROR;
		else if (pg_strcasecmp(target, "REMOVE_NEW") == 0)
			rd->on_duplicate = ON_DUPLICATE_REMOVE_NEW;
		else if (pg_strcasecmp(target, "REMOVE_OLD") == 0)
			rd->on_duplicate = ON_DUPLICATE_REMOVE_OLD;
		else
			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							errmsg("invalid ON_DUPLICATE \"%s\"", target)));
	}
	else if (rd->ci_parser == NULL ||
			!ParserParam(rd->ci_parser, keyword, target))
	{
		ereport(ERROR,
			(errcode(ERRCODE_UNDEFINED_OBJECT),
			 errmsg("invalid keyword \"%s\"", keyword)));
	}
}

/**
 * @brief Reading information from control file
 *
 * Processing flow
 * -# reading one line from control file
 * -# executing the following processes with looping
 *	 -# getting a keyword and an input string
 *	 -# copy information from the control file to structures
 *	 -# checking necessary items
 * @return void
 */
static void
ParseControlFile(Reader *rd, const char *fname, const char *options)
{
	char					buf[LINEBUF];
	ControlFileLine			line = { 0 };
	ErrorContextCallback	errcontext;

	errcontext.callback = ParseErrorCallback;
	errcontext.arg = &line;
	errcontext.previous = error_context_stack;
	error_context_stack = &errcontext;

	/* extract keywords and values from control file */
	if (fname && fname[0])
	{
		FILE	   *file;

		if (!is_absolute_path(fname))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_NAME),
					 errmsg("control file name must be absolute path")));

		if ((file = AllocateFile(fname, "rt")) == NULL)
			ereport(ERROR, (errcode_for_file_access(),
							errmsg("could not open \"%s\" %m", fname)));

		while (fgets(buf, LINEBUF, file) != NULL)
			ParseControlFileLine(rd, &line, buf);

		FreeFile(file);
	}

	/* extract keywords and values from text options */
	if (options && options[0])
	{
		char *r;
		for (r = strchr(options, '\n'); r; r = strchr(options, '\n'))
		{
			size_t	len = Min(r - options + 1, LINEBUF);
			memcpy(buf, options, len);
			buf[len] = '\0';
			ParseControlFileLine(rd, &line, buf);
			options = r + 1;
		}
	}

	error_context_stack = errcontext.previous;

	/*
	 * checking necessary common setting items
	 */
	if (rd->ci_parser == NULL)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("no TYPE specified")));
	if (rd->ci_rv == NULL)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("no TABLE specified")));
	if (rd->ci_infname == NULL)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("no INFILE specified")));

	/*
	 * Set defaults to unspecified parameters.
	 */
	if (rd->ci_loader == NULL)
		rd->ci_loader = CreateDirectLoader;
	if (rd->ci_max_err_cnt < 0)
		rd->ci_max_err_cnt = 0;
	if (rd->ci_offset < 0)
		rd->ci_offset = 0;
}

/**
 * @brief clean up Reader structure.
 *
 * Processing flow
 * -# close relation
 * -# free Reader structure
 * @param rd [in/out] control information
 * @return void
 */
void
ReaderClose(Reader *rd)
{
	if (rd == NULL)
		return;

	/* Terminate parser. Be sure to set ci_parser to NULL. */
	if (rd->ci_parser)
	{
		ParserTerm(rd->ci_parser);
		rd->ci_parser = NULL;
	}

	/* If an error has been found, abort. */
	if (rd->ci_err_cnt > 0)
	{
		if (rd->ci_err_cnt > rd->ci_max_err_cnt)
		{
			ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("%d error(s) found in input file",
						rd->ci_err_cnt)));
		}
		else
		{
			ereport(WARNING,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("skip %d error(s) in input file",
						rd->ci_err_cnt)));
		}
	}

	if (rd->source == COPY_FILE &&
		rd->ci_infd != NULL &&
		rd->ci_infd != stdin &&
		FreeFile(rd->ci_infd) < 0)
	{
		ereport(WARNING, (errcode_for_file_access(),
			errmsg("could not close \"%s\" %m", rd->ci_infname)));
	}

	heap_close(rd->ci_rel, AccessShareLock);

	/*
	 * FIXME: We might not need to free each fields because memories
	 * are automatically freeed at the end of query.
	 */

	if (rd->ci_rv != NULL)
		pfree(rd->ci_rv);

	if (rd->ci_infname != NULL)
		pfree(rd->ci_infname);

	if (rd->ci_typeioparams != NULL)
		pfree(rd->ci_typeioparams);

	if (rd->ci_in_functions)
		pfree(rd->ci_in_functions);

	if (rd->ci_values)
		pfree(rd->ci_values);

	if (rd->ci_isnull)
		pfree(rd->ci_isnull);

	if (rd->ci_attnumlist)
		pfree(rd->ci_attnumlist);
}

/**
 * @brief log extra information during parsing control file.
 */
static void
ParseErrorCallback(void *arg)
{
	ControlFileLine *line = (ControlFileLine *) arg;

	if (line->keyword && line->value)
		errcontext("line %d: \"%s = %s\"",
			line->line, line->keyword, line->value);
	else
		errcontext("line %d", line->line);
}

/**
 * @brief read some data from source
 *
 * borrowed from CopyGetData.
 */
size_t
SourceRead(Reader *rd, void *buffer, size_t len)
{
	size_t		bytesread = 0;

	switch (rd->source)
	{
		case COPY_FILE:
			bytesread = fread(buffer, 1, len, rd->ci_infd);
			if (ferror(rd->ci_infd))
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not read from COPY file: %m")));
			break;
		case COPY_OLD_FE:
			if (pq_getbytes((char *) buffer, 1))
			{
				/* Only a \. terminator is legal EOF in old protocol */
				ereport(ERROR,
						(errcode(ERRCODE_CONNECTION_FAILURE),
						 errmsg("unexpected EOF on client connection")));
			}
			bytesread = 1;
			break;
		case COPY_NEW_FE:
			while (len > 0 && bytesread < 1 && !rd->source_eof)
			{
				int			avail;

				while (rd->fe_msgbuf->cursor >= rd->fe_msgbuf->len)
				{
					/* Try to receive another message */
					int			mtype;

			readmessage:
					mtype = pq_getbyte();
					if (mtype == EOF)
						ereport(ERROR,
								(errcode(ERRCODE_CONNECTION_FAILURE),
							 errmsg("unexpected EOF on client connection")));
					if (pq_getmessage(rd->fe_msgbuf, 0))
						ereport(ERROR,
								(errcode(ERRCODE_CONNECTION_FAILURE),
							 errmsg("unexpected EOF on client connection")));
					switch (mtype)
					{
						case 'd':		/* CopyData */
							break;
						case 'c':		/* CopyDone */
							/* COPY IN correctly terminated by frontend */
							rd->source_eof = true;
							return bytesread;
						case 'f':		/* CopyFail */
							ereport(ERROR,
									(errcode(ERRCODE_QUERY_CANCELED),
									 errmsg("COPY from stdin failed: %s",
									   pq_getmsgstring(rd->fe_msgbuf))));
							break;
						case 'H':		/* Flush */
						case 'S':		/* Sync */

							/*
							 * Ignore Flush/Sync for the convenience of client
							 * libraries (such as libpq) that may send those
							 * without noticing that the command they just
							 * sent was COPY.
							 */
							goto readmessage;
						default:
							ereport(ERROR,
									(errcode(ERRCODE_PROTOCOL_VIOLATION),
									 errmsg("unexpected message type 0x%02X during COPY from stdin",
											mtype)));
							break;
					}
				}
				avail = rd->fe_msgbuf->len - rd->fe_msgbuf->cursor;
				if (avail > len)
					avail = len;
				pq_copymsgbytes(rd->fe_msgbuf, buffer, avail);
				buffer = (void *) ((char *) buffer + avail);
				len -= avail;
				bytesread += avail;
			}
			break;
	}

	return bytesread;
}

#define IsBinaryCopy(rd)	(false)

static void
ReaderCopyBegin(Reader *rd)
{
	if (PG_PROTOCOL_MAJOR(FrontendProtocol) >= 3)
	{
		/* new way */
		StringInfoData buf;
		int			natts = rd->ci_attnumcnt;
		int16		format = (IsBinaryCopy(rd) ? 1 : 0);
		int			i;

		pq_beginmessage(&buf, 'G');
		pq_sendbyte(&buf, format);		/* overall format */
		pq_sendint(&buf, natts, 2);
		for (i = 0; i < natts; i++)
			pq_sendint(&buf, format, 2);		/* per-column formats */
		pq_endmessage(&buf);
		rd->source = COPY_NEW_FE;
		rd->fe_msgbuf = makeStringInfo();
	}
	else if (PG_PROTOCOL_MAJOR(FrontendProtocol) >= 2)
	{
		/* old way */
		if (IsBinaryCopy(rd))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			errmsg("COPY BINARY is not supported to stdout or from stdin")));
		pq_putemptymessage('G');
		rd->source = COPY_OLD_FE;
	}
	else
	{
		/* very old way */
		if (IsBinaryCopy(rd))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			errmsg("COPY BINARY is not supported to stdout or from stdin")));
		pq_putemptymessage('D');
		rd->source = COPY_OLD_FE;
	}
	/* We *must* flush here to ensure FE knows it can send. */
	pq_flush();
}

/**
 * @brief Read the next tuple from parser.
 * @param rd  [in/out] reader
 * @return type
 */
HeapTuple
ReaderNext(Reader *rd)
{
	HeapTuple		tuple;
	MemoryContext	ccxt;
	bool			eof;

	ccxt = CurrentMemoryContext;

	eof = false;
	do
	{
		tuple = NULL;
		rd->ci_parsing_field = 0;

		PG_TRY();
		{
			if (ParserRead(rd->ci_parser, rd))
				tuple = heap_form_tuple(RelationGetDescr(rd->ci_rel),
										rd->ci_values, rd->ci_isnull);
			else
				eof = true;
		}
		PG_CATCH();
		{
			ErrorData	   *errdata;
			MemoryContext	ecxt;
			char		   *message;

			if (rd->ci_parsing_field <= 0)
				PG_RE_THROW();	/* should not ignore */

			ecxt = MemoryContextSwitchTo(ccxt);
			errdata = CopyErrorData();

			/* We cannot ignore query aborts. */
			switch (errdata->sqlerrcode)
			{
				case ERRCODE_ADMIN_SHUTDOWN:
				case ERRCODE_QUERY_CANCELED:
					MemoryContextSwitchTo(ecxt);
					PG_RE_THROW();
					break;
			}

			/* Absorb general errors. */
			rd->ci_err_cnt++;
			if (errdata->message)
				message = pstrdup(errdata->message);
			else
				message = "<no error message>";
			FlushErrorState();
			FreeErrorData(errdata);

			ereport(WARNING,
				(errmsg("BULK LOAD ERROR (row=" int64_FMT ", col=%d) %s",
					rd->ci_read_cnt, rd->ci_parsing_field, message)));

			/* Terminate if MAX_ERR_CNT has been reached. */
			if (rd->ci_err_cnt > rd->ci_max_err_cnt)
				eof = true;
		}
		PG_END_TRY();

	} while (!eof && !tuple);

	if (!tuple)
		return NULL;	/* EOF */

	if (rd->ci_rel->rd_rel->relhasoids)
	{
		Assert(!OidIsValid(HeapTupleGetOid(tuple)));
		HeapTupleSetOid(tuple, GetNewOid(rd->ci_rel));
	}

	return tuple;
}
