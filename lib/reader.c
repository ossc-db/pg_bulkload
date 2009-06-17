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
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "storage/fd.h"
#include "tcop/tcopprot.h"

#include "pg_strutil.h"
#include "reader.h"
#include "writer.h"

extern PGDLLIMPORT CommandDest		whereToSendOutput;

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
	Relation	rel;
	TupleDesc	desc;

	memset(rd, 0, sizeof(Reader));
	rd->max_err_cnt = -1;
	rd->limit = INT64_MAX;

	ParseControlFile(rd, fname, options);

	/* create tuple descriptor without any relation locks */
	rel = heap_open(rd->relid, NoLock);
	desc = RelationGetDescr(rel);

	/*
	 * open source
	 */

	if (pg_strcasecmp(rd->infile, "stdin") == 0)
	{
		if (whereToSendOutput != DestRemote)
			ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("local stdin read is not supported")));

		rd->source = CreateRemoteSource(NULL, desc);
	}
	else if (rd->infile[0] == ':')
	{
		/* shmem id in ":NNNN" form */
		rd->source = CreateQueueSource(rd->infile, desc);
	}
	else
	{
		if (!is_absolute_path(rd->infile))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("relative path not allowed for INFILE: %s", rd->infile)));

		/* must be the super user if load from a file */
		if (!superuser())
			ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to use pg_bulkload from a file"),
				 errhint("Anyone can use pg_bulkload from stdin")));

		rd->source = CreateFileSource(rd->infile, desc);
	}

	/* initialize parser */
	ParserInit(rd->parser, desc);

	heap_close(rel, NoLock);
}

const char *ON_DUPLICATE_NAMES[] =
{
	"ERROR",
	"REMOVE_NEW",
	"REMOVE_OLD"
};

static size_t
choice(const char *name, const char *key, const char *keys[], size_t nkeys)
{
	size_t		i;

	for (i = 0; i < nkeys; i++)
	{
		if (pg_strcasecmp(key, keys[i]) == 0)
			return i;
	}

	ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			errmsg("invalid %s \"%s\"", name, key)));
	return 0;	/* keep compiler quiet */
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
		ASSERT_ONCE(rd->relid == InvalidOid);

		rd->relid = RangeVarGetRelid(makeRangeVarFromNameList(
						stringToQualifiedNameList(target)), false);
	}
	else if (pg_strcasecmp(keyword, "INFILE") == 0)
	{
		ASSERT_ONCE(rd->infile == NULL);

		rd->infile = pstrdup(target);
	}
	else if (pg_strcasecmp(keyword, "TYPE") == 0)
	{
		const char *keys[] =
		{
			"BINARY",
			"FIXED",	/* alias for backward compatibility. */
			"CSV",
			"TUPLE",
		};
		const ParserCreate values[] =
		{
			CreateBinaryParser,
			CreateBinaryParser,
			CreateCSVParser,
			CreateTupleParser,
		};

		ASSERT_ONCE(rd->parser == NULL);
		rd->parser = values[choice(keyword, target, keys, lengthof(keys))]();
	}
	else if (pg_strcasecmp(keyword, "WRITER") == 0 ||
			 pg_strcasecmp(keyword, "LOADER") == 0)
	{
		const char *keys[] =
		{
			"DIRECT",
			"BUFFERED",
			"PARALLEL"
		};
		const WriterCreate values[] =
		{
			CreateDirectWriter,
			CreateBufferedWriter,
			CreateParallelWriter
		};

		ASSERT_ONCE(rd->writer == NULL);
		rd->writer = values[choice(keyword, target, keys, lengthof(keys))];
	}
	else if (pg_strcasecmp(keyword, "MAX_ERR_CNT") == 0)
	{
		ASSERT_ONCE(rd->max_err_cnt < 0);
		rd->max_err_cnt = ParseInt32(target, 0);
	}
	else if (pg_strcasecmp(keyword, "LIMIT") == 0)
	{
		ASSERT_ONCE(rd->limit == INT64_MAX);
		rd->limit = ParseInt64(target, 0);
	}
	else if (pg_strcasecmp(keyword, "ON_DUPLICATE") == 0)
	{
		const ON_DUPLICATE values[] =
		{
			ON_DUPLICATE_ERROR,
			ON_DUPLICATE_REMOVE_NEW,
			ON_DUPLICATE_REMOVE_OLD
		};

		rd->on_duplicate = values[choice(keyword, target, ON_DUPLICATE_NAMES, lengthof(values))];
	}
	else if (rd->parser == NULL ||
			!ParserParam(rd->parser, keyword, target))
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
	if (rd->parser == NULL)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("no TYPE specified")));
	if (rd->relid == InvalidOid)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("no TABLE specified")));
	if (rd->infile == NULL)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("no INFILE specified")));

	/*
	 * Set defaults to unspecified parameters.
	 */
	if (rd->writer == NULL)
		rd->writer = CreateDirectWriter;
	if (rd->max_err_cnt < 0)
		rd->max_err_cnt = 0;
}

/**
 * @brief clean up Reader structure.
 *
 * @param rd [in/out] reader
 * @return void
 */
void
ReaderClose(Reader *rd)
{
	if (rd == NULL)
		return;

	/* Close and release members. */
	if (rd->parser)
		ParserTerm(rd->parser);
	if (rd->source)
		SourceClose(rd->source);
	if (rd->infile != NULL)
		pfree(rd->infile);

	/* Report error count and abort if limit exceeded. */
	if (rd->errors > 0)
	{
		if (rd->errors > rd->max_err_cnt)
		{
			ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("%d error(s) found in input file", rd->errors)));
		}
		else
		{
			ereport(WARNING,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("skip %d error(s) in input file", rd->errors)));
		}
	}
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
	Parser		   *parser = rd->parser;
	Source		   *source = rd->source;

	ccxt = CurrentMemoryContext;

	eof = false;
	do
	{
		tuple = NULL;
		parser->parsing_field = 0;

		PG_TRY();
		{
			tuple = ParserRead(parser, source);
			if (tuple == NULL)
				eof = true;
		}
		PG_CATCH();
		{
			ErrorData	   *errdata;
			MemoryContext	ecxt;
			char		   *message;

			if (parser->parsing_field <= 0)
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
			rd->errors++;
			if (errdata->message)
				message = pstrdup(errdata->message);
			else
				message = "<no error message>";
			FlushErrorState();
			FreeErrorData(errdata);

			ereport(WARNING,
				(errmsg("BULK LOAD ERROR (row=" int64_FMT ", col=%d) %s",
					parser->count, parser->parsing_field, message)));

			/* Terminate if MAX_ERR_CNT has been reached. */
			if (rd->errors > rd->max_err_cnt)
				eof = true;
		}
		PG_END_TRY();

	} while (!eof && !tuple);

	return tuple;
}

void
TupleFormerInit(TupleFormer *former, TupleDesc desc)
{
	Form_pg_attribute  *attrs;
	AttrNumber			natts;
	int					i;

	former->desc = CreateTupleDescCopy(desc);

	/*
	 * allocate buffer to store columns
	 */
	natts = desc->natts;
	former->values = palloc(sizeof(Datum) * natts);
	former->isnull = palloc(sizeof(bool) * natts);
	MemSet(former->isnull, true, sizeof(bool) * natts);

	/*
	 * get column information of the target relation
	 */
	attrs = desc->attrs;
	former->typIOParam = (Oid *) palloc(natts * sizeof(Oid));
	former->typInput = (FmgrInfo *) palloc(natts * sizeof(FmgrInfo));
	former->attnum = palloc(natts * sizeof(int));
	former->nfields = 0;
	for (i = 0; i < natts; i++)
	{
		Oid	in_func_oid;

		/* ignore dropped columns */
		if (attrs[i]->attisdropped)
			continue;

		/* get type information and input function */
		getTypeInputInfo(attrs[i]->atttypid,
					 &in_func_oid, &former->typIOParam[i]);
		fmgr_info(in_func_oid, &former->typInput[i]);

		/* update valid column information */
		former->attnum[former->nfields] = i;
		former->nfields++;
	}
}

void
TupleFormerTerm(TupleFormer *former)
{
	if (former->typIOParam)
		pfree(former->typIOParam);

	if (former->typInput)
		pfree(former->typInput);

	if (former->values)
		pfree(former->values);

	if (former->isnull)
		pfree(former->isnull);

	if (former->attnum)
		pfree(former->attnum);

	if (former->desc)
		FreeTupleDesc(former->desc);
}

HeapTuple
TupleFormerForm(TupleFormer *former)
{
	return heap_form_tuple(former->desc, former->values, former->isnull);
}
