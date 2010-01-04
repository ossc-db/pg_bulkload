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
#include <time.h>
#include <unistd.h>

#include "access/heapam.h"
#include "catalog/catalog.h"
#include "catalog/namespace.h"
#include "commands/dbcommands.h"
#include "executor/executor.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "storage/fd.h"
#include "tcop/tcopprot.h"

#include "logger.h"
#include "pg_loadstatus.h"
#include "pg_strutil.h"
#include "reader.h"
#include "writer.h"

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
static void	ParseControlFile(Reader *rd, const char *fname, const char *options, time_t tm);
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
 * @param fname   [in] path of the control file (absolute path)
 * @param options [in] additonal options
 * @return reader.
 */
Reader *
ReaderCreate(const char *fname, const char *options, time_t tm)
{
	Reader	   *self;

	self = palloc0(sizeof(Reader));
	self->max_parse_errors = -2;
	self->max_dup_errors = -2;
	self->limit = INT64_MAX;

	ParseControlFile(self, fname, options, tm);

	/* initialize parser */
	ParserInit(self->parser, self->infile, self->relid);

	return self;
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
	else if (pg_strcasecmp(keyword, "LOGFILE") == 0)
	{
		ASSERT_ONCE(rd->logfile == NULL);

		rd->logfile = pstrdup(target);
	}
	else if (pg_strcasecmp(keyword, "PARSE_BADFILE") == 0)
	{
		ASSERT_ONCE(rd->parse_badfile == NULL);

		rd->parse_badfile = pstrdup(target);
	}
	else if (pg_strcasecmp(keyword, "DUPLICATE_BADFILE") == 0)
	{
		ASSERT_ONCE(rd->dup_badfile == NULL);

		rd->dup_badfile = pstrdup(target);
	}
	else if (pg_strcasecmp(keyword, "TYPE") == 0)
	{
		const char *keys[] =
		{
			"BINARY",
			"FIXED",	/* alias for backward compatibility. */
			"CSV",
			"TUPLE",
			"FUNCTION",
		};
		const ParserCreate values[] =
		{
			CreateBinaryParser,
			CreateBinaryParser,
			CreateCSVParser,
			CreateTupleParser,
			CreateFunctionParser,
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
	else if (pg_strcasecmp(keyword, "PARSE_ERRORS") == 0 ||
			 pg_strcasecmp(keyword, "MAX_ERR_CNT") == 0)
	{
		ASSERT_ONCE(rd->max_parse_errors < -1);
		rd->max_parse_errors = ParseInt64(target, -1);
		if (rd->max_parse_errors == -1)
			rd->max_parse_errors = INT64_MAX;
	}
	else if (pg_strcasecmp(keyword, "DUPLICATE_ERRORS") == 0)
	{
		ASSERT_ONCE(rd->max_dup_errors < -1);
		rd->max_dup_errors = ParseInt64(target, -1);
		if (rd->max_dup_errors == -1)
			rd->max_dup_errors = INT64_MAX;
	}
	else if (pg_strcasecmp(keyword, "LOAD") == 0 ||
			 pg_strcasecmp(keyword, "LIMIT") == 0)
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
	else if (pg_strcasecmp(keyword, "VERBOSE") == 0)
	{
		rd->verbose = ParseBoolean(target, false);
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
ParseControlFile(Reader *rd, const char *fname, const char *options,
				 time_t tm)
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
	if (rd->logfile == NULL || rd->parse_badfile == NULL ||
		rd->dup_badfile == NULL)
	{
		struct tm  *tp;
		char		path[MAXPGPATH];
		int			len;
		int			elen;
		char	   *dbname;
		char	   *nspname;
		char	   *relname;

		len = snprintf(path, MAXPGPATH, BULKLOAD_LSF_DIR "/");

		tp = localtime(&tm);
		len += strftime(path + len, MAXPGPATH - len, "%Y%m%d%H%M%S_", tp);

		dbname = get_database_name(MyDatabaseId);
		nspname = get_namespace_name(get_rel_namespace(rd->relid));
		relname = get_rel_name(rd->relid);
		len += snprintf(path + len, MAXPGPATH - len, "%s_%s_%s.",dbname,
						nspname, relname);
		pfree(dbname);
		pfree(nspname);
		pfree(relname);

		if (len >= MAXPGPATH)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("default loader output file name is too long")));

		if (rd->logfile == NULL)
		{
			char   *str;

			elen = snprintf(path + len, MAXPGPATH - len, "log");
			if (elen + len >= MAXPGPATH)
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("default loader log file name is too long")));

			str = make_absolute_path(path);
			rd->logfile = pstrdup(str);
			free(str);
		}
		if (rd->parse_badfile == NULL)
		{
			char   *filename;
			char   *extension;
			char   *str;

			filename = strrchr(rd->infile, '/');
			extension = strrchr(rd->infile, '.');
			if (filename && extension && filename < extension)
				extension++;
			else
				extension = "";

			elen = snprintf(path + len, MAXPGPATH - len, "prs.%s", extension);
			if (elen + len >= MAXPGPATH)
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("default parse bad file name is too long")));

			str = make_absolute_path(path);
			rd->parse_badfile = pstrdup(str);
			free(str);
		}
		if (rd->dup_badfile == NULL)
		{
			char   *str;

			elen = snprintf(path + len, MAXPGPATH - len, "dup.csv");
			if (elen + len >= MAXPGPATH)
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("default duplicate bad file name is too long")));

			str = make_absolute_path(path);
			rd->dup_badfile = pstrdup(str);
			free(str);
		}
	}

	if (rd->writer == NULL)
		rd->writer = CreateDirectWriter;
	if (rd->max_parse_errors < -1)
		rd->max_parse_errors = 50;
	if (rd->max_dup_errors < -1)
		rd->max_dup_errors = 50;

	/*
	 * check it whether there is not the same file name.
	 */
	if (strcmp(rd->infile, rd->logfile) == 0 ||
		strcmp(rd->infile, rd->parse_badfile) == 0 ||
		strcmp(rd->infile, rd->dup_badfile) == 0 ||
		strcmp(rd->logfile, rd->parse_badfile) == 0 ||
		strcmp(rd->logfile, rd->dup_badfile) == 0 ||
		strcmp(rd->parse_badfile, rd->dup_badfile) == 0)
		ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("INFILE, PARSE_BADFILE, DUPLICATE_BADFILE and LOGFILE cannot set the same file name.")
#ifdef NOT_USED
			 , errdetail("INFILE = %s\nPARSE_BADFILE = %s\nDUPLICATE_BADFILE = %s\nLOGFILE = %s",
				rd->infile, rd->parse_badfile, rd->dup_badfile, rd->logfile)
#endif
			));
}

/**
 * @brief clean up Reader structure.
 */
int64
ReaderClose(Reader *rd, bool onError)
{
	int64	skip = 0;

	if (rd == NULL)
		return 0;

	/* Close and release members. */
	if (rd->parser)
		skip = ParserTerm(rd->parser);

	if (!onError)
	{
		if (rd->parse_fp != NULL && FreeFile(rd->parse_fp) < 0)
			ereport(WARNING,
					(errcode_for_file_access(),
					 errmsg("could not close parse bad file \"%s\": %m",
							rd->parse_badfile)));
		if (rd->infile != NULL)
			pfree(rd->infile);
		if (rd->logfile != NULL)
			pfree(rd->logfile);
		if (rd->parse_badfile != NULL)
			pfree(rd->parse_badfile);
		if (rd->dup_badfile != NULL)
			pfree(rd->dup_badfile);

		pfree(rd);
	}

	return skip;
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

	ccxt = CurrentMemoryContext;

	eof = false;
	do
	{
		tuple = NULL;
		parser->parsing_field = -1;

		PG_TRY();
		{
			tuple = ParserRead(parser);
			if (tuple == NULL)
				eof = true;
		}
		PG_CATCH();
		{
			ErrorData	   *errdata;
			MemoryContext	ecxt;
			char		   *message;
			StringInfoData	buf;

			if (parser->parsing_field < 0)
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

			/* Absorb parse errors. */
			rd->parse_errors++;
			if (errdata->message)
				message = pstrdup(errdata->message);
			else
				message = "<no error message>";
			FlushErrorState();
			FreeErrorData(errdata);

			initStringInfo(&buf);
			appendStringInfo(&buf, "Parse error Record " int64_FMT
				": Input Record " int64_FMT ": Rejected",
				rd->parse_errors, parser->count);

			if (parser->parsing_field > 0)
				appendStringInfo(&buf, " - column %d", parser->parsing_field);

			appendStringInfo(&buf, ". %s\n", message);

			LoggerLog(WARNING, buf.data);

			/* Terminate if PARSE_ERRORS has been reached. */
			if (rd->parse_errors > rd->max_parse_errors)
			{
				eof = true;
				LoggerLog(WARNING,
					"Maximum parse error count exceeded - " int64_FMT
					" error(s) found in input file\n",
					rd->parse_errors);
			}

			/* output parse bad file. */
			if (rd->parse_fp == NULL)
				if ((rd->parse_fp = AllocateFile(rd->parse_badfile, "w")) == NULL)
					ereport(ERROR,
							(errcode_for_file_access(),
							 errmsg("could not open parse bad file \"%s\": %m",
									rd->parse_badfile)));

			ParserDumpRecord(parser, rd->parse_fp, rd->parse_badfile);
		}
		PG_END_TRY();

	} while (!eof && !tuple);

	return tuple;
}

void
ReaderDumpParams(Reader *self)
{
	char		   *tablename;
	char		   *nspname;
	char		   *relname;
	char		   *str;
	StringInfoData	buf;

	initStringInfo(&buf);

	str = QuoteString(self->infile);
	appendStringInfo(&buf, "INFILE = %s\n", str);
	pfree(str);

	str = QuoteString(self->parse_badfile);
	appendStringInfo(&buf, "PARSE_BADFILE = %s\n", str);
	pfree(str);

	str = QuoteString(self->dup_badfile);
	appendStringInfo(&buf, "DUPLICATE_BADFILE = %s\n", str);
	pfree(str);

	str = QuoteString(self->logfile);
	appendStringInfo(&buf, "LOGFILE = %s\n", str);
	pfree(str);

	nspname = get_namespace_name(get_rel_namespace(self->relid));
	relname = get_rel_name(self->relid);
	tablename = quote_qualified_identifier(nspname, relname);
	str = QuoteString(tablename);
	appendStringInfo(&buf, "TABLE = %s\n", tablename);
	pfree(str);
	pfree(tablename);
	pfree(nspname);
	pfree(relname);

	if (self->max_parse_errors == INT64_MAX)
		appendStringInfo(&buf, "PARSE_ERRORS = INFINITE\n");
	else
		appendStringInfo(&buf, "PARSE_ERRORS = " int64_FMT "\n",
						 self->max_parse_errors);
	if (self->max_dup_errors == INT64_MAX)
		appendStringInfo(&buf, "DUPLICATE_ERRORS = INFINITE\n");
	else
		appendStringInfo(&buf, "DUPLICATE_ERRORS = " int64_FMT "\n",
						 self->max_dup_errors);
	appendStringInfo(&buf, "ON_DUPLICATE = %s\n",
					 ON_DUPLICATE_NAMES[self->on_duplicate]);
	appendStringInfo(&buf, "VERBOSE = %s\n", self->verbose ? "YES" : "NO");
	if (self->limit == INT64_MAX)
		appendStringInfo(&buf, "LOAD = INFINITE\n");
	else
		appendStringInfo(&buf, "LOAD = " int64_FMT "\n", self->limit);

	LoggerLog(INFO, buf.data);
	pfree(buf.data);

	ParserDumpParams(self->parser);
}

void
CheckerInit(Checker *checker, Relation rel)
{
	TupleDesc	desc;

	checker->rel = rel;

	/*
	 * When specify ENCODING, we check the input data encoding.
	 * Convert encoding if the client and server encodings are different.
	 */
	checker->db_encoding = GetDatabaseEncoding();
	if (checker->encoding != -1 && checker->encoding != checker->db_encoding &&
		checker->encoding != PG_SQL_ASCII &&
		checker->db_encoding != PG_SQL_ASCII)
		checker->need_convert = true;

	/* When specify CHECK_CONSTRAINTS, we check the constraints */
	desc = RelationGetDescr(rel);
	if (desc->constr &&
		(checker->check_constraints || desc->constr->has_not_null))
	{
		if (checker->check_constraints)
			checker->need_check_constraint = true;

		if (desc->constr->has_not_null)
			checker->need_check_not_null = true;

		checker->resultRelInfo = makeNode(ResultRelInfo);
		checker->resultRelInfo->ri_RangeTableIndex = 1;		/* dummy */
		checker->resultRelInfo->ri_RelationDesc = rel;
		checker->resultRelInfo->ri_TrigDesc = NULL; /* TRIGGER is not supported */
		checker->resultRelInfo->ri_TrigInstrument = NULL;
	}

	if (checker->need_check_constraint)
	{
		checker->estate = CreateExecutorState();
		checker->estate->es_result_relations = checker->resultRelInfo;
		checker->estate->es_num_result_relations = 1;
		checker->estate->es_result_relation_info = checker->resultRelInfo;

		/* Set up a tuple slot too */
		checker->slot = MakeSingleTupleTableSlot(desc);
	}

	if (!checker->need_check_constraint && !checker->need_check_not_null)
	{
		heap_close(rel, NoLock);
		checker->rel = NULL;
	}
}

void
CheckerTerm(Checker *checker)
{
	if (checker->rel)
		heap_close(checker->rel, NoLock);

	if (checker->slot)
		ExecDropSingleTupleTableSlot(checker->slot);

	if (checker->estate)
		FreeExecutorState(checker->estate);
}

char *
CheckerConversion(Checker *checker, char *src)
{
	return (char *) pg_do_encoding_conversion((unsigned char *) src,
											  strlen(src),
											  checker->encoding,
											  checker->db_encoding);
}

void
CheckerConstraints(Checker *checker, HeapTuple tuple)
{
	/* Place tuple in tuple slot */
	ExecStoreTuple(tuple, checker->slot, InvalidBuffer, false);

	/* Check the constraints of the tuple */
	ExecConstraints(checker->resultRelInfo, checker->slot, checker->estate);
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

		former->desc->attrs[i]->attnotnull = attrs[i]->attnotnull;
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
TupleFormerTuple(TupleFormer *former)
{
	return heap_form_tuple(former->desc, former->values, former->isnull);
}

/* Read null-terminated string and convert to internal format */
Datum
TupleFormerValue(TupleFormer *former, const char *str, int col)
{
	return FunctionCall3(&former->typInput[col],
		CStringGetDatum(str),
		ObjectIdGetDatum(former->typIOParam[col]),
		Int32GetDatum(former->desc->attrs[col]->atttypmod));
}
