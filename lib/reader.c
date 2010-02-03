/*
 * pg_bulkload: lib/reader.c
 *
 *	  Copyright (c) 2007-2010, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
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
#include "access/reloptions.h"
#include "catalog/catalog.h"
#include "catalog/namespace.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "commands/dbcommands.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "funcapi.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/resowner.h"
#include "utils/syscache.h"
#include "utils/typcache.h"
#include "storage/fd.h"
#include "tcop/tcopprot.h"

#include "logger.h"
#include "pg_loadstatus.h"
#include "pg_strutil.h"
#include "reader.h"
#include "writer.h"

#define DEFAULT_MAX_PARSE_ERRORS		50
#define DEFAULT_MAX_DUP_ERRORS			50

/*
 * prototype declaration of internal function
 */
static void ParseOption(Reader *rd, DefElem *opt);

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
ReaderCreate(Datum options, time_t tm)
{
	Reader	   *self;
	List	   *defs;
	ListCell   *cell;

	self = palloc0(sizeof(Reader));
	self->max_parse_errors = -2;
	self->wo.max_dup_errors = -2;
	self->limit = INT64_MAX;

	/* parse for each option */
	defs = untransformRelOptions(options);
	foreach (cell, defs)
		ParseOption(self, lfirst(cell));

	/*
	 * checking necessary common setting items
	 */
	if (self->parser == NULL)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("TYPE option required")));
	if (self->relid == InvalidOid)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("TABLE option required")));
	if (self->infile == NULL)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("INFILE option required")));

	/*
	 * Set defaults to unspecified parameters.
	 */
	if (self->writer == NULL)
		self->writer = CreateDirectWriter;
	if (self->max_parse_errors < -1)
		self->max_parse_errors = DEFAULT_MAX_PARSE_ERRORS;
	if (self->wo.max_dup_errors < -1)
		self->wo.max_dup_errors = DEFAULT_MAX_DUP_ERRORS;

	/*
	 * If log paths are not specified, generate them with the following rules:
	 *   {basename}        : $PGDATA/pg_bulkload/YYYYMMDDHHMISS_{db}_{nsp}_{tbl}
	 *   LOGFILE           : {basename}.log
	 *   PARSE_BADFILE     : {basename}.prs.{extension-of-infile}
	 *   DUPLICATE_BADFILE : {basename}.dup.csv
	 */
	if (self->logfile == NULL ||
		self->parse_badfile == NULL ||
		self->wo.dup_badfile == NULL)
	{
		char		path[MAXPGPATH] = BULKLOAD_LSF_DIR "/";
		int			len;
		int			elen;
		char	   *dbname;
		char	   *nspname;
		char	   *relname;

		if (getcwd(path, MAXPGPATH) == NULL)
			elog(ERROR, "could not get current working directory: %m");
		len = strlen(path);
		len += snprintf(path + len, MAXPGPATH - len, "/%s/", BULKLOAD_LSF_DIR);
		len += strftime(path + len, MAXPGPATH - len, "%Y%m%d%H%M%S_",
						localtime(&tm));

		/* TODO: cleanup characters in object names, ex. ?, *, etc. */
		dbname = get_database_name(MyDatabaseId);
		nspname = get_namespace_name(get_rel_namespace(self->relid));
		relname = get_rel_name(self->relid);
		len += snprintf(path + len, MAXPGPATH - len, "%s_%s_%s.",
						dbname, nspname, relname);
		pfree(dbname);
		pfree(nspname);
		pfree(relname);

		if (len >= MAXPGPATH)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("default loader output file name is too long")));

		canonicalize_path(path);

		if (self->logfile == NULL)
		{
			elen = snprintf(path + len, MAXPGPATH - len, "log");
			if (elen + len >= MAXPGPATH)
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("default loader log file name is too long")));

			self->logfile = pstrdup(path);
		}

		if (self->parse_badfile == NULL)
		{
			char   *filename;
			char   *extension;

			/* find the extension of infile */
			filename = strrchr(self->infile, '/');
			extension = strrchr(self->infile, '.');
			if (!filename || !extension || filename >= extension)
				extension = "";

			elen = snprintf(path + len, MAXPGPATH - len, "prs%s", extension);
			if (elen + len >= MAXPGPATH)
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("default parse bad file name is too long")));

			self->parse_badfile = pstrdup(path);
		}

		if (self->wo.dup_badfile == NULL)
		{
			elen = snprintf(path + len, MAXPGPATH - len, "dup.csv");
			if (elen + len >= MAXPGPATH)
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("default duplicate bad file name is too long")));

			self->wo.dup_badfile = pstrdup(path);
		}
	}

	/*
	 * check it whether there is not the same file name.
	 */
	if (strcmp(self->infile, self->logfile) == 0 ||
		strcmp(self->infile, self->parse_badfile) == 0 ||
		strcmp(self->infile, self->wo.dup_badfile) == 0 ||
		strcmp(self->logfile, self->parse_badfile) == 0 ||
		strcmp(self->logfile, self->wo.dup_badfile) == 0 ||
		strcmp(self->parse_badfile, self->wo.dup_badfile) == 0)
		ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("INFILE, PARSE_BADFILE, DUPLICATE_BADFILE and LOGFILE cannot set the same file name.")
#ifdef NOT_USED
			 , errdetail("INFILE = %s\nPARSE_BADFILE = %s\nDUPLICATE_BADFILE = %s\nLOGFILE = %s",
				self->infile, self->parse_badfile, self->wo.dup_badfile, self->logfile)
#endif
			));

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
 * @param opt  [in] option
 * @return None
 */
static void
ParseOption(Reader *rd, DefElem *opt)
{
	char	   *keyword;
	char	   *target;

	if (opt->arg == NULL)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("option %s has no value", opt->defname)));

	keyword = opt->defname;
	target = strVal(opt->arg);

	/*
	 * result
	 */
	if (CompareKeyword(keyword, "TABLE"))
	{
		ASSERT_ONCE(rd->relid == InvalidOid);

		rd->relid = RangeVarGetRelid(makeRangeVarFromNameList(
						stringToQualifiedNameList(target)), false);
	}
	else if (CompareKeyword(keyword, "INFILE"))
	{
		ASSERT_ONCE(rd->infile == NULL);

		rd->infile = pstrdup(target);
	}
	else if (CompareKeyword(keyword, "LOGFILE"))
	{
		ASSERT_ONCE(rd->logfile == NULL);

		rd->logfile = pstrdup(target);
	}
	else if (CompareKeyword(keyword, "PARSE_BADFILE"))
	{
		ASSERT_ONCE(rd->parse_badfile == NULL);

		rd->parse_badfile = pstrdup(target);
	}
	else if (CompareKeyword(keyword, "DUPLICATE_BADFILE"))
	{
		ASSERT_ONCE(rd->wo.dup_badfile == NULL);

		rd->wo.dup_badfile = pstrdup(target);
	}
	else if (CompareKeyword(keyword, "TYPE"))
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
	else if (CompareKeyword(keyword, "WRITER") ||
			 CompareKeyword(keyword, "LOADER"))
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
	else if (CompareKeyword(keyword, "PARSE_ERRORS") ||
			 CompareKeyword(keyword, "MAX_ERR_CNT"))
	{
		ASSERT_ONCE(rd->max_parse_errors < -1);
		rd->max_parse_errors = ParseInt64(target, -1);
		if (rd->max_parse_errors == -1)
			rd->max_parse_errors = INT64_MAX;
	}
	else if (CompareKeyword(keyword, "DUPLICATE_ERRORS"))
	{
		ASSERT_ONCE(rd->wo.max_dup_errors < -1);
		rd->wo.max_dup_errors = ParseInt64(target, -1);
		if (rd->wo.max_dup_errors == -1)
			rd->wo.max_dup_errors = INT64_MAX;
	}
	else if (CompareKeyword(keyword, "LOAD") ||
			 CompareKeyword(keyword, "LIMIT"))
	{
		ASSERT_ONCE(rd->limit == INT64_MAX);
		rd->limit = ParseInt64(target, 0);
	}
	else if (CompareKeyword(keyword, "ON_DUPLICATE"))
	{
		const ON_DUPLICATE values[] =
		{
			ON_DUPLICATE_ERROR,
			ON_DUPLICATE_REMOVE_NEW,
			ON_DUPLICATE_REMOVE_OLD
		};

		rd->wo.on_duplicate = values[choice(keyword, target, ON_DUPLICATE_NAMES, lengthof(values))];
	}
	else if (CompareKeyword(keyword, "VERBOSE"))
	{
		rd->verbose = ParseBoolean(target, false);
	}
	else if (CompareKeyword(keyword, "TRUNCATE"))
	{
		rd->wo.truncate = ParseBoolean(target, false);
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
		if (rd->wo.dup_badfile != NULL)
			pfree(rd->wo.dup_badfile);

		pfree(rd);
	}

	return skip;
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

	str = QuoteString(self->wo.dup_badfile);
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

	appendStringInfo(&buf, "TRUNCATE = %s\n", self->wo.truncate ? "YES" : "NO");
	if (self->limit == INT64_MAX)
		appendStringInfo(&buf, "LIMIT = INFINITE\n");
	else
		appendStringInfo(&buf, "LIMIT = " int64_FMT "\n", self->limit);

	if (self->max_parse_errors == INT64_MAX)
		appendStringInfo(&buf, "PARSE_ERRORS = INFINITE\n");
	else
		appendStringInfo(&buf, "PARSE_ERRORS = " int64_FMT "\n",
						 self->max_parse_errors);
	if (self->wo.max_dup_errors == INT64_MAX)
		appendStringInfo(&buf, "DUPLICATE_ERRORS = INFINITE\n");
	else
		appendStringInfo(&buf, "DUPLICATE_ERRORS = " int64_FMT "\n",
						 self->wo.max_dup_errors);
	appendStringInfo(&buf, "ON_DUPLICATE = %s\n",
					 ON_DUPLICATE_NAMES[self->wo.on_duplicate]);
	appendStringInfo(&buf, "VERBOSE = %s\n", self->verbose ? "YES" : "NO");

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
	if (checker->encoding != -1 &&
		checker->encoding != PG_SQL_ASCII &&
		checker->db_encoding != PG_SQL_ASCII)
		checker->check_encoding = true;

	/* When specify CHECK_CONSTRAINTS, we check the constraints */
	desc = RelationGetDescr(rel);
	if (desc->constr &&
		(checker->check_constraints || desc->constr->has_not_null))
	{
		if (checker->check_constraints)
			checker->has_constraints = true;

		if (desc->constr->has_not_null)
			checker->has_not_null = true;

		checker->resultRelInfo = makeNode(ResultRelInfo);
		checker->resultRelInfo->ri_RangeTableIndex = 1;		/* dummy */
		checker->resultRelInfo->ri_RelationDesc = rel;
		checker->resultRelInfo->ri_TrigDesc = NULL; /* TRIGGER is not supported */
		checker->resultRelInfo->ri_TrigInstrument = NULL;
	}

	if (checker->has_constraints)
	{
		checker->estate = CreateExecutorState();
		checker->estate->es_result_relations = checker->resultRelInfo;
		checker->estate->es_num_result_relations = 1;
		checker->estate->es_result_relation_info = checker->resultRelInfo;

		/* Set up a tuple slot too */
		checker->slot = MakeSingleTupleTableSlot(desc);
	}

	if (!checker->has_constraints && !checker->has_not_null)
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
	int	len = strlen(src);

	if (checker->encoding == checker->db_encoding ||
		checker->encoding == PG_SQL_ASCII)
	{
		/*
		 * No conversion is needed, but we must still validate the data.
		 */
		pg_verify_mbstr(checker->db_encoding, src, len, false);
		return src;
	}

	if (checker->db_encoding == PG_SQL_ASCII)
	{
		/*
		 * No conversion is possible, but we must still validate the data,
		 * because the client-side code might have done string escaping using
		 * the selected client_encoding.  If the client encoding is ASCII-safe
		 * then we just do a straight validation under that encoding.  For an
		 * ASCII-unsafe encoding we have a problem: we dare not pass such data
		 * to the parser but we have no way to convert it.	We compromise by
		 * rejecting the data if it contains any non-ASCII characters.
		 */
		if (PG_VALID_BE_ENCODING(checker->encoding))
			pg_verify_mbstr(checker->encoding, src, len, false);
		else
		{
			int			i;

			for (i = 0; i < len; i++)
			{
				if (src[i] == '\0' || IS_HIGHBIT_SET(src[i]))
					ereport(ERROR,
							(errcode(ERRCODE_CHARACTER_NOT_IN_REPERTOIRE),
					 errmsg("invalid byte value for encoding \"%s\": 0x%02x",
							pg_enc2name_tbl[PG_SQL_ASCII].name,
							(unsigned char) src[i])));
			}
		}
		return src;
	}

	/* Convert the input into the database encoding. */
	return (char *) pg_do_encoding_conversion((unsigned char *) src,
											  len,
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
TupleFormerInit(TupleFormer *former, Filter *filter, TupleDesc desc)
{
	AttrNumber			natts;
	AttrNumber			maxatts;
	int					i;
	Oid					in_func_oid;

	former->desc = CreateTupleDescCopy(desc);
	for (i = 0; i < desc->natts; i++)
		former->desc->attrs[i]->attnotnull = desc->attrs[i]->attnotnull;

	/*
	 * allocate buffer to store columns or function arguments
	 */
	if (filter->funcstr)
	{
		natts = filter->nargs;
		maxatts = Max(natts, desc->natts);
	}
	else
		natts = maxatts = desc->natts;

	former->values = palloc(sizeof(Datum) * maxatts);
	former->isnull = palloc(sizeof(bool) * maxatts);
	MemSet(former->isnull, true, sizeof(bool) * maxatts);

	/*
	 * get column information of the target relation
	 */
	former->typIOParam = (Oid *) palloc(natts * sizeof(Oid));
	former->typInput = (FmgrInfo *) palloc(natts * sizeof(FmgrInfo));
	former->typMod = (Oid *) palloc(natts * sizeof(Oid));
	former->attnum = palloc(natts * sizeof(int));

	if (filter->funcstr)
	{
		former->maxfields = natts;
		former->minfields = former->maxfields - filter->fn_ndargs;

		for (i = 0; i < natts; i++)
		{
			/* get type information and input function */
			getTypeInputInfo(filter->argtypes[i],
						 &in_func_oid, &former->typIOParam[i]);
			fmgr_info(in_func_oid, &former->typInput[i]);

			former->typMod[i] = -1;
			former->attnum[i] = i;
		}
	}
	else
	{
		Form_pg_attribute  *attrs;

		attrs = desc->attrs;
		former->maxfields = 0;
		for (i = 0; i < natts; i++)
		{
			/* ignore dropped columns */
			if (attrs[i]->attisdropped)
				continue;

			/* get type information and input function */
			getTypeInputInfo(attrs[i]->atttypid,
							 &in_func_oid, &former->typIOParam[i]);
			fmgr_info(in_func_oid, &former->typInput[i]);

			former->typMod[i] = attrs[i]->atttypmod;

			/* update valid column information */
			former->attnum[former->maxfields] = i;
			former->maxfields++;
		}

		former->minfields = former->maxfields;
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

static HeapTuple
TupleFormerNullTuple(TupleFormer *former)
{
	memset(former->values, 0, former->desc->natts * sizeof(Datum));
	memset(former->isnull, true, former->desc->natts * sizeof(bool));

	return TupleFormerTuple(former);
}

/* Read null-terminated string and convert to internal format */
Datum
TupleFormerValue(TupleFormer *former, const char *str, int col)
{
	return FunctionCall3(&former->typInput[col],
		CStringGetDatum(str),
		ObjectIdGetDatum(former->typIOParam[col]),
		Int32GetDatum(former->typMod[col]));
}

/*
 * Check that function result tuple type (src_tupdesc) matches or can
 * be considered to match what the target table (dst_tupdesc). If
 * they don't match, ereport.
 *
 * We really only care about number of attributes and data type.
 * Also, we can ignore type mismatch on columns that are dropped in the
 * destination type, so long as the physical storage matches.  This is
 * helpful in some cases involving out-of-date cached plans.
 */
static void
tupledesc_match(TupleDesc dst_tupdesc, TupleDesc src_tupdesc)
{
	int			i;

	if (dst_tupdesc->natts != src_tupdesc->natts)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("function return row and target table row do not match"),
				 errdetail_plural("Returned row contains %d attribute, but target table expects %d.",
				"Returned row contains %d attributes, but query expects %d.",
								  src_tupdesc->natts,
								  src_tupdesc->natts, dst_tupdesc->natts)));

	for (i = 0; i < dst_tupdesc->natts; i++)
	{
		Form_pg_attribute dattr = dst_tupdesc->attrs[i];
		Form_pg_attribute sattr = src_tupdesc->attrs[i];

		if (dattr->atttypid == sattr->atttypid)
			continue;			/* no worries */
		if (!dattr->attisdropped)
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("function return row and target table row do not match"),
					 errdetail("Returned type %s at ordinal position %d, but target table %s.",
							   format_type_be(sattr->atttypid),
							   i + 1,
							   format_type_be(dattr->atttypid))));

		if (dattr->attlen != sattr->attlen ||
			dattr->attalign != sattr->attalign)
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("function return row and target table row do not match"),
					 errdetail("Physical storage mismatch on dropped attribute at ordinal position %d.",
							   i + 1)));
	}
}

void
FilterInit(Filter *filter, TupleDesc desc)
{
	int				i;
	ParsedFunction	func;
	HeapTuple		ftup;
	Form_pg_proc	pp;

	if (filter->funcstr == NULL)
		return;

	/* parse filter function */
	func = ParseFunction(filter->funcstr, true);

	filter->funcid = func.oid;
	filter->nargs = func.nargs;
	for (i = 0; i < filter->nargs; i++)
	{
		/* Check for polymorphic types and internal pseudo-type argument */
		if (IsPolymorphicType(func.argtypes[i]) ||
			func.argtypes[i] == INTERNALOID)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("filter function does not support a polymorphic function and having a internal pseudo-type argument function: %s",
							get_func_name(filter->funcid))));

		filter->argtypes[i] = func.argtypes[i];
	}

	ftup = SearchSysCache(PROCOID, ObjectIdGetDatum(filter->funcid), 0, 0, 0);
	pp = (Form_pg_proc) GETSTRUCT(ftup);

	if (pp->proretset)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("filter function must not return set")));

	/* Check data type of the function result value */
	if (pp->prorettype == desc->tdtypeid)
		filter->tupledesc_matched = true;
	else if (pp->prorettype == RECORDOID)
	{
		TupleDesc	resultDesc = NULL;

		/* Check for OUT parameters defining a RECORD result */
		resultDesc = build_function_result_tupdesc_t(ftup);

		if (resultDesc)
		{
			tupledesc_match(desc, resultDesc);
			filter->tupledesc_matched = true;
		}
	}
	else if (get_typtype(pp->prorettype) != TYPTYPE_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("function return data type and target table data type do not match")));

	/* Get default values */
#if PG_VERSION_NUM >= 80400
	filter->fn_ndargs = pp->pronargdefaults;
	if (filter->fn_ndargs > 0)
	{
		Datum		proargdefaults;
		bool		isnull;
		char	   *str;
		List	   *defaults;
		ListCell   *l;

		filter->defaultValues = palloc(sizeof(Datum) * filter->fn_ndargs);
		filter->defaultIsnull = palloc(sizeof(bool) * filter->fn_ndargs);

		proargdefaults = SysCacheGetAttr(PROCOID, ftup,
										 Anum_pg_proc_proargdefaults,
										 &isnull);
		Assert(!isnull);
		str = TextDatumGetCString(proargdefaults);
		defaults = (List *) stringToNode(str);
		Assert(IsA(defaults, List));
		pfree(str);

		filter->econtext = CreateStandaloneExprContext();
		i = 0;
		foreach(l, defaults)
		{
			Expr		   *expr = (Expr *) lfirst(l);
			ExprState	   *argstate;
			ExprDoneCond	thisArgIsDone;

			argstate = ExecInitExpr(expr, NULL);

			filter->defaultValues[i] = ExecEvalExpr(argstate,
													filter->econtext,
													&filter->defaultIsnull[i],
													&thisArgIsDone);

			if (thisArgIsDone != ExprSingleResult)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("functions and operators can take at most one set argument")));

			i++;
		}
	}

	if (OidIsValid(pp->provariadic))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("filter function does not support a valiadic function %s",
						get_func_name(filter->funcid))));
#else
	filter->fn_ndargs = 0;
#endif

	filter->fn_strict = pp->proisstrict;

	ReleaseSysCache(ftup);
}

void
FilterTerm(Filter *filter)
{
	if (filter->funcstr)
		pfree(filter->funcstr);
	if (filter->defaultValues)
		pfree(filter->defaultValues);
	if (filter->defaultIsnull)
		pfree(filter->defaultIsnull);
	if (filter->econtext)
		FreeExprContext(filter->econtext, true);
}

HeapTuple
FilterTuple(Filter *filter, TupleFormer *former, int *parsing_field)
{
#if PG_VERSION_NUM >= 80400
	PgStat_FunctionCallUsage	fcusage;
#endif
	int						i;
	FunctionCallInfoData	fcinfo;
	FmgrInfo				flinfo;
	MemoryContext			oldcontext;
	ResourceOwner			oldowner;
	Datum					datum;

	/*
	 * If function is strict, and there are any NULL arguments, return tuple,
	 * it's all columns of null.
	 */
	if (filter->fn_strict)
	{
		for (i = 0; i < filter->nargs; i++)
		{
			if (former->isnull[i])
				return TupleFormerNullTuple(former);
		}
	}

	fmgr_info(filter->funcid, &flinfo);

	InitFunctionCallInfoData(fcinfo, &flinfo, filter->nargs, NULL, NULL);

	for (i = 0; i < filter->nargs; i++)
	{
		fcinfo.arg[i] = former->values[i];
		fcinfo.argnull[i] = former->isnull[i];
	}

	/*
	 * Execute the function inside a sub-transaction, so we can cope with
	 * errors sanely
	 */
	oldcontext = CurrentMemoryContext;
	oldowner = CurrentResourceOwner;
	BeginInternalSubTransaction(NULL);

	/* Want to run inside per tuple memory context */
	MemoryContextSwitchTo(oldcontext);

	*parsing_field = 0;
	pgstat_init_function_usage(&fcinfo, &fcusage);

	fcinfo.isnull = false;

	PG_TRY();
	{
		datum = FunctionCallInvoke(&fcinfo);
	}
	PG_CATCH();
	{
		pgstat_end_function_usage(&fcusage, true);

		/* Abort the inner transaction */
		RollbackAndReleaseCurrentSubTransaction();
		MemoryContextSwitchTo(oldcontext);
		CurrentResourceOwner = oldowner;

		PG_RE_THROW();
	}
	PG_END_TRY();

	pgstat_end_function_usage(&fcusage, true);

	*parsing_field = -1;

	/* Commit the inner transaction, return to outer xact context */
	ReleaseCurrentSubTransaction();
	MemoryContextSwitchTo(oldcontext);
	CurrentResourceOwner = oldowner;

	/*
	 * If function result is NULL, return tuple, it's all columns of null.
	 */
	if (fcinfo.isnull)
		return TupleFormerNullTuple(former);

	/* Check tupdesc of the function result value */
	if (!filter->tupledesc_matched)
	{
		HeapTupleHeader	td = DatumGetHeapTupleHeader(datum);
		TupleDesc		resultDesc;

		resultDesc = lookup_rowtype_tupdesc(HeapTupleHeaderGetTypeId(td),
											HeapTupleHeaderGetTypMod(td));
		tupledesc_match(former->desc, resultDesc);

		ReleaseTupleDesc(resultDesc);

		if (HeapTupleHeaderGetTypeId(td) != RECORDOID)
			filter->tupledesc_matched = true;
	}

	filter->tuple.t_data = DatumGetHeapTupleHeader(datum);
	filter->tuple.t_len = HeapTupleHeaderGetDatumLength(filter->tuple.t_data);

	return &filter->tuple;
}
