/*
 * pg_bulkload: lib/reader.c
 *
 *	  Copyright (c) 2007-2024, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

/**
 * @file
 * @brief Implementation of reader module
 */
#include "pg_bulkload.h"

#include <fcntl.h>
#include <string.h>

#include "access/heapam.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_language.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "mb/pg_wchar.h"
#include "nodes/parsenodes.h"
#include "parser/parse_coerce.h"
#include "pgstat.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/resowner.h"
#include "utils/syscache.h"
#include "utils/typcache.h"

#include "logger.h"
#include "pg_profile.h"
#include "pg_strutil.h"
#include "pgut/pgut-be.h"
#include "reader.h"

#include "storage/fd.h"

#define DEFAULT_MAX_PARSE_ERRORS		0

/**
 * @brief Create Reader
 */
Reader *
ReaderCreate(char *type)
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

	Reader	   *self;

	/* default of type is CSV */
	if (type == NULL)
		type = "CSV";

	self = palloc0(sizeof(Reader));
	self->max_parse_errors = -2;
	self->limit = INT64_MAX;
	self->checker.encoding = -1;

	self->parser = values[choice("TYPE", type, keys, lengthof(keys))]();

	return self;
}

void
ReaderInit(Reader *self)
{
	/*
	 * Set defaults to unspecified parameters.
	 */
	if (self->max_parse_errors < -1)
		self->max_parse_errors = DEFAULT_MAX_PARSE_ERRORS;

	/*
	 * Use the client_encoding case of ENCODING is not specified and INPUT is
	 * STDIN.
	 */
	if (self->checker.encoding == -1 &&
		pg_strcasecmp(self->infile, "stdin") == 0)
		self->checker.encoding = pg_get_client_encoding();
}

size_t
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
 */
bool
ReaderParam(Reader *rd, const char *keyword, char *target)
{
	/*
	 * result
	 */
	if (CompareKeyword(keyword, "INFILE") ||
		CompareKeyword(keyword, "INPUT"))
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
	else if (CompareKeyword(keyword, "PARSE_ERRORS") ||
			 CompareKeyword(keyword, "MAX_ERR_CNT"))
	{
		ASSERT_ONCE(rd->max_parse_errors < -1);
		rd->max_parse_errors = ParseInt64(target, -1);
		if (rd->max_parse_errors == -1)
			rd->max_parse_errors = INT64_MAX;
	}
	else if (CompareKeyword(keyword, "LOAD") ||
			 CompareKeyword(keyword, "LIMIT"))
	{
		ASSERT_ONCE(rd->limit == INT64_MAX);
		rd->limit = ParseInt64(target, 0);
	}
	else if (CompareKeyword(keyword, "CHECK_CONSTRAINTS"))
	{
		rd->checker.check_constraints = ParseBoolean(target);
	}
	else if (CompareKeyword(keyword, "ENCODING"))
	{
		ASSERT_ONCE(rd->checker.encoding < 0);
		rd->checker.encoding = pg_valid_client_encoding(target);
		if (rd->checker.encoding < 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid encoding for parameter \"ENCODING\": \"%s\"",
						target)));
	}
	else if (rd->parser == NULL ||
			!ParserParam(rd->parser, keyword, target))
		return false;

	return true;
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

	CheckerTerm(&rd->checker);

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
			tuple = ParserRead(parser, &rd->checker);
			if (tuple == NULL)
				eof = true;
			else
			{
				tuple = CheckerTuple(&rd->checker, tuple,
									 &parser->parsing_field);
				CheckerConstraints(&rd->checker, tuple, &parser->parsing_field);
			}
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

			LoggerLog(WARNING, buf.data, 0);

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

			MemoryContextReset(ccxt);
			// Without the below line, the regression tests shows the different result on debug-build mode.
			tuple = NULL;
		}
		PG_END_TRY();

	} while (!eof && !tuple);

	BULKLOAD_PROFILE(&prof_reader_parser);
	return tuple;
}

void
ReaderDumpParams(Reader *self)
{
	char		   *str;
	StringInfoData	buf;

	initStringInfo(&buf);

	str = QuoteString(self->infile);
	appendStringInfo(&buf, "INPUT = %s\n", str);
	pfree(str);

	str = QuoteString(self->parse_badfile);
	appendStringInfo(&buf, "PARSE_BADFILE = %s\n", str);
	pfree(str);

	str = QuoteString(self->logfile);
	appendStringInfo(&buf, "LOGFILE = %s\n", str);
	pfree(str);

	if (self->limit == INT64_MAX)
		appendStringInfo(&buf, "LIMIT = INFINITE\n");
	else
		appendStringInfo(&buf, "LIMIT = " int64_FMT "\n", self->limit);

	if (self->max_parse_errors == INT64_MAX)
		appendStringInfo(&buf, "PARSE_ERRORS = INFINITE\n");
	else
		appendStringInfo(&buf, "PARSE_ERRORS = " int64_FMT "\n",
						 self->max_parse_errors);
	if (PG_VALID_FE_ENCODING(self->checker.encoding))
		appendStringInfo(&buf, "ENCODING = %s\n",
						 pg_encoding_to_char(self->checker.encoding));
	appendStringInfo(&buf, "CHECK_CONSTRAINTS = %s\n",
		self->checker.check_constraints ? "YES" : "NO");

	LoggerLog(INFO, buf.data, 0);
	pfree(buf.data);

	ParserDumpParams(self->parser);
}

void
CheckerInit(Checker *checker, Relation rel, TupleChecker *tchecker)
{
	TupleDesc       desc;
#if PG_VERSION_NUM >= 160000
	RTEPermissionInfo   *rtep;
	List	   		*perminfos = NIL;
#endif
	RangeTblEntry	*rte;
	List            *range_table = NIL;
#if PG_VERSION_NUM >= 80400
	TupleDesc       tupDesc;
	int             attnums, i;
#endif

	checker->tchecker = tchecker;

	/*
	 * When specify ENCODING, we check the input data encoding.
	 * Convert encoding if the client and server encodings are different.
	 */
	checker->db_encoding = GetDatabaseEncoding();
	if (checker->encoding != -1 &&
		(checker->encoding != PG_SQL_ASCII ||
		checker->db_encoding != PG_SQL_ASCII))
		checker->check_encoding = true;

	if (!rel)
		return;

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
#if PG_VERSION_NUM >= 140000
		checker->estate->es_opened_result_relations =
			lappend(checker->estate->es_opened_result_relations, checker->resultRelInfo);
#else
		checker->estate->es_result_relations = checker->resultRelInfo;
		checker->estate->es_num_result_relations = 1;
		checker->estate->es_result_relation_info = checker->resultRelInfo;
#endif

        /* Set up RangeTblEntry */
        rte = makeNode(RangeTblEntry);
        rte->rtekind = RTE_RELATION;
        rte->relid = RelationGetRelid(rel);
#if PG_VERSION_NUM >= 160000
        rtep = makeNode(RTEPermissionInfo);
        rtep->relid = rte->relid;
        rtep->inh = rte->inh;
        perminfos = lappend(perminfos, rtep);
        rte->perminfoindex = list_length(perminfos);
#endif

#if PG_VERSION_NUM >= 90100
        rte->relkind = rel->rd_rel->relkind;
#endif
#if PG_VERSION_NUM >= 160000
		rtep->requiredPerms = ACL_INSERT;
#else
        rte->requiredPerms = ACL_INSERT;
#endif
        range_table = list_make1(rte);

#if PG_VERSION_NUM >= 80400
        tupDesc = RelationGetDescr(rel);
        attnums = tupDesc->natts;
        for(i = 0; i <= attnums; i++) 
        {
#if PG_VERSION_NUM >= 160000
			rtep->insertedCols = bms_add_member(rtep->insertedCols, i);
#elif PG_VERSION_NUM >= 90500
			rte->insertedCols = bms_add_member(rte->insertedCols, i);
#else
			rte->modifiedCols = bms_add_member(rte->modifiedCols, i);
#endif
        }
#endif

#if PG_VERSION_NUM >= 160000
		ExecCheckPermissions(range_table, perminfos, true);
#elif PG_VERSION_NUM >= 90100
        /* This API is published only from 9.1. 
         * This is used for permission check, but currently pg_bulkload
         * is called only from super user and so the below code maybe
         * is not essential. */
        ExecCheckRTPerms(range_table, true);
#endif
	
#if PG_VERSION_NUM >= 160000
		ExecInitRangeTable(checker->estate, range_table, perminfos);
#elif PG_VERSION_NUM >= 120000
		/* Some APIs have changed significantly as of v12. */
		ExecInitRangeTable(checker->estate, range_table);
#else
		checker->estate->es_range_table = range_table;
#endif

#if PG_VERSION_NUM >= 120000
		checker->slot = MakeSingleTupleTableSlot(desc, &TTSOpsHeapTuple);
#else
		checker->slot = MakeSingleTupleTableSlot(desc);
#endif
	}

	if (!checker->has_constraints && checker->has_not_null)
	{
		int	n;

		checker->desc = CreateTupleDescCopy(desc);
		for (n = 0; n < desc->natts; n++)
#if PG_VERSION_NUM >= 110000
			checker->desc->attrs[n].attnotnull = desc->attrs[n].attnotnull;
#else
			checker->desc->attrs[n]->attnotnull = desc->attrs[n]->attnotnull;
#endif
	}
}

void
CheckerTerm(Checker *checker)
{
	if (checker->slot)
		ExecDropSingleTupleTableSlot(checker->slot);

	if (checker->estate)
		FreeExecutorState(checker->estate);
}

char *
CheckerConversion(Checker *checker, char *src)
{
	int	len;

	if (!checker->check_encoding)
		return src;

	len = strlen(src);

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

HeapTuple
CheckerConstraints(Checker *checker, HeapTuple tuple, int *parsing_field)
{
	if (checker->has_constraints)
	{
		*parsing_field = 0;

		/* Place tuple in tuple slot */
#if PG_VERSION_NUM >= 120000
		ExecStoreHeapTuple(tuple, checker->slot, false);
#else
		ExecStoreTuple(tuple, checker->slot, InvalidBuffer, false);
#endif

		/* Check the constraints of the tuple */
		ExecConstraints(checker->resultRelInfo, checker->slot, checker->estate);
	}
	else if (checker->has_not_null && HeapTupleHasNulls(tuple))
	{
		/*
		 * Even if CHECK_CONSTRAINTS is not specified, check NOT NULL constraint
		 */
		TupleDesc	desc = checker->desc;
		int			i;

		for (i = 0; i < desc->natts; i++)
		{
#if PG_VERSION_NUM >= 110000
			if (desc->attrs[i].attnotnull &&
				att_isnull(i, tuple->t_data->t_bits))
#else
			if (desc->attrs[i]->attnotnull &&
				att_isnull(i, tuple->t_data->t_bits))
#endif

			{
				*parsing_field = i + 1;	/* 1 origin */
				ereport(ERROR,
						(errcode(ERRCODE_NOT_NULL_VIOLATION),
						 errmsg("null value in column \"%s\" violates not-null constraint",
#if PG_VERSION_NUM >= 110000
						NameStr(desc->attrs[i].attname))));
#else
						NameStr(desc->attrs[i]->attname))));
#endif
			}
		}
	}

	return tuple;
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
#if PG_VERSION_NUM >= 110000
		former->desc->attrs[i].attnotnull = desc->attrs[i].attnotnull;
#else
		former->desc->attrs[i]->attnotnull = desc->attrs[i]->attnotnull;
#endif

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
	former->typId = (Oid *) palloc(natts * sizeof(Oid));
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
			former->typId[i] = filter->argtypes[i];
		}
	}
	else
	{
#if PG_VERSION_NUM >= 110000
		FormData_pg_attribute  *attrs;
#else
		Form_pg_attribute  *attrs;
#endif

		attrs = desc->attrs;
		former->maxfields = 0;
		for (i = 0; i < natts; i++)
		{
#if PG_VERSION_NUM >= 110000
			/* ignore dropped columns */
			if (attrs[i].attisdropped)
				continue;

			/* get type information and input function */
			getTypeInputInfo(attrs[i].atttypid,
							 &in_func_oid, &former->typIOParam[i]);
			fmgr_info(in_func_oid, &former->typInput[i]);

			former->typMod[i] = attrs[i].atttypmod;
			former->typId[i] = attrs[i].atttypid;
#else
			/* ignore dropped columns */
			if (attrs[i]->attisdropped)
				continue;

			/* get type information and input function */
			getTypeInputInfo(attrs[i]->atttypid,
							 &in_func_oid, &former->typIOParam[i]);
			fmgr_info(in_func_oid, &former->typInput[i]);

			former->typMod[i] = attrs[i]->atttypmod;
			former->typId[i] = attrs[i]->atttypid;
#endif

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
	if (former->typId)
		pfree(former->typId);

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
bool
tupledesc_match(TupleDesc dst_tupdesc, TupleDesc src_tupdesc)
{
	int			i;

#if PG_VERSION_NUM < 120000
	if (dst_tupdesc->tdhasoid != src_tupdesc->tdhasoid)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("function return record definition and target table record definition do not match"),
				 errdetail("Returned record hasoid %d, but target table hasoid %d.",
						   src_tupdesc->tdhasoid, dst_tupdesc->tdhasoid)));
#endif

	if (dst_tupdesc->natts != src_tupdesc->natts)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("function return row and target table row do not match"),
				 errdetail("Returned row contains %d attribute(s), but target table expects %d.",
						   src_tupdesc->natts, dst_tupdesc->natts)));

	for (i = 0; i < dst_tupdesc->natts; i++)
	{
#if PG_VERSION_NUM >= 110000
		FormData_pg_attribute dattr = dst_tupdesc->attrs[i];
		FormData_pg_attribute sattr = src_tupdesc->attrs[i];

		if (dattr.atttypid == sattr.atttypid)
			continue;			/* no worries */
		if (!dattr.attisdropped)
			return false;

		if (dattr.attlen != sattr.attlen ||
			dattr.attalign != sattr.attalign)
			return false;
#else
		Form_pg_attribute dattr = dst_tupdesc->attrs[i];
		Form_pg_attribute sattr = src_tupdesc->attrs[i];

		if (dattr->atttypid == sattr->atttypid)
			continue;			/* no worries */
		if (!dattr->attisdropped)
			return false;

		if (dattr->attlen != sattr->attlen ||
			dattr->attalign != sattr->attalign)
			return false;
#endif
	}

	return true;
}

TupleCheckStatus
FilterInit(Filter *filter, TupleDesc desc, Oid collation)
{
	int				i;
	ParsedFunction	func;
	HeapTuple		ftup;
	HeapTuple		ltup;
	Form_pg_proc	pp;
	Form_pg_language	lp;
	TupleCheckStatus	status = NEED_COERCION_CHECK;

	if (filter->funcstr == NULL)
		return NO_COERCION;

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
	if (pp->prorettype == desc->tdtypeid && pp->prorettype != RECORDOID)
		status = NO_COERCION;
	else if (pp->prorettype == RECORDOID)
	{
		TupleDesc	resultDesc = NULL;

		/* Check for OUT parameters defining a RECORD result */
		resultDesc = build_function_result_tupdesc_t(ftup);

		if (resultDesc)
		{
			if (tupledesc_match(desc, resultDesc))
				status = NO_COERCION;

			FreeTupleDesc(resultDesc);
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
#if PG_VERSION_NUM < 100000
			ExprDoneCond	thisArgIsDone;
#endif

			argstate = ExecInitExpr(expr, NULL);

			filter->defaultValues[i] = ExecEvalExpr(argstate,
													filter->econtext,
													&filter->defaultIsnull[i]
#if PG_VERSION_NUM < 100000
													,&thisArgIsDone
#endif
													);

#if PG_VERSION_NUM < 100000
			if (thisArgIsDone != ExprSingleResult)
				ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("functions and operators can take at most one set argument")));
#endif

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
	filter->fn_rettype = pp->prorettype;

	filter->collation = collation;

	/* checking if the filter function is a SQL function */
	ltup = SearchSysCache(LANGOID, ObjectIdGetDatum(pp->prolang), 0, 0, 0);
	lp = (Form_pg_language) GETSTRUCT(ltup);
	
	if(strcmp(NameStr(lp->lanname), "sql") == 0)
		filter->is_funcid_sql = true;
	else	
		filter->is_funcid_sql = false;
	
	
	ReleaseSysCache(ltup);
	ReleaseSysCache(ftup);

	/* flag set */
	filter->is_first_time_call = true;
	filter->context = CurrentMemoryContext;

	return status;
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
#if PG_VERSION_NUM >= 120000
	LOCAL_FCINFO(fcinfo, FUNC_MAX_ARGS);
#else
	FunctionCallInfoData	fcinfo;
#endif
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

	/* From PostgreSQL 9.2.4, fmgr_sql behavior has changed. */
	/* So, we have to change to filter's memory context before fmgr_info() call.*/
	/* See PostgreSQL commit "Fix SQL function execution to be safe with long-lived FmgrInfos."*/
#if PG_VERSION_NUM >= 90204
	oldcontext = CurrentMemoryContext;
	oldowner = CurrentResourceOwner;

	MemoryContextSwitchTo(filter->context);
#endif

	fmgr_info(filter->funcid, &flinfo);

#if PG_VERSION_NUM >= 90204
	MemoryContextSwitchTo(oldcontext);
	CurrentResourceOwner = oldowner;

	/* set fn_extra except the first time call */
	if ( filter->is_first_time_call == false &&
		MemoryContextIsValid(filter->fn_extra.fcontext) &&
		filter->is_funcid_sql) {
		flinfo.fn_extra = (SQLFunctionCache *) palloc0(sizeof(SQLFunctionCache));
		memmove((SQLFunctionCache *)flinfo.fn_extra, &(filter->fn_extra),
							sizeof(SQLFunctionCache));
	} else {

		filter->is_first_time_call = true;	
	}
#endif

#if PG_VERSION_NUM >= 120000
	InitFunctionCallInfoData(*fcinfo, &flinfo, filter->nargs,
							 filter->collation, NULL, NULL);
#elif PG_VERSION_NUM >= 90100
	InitFunctionCallInfoData(fcinfo, &flinfo, filter->nargs, filter->collation, NULL, NULL);
#else
	InitFunctionCallInfoData(fcinfo, &flinfo, filter->nargs, NULL, NULL);
#endif

	for (i = 0; i < filter->nargs; i++)
	{
#if PG_VERSION_NUM >= 120000
		fcinfo->args[i].value = former->values[i];
		fcinfo->args[i].isnull = former->isnull[i];

#else
		fcinfo.arg[i] = former->values[i];
		fcinfo.argnull[i] = former->isnull[i];
#endif
	}

	/*
	 * Execute the function inside a sub-transaction, so we can cope with
	 * errors sanely
	 */

#if PG_VERSION_NUM < 90204
	oldcontext = CurrentMemoryContext;
	oldowner = CurrentResourceOwner;
#endif

	BeginInternalSubTransaction(NULL);

	/* Want to run inside per tuple memory context */
	MemoryContextSwitchTo(oldcontext);

	*parsing_field = 0;
#if PG_VERSION_NUM >= 120000
	pgstat_init_function_usage(fcinfo, &fcusage);
#else
	pgstat_init_function_usage(&fcinfo, &fcusage);
#endif

#if PG_VERSION_NUM >= 120000
	fcinfo->isnull = false;
#else
	fcinfo.isnull = false;
#endif

	PG_TRY();
	{
#if PG_VERSION_NUM >= 120000
		datum = FunctionCallInvoke(fcinfo);
#else
		datum = FunctionCallInvoke(&fcinfo);
#endif
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
#if PG_VERSION_NUM >= 120000
	if (fcinfo->isnull)
#else
	if (fcinfo.isnull)
#endif
		return TupleFormerNullTuple(former);

	filter->tuple.t_data = DatumGetHeapTupleHeader(datum);
	filter->tuple.t_len = HeapTupleHeaderGetDatumLength(filter->tuple.t_data);

	/* save fn_extra, and unset the flag */
#if PG_VERSION_NUM >= 90204
	if ( filter->is_first_time_call == true &&
		 filter->is_funcid_sql) {
		filter->is_first_time_call = false;
		memmove(&(filter->fn_extra),(SQLFunctionCache *) flinfo.fn_extra,
						sizeof(SQLFunctionCache));
	}

	if(!SubTransactionIsActive(filter->fn_extra.subxid))
		filter->fn_extra.subxid++;
#endif

	return &filter->tuple;
}

TupleChecker *
CreateTupleChecker(TupleDesc desc)
{
	TupleChecker   *self;
	MemoryContext	context;
	MemoryContext	oldcontext;

	context = AllocSetContextCreate(CurrentMemoryContext,
									"TupleChecker",
#if PG_VERSION_NUM >= 90600
									ALLOCSET_DEFAULT_SIZES);
#else
									ALLOCSET_SMALL_MINSIZE,
									ALLOCSET_SMALL_INITSIZE,
									ALLOCSET_DEFAULT_MAXSIZE);
#endif

	oldcontext = MemoryContextSwitchTo(context);

	self = palloc0(sizeof(TupleChecker));
	self->status = NEED_COERCION_CHECK;
	self->sourceDesc = NULL;
	self->targetDesc = CreateTupleDescCopy(desc);
	self->context = context;
	self->values = (Datum *) palloc(desc->natts * sizeof(Datum));
	self->nulls = (bool *) palloc(desc->natts * sizeof(bool));
	self->opt = NULL;
	self->coercionChecker = NULL;

	MemoryContextSwitchTo(oldcontext);

	return self;
}

void
UpdateTupleCheckStatus(TupleChecker *self, HeapTuple tuple)
{
	HeapTupleHeader		td;
	Oid					typeid;
	TupleDesc			resultDesc;
	MemoryContext		oldcontext;

	td = tuple->t_data;
	typeid = HeapTupleHeaderGetTypeId(td);

	/*
	 * We must not call lookup_rowtype_tupdesc, because parallel writer
	 * process a deadlock could occur, when typeid same target table's row
	 * type.
	 */
	if (self->targetDesc->tdtypeid == typeid && typeid != RECORDOID)
	{
		self->status = NO_COERCION;
		return;
	}

	resultDesc = lookup_rowtype_tupdesc(typeid,
										HeapTupleHeaderGetTypMod(td));
	if (tupledesc_match(self->targetDesc, resultDesc))
	{
		self->status = NO_COERCION;
		ReleaseTupleDesc(resultDesc);
		return;
	}

	self->status = NEED_COERCION;

	oldcontext = MemoryContextSwitchTo(self->context);
	self->sourceDesc = CreateTupleDescCopy(resultDesc);
	MemoryContextSwitchTo(oldcontext);
	ReleaseTupleDesc(resultDesc);
}

void
CoercionDeformTuple(TupleChecker *self, HeapTuple tuple, int *parsing_field)
{
	int	i;
	int	natts;

	natts = self->targetDesc->natts;

	if (self->typIsVarlena == NULL)
	{
		Oid				iofunc;
		MemoryContext	oldcontext;

		oldcontext = MemoryContextSwitchTo(self->context);
		self->typIsVarlena = (bool *) palloc(natts * sizeof(bool));
		self->typOutput = (FmgrInfo *) palloc(natts * sizeof(FmgrInfo));
		self->typIOParam = (Oid *) palloc(natts * sizeof(Oid));
		self->typInput = (FmgrInfo *) palloc(natts * sizeof(FmgrInfo));

		for (i = 0; i < natts; i++)
		{
#if PG_VERSION_NUM >= 110000
			if (self->sourceDesc->attrs[i].atttypid ==
				self->targetDesc->attrs[i].atttypid)
				continue;

			getTypeOutputInfo(self->sourceDesc->attrs[i].atttypid,
							  &iofunc, &self->typIsVarlena[i]);
			fmgr_info(iofunc, &self->typOutput[i]);

			getTypeInputInfo(self->targetDesc->attrs[i].atttypid, &iofunc,
							 &self->typIOParam[i]);
			fmgr_info(iofunc, &self->typInput[i]);
#else
			if (self->sourceDesc->attrs[i]->atttypid ==
				self->targetDesc->attrs[i]->atttypid)
				continue;

			getTypeOutputInfo(self->sourceDesc->attrs[i]->atttypid,
							  &iofunc, &self->typIsVarlena[i]);
			fmgr_info(iofunc, &self->typOutput[i]);

			getTypeInputInfo(self->targetDesc->attrs[i]->atttypid, &iofunc,
							 &self->typIOParam[i]);
			fmgr_info(iofunc, &self->typInput[i]);
#endif
		}

		MemoryContextSwitchTo(oldcontext);
	}
	
	heap_deform_tuple(tuple, self->sourceDesc, self->values, self->nulls);

	for (i = 0; i < natts; i++)
	{
		*parsing_field = i + 1;

#if PG_VERSION_NUM >= 110000
		/* Ignore dropped columns in datatype */
		if (self->targetDesc->attrs[i].attisdropped)
			continue;

		if (self->nulls[i])
		{
			/* emit nothing... */
			continue;
		}
		else if (self->sourceDesc->attrs[i].atttypid ==
				 self->targetDesc->attrs[i].atttypid)
		{
			continue;
		}
		else
		{
			char   *value;

			value = OutputFunctionCall(&self->typOutput[i], self->values[i]);
			self->values[i] = InputFunctionCall(&self->typInput[i], value,
										self->typIOParam[i],
										self->targetDesc->attrs[i].atttypmod);
		}
	}
#else
		/* Ignore dropped columns in datatype */
		if (self->targetDesc->attrs[i]->attisdropped)
			continue;

		if (self->nulls[i])
		{
			/* emit nothing... */
			continue;
		}
		else if (self->sourceDesc->attrs[i]->atttypid ==
				 self->targetDesc->attrs[i]->atttypid)
		{
			continue;
		}
		else
		{
			char   *value;

			value = OutputFunctionCall(&self->typOutput[i], self->values[i]);
			self->values[i] = InputFunctionCall(&self->typInput[i], value,
										self->typIOParam[i],
										self->targetDesc->attrs[i]->atttypmod);
		}
	}
#endif

	*parsing_field = -1;
}

HeapTuple
CoercionCheckerTuple(TupleChecker *self, HeapTuple tuple, int *parsing_field)
{
	HeapTuple	retTuple;

	if (self->status == NEED_COERCION_CHECK)
		UpdateTupleCheckStatus(self, tuple);

	if (self->status == NO_COERCION)
		return tuple;

	CoercionDeformTuple(self, tuple, parsing_field);

	retTuple = heap_form_tuple(self->targetDesc, self->values, self->nulls);

	return retTuple;
}
