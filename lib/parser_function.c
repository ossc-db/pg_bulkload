/*
 * pg_bulkload: lib/parser_function.c
 *
 *	  Copyright(C) 2009, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

/**
 * @file
 * @brief Binary HeapTuple format handling module implementation.
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/htup.h"
#include "catalog/namespace.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "parser/parse_func.h"
#include "pgstat.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"

#include "logger.h"
#include "pg_profile.h"
#include "reader.h"

typedef struct FunctionParser
{
	Parser	base;

	FmgrInfo				flinfo;
	FunctionCallInfoData	fcinfo;
	TupleDesc				desc;
	EState				   *estate;
	ExprContext			   *econtext;
	ReturnSetInfo			rsinfo;
	HeapTupleData			tuple;
} FunctionParser;

static void	FunctionParserInit(FunctionParser *self, const char *infile, Oid relid);
static HeapTuple FunctionParserRead(FunctionParser *self);
static int64	FunctionParserTerm(FunctionParser *self);
static bool FunctionParserParam(FunctionParser *self, const char *keyword, char *value);
static void FunctionParserDumpParams(FunctionParser *self);
static void FunctionParserDumpRecord(FunctionParser *self, FILE *fp, char *bafdile);

/* ========================================================================
 * FunctionParser
 * ========================================================================*/

static bool
GetNextArgument(const char *ptr, char **arg, const char **endptr, const char *path)
{
	const char	   *p;

	p = ptr;
	while (isspace((unsigned char) *p))
		p++;

	if (*p == '\0')
		return false;

	if (*p == '\'')
	{
		StringInfoData	buf;

		initStringInfo(&buf);

		p++;
		while (*p != '\0')
		{
			if (*p == '\'')
			{
				if (*(p + 1) == '\'')
					p++;
				else
					break;
			}

			appendStringInfoCharMacro(&buf, *p++);
		}

		if (*p != '\'')
			ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("function call syntax error: %s", path)));

		p++;
		*arg = buf.data;
	}
	else if (pg_strncasecmp(p, "NULL", strlen("NULL")) == 0)
	{
		p += strlen("NULL");
		*arg = NULL;
	}
	else
	{
		bool		minus;
		const char *startptr;
		int			len;
		char	   *str;

		minus = false;
		while (*p == '+' || *p == '-')
		{
			if (*p == '-') 
			{
				if (*(p + 1) == '-')
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("function call syntax error: %s", path)));
				minus = !minus;
			}

			p++;
			while (isspace((unsigned char) *p))
				p++;
		}

		startptr = p;
		while (!isspace((unsigned char) *p) && *p != ',' && *p != ')')
			p++;

		len = p - startptr;
		str = palloc(len + 2);
		snprintf(str, len + 2, "%c%s", minus ? '-' : '+', startptr);

		/* Check for numeric constants. */
		DirectFunctionCall3(numeric_in, CStringGetDatum(str + 1),
							ObjectIdGetDatum(InvalidOid), -1);
		*arg = str;
	}

	while (isspace((unsigned char) *p))
		p++;

	if (*p == ')' || *p == ',')
		p++;
	else
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("function call syntax error: %s", path)));

	*endptr = p;

	return true;
}

/**
 * @brief Create a new binary parser.
 */
Parser *
CreateFunctionParser(void)
{
	FunctionParser *self = palloc0(sizeof(FunctionParser));
	self->base.init = (ParserInitProc) FunctionParserInit;
	self->base.read = (ParserReadProc) FunctionParserRead;
	self->base.term = (ParserTermProc) FunctionParserTerm;
	self->base.param = (ParserParamProc) FunctionParserParam;
	self->base.dumpParams = (ParserDumpParamsProc) FunctionParserDumpParams;
	self->base.dumpRecord = (ParserDumpRecordProc) FunctionParserDumpRecord;

	return (Parser *)self;
}

static void
FunctionParserInit(FunctionParser *self, const char *infile, Oid relid)
{
	int					i;
	const char		   *p;
	char			   *funcname;
	List			   *names;
	ListCell		   *l;
	int					len;
	int					nargs;
	char			   *arg;
	char			   *args[FUNC_MAX_ARGS];
	FuncCandidateList	candidates;
	Oid					actual_arg_types[FUNC_MAX_ARGS];
	Oid					funcid;
	HeapTuple			ftup;
	Form_pg_proc		pp;
	AclResult			aclresult;
	Relation			rel;
	TupleDesc			desc;

	/* parse function name */
	p = strchr(infile, '(');
	if (p == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("function call syntax error: %s", infile)));

	len = p - infile;
	while (isspace((unsigned char) infile[len]))
		len--;
	funcname = palloc(len + 1);
	memcpy(funcname, infile, len);
	funcname[len] = '\0';

	/* parse function arguments */
	p++;
	nargs = 0;
	while (GetNextArgument(p, &arg, &p, infile))
	{
		args[nargs++] = arg;
		if (nargs > FUNC_MAX_ARGS)
			ereport(ERROR,
				(errcode(ERRCODE_TOO_MANY_ARGUMENTS),
				 errmsg("functions cannot have more than %d arguments", FUNC_MAX_ARGS)));
	}

	names = stringToQualifiedNameList(funcname);
	pfree(funcname);

	/* Get list of possible candidates from namespace search */
	candidates = FuncnameGetCandidates(names, nargs, NIL, true, true);

	for (i = 0; i < nargs; i++)
		actual_arg_types[i] = UNKNOWNOID;

	if (candidates == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION),
				 errmsg("function %s does not exist",
						func_signature_string(names, nargs, NIL,
											  actual_arg_types)),
		errhint("No function matches the given name and argument types.")));
	else if (candidates->next != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_AMBIGUOUS_FUNCTION),
				 errmsg("function %s is not unique",
						func_signature_string(names, nargs, NIL,
											  actual_arg_types)),
				 errhint("Could not choose a best candidate function.")));

	foreach (l, names)
	{
		Value  *v = lfirst(l);

		pfree(strVal(v));
		pfree(v);
	}
	list_free(names);

	funcid = candidates->oid;
	fmgr_info(funcid, &self->flinfo);

	if (!self->flinfo.fn_retset)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function must return set")));

	ftup = SearchSysCache(PROCOID, ObjectIdGetDatum(funcid), 0, 0, 0);
	pp = (Form_pg_proc) GETSTRUCT(ftup);

	/* Check permission to access and call function. */
	aclresult = pg_namespace_aclcheck(pp->pronamespace, GetUserId(), ACL_USAGE);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult, ACL_KIND_NAMESPACE,
					   get_namespace_name(pp->pronamespace));

	aclresult = pg_proc_aclcheck(funcid, GetUserId(), ACL_EXECUTE);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult, ACL_KIND_PROC,
					   get_func_name(funcid));

	/*
	 * assign arguments
	 */
	for (i = 0;
#if PG_VERSION_NUM >= 80400
		i < nargs - candidates->nvargs;
#else
		i < nargs;
#endif
		++i)
	{
		if (args[i] == NULL)
		{
			if (self->flinfo.fn_strict)
				ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("function is strict, but argument %d is NULL", i)));
			self->fcinfo.argnull[i] = true;
		}
		else
		{
			Oid			typinput;
			Oid			typioparam;

			getTypeInputInfo(pp->proargtypes.values[i], &typinput, &typioparam);
			self->fcinfo.arg[i] = OidInputFunctionCall(typinput,
									(char *) args[i], typioparam, -1);
			self->fcinfo.argnull[i] = false;
			pfree(args[i]);
		}
	}

	/*
	 * assign variadic arguments
	 */
#if PG_VERSION_NUM >= 80400
	if (candidates->nvargs > 0)
	{
		int			nfixedarg;
		Oid			func;
		Oid			element_type;
		int16		elmlen;
		bool		elmbyval;
		char		elmalign;
		char		elmdelim;
		Oid			elmioparam;
		Datum	   *elems;
		bool	   *nulls;
		int			dims[1];
		int			lbs[1];
		ArrayType  *arry;

		nfixedarg = i;
		element_type = pp->provariadic;

		/*
		 * Get info about element type, including its input conversion proc
		 */
		get_type_io_data(element_type, IOFunc_input,
						 &elmlen, &elmbyval, &elmalign, &elmdelim,
						 &elmioparam, &func);

		elems = (Datum *) palloc(candidates->nvargs * sizeof(Datum));
		nulls = (bool *) palloc0(candidates->nvargs * sizeof(bool));
		for (i = 0; i < candidates->nvargs; i++)
		{
			if (args[nfixedarg + i] == NULL)
				nulls[i] = true;
			else
			{
				elems[i] = OidInputFunctionCall(func,
								(char *) args[nfixedarg + i], elmioparam, -1);
				pfree(args[nfixedarg + i]);
			}
		}

		dims[0] = candidates->nvargs;
		lbs[0] = 1;
		arry = construct_md_array(elems, nulls, 1, dims, lbs, element_type,
								  elmlen, elmbyval, elmalign);
		self->fcinfo.arg[nfixedarg] = PointerGetDatum(arry);
	}

	/*
	 * assign default arguments
	 */
	if (candidates->ndargs > 0)
	{
		Datum		proargdefaults;
		bool		isnull;
		char	   *str;
		List	   *defaults;
		int			ndelete;

		/* shouldn't happen, FuncnameGetCandidates messed up */
		if (candidates->ndargs > pp->pronargdefaults)
			elog(ERROR, "not enough default arguments");

		proargdefaults = SysCacheGetAttr(PROCOID, ftup,
										 Anum_pg_proc_proargdefaults,
										 &isnull);
		Assert(!isnull);
		str = TextDatumGetCString(proargdefaults);
		defaults = (List *) stringToNode(str);
		Assert(IsA(defaults, List));
		pfree(str);
		/* Delete any unused defaults from the returned list */
		ndelete = list_length(defaults) - candidates->ndargs;
		while (ndelete-- > 0)
			defaults = list_delete_first(defaults);

		foreach(l, defaults)
		{
			Node	   *expr = (Node *) lfirst(l);

			/* probably shouldn't happen ... */
			if (nargs >= FUNC_MAX_ARGS)
				ereport(ERROR,
						(errcode(ERRCODE_TOO_MANY_ARGUMENTS),
				 errmsg("cannot pass more than %d arguments to a function", FUNC_MAX_ARGS)));

			if (!IsA(expr, Const))
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("default arguments of the function supports only constants")));

			if (((Const *) expr)->constisnull)
				self->fcinfo.argnull[nargs] = false;
			else
			{
				self->fcinfo.arg[nargs] = ((Const *) expr)->constvalue;
				self->fcinfo.argnull[nargs] = false;
			}

			nargs++;
		}
	}
#endif

	ReleaseSysCache(ftup);
	pfree(candidates);

	InitFunctionCallInfoData(self->fcinfo, &self->flinfo, nargs, NULL,
							 (Node *) &self->rsinfo);

	/* create tuple descriptor without any relation locks */
	rel = heap_open(relid, NoLock);
	desc = RelationGetDescr(rel);

	self->desc = CreateTupleDescCopy(desc);
	for (i = 0; i < desc->natts; i++)
		self->desc->attrs[i]->attnotnull = desc->attrs[i]->attnotnull;

	self->estate = CreateExecutorState();
	self->econtext = GetPerTupleExprContext(self->estate);
	self->rsinfo.type = T_ReturnSetInfo;
	self->rsinfo.econtext = self->econtext;
	self->rsinfo.expectedDesc = self->desc;
	self->rsinfo.allowedModes = (int) (SFRM_ValuePerCall | SFRM_Materialize);
	self->rsinfo.returnMode = SFRM_ValuePerCall;
	self->rsinfo.isDone = ExprSingleResult;
	self->rsinfo.setResult = NULL;
	self->rsinfo.setDesc = NULL;
	for (i = 0; i < desc->natts; i++)
		self->desc->attrs[i]->attnotnull = desc->attrs[i]->attnotnull;

	heap_close(rel, NoLock);
}

static int64
FunctionParserTerm(FunctionParser *self)
{
	if (self->econtext)
		FreeExprContext(self->econtext, true);
	if (self->estate)
		FreeExecutorState(self->estate);
	pfree(self);

	return 0;
}

static HeapTuple
FunctionParserRead(FunctionParser *self)
{
	Datum		datum;

#if PG_VERSION_NUM >= 80400
	PgStat_FunctionCallUsage	fcusage;
#endif

	BULKLOAD_PROFILE(&prof_reader_parser);

	pgstat_init_function_usage(&self->fcinfo, &fcusage);

	self->fcinfo.isnull = false;
	self->rsinfo.isDone = ExprSingleResult;
	datum = FunctionCallInvoke(&self->fcinfo);

	pgstat_end_function_usage(&fcusage,
							self->rsinfo.isDone != ExprMultipleResult);

	BULKLOAD_PROFILE(&prof_reader_source);

	if (self->rsinfo.isDone == ExprEndResult)
		return NULL;

	self->tuple.t_data = (HeapTupleHeader) DatumGetPointer(datum);
	self->tuple.t_len = HeapTupleHeaderGetDatumLength(self->tuple.t_data);
	self->base.count++;

	/* Check NOT NULL constraint. */
	if (HeapTupleHasNulls(&self->tuple))
	{
		int		i;
		bits8  *bp = self->tuple.t_data->t_bits;

		for (i = 0; i < self->desc->natts; i++)
		{
			self->base.parsing_field = i + 1;	/* 1 origin */
			if (att_isnull(i, bp) && self->desc->attrs[i]->attnotnull)
				ereport(ERROR,
					(errcode(ERRCODE_NOT_NULL_VIOLATION),
					 errmsg("null value in column \"%s\" violates not-null constraint",
						  NameStr(self->desc->attrs[i]->attname))));
		}
	}

	self->base.parsing_field = -1;

	return &self->tuple;

}

static bool
FunctionParserParam(FunctionParser *self, const char *keyword, char *value)
{
	/* FunctionParser does not support OFFSET */
	return false;	/* no parameters supported */
}

static void
FunctionParserDumpParams(FunctionParser *self)
{
	LoggerLog(INFO, "TYPE = FUNCTION\n");
	/* no parameters supported */
}

static void
FunctionParserDumpRecord(FunctionParser *self, FILE *fp, char *badfile)
{
	char   *str;

	str = tuple_to_cstring(self->desc, &self->tuple);
	if (fprintf(fp, "%s\n", str) < 0 || fflush(fp))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write parse badfile \"%s\": %m",
						badfile)));

	pfree(str);
}
