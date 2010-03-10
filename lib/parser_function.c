/*
 * pg_bulkload: lib/parser_function.c
 *
 *	  Copyright (c) 2009-2010, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

/**
 * @file
 * @brief Binary HeapTuple format handling module implementation.
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/htup.h"
#include "catalog/pg_proc.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

#include "logger.h"
#include "pg_profile.h"
#include "pg_strutil.h"
#include "reader.h"
#include "pgut/pgut-be.h"

typedef struct FunctionParser
{
	Parser	base;

	FmgrInfo				flinfo;
	FunctionCallInfoData	fcinfo;
	TupleDesc				desc;
	EState				   *estate;
	ExprContext			   *econtext;
	ExprContext			   *arg_econtext;
	ReturnSetInfo			rsinfo;
	HeapTupleData			tuple;
} FunctionParser;

static void	FunctionParserInit(FunctionParser *self, Checker *checker, const char *infile, TupleDesc desc);
static HeapTuple FunctionParserRead(FunctionParser *self, Checker *checker);
static int64	FunctionParserTerm(FunctionParser *self);
static bool FunctionParserParam(FunctionParser *self, const char *keyword, char *value);
static void FunctionParserDumpParams(FunctionParser *self);
static void FunctionParserDumpRecord(FunctionParser *self, FILE *fp, char *bafdile);

/* ========================================================================
 * FunctionParser
 * ========================================================================*/

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
FunctionParserInit(FunctionParser *self, Checker *checker, const char *infile, TupleDesc desc)
{
	int					i;
	ParsedFunction		function;
	int					nargs;
	Oid					funcid;
	HeapTuple			ftup;
	Form_pg_proc		pp;

	if (checker->encoding != -1)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("does not support parameter \"ENCODING\" in \"TYPE = FUNCTION\"")));

	function = ParseFunction(infile, false);

	funcid = function.oid;
	fmgr_info(funcid, &self->flinfo);

	if (!self->flinfo.fn_retset)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function must return set")));

	ftup = SearchSysCache(PROCOID, ObjectIdGetDatum(funcid), 0, 0, 0);
	pp = (Form_pg_proc) GETSTRUCT(ftup);

	/*
	 * assign arguments
	 */
	nargs = function.nargs;
	for (i = 0;
#if PG_VERSION_NUM >= 80400
		i < nargs - function.nvargs;
#else
		i < nargs;
#endif
		++i)
	{
		if (function.args[i] == NULL)
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
									(char *) function.args[i], typioparam, -1);
			self->fcinfo.argnull[i] = false;
			pfree(function.args[i]);
		}
	}

	/*
	 * assign variadic arguments
	 */
#if PG_VERSION_NUM >= 80400
	if (function.nvargs > 0)
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

		elems = (Datum *) palloc(function.nvargs * sizeof(Datum));
		nulls = (bool *) palloc0(function.nvargs * sizeof(bool));
		for (i = 0; i < function.nvargs; i++)
		{
			if (function.args[nfixedarg + i] == NULL)
				nulls[i] = true;
			else
			{
				elems[i] = OidInputFunctionCall(func,
								(char *) function.args[nfixedarg + i], elmioparam, -1);
				pfree(function.args[nfixedarg + i]);
			}
		}

		dims[0] = function.nvargs;
		lbs[0] = 1;
		arry = construct_md_array(elems, nulls, 1, dims, lbs, element_type,
								  elmlen, elmbyval, elmalign);
		self->fcinfo.arg[nfixedarg] = PointerGetDatum(arry);
	}

	/*
	 * assign default arguments
	 */
	if (function.ndargs > 0)
	{
		Datum		proargdefaults;
		bool		isnull;
		char	   *str;
		List	   *defaults;
		int			ndelete;
		ListCell   *l;

		/* shouldn't happen, FuncnameGetCandidates messed up */
		if (function.ndargs > pp->pronargdefaults)
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
		ndelete = list_length(defaults) - function.ndargs;
		while (ndelete-- > 0)
			defaults = list_delete_first(defaults);

		self->arg_econtext = CreateStandaloneExprContext();
		foreach(l, defaults)
		{
			Expr	   *expr = (Expr *) lfirst(l);
			ExprState  *argstate;
			ExprDoneCond thisArgIsDone;

			/* probably shouldn't happen ... */
			if (nargs >= FUNC_MAX_ARGS)
				ereport(ERROR,
						(errcode(ERRCODE_TOO_MANY_ARGUMENTS),
				 errmsg("cannot pass more than %d arguments to a function", FUNC_MAX_ARGS)));

			argstate = ExecInitExpr(expr, NULL);

			self->fcinfo.arg[nargs] = ExecEvalExpr(argstate,
												   self->arg_econtext,
												   &self->fcinfo.argnull[nargs],
												   &thisArgIsDone);

			if (thisArgIsDone != ExprSingleResult)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("functions and operators can take at most one set argument")));

			nargs++;
		}
	}
#endif

	ReleaseSysCache(ftup);

	InitFunctionCallInfoData(self->fcinfo, &self->flinfo, nargs, NULL,
							 (Node *) &self->rsinfo);

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
}

static int64
FunctionParserTerm(FunctionParser *self)
{
	if (self->arg_econtext)
		FreeExprContext(self->arg_econtext, true);
	if (self->econtext)
		FreeExprContext(self->econtext, true);
	if (self->estate)
		FreeExecutorState(self->estate);
	pfree(self);

	return 0;
}

static HeapTuple
FunctionParserRead(FunctionParser *self, Checker *checker)
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

	self->tuple.t_data = DatumGetHeapTupleHeader(datum);
	self->tuple.t_len = HeapTupleHeaderGetDatumLength(self->tuple.t_data);
	self->base.count++;

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
