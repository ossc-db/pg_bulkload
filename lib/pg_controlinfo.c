/*
 * pg_bulkload: lib/pg_controlinfo.c
 *
 *	  Copyright(C) 2007-2009, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

/**
 * @file
 * @brief Implementation of control information process module
 */
#include "postgres.h"

#include <fcntl.h>
#include <limits.h>
#include <unistd.h>

#include "access/heapam.h"
#include "catalog/namespace.h"
#include "executor/executor.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "storage/fd.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/relcache.h"

#include "pg_bulkload.h"
#include "pg_controlinfo.h"
#include "pg_strutil.h"

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
static void	ParseControlFile(ControlInfo *ci, FILE *file);
static void	PrepareControlInfo(ControlInfo *ci);
static void ParseErrorCallback(void *arg);

/**
 * @brief Initialize ControlInfo
 *
 * @param fname [in] path of the control file(absolute path)
 * @return ControlInfo structure with control file information
 */
ControlInfo *
OpenControlInfo(const char *fname)
{
	ControlInfo	   *ci;
	FILE		   *file;

	if (!is_absolute_path(fname))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_NAME),
				 errmsg("control file name must be absolute path")));

	/*
	 * initialization ControlInfo sttucture
	 */
	ci = (ControlInfo *) palloc0(sizeof(ControlInfo));
	ci->ci_max_err_cnt = -1;
	ci->ci_offset = -1;
	ci->ci_limit = INT_MAX;
	ci->ci_infd = -1;

	/*
	 * open file
	 */
	if ((file = fopen(fname, "rt")) == NULL)
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not open \"%s\" %m", fname)));

	PG_TRY();
	{
		ParseControlFile(ci, file);
		PrepareControlInfo(ci);
	}
	PG_CATCH();
	{
		fclose(file);	/* ignore errors */
		CloseControlInfo(ci);
		PG_RE_THROW();
	}
	PG_END_TRY();

	/* close control file */
	if (fclose(file) < 0)
		ereport(ERROR, (errcode_for_file_access(),
					errmsg("could not close \"%s\" %m", fname)));

	return ci;
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
ParseControlFile(ControlInfo *ci, FILE *file)
{
	char					buf[LINEBUF];
	ControlFileLine			line;
	ErrorContextCallback	errcontext;

	errcontext.callback = ParseErrorCallback;
	errcontext.arg = &line;
	errcontext.previous = error_context_stack;
	error_context_stack = &errcontext;

	/*
	 * extract keywords and input values
	 */
	line.line = 0;
	while (fgets(buf, LINEBUF, file) != NULL)
	{
		char	   *keyword = NULL;
		char	   *target = NULL;
		char	   *p;
		char	   *q;

		line.line++;
		line.keyword = NULL;
		line.value = NULL;

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
			continue;

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

		line.keyword = keyword;
		line.value = target;

		/*
		 * result
		 */
		if (strcmp(keyword, "TABLE") == 0)
		{
			ASSERT_ONCE(ci->ci_rv == NULL);

			ci->ci_rv = makeRangeVarFromNameList(
							stringToQualifiedNameList(target));
		}
		else if (strcmp(keyword, "INFILE") == 0)
		{
			ASSERT_ONCE(ci->ci_infname == NULL);

			if (!is_absolute_path(target))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("relative path not allowed for INFILE: %s", target)));

			ci->ci_infname = pstrdup(target);
		}
		else if (strcmp(keyword, "TYPE") == 0)
		{
			ASSERT_ONCE(ci->ci_parser == NULL);

			if (strcmp(target, "FIXED") == 0)
				ci->ci_parser = CreateFixedParser();
			else if (strcmp(target, "CSV") == 0)
				ci->ci_parser = CreateCSVParser();
			else
				ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								errmsg("invalid file type \"%s\"", target)));
		}
		else if (strcmp(keyword, "LOADER") == 0)
		{
			ASSERT_ONCE(ci->ci_loader == NULL);
			
			if (strcmp(target, "DIRECT") == 0)
				ci->ci_loader = DirectHeapLoad;
			else if (strcmp(target, "BUFFERED") == 0)
				ci->ci_loader = BufferedHeapLoad;
			else
				ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								errmsg("invalid loader \"%s\"", target)));
		}
		else if (strcmp(keyword, "MAX_ERR_CNT") == 0)
		{
			ASSERT_ONCE(ci->ci_max_err_cnt < 0);
			ci->ci_max_err_cnt = ParseInteger(target, 0);
		}
		else if (strcmp(keyword, "OFFSET") == 0)
		{
			ASSERT_ONCE(ci->ci_offset < 0);
			ci->ci_offset = ParseInteger(target, 0);
		}
		else if (strcmp(keyword, "LIMIT") == 0)
		{
			ASSERT_ONCE(ci->ci_limit == INT_MAX);
			ci->ci_limit = ParseInteger(target, 0);
		}
		else
		{
			if (ci->ci_parser == NULL ||
				!ParserParam(ci->ci_parser, keyword, target))
			{
				ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("invalid keyword \"%s\"", keyword)));
			}
		}
	}

	/*
	 * checking necessary common setting items
	 */
	if (ci->ci_parser == NULL)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("no TYPE specified")));
	if (ci->ci_rv == NULL)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("no TABLE specified")));
	if (ci->ci_infname == NULL)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("no INFILE specified")));

	/*
	 * Set defaults to unspecified parameters.
	 */
	if (ci->ci_loader == NULL)
		ci->ci_loader = DirectHeapLoad;
	if (ci->ci_max_err_cnt < 0)
		ci->ci_max_err_cnt = 1;
	if (ci->ci_offset < 0)
		ci->ci_offset = 0;

	error_context_stack = errcontext.previous;
}

/**
 * @brief Preparing control information
 *
 * Processing flow
 *	 -# open relation
 *	 -# open input file
 *	 -# open data file
 *	 -# get function information for type information and type transformation.
 * @param ci [in/out] control information
 * @return void
 */
static void
PrepareControlInfo(ControlInfo *ci)
{
	Relation			rel;
	ResultRelInfo	   *relinfo;
	Form_pg_attribute  *attr;
	int					attnum;
	Oid					in_func_oid;
	AttrNumber			natts;
	AclResult			aclresult;

	/*
	 * open relation and do a sanity check
	 */
	rel = heap_openrv(ci->ci_rv, AccessExclusiveLock);

	if (rel->rd_rel->relkind != RELKIND_RELATION)
	{
		const char *type;
		switch (rel->rd_rel->relkind)
		{
			case RELKIND_VIEW:
				type = "view";
				break;
			case RELKIND_SEQUENCE:
				type = "sequence";
				break;
			default:
				type = "non-table relation";
				break;
		}
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("cannot load to %s \"%s\"",
					type, RelationGetRelationName(rel))));
	}

	aclresult = pg_class_aclcheck(
		RelationGetRelid(rel), GetUserId(), ACL_INSERT);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult, ACL_KIND_CLASS,
					   RelationGetRelationName(rel));

	ci->ci_rel = rel;

	/*
	 * open input data file
	 */
	ci->ci_infd = BasicOpenFile(ci->ci_infname, O_RDONLY | PG_BINARY, 0);
	if (ci->ci_infd == -1)
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not open \"%s\" %m", ci->ci_infname)));

	/*
	 * create estate
	 */
	relinfo = makeNode(ResultRelInfo);
	relinfo->ri_RangeTableIndex = 1;	/* dummy */
	relinfo->ri_RelationDesc = rel;
	relinfo->ri_TrigDesc = NULL;	/* TRIGGER is not supported */
	relinfo->ri_TrigInstrument = NULL;
	ExecOpenIndices(relinfo);

	ci->ci_estate = CreateExecutorState();
	ci->ci_estate->es_num_result_relations = 1;
	ci->ci_estate->es_result_relations = relinfo;
	ci->ci_estate->es_result_relation_info = relinfo;
	ci->ci_slot = MakeSingleTupleTableSlot(RelationGetDescr(rel));

	/*
	 * get column information of the target relation
	 */
	natts = RelationGetDescr(ci->ci_rel)->natts;
	attr = RelationGetDescr(ci->ci_rel)->attrs;

	ci->ci_typeioparams = (Oid *) palloc(natts * sizeof(Oid));
	ci->ci_in_functions = (FmgrInfo *) palloc(natts * sizeof(FmgrInfo));
	ci->ci_attnumlist = palloc(natts * sizeof(int));
	ci->ci_attnumcnt = 0;
	for (attnum = 1; attnum <= natts; attnum++)
	{
		/* ignore dropped columns */
		if (attr[attnum - 1]->attisdropped)
			continue;

		/* get type information and input function */
		getTypeInputInfo(attr[attnum - 1]->atttypid,
						 &in_func_oid, &ci->ci_typeioparams[attnum - 1]);
		fmgr_info(in_func_oid, &ci->ci_in_functions[attnum - 1]);

		/* update valid column information */
		ci->ci_attnumlist[ci->ci_attnumcnt] = attnum - 1;
		ci->ci_attnumcnt++;
	}

	ci->ci_values = palloc0(sizeof(Datum) * natts);
	ci->ci_isnull = palloc(sizeof(bool) * natts);
	MemSet(ci->ci_isnull, true, sizeof(bool) * natts);
}

/**
 * @brief clean up ControlInfo structure.
 *
 * Processing flow
 * -# close relation
 * -# free ControlInfo structure
 * @param ci [in/out] control information
 * @return void
 */
void
CloseControlInfo(ControlInfo *ci)
{
	if (ci == NULL)
		return;

	if (ci->ci_rel)
		heap_close(ci->ci_rel, NoLock);

	if (ci->ci_infd != -1 && close(ci->ci_infd) < 0)
		ereport(WARNING, (errcode_for_file_access(),
			errmsg("could not close \"%s\" %m", ci->ci_infname)));

	/*
	 * FIXME: We might not need to free each fields because memories
	 * are automatically freeed at the end of query.
	 */

	if (ci->ci_rv != NULL)
		pfree(ci->ci_rv);

	if (ci->ci_parser)
		ParserCleanUp(ci->ci_parser);

	if (ci->ci_infname != NULL)
		pfree(ci->ci_infname);

	if (ci->ci_typeioparams != NULL)
		pfree(ci->ci_typeioparams);

	if (ci->ci_in_functions)
		pfree(ci->ci_in_functions);

	if (ci->ci_values)
		pfree(ci->ci_values);

	if (ci->ci_isnull)
		pfree(ci->ci_isnull);

	if (ci->ci_attnumlist)
		pfree(ci->ci_attnumlist);

	/* Release info used in rebuilding indexes. */
	if (ci->ci_slot)
		ExecDropSingleTupleTableSlot(ci->ci_slot);

	if (ci->ci_estate)
	{
		if (ci->ci_estate->es_result_relation_info)
			ExecCloseIndices(ci->ci_estate->es_result_relation_info);
		FreeExecutorState(ci->ci_estate);
	}

	pfree(ci);
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
