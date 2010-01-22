/*
 * pg_bulkload: lib/pg_strutil.c
 *
 *	  Copyright (c) 2007-2010, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

/**
 * @file
 * @brief Implementation of module for treating character string.
 */
#include "postgres.h"

#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "catalog/pg_proc.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "parser/parse_func.h"
#include "parser/parse_type.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/int8.h"
#include "utils/syscache.h"

#include "pg_strutil.h"
#include "pgut/pgut-be.h"

#define IsSpace(c)		(isspace((unsigned char)(c)))

/**
 * @brief Trim white spaces before and after input value.
 *
 * Flow
 * <ol>
 *	 <li>Trim spaces after input value. </li>
 *	 <li>Search the first non-space character, and return the pointer. </li>
 * </ol>
 * @param input		[in/out] Input character string
 * @return The pointer for the head of the character string after triming spaces
 * @note Input string is over written.
 * @note The returned value points the middle of input string.
 */
char *
TrimSpace(char *input)
{
	char	   *p;

	/*
	 * No process for empty string
	 */
	if (*input == '\0')
		return input;

	/*
	 * Search the end of non-space character
	 */
	for (p = input + strlen(input) - 1; p >= input && IsSpace(*p); p--);

	/*
	 * Exit if all the characters are space
	 */
	if (p < input)
	{
		*(p + 1) = '\0';
		return input;
	}
	*(p + 1) = '\0';

	/*
	 * Search the first non-space character and return the position
	 */
	for (p = input; *p && IsSpace(*p); p++);

	return p;
}

/**
 * @brief Trim quotes surrounding string
 *
 * Quoting character(i.e. quote and escape character) is transformed as follows.
 * <ul>
 *	 <li>abc -> abc</li>
 *	 <li>"abc" -> abc</li>
 *	 <li>"abc\"123" -> abc"123</li>
 *	 <li>"abc\\123" -> abc\123</li>
 *	 <li>"abc\123" -> abc\123</li>
 *	 <li>"abc"123 -> abc123</li>
 *	 <li>"abc""123" -> abc123</li>
 *	 <li>"abc -> NG(error occuring) </li>
 * </ul>
 * @param str [in/out] Proccessed string
 * @param quote [in] Quote mark character
 * @param escape [in] Escape character
 * @retval !NULL String not surrounding quote mark character
 * @retval NULL  Error(not closed by quote mark)
 */
char *
UnquoteString(char *str, char quote, char escape)
{
	int			i;				/* Read position */
	int			j;				/* Write position */
	int			in_quote = 0;


	for (i = 0, j = 0; str[i]; i++)
	{
		/*
		 * Find an opened quote mark.
		 */
		if (!in_quote && str[i] == quote)
		{
			in_quote = 1;
			continue;
		}

		/*
		 * Find an closing quote mark.
		 */
		if (in_quote && str[i] == quote)
		{
			in_quote = 0;
			continue;
		}

		/*
		 * Find an escape character.
		 * Process if the next is meta character.
		 */
		if (in_quote && str[i] == escape)
		{
			if (str[i + 1] == quote)
			{
				str[j++] = quote;
				i++;
				continue;
			}
			else if (str[i + 1] == escape)
			{
				str[j++] = escape;
				i++;
				continue;
			}
		}

		/*
		 * If it is ordinal character, copy it without modification.
		 */
		str[j++] = str[i];
	}
	str[j] = '\0';

	/*
	 * Quote mark is not closed
	 */
	if (in_quote)
		return NULL;

	return str;
}

char *
QuoteString(char *str)
{
	char   *qstr;
	int		i;
	int		len;
	bool	need_quote;
	char	c;

	len = strlen(str);
	qstr = palloc0(len * 2 + 2 + 1);

	need_quote = false;
	for (i = 0; i < len; i++)
	{
		c = str[i];

		if (c == '"' || c == '#' || c == ' ' || c == '\t')
		{
			need_quote = true;
			break;
		}
	}

	if (need_quote)
	{
		int	j;

		j = 0;
		qstr[j++] = '"';

		for (i = 0; i < len; i++)
		{
			c = str[i];

			if (c == '"' || c == '\\')
				qstr[j++] = '\\';

			qstr[j++] = c;
		}
		qstr[j] = '"';
	}
	else
		memcpy(qstr, str, len);

	return qstr;
}

char *
QuoteSingleChar(char c)
{
	char   *qstr;

	qstr = palloc(5);

	if (c == '"' || c == '#' || c == ' ' || c == '\t')
	{
		if (c == '"' || c == '\\')
			sprintf(qstr, "\"\\%c\"", c);
		else
			sprintf(qstr, "\"%c\"", c);
	}
	else
		sprintf(qstr, "%c", c);

	return qstr;
}

/**
 * @brief Find the first specified character outside of quote mark
 * @param str [in] Searched string
 * @param target [in] Searched character
 * @param quote [in] Quote mark
 * @param escape [in] Escape character
 * @return If the specified character is found outside quoted string, return the
 * pointer. If it is not found, return NULL.
 */
char *
FindUnquotedChar(char *str, char target, char quote, char escape)
{
	int			i;
	bool		in_quote = false;

	for (i = 0; str[i]; i++)
	{
		if (str[i] == escape)
		{
			/*
			 * Treat it as escape character if it is before meta character
			 */
			if (str[i + 1] == escape || str[i + 1] == quote)
				i++;
		}
		else if (str[i] == quote)
			in_quote = !in_quote;
		else if (!in_quote && str[i] == target)
			return str + i;
	}

	return NULL;
}


/**
 * @brief Parse boolean expression
 */
bool
ParseBoolean(const char *value, bool defaultValue)
{
	if (value == NULL || value[0] == '\0')
		return defaultValue;
	/* XXX: use parse_bool() instead? */
	return DatumGetBool(DirectFunctionCall1(boolin, CStringGetDatum(value)));
}

/**
 * @brief Parse single character expression
 */
char
ParseSingleChar(const char *value)
{
	if (strlen(value) != 1)
		ereport(ERROR,
		(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("must be a single one-byte character: \"%s\"", value)));
	return value[0];
}

/**
 * @brief Parse int32 expression
 */
int
ParseInt32(char *value, int minValue)
{
	int32	i;
	
	i = pg_atoi(value, sizeof(int32), 0);
	if (i < minValue)
		ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
             errmsg("value \"%s\" is out of range", value)));
	return i;
}

/**
 * @brief Parse int64 expression
 */
int64
ParseInt64(char *value, int64 minValue)
{
	int64	i;

	if (pg_strcasecmp(value, "INFINITE") == 0)
		return INT64_MAX;

	i = DatumGetInt64(DirectFunctionCall1(int8in, CStringGetDatum(value)));
	if (i < minValue)
		ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
             errmsg("value \"%s\" is out of range", value)));
	return i;
}

static bool
GetNextArgument(const char *ptr, char **arg, Oid *argtype, const char **endptr, const char *path, bool argistype)
{
	const char *p;
	bool		first_arg = false;
	int			len;

	p = ptr;
	while (isspace((unsigned char) *p))
		p++;

	if (*p == '(')
		first_arg = true;
	else if (*p == ',')
		first_arg = false;
	else if (*p == ')')
	{
		p++;
		while (isspace((unsigned char) *p))
			p++;

		if (*p != '\0')
			ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("function call syntax error: %s", path)));

		*endptr = p;
		return false;
	}
	else
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("function call syntax error: %s", path)));

	p++;
	while (isspace((unsigned char) *p))
		p++;

	*argtype = UNKNOWNOID;
	if (argistype)
	{
		const char *startptr;
		bool		inparenthesis;
		int			nparentheses;
		char	   *str;
		int32		typmod;

		startptr = p;
		inparenthesis = false;
		nparentheses = 0;
		while (*p != '\0' && (inparenthesis || (*p != ',' && *p != ')')))
		{
			if (*p == '(')
			{
				if (inparenthesis)
					ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("function call syntax error: %s", path)));

				inparenthesis = true;
				nparentheses++;
				if (nparentheses > 1)
					ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("function call syntax error: %s", path)));
			}

			if (*p == ')')
				inparenthesis = false;

			p++;
		}

		if (p == startptr)
			ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("function call syntax error: %s", path)));

		while (isspace((unsigned char) *(p - 1)))
			p--;

		len = p - startptr;
		str = palloc(len + 1);
		memcpy(str, startptr, len);
		str[len] = '\0';
		*arg = str;

		/* Use full parser to resolve the type name */
		parseTypeString(*arg, argtype, &typmod);
	}
	else if (*p == '\'')
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
		if (len == 0)
		{
			if (first_arg)
			{
				while (isspace((unsigned char) *p))
					p++;

				if (*p == ')')
				{
					p++;
					while (isspace((unsigned char) *p))
						p++;

					if (*p != '\0')
						ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
				 			errmsg("function call syntax error: %s", path)));

					*endptr = p;
					return false;
				}
			}

			ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("function call syntax error: %s", path)));
		}

		str = palloc(len + 2);
		snprintf(str, len + 2, "%c%s", minus ? '-' : '+', startptr);

		/* Check for numeric constants. */
		DirectFunctionCall3(numeric_in, CStringGetDatum(str + 1),
							ObjectIdGetDatum(InvalidOid), -1);
		*arg = str;
	}

	*endptr = p;
	return true;
}

/**
 * @brief Parse function expression
 */
ParsedFunction
ParseFunction(const char *value, bool argistype)
{
	int					i;
	ParsedFunction		ret;
	char			   *buf;
	const char		   *nextp;
	bool				done = false;
	List			   *names;
	ListCell		   *l;
	int					nargs;
	FuncCandidateList	candidates;
	FuncCandidateList	find = NULL;
	HeapTuple			ftup;
	Form_pg_proc		pp;
	AclResult			aclresult;

	buf = palloc(strlen(value) + 1);

	/* parse function name */
	nextp = value;
	do
	{
		if (*nextp == '\"')
		{
			/* Quoted name */
			for (;;)
			{
				nextp = strchr(nextp + 1, '\"');

				/* mismatched quotes */
				if (nextp == NULL)
					ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("function call syntax error: %s", value)));

				if (nextp[1] != '\"')
					break;		/* found end of quoted name */
			}

			/* nextp now points at the terminating quote */
			nextp = nextp + 1;
		}
		else
		{
			/* Unquoted name */
			const char *startp;

			startp = nextp;
			while (*nextp && *nextp != '.' && *nextp != '(' &&
				   !isspace((unsigned char) *nextp))
				nextp++;

			/* empty unquoted name not allowed */
			if (startp == nextp)
				ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("function call syntax error: %s", value)));
		}

		while (isspace((unsigned char) *nextp))
			nextp++;			/* skip trailing whitespace */

		if (*nextp == '.')
		{
			nextp++;
			while (isspace((unsigned char) *nextp))
				nextp++;		/* skip leading whitespace for next */
			/* we expect another name, so done remains false */
		}
		else if (*nextp == '\0' || *nextp == '(')
			done = true;
		else
		{
			/* invalid syntax */
			ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("function call syntax error: %s", value)));
		}

		/* Loop back if we didn't reach end of function name */
	} while (!done);

	strncpy(buf, value, nextp - value);
	buf[nextp - value] = '\0';

	names = stringToQualifiedNameList(buf);
	pfree(buf);

	if (*nextp == '\0')
	{
		if (!argistype)
			ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("function call syntax error: %s", value)));

		nargs = -1;

		/* Get list of possible candidates from namespace search */
		candidates = FuncnameGetCandidates(names, nargs, NIL, false, false);
	}
	else
	{
		/* parse function arguments */
		nargs = 0;
		while (GetNextArgument(nextp, &ret.args[nargs], &ret.argtypes[nargs], &nextp, value, argistype))
		{
			nargs++;
			if (nargs > FUNC_MAX_ARGS)
				ereport(ERROR,
					(errcode(ERRCODE_TOO_MANY_ARGUMENTS),
					 errmsg("functions cannot have more than %d arguments", FUNC_MAX_ARGS)));
		}

		/* Get list of possible candidates from namespace search */
		candidates = FuncnameGetCandidates(names, nargs, NIL, true, true);
	}

	if (candidates == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION),
				 errmsg("function %s does not exist",
						func_signature_string(names, nargs, NIL, ret.argtypes)),
				 errhint("No function matches the given name and argument types.")));
	else if (argistype && nargs > 0)
	{
		for (find = candidates; find; find = find->next)
		{
			if (memcmp(find->args, ret.argtypes, nargs * sizeof(Oid)) == 0)
				break;
		}

		if (find == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_FUNCTION),
					 errmsg("function %s does not exist",
							func_signature_string(names, nargs, NIL,
												  ret.argtypes)),
				errhint("No function matches the given name and argument types.")));
	}
	else if (candidates->next != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_AMBIGUOUS_FUNCTION),
				 errmsg("function %s is not unique",
						func_signature_string(names, nargs, NIL,
											  ret.argtypes)),
				 errhint("Could not choose a best candidate function.")));
	else
		find = candidates;

	if (!OidIsValid(find->oid))
		ereport(ERROR,
				(errcode(ERRCODE_AMBIGUOUS_FUNCTION),
				 errmsg("function %s is not unique",
						func_signature_string(names, nargs, NIL,
											  ret.argtypes)),
				 errhint("Could not choose a best candidate function.")));

	foreach (l, names)
	{
		Value  *v = lfirst(l);

		pfree(strVal(v));
		pfree(v);
	}
	list_free(names);

	ret.oid = find->oid;
#if PG_VERSION_NUM >= 80400
	ret.nvargs = find->nvargs;
	ret.ndargs = find->ndargs;
#endif
	if (nargs == -1)
	{
		ret.nargs = find->nargs;
		for (i = 0; i < find->nargs; i++)
			ret.argtypes[i] = find->args[i];
	}
	else
		ret.nargs = nargs;

	while (candidates)
	{
		FuncCandidateList	c = candidates;
		candidates = candidates->next;
		pfree(c);
	}

	ftup = SearchSysCache(PROCOID, ObjectIdGetDatum(ret.oid), 0, 0, 0);
	pp = (Form_pg_proc) GETSTRUCT(ftup);

	/* Check permission to access and call function. */
	aclresult = pg_namespace_aclcheck(pp->pronamespace, GetUserId(), ACL_USAGE);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult, ACL_KIND_NAMESPACE,
					   get_namespace_name(pp->pronamespace));

	aclresult = pg_proc_aclcheck(ret.oid, GetUserId(), ACL_EXECUTE);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult, ACL_KIND_PROC,
					   get_func_name(ret.oid));

	ReleaseSysCache(ftup);

	return ret;
}
