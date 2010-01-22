/*
 * pg_bulkload: include/pg_strutil.h
 *
 *	  Copyright (c) 2007-2010, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */
#ifndef STRUTIL_H_INCLUDED
#define STRUTIL_H_INCLUDED

typedef struct ParsedFunction
{
	char   *args[FUNC_MAX_ARGS];
	Oid		argtypes[FUNC_MAX_ARGS];
	Oid		oid;
	int		nargs;
	int		nvargs;
	int		ndargs;
} ParsedFunction;

/*
 * Function prototypes
 */
extern char *TrimSpace(char *str);
extern char *UnquoteString(char *str, char quote, char escape);
extern char *QuoteString(char *str);
extern char *QuoteSingleChar(char c);
extern char *FindUnquotedChar(char *str, char target, char quote, char escape);
extern bool	ParseBoolean(const char *value, bool defaultValue);
extern char	ParseSingleChar(const char *value);
extern int	ParseInt32(char *value, int minValue);
extern int64	ParseInt64(char *value, int64 minValue);
extern ParsedFunction ParseFunction(const char *value, bool argistype);

#endif   /* STRUTIL_H_INCLUDED */
