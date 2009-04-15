/*
 * pg_bulkload: include/pg_strutil.h
 *
 *	  Copyright(C) 2007-2009, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */
#ifndef STRUTIL_H_INCLUDED
#define STRUTIL_H_INCLUDED

/*
 * Function prototypes
 */
extern char *TrimSpace(char *str);
extern char *UnquoteString(char *str, char quote, char escape);
extern char *FindUnquotedChar(char *str, char target, char quote, char escape);
extern bool	ParseBoolean(const char *value, bool defaultValue);
extern char	ParseSingleChar(const char *value);
extern int	ParseInteger(char *value, int minValue);

#endif   /* STRUTIL_H_INCLUDED */
