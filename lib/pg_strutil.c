/*
 * pg_bulkload: lib/pg_strutil.c
 *
 *	  Copyright(C) 2007-2009, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

/**
 * @file
 * @brief Implementation of module for treating character string.
 */
#include "postgres.h"

#include "utils/builtins.h"
#include "utils/int8.h"

#include "pg_strutil.h"

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
