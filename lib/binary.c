/*
 * pg_bulkload: lib/binary.c
 *
 *	  Copyright (c) 2011-2021, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

/**
 * @file
 * @brief Implementation of binary file module
 */
#include "pg_bulkload.h"

#include "catalog/pg_type.h"
#include "utils/builtins.h"

#include "binary.h"
#include "pg_strutil.h"

static Datum Read_char(TupleFormer *former, char *in, const Field* field, int i, bool *isnull);
static Datum Read_varchar(TupleFormer *former, char *in, const Field* field, int i, bool *isnull);
static Datum Read_int16(TupleFormer *former, char *in, const Field* field, int i, bool *isnull);
static Datum Read_int32(TupleFormer *former, char *in, const Field* field, int i, bool *isnull);
static Datum Read_int64(TupleFormer *former, char *in, const Field* field, int i, bool *isnull);
static Datum Read_uint16(TupleFormer *former, char *in, const Field* field, int i, bool *isnull);
static Datum Read_uint32(TupleFormer *former, char *in, const Field* field, int i, bool *isnull);
static Datum Read_float4(TupleFormer *former, char *in, const Field* field, int i, bool *isnull);
static Datum Read_float8(TupleFormer *former, char *in, const Field* field, int i, bool *isnull);

static void Write_char(char *out, size_t len, Datum value, bool null);
static void Write_int16(char *out, size_t len, Datum value, bool null);
static void Write_int32(char *out, size_t len, Datum value, bool null);
static void Write_int64(char *out, size_t len, Datum value, bool null);
static void Write_uint16(char *out, size_t len, Datum value, bool null);
static void Write_uint32(char *out, size_t len, Datum value, bool null);
static void Write_float4(char *out, size_t len, Datum value, bool null);
static void Write_float8(char *out, size_t len, Datum value, bool null);

typedef enum TypeId
{
	T_CHAR,
	T_VARCHAR,
	T_INT2,
	T_INT4,
	T_INT8,
	T_UINT2,
	T_UINT4,
	T_FLOAT4,
	T_FLOAT8,
} TypeId;

struct TypeInfo
{
	const char *name;
	Read		read;
	Write		write;
	int			len;
	Oid			typeid;
}
TYPES[] =
{
	{ "CHAR"				, Read_char		, Write_char	, 0					, CSTRINGOID},
	{ "VARCHAR"				, Read_varchar	, Write_char	, 0					, CSTRINGOID},
	{ "SMALLINT"			, Read_int16	, Write_int16	, sizeof(int16)		, INT2OID	},
	{ "INTEGER"				, Read_int32	, Write_int32	, sizeof(int32)		, INT4OID	},
	{ "BIGINT"				, Read_int64	, Write_int64	, sizeof(int64)		, INT8OID	},
	{ "UNSIGNED SMALLINT"	, Read_uint16	, Write_uint16	, sizeof(uint16)	, INT4OID	},
	{ "UNSIGNED INTEGER"	, Read_uint32	, Write_uint32	, sizeof(uint32)	, INT8OID	},
	{ "FLOAT"				, Read_float4	, Write_float4	, sizeof(float4)	, FLOAT4OID	},
	{ "DOUBLE"				, Read_float8	, Write_float8	, sizeof(float8)	, FLOAT8OID	},
};

struct TypeAlias
{
	const char *name;
	TypeId		id;
}
ALIASES[] =
{
	/* aliases (SQL) */
	{ "CHARACTER"			, T_CHAR },
	{ "CHARACTER VARYING"	, T_VARCHAR },
	{ "REAL"				, T_FLOAT4 },
	/* aliases (C) */
	{ "SHORT"				, T_INT2 },
	{ "INT"					, T_INT4 },
	{ "LONG"				, T_INT8 },
	{ "UNSIGNED SHORT"		, T_UINT2 },
	{ "UNSIGNED INT"		, T_UINT4 },
};


/*
 * Parse typename and return the type id.
 */
static TypeId
ParseTypeName(const char *value)
{
	int		i;

	/* Search from types. */
	for (i = 0; i < lengthof(TYPES); i++)
	{
		if (pg_strcasecmp(value, TYPES[i].name) == 0)
			return (TypeId) i;
	}

	/* Search from aliases. */
	for (i = 0; i < lengthof(ALIASES); i++)
	{
		if (pg_strcasecmp(value, ALIASES[i].name) == 0)
			return ALIASES[i].id;
	}

	/* not found */
	ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
		errmsg("invalid typename : %s", value)));
	return 0;	/* keep complier quiet */
}

/*
 * Is string terminated with ')' ?
 */
static bool
CheckRightparenthesis(const char *s)
{
	while (isspace((unsigned char) *s))
		s++;
	return *s == ')';
}

/*
 * Parse length and offset from the field description.
 */
static char *
ParseLengthAndOffset(const char *value, Field *field, bool length_only)
{
	unsigned long	n1;
	unsigned long	n2;
	char		   *p1;
	char		   *p2;

	n1 = strtoul(value, &p1, 0);
	if (value < p1)
	{
		/* skip spaces */
		while (isspace((unsigned char) *p1))
			p1++;
		switch (*p1)
		{
		case ')': /* TYPE(LEN) */
			field->len = n1;
			return p1;
		case '+': /* TYPE(OFFSET+LEN)*/
			if (length_only)
				break;

			p1++;
			n2 = strtoul(p1, &p2, 0);
			if (p1 < p2 && CheckRightparenthesis(p2))
			{
				field->offset = (int) (n1 - 1);	/* start with 1 */
				field->len = n2;
				return p2;
			}
			break;
		case ':': /* TYPE(BEGIN:END) */
			if (length_only)
				break;

			p1++;
			n2 = strtoul(p1, &p2, 0);
			if (p1 < p2 && CheckRightparenthesis(p2))
			{
				field->offset = (int) (n1 - 1);	/* start with 1 */
				field->len = (n2 - n1 + 1);
				return p2;
			}
			break;
		}
	}

	if (length_only)
		ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
			errmsg("TYPE argument must be ( L )")));
	else
		ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
			errmsg("TYPE argument must be ( { L | B+L | B:E } )")));
	return 0;	/* keep complier quiet */
}

static int
hex_in(char c)
{
	if ('0' <= c && c <= '9')
		return c - '0';
	else if ('A' <= c && c <= 'F')
		return 0xA + c - 'A';
	else if ('a' <= c && c <= 'f')
		return 0xA + c - 'a';
	ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
		errmsg("NULLIF argument must be '...' or hex digits")));
	return 0;
}

static char
hex_out(int c)
{
	if (0 <= c && c <= 9)
		return '0' + c;
	else if (0xA <= c && c <= 0xF)
		return 'A' + c - 0xA;
	ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
		errmsg("NULLIF argument must be '...' or hex digits")));
	return '0';
}

/*
 * Parse field format like "TYPE(STRIDE) NULLIF { 'str' | hex }"
 */
static void
ParseFormat(const char *value, Field *field, bool length_only)
{
	StringInfoData	buf;
	TypeId		id;
	size_t		nulliflen;
	const char *p;
	const struct TypeInfo *type;

	initStringInfo(&buf);
	nulliflen = strlen("NULLIF");

	/* parse typename */
	p = value;
	while (1)
	{
		int	n = 0;

		while (!isspace(p[n]) && p[n] != '(' && p[n] != '\0')
			n++;

		if (p != value)
			appendStringInfoChar(&buf, ' ');

		appendBinaryStringInfo(&buf, p, n);

		p += n;
		while (isspace((unsigned char) *p))
			p++;

		if (*p == '(' || *p == '\0' ||
			pg_strncasecmp(p, "NULLIF", nulliflen) == 0)
			break;
	}

	id = ParseTypeName(buf.data);
	type = &TYPES[id];

	/* parse length and offset */
	field->len = 0;
	if (*p == '(')
	{
		/* typename (N | B:E) */
		p = ParseLengthAndOffset(p + 1, field, length_only);
		if (field->len <= 0)
			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("TYPE length must be positive")));
		if (field->offset < 0)
			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("TYPE offset must be positive")));

		/* skip spaces and ')' */
		while (isspace((unsigned char) *p))
			p++;
		p++;
		while (isspace((unsigned char) *p))
			p++;
	}
	else
		field->len = type->len;	/* use default */

	switch (id)
	{
	case T_CHAR:
	case T_VARCHAR:
		if (field->len <= 0)
			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("length of %s should be specified", type->name)));
		break;
	case T_INT2:
	case T_INT8:
	case T_UINT2:
	case T_FLOAT8:
		if (field->len != type->len)
			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("length of %s should be %d or default", type->name, type->len)));
		break;
	case T_INT4:
		switch (field->len)
		{
		case sizeof(int16):		/* INTEGER(2) */
			type = &TYPES[T_INT2];
			break;
		case sizeof(int32):		/* INTEGER(4) */
			type = &TYPES[T_INT4];
			break;
		case sizeof(int64):		/* INTEGER(8) */
			type = &TYPES[T_INT8];
			break;
		default:
			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("length of INTEGER should be 2, 4 or 8")));
		}
		break;
	case T_UINT4:
		switch (field->len)
		{
		case sizeof(uint16):	/* UNSIGNED INTEGER(2) */
			type = &TYPES[T_UINT2];
			break;
		case sizeof(uint32):	/* UNSIGNED INTEGER(4) */
			type = &TYPES[T_UINT4];
			break;
		default:
			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("length of UNSIGNED INTEGER should be 2 or 4")));
		}
		break;
	case T_FLOAT4:
		switch (field->len)
		{
		case sizeof(float4):	/* FLOAT(4) */
			type = &TYPES[T_FLOAT4];
			break;
		case sizeof(float8):	/* FLOAT(8) */
			type = &TYPES[T_FLOAT8];
			break;
		default:
			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("length of FLOAT should be 4 or 8")));
		}
		break;
	default:
		break;
	}

	/* parse nullif */
	if (pg_strncasecmp(p, "NULLIF", nulliflen) == 0 && isspace(p[nulliflen]))
	{
		/* Seek to the argument of NULLIF */
		p += nulliflen + 1;
		while (isspace((unsigned char) *p))
			p++;

		if (*p == '\'' || *p == '"')
		{
			/* string format */
			char		quote = *p;
			int			n;

			p++;
			for (n = 0; p[n] != quote; n++)
			{
				if (p[n] == '\0')
					ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
						errmsg("NULLIF argument is not terminated : %s", value)));
			}

			field->nulllen = n;
			field->nullif = palloc(n + 1);
			memcpy(field->nullif, p, n);
			field->nullif[n] = '\0';
			p += n + 1;
		}
		else
		{
			/* hex format */
			int		i;
			size_t	n = strlen(p);

			while (isspace((unsigned char) p[n - 1]))
				n--;
			n = (1 + n) / 2;

			field->nulllen = n;
			field->nullif = palloc(n + 1);
			field->nullif[n] = '\0';
			for (i = 0; i < n; i++)
			{
				field->nullif[i] = (char)
					((hex_in(p[i*2]) << 4) + hex_in(p[i*2+1]));
			}
			p += n * 2;
		}

		while (isspace((unsigned char) *p))
			p++;
	}

	if (*p != '\0')
		ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
			errmsg("syntax error at or near \"%s\" : %s", p, value)));

	field->read = type->read;
	field->write = type->write;
	field->typeid = type->typeid;
	field->character =
		(field->read == Read_char || field->read == Read_varchar) ?
			true : false;

	if ((type->len == 0 && field->len < field->nulllen) ||
        (type->len > 0 && field->nulllen > 0 && field->len != field->nulllen))
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			errmsg("length of NULLIF argument should be %d bytes %s(%d bytes given) : %s",
				field->len, (type->len == 0 ? "or less " : ""),
				field->nulllen, value)));
	}

	pfree(buf.data);
}

void
BinaryParam(Field **fields, int *nfield, char *value, bool preserve_blanks, bool length_only)
{
	Field  *field;

	if (*fields)
		*fields = repalloc(*fields, sizeof(Field) * (*nfield + 1));
	else
		*fields = palloc(sizeof(Field) * (*nfield + 1));

	field = *fields + *nfield;

	if (*nfield > 0)
		field->offset = field[-1].offset + field[-1].len;
	else
		field->offset = 0;
	field->nullif = "";
	field->nulllen = 0;

	if (isdigit((unsigned char) value[0]))
	{
		if (length_only)
			ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
					errmsg("invalid typename : %s", value)));

		/* default is CHAR or VARCHAR (compatible with 2.2.x) */
		field->read = (preserve_blanks ? Read_varchar : Read_char);
		field->write = Write_char;
		field->len = ParseInt32(value, 1);
		field->character = true;
		field->typeid = CSTRINGOID;
	}
	else
	{
		/* Overwrite field parameters */
		ParseFormat(value, field, length_only);
	}

	(*nfield)++;
}

int
BinaryDumpParam(Field *field, StringInfo buf, int offset)
{
	int	i;

	for (i = 0; i < lengthof(TYPES); i++)
	{
		if (TYPES[i].read == field->read)
		{
			if (offset == field->offset)
				appendStringInfo(buf, "%s (%d)", TYPES[i].name, field->len);
			else
				appendStringInfo(buf, "%s (%d + %d)", TYPES[i].name, field->offset + 1, field->len);

			offset = field->offset + field->len;
			break;
		}
	}

	if (i == lengthof(TYPES))
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			errmsg("invalid type")));

	if (field->nulllen > 0)
	{
		bool	hex;

		hex = false;
		for (i = 0; i < field->nulllen; i++)
		{
			if (!isalnum(field->nullif[i]) && !isspace(field->nullif[i]))
			{
				hex = true;
				break;
			}
		}

		if (hex)
		{
			appendStringInfoString(buf, " NULLIF ");
			for (i = 0; i < field->nulllen; i++)
			{
				appendStringInfoCharMacro(buf,
										  hex_out((unsigned char) field->nullif[i] >> 4));
				appendStringInfoCharMacro(buf,
										  hex_out((unsigned char) field->nullif[i] % 0x10));
			}
		}
		else
			appendStringInfo(buf, " NULLIF '%s'", field->nullif);
	}
	return offset;
}

void
BinaryDumpParams(Field *fields, int nfield, StringInfo buf, char *param)
{
	int	i;
	int	offset;

	offset = 0;
	for (i = 0; i < nfield; i++)
	{
		Field  *field;

		field = fields + i;

		appendStringInfo(buf, "%s = ", param);
		offset = BinaryDumpParam(field, buf, offset);
		appendStringInfoCharMacro(buf, '\n');
	}
}

#define IsWhiteSpace(c)	((c) == ' ' || (c) == '\0')

static Datum
Read_char(TupleFormer *former, char *in, const Field* field, int idx, bool *isnull)
{
	Datum		value;

	if (in[field->nulllen] == '\0' &&
		strncmp(in, field->nullif, field->nulllen) == 0)
	{
		*isnull = true;
		value = 0;
	}
	else
	{
		int	len = strlen(in);

		/* Trim trailing spaces */
		for (; len > 0 && IsWhiteSpace(in[len - 1]); len--);

		memcpy(field->str, in, len);
		field->str[len] = '\0';

		*isnull = false;
		value = TupleFormerValue(former, field->str, idx);
	}

	return value;
}

static Datum
Read_varchar(TupleFormer *former, char *in, const Field* field, int idx, bool *isnull)
{
	Datum		value;

	if (in[field->nulllen] == '\0' &&
		strncmp(in, field->nullif, field->nulllen) == 0)
	{
		*isnull = true;
		value = 0;
	}
	else
	{
		*isnull = false;
		value = TupleFormerValue(former, in, idx);
	}

	return value;
}

static void
Write_char(char *out, size_t len, Datum value, bool null)
{
	size_t	size = 0;
	char   *str = DatumGetCString(value);

	size = strlen(str);
	if (size > len)
		ereport(ERROR,
				(errcode(ERRCODE_STRING_DATA_RIGHT_TRUNCATION),
				 errmsg("value too long for type character(%d)", (int) len)));

	memcpy(out, str, size);
	memset(out + size, ' ', len - size);
}

#define DatumGet_int16	DatumGetInt16
#define DatumGet_int32	DatumGetInt32
#define DatumGet_int64	DatumGetInt64
#define DatumGet_uint16	(uint16) DatumGetInt32
#define DatumGet_uint32	(uint32) DatumGetInt64
#define DatumGet_float4	DatumGetFloat4
#define DatumGet_float8	DatumGetFloat8

#define DefineWrite(T) \
static void \
Write_##T(char *out, size_t len, Datum value, bool null) \
{ \
	if (!null) \
	{ \
		T	v = DatumGet_##T(value); \
		memcpy(out, &v, len); \
	} \
	else \
	{ \
		memcpy(out, DatumGetPointer(value), len); \
	} \
}

DefineWrite(int16)
DefineWrite(int32)
DefineWrite(int64)
DefineWrite(uint16)
DefineWrite(uint32)
DefineWrite(float4)
DefineWrite(float8)

#define int16_numeric	int2_numeric
#define int32_numeric	int4_numeric
#define int64_numeric	int8_numeric
#define uint16_numeric	int4_numeric
#define uint32_numeric	int8_numeric
/* float4_numeric exists */
/* float8_numeric exists */
#define int16_GetDatum		Int16GetDatum
#define int32_GetDatum		Int32GetDatum
#define int64_GetDatum		Int64GetDatum
#define uint16_GetDatum(v)	Int32GetDatum((int32)v)
#define uint32_GetDatum(v)	Int64GetDatum((int64)v)
#define float4_GetDatum		Float4GetDatum
#define float8_GetDatum		Float8GetDatum
#define int16_FMT		"%d"
#define int32_FMT		"%d"
#define uint16_FMT		"%u"
#define uint32_FMT		"%u"
#define float4_FMT		"%f"
#define float8_FMT		"%f"

/*
 * Use direct convertion (C-cast) for integers and floats.
 * Use cast operator for numeric.
 * Use indirect conversion through string representation in other case.
 */
#define DefineRead(T) \
static Datum \
Read_##T(TupleFormer *former, char *in, const Field* field, int idx, bool *isnull) \
{ \
	char	str[32]; \
	T		v; \
	if (field->len == field->nulllen && \
		memcmp(in, field->nullif, field->nulllen) == 0) \
	{ \
		*isnull = true; \
		return 0; \
	} \
	memcpy(&v, in, sizeof(v)); \
	*isnull = false; \
	switch (former->typId[idx]) \
	{ \
	case INT2OID: \
		return Int16GetDatum((int16)v); \
	case INT4OID: \
		return Int32GetDatum((int32)v); \
	case INT8OID: \
		return Int64GetDatum((int64)v); \
	case FLOAT4OID: \
		return Float4GetDatum((float4)v); \
	case FLOAT8OID: \
		return Float8GetDatum((float8)v); \
	case NUMERICOID: \
		return DirectFunctionCall1(T##_numeric, T##_GetDatum(v)); \
	default: \
		snprintf(str, lengthof(str), T##_FMT, v); \
		return TupleFormerValue(former, str, idx); \
	} \
}

DefineRead(int16)
DefineRead(int32)
DefineRead(int64)
DefineRead(uint16)
DefineRead(uint32)
DefineRead(float4)
DefineRead(float8)
