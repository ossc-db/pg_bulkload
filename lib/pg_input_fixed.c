/*
 * pg_bulkload: lib/pg_input_fixed.c
 *
 *	  Copyright(C) 2007-2008 NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

/**
 * @file
 * @brief Implementation of fixed file processing module
 */
#include "postgres.h"

#include <unistd.h>

#include "access/htup.h"
#include "catalog/pg_type.h"
#include "utils/builtins.h"

#include "pg_controlinfo.h"
#include "pg_strutil.h"

typedef struct Field	Field;
typedef Datum (*Reader)(ControlInfo *ci, char *in, const Field* field, int i, bool *isnull);

static Datum Read_char(ControlInfo *ci, char *in, const Field* field, int i, bool *isnull);
static Datum Read_varchar(ControlInfo *ci, char *in, const Field* field, int i, bool *isnull);
static Datum Read_int16(ControlInfo *ci, char *in, const Field* field, int i, bool *isnull);
static Datum Read_int32(ControlInfo *ci, char *in, const Field* field, int i, bool *isnull);
static Datum Read_int64(ControlInfo *ci, char *in, const Field* field, int i, bool *isnull);
static Datum Read_uint16(ControlInfo *ci, char *in, const Field* field, int i, bool *isnull);
static Datum Read_uint32(ControlInfo *ci, char *in, const Field* field, int i, bool *isnull);
static Datum Read_float4(ControlInfo *ci, char *in, const Field* field, int i, bool *isnull);
static Datum Read_float8(ControlInfo *ci, char *in, const Field* field, int i, bool *isnull);

/**
 * @brief  The number of records read at one time
 */
#define READ_LINE_NUM		100

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
	Reader		read;
	int			len;
}
TYPES[] =
{
	{ "CHAR"				, Read_char		, 0				},
	{ "VARCHAR"				, Read_varchar	, 0				},
	{ "SMALLINT"			, Read_int16	, sizeof(int16)	},
	{ "INTEGER"				, Read_int32	, sizeof(int32)	},
	{ "BIGINT"				, Read_int64	, sizeof(int64)	},
	{ "UNSIGNED SMALLINT"	, Read_uint16	, sizeof(uint16)},
	{ "UNSIGNED INTEGER"	, Read_uint32	, sizeof(uint32)},
	{ "FLOAT"				, Read_float4	, sizeof(float4)},
	{ "DOUBLE"				, Read_float8	, sizeof(float8)},
};

/*
 * TODO: Recognize blanks flexibly like "CHARACTER[\b]+VARYING" and
 * "UNSIGNED[\b]+TYPE".
 */
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

struct Field
{
	Reader	read;		/**< reader of the field */
	int		offset;		/**< offset from head */
	int		len;		/**< byte length of the field */
	char   *nullif;		/**< null pattern, if any */
	int		nulllen;	/**< length of nullif */
};

typedef struct FixedParser
{
	Parser	base;

	int		rec_len;			/**< One record length */
	char   *record_buf;			/**< Record buffer to keep input data */
	int		total_rec_cnt;		/**< # of records in record_buf */
	int		used_rec_cnt;		/**< # of returned records in record_buf */
	char	next_head;			/**< Preserved the head of next record */

	bool	preserve_blanks;	/**< preserve trailing spaces? */
	int		nfield;				/**< number of fields */
	Field  *fields;				/**< array of field descriptor */
} FixedParser;

/*
 * Prototype declaration for local functions
 */
static void	InitializeFixed(FixedParser *self, ControlInfo *ci);
static bool	ReadLineFromFixed(FixedParser *self, ControlInfo *ci);
static void	CleanUpFixed(FixedParser *self);
static bool ParamFixed(FixedParser *self, const char *keyword, char *value);

static void ExtractValuesFromFixed(FixedParser *self, ControlInfo *ci, char *record);

/**
 * @brief Create a new CSV parser.
 */
Parser *
CreateFixedParser(void)
{
	FixedParser* self = palloc0(sizeof(FixedParser));
	self->base.initialize = (ParserInitProc) InitializeFixed;
	self->base.read_line = (ParserReadProc) ReadLineFromFixed;
	self->base.cleanup = (ParserTermProc) CleanUpFixed;
	self->base.param = (ParserParamProc) ParamFixed;

	return (Parser *)self;
}

/**
 * @brief Initialize a module for reading fixed file
 *
 * Process flow
 *	 -# Acquire a list of valid column numbers
 *	 -# Compute a record length
 *	 -# Allocate ((record length) * READ_LINE_NUM + 1) bytes for record buffer
 * @param ci [in] Control information
 * @return void
 * @note Return to caller by ereport() if the number of fields in the table
 * definition is not correspond to the number of fields in input file.
 * @note Caller must release the resource by calling CleanUpFixed() after calling this
 * function.
 */
static void
InitializeFixed(FixedParser *self, ControlInfo *ci)
{
	int		i;
	int		maxlen;

	/*
	 * checking necessary setting items for fixed length file
	 */
	if (self->nfield == 0)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("no COL specified")));

	/*
	 * Error if the number of valid fields is not equal to the number of
	 * fields
	 */
	if (ci->ci_attnumcnt != self->nfield)
		ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION),
						errmsg("invalid field count (%d)", self->nfield)));

	/*
	 * Acquire record buffer as much as input file record length
	 */
	maxlen = 0;
	for (i = 0; i < self->nfield; i++)
	{
		int		len = self->fields[i].offset + self->fields[i].len;
		maxlen = Max(maxlen, len);
	}
	if (self->rec_len <= 0)
		self->rec_len = maxlen;
	else if (self->rec_len < maxlen)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			errmsg("STRIDE should be %d or greater (%d given)", maxlen, self->rec_len)));
	self->record_buf = palloc(self->rec_len * READ_LINE_NUM + 1);

	/* Skip first ci_offset lines in the input file */
	if (ci->ci_offset > 0)
	{
		long	skipbytes;

		skipbytes = ci->ci_offset * self->rec_len;

		if (skipbytes > lseek(ci->ci_infd, 0, SEEK_END) ||
			skipbytes != lseek(ci->ci_infd, skipbytes, SEEK_SET))
		{
			if (errno == 0)
				errno = EINVAL;
			ereport(ERROR, (errcode_for_file_access(),
							errmsg("could not skip %d lines (%ld bytes) in the input file: %m",
							ci->ci_offset, skipbytes)));
		}
	}
}

/**
 * @brief Free the resources used in the reading fixed file module.
 *
 * Process flow
 * -# Release self->record_buf
 *
 * @param void
 * @return void
 */
static void
CleanUpFixed(FixedParser *self)
{
	if (self->record_buf)
		pfree(self->record_buf);
	if (self->fields)
		pfree(self->fields);
	pfree(self);
}

/**
 * @brief Read one record from input file and transfer literal string to
 * PostgreSQL internal format.
 *
 * Process flow
 *	 - If record buffer is empty
 *	   + Read records up to READ_LINE_NUM by read(2)
 *		 * Return 0 if we reach EOF.
 *		 * If an error occurs, notify it to caller by ereport().
 *	   + Count the number of records in the record buffer.
 *	   + Initialize the number of used records to 0.
 *	   + Store the head byte of the next record.
 *	 - If the number of records remained in the record buffer and there is not
 *	   enough room, notify it to the caller by ereport().
 *	 - Get back the stored head byte, and store the head byte of the next record.
 *	 - Update the number of records used.
 * @param ci [in/out] Control information
 * @return	Return true if there is a next record, or false if EOF.
 */
static bool
ReadLineFromFixed(FixedParser *self, ControlInfo *ci)
{
	char	   *record;

	/*
	 * If the record buffer is exhausted, read next records from file
	 * up to READ_LINE_NUM rows at once.
	 */
	if (self->used_rec_cnt >= self->total_rec_cnt)
	{
		int		len;
		div_t	v;

		while ((len = read(ci->ci_infd,
			self->record_buf, self->rec_len * READ_LINE_NUM)) < 0)
		{
			if (errno != EAGAIN && errno != EINTR)
				ereport(ERROR, (errcode_for_file_access(),
								errmsg("could not read input file: %m")));
		}

		/*
		 * Calculate the actual number of rows. Trailing remainder bytes
		 * at the end of the input file are ingored with WARNING.
		 */
		v = div(len, self->rec_len);
		if (v.rem != 0)
			elog(WARNING, "Ignore %d bytes at the end of file", v.rem);

		self->total_rec_cnt = v.quot;
		self->used_rec_cnt = 0;

		if (self->total_rec_cnt <= 0)
			return false;	/* eof */

		record = self->record_buf;
	}
	else
	{
		record = self->record_buf + (self->rec_len * self->used_rec_cnt);
		record[0] = self->next_head;	/* restore the head */
	}

	/*
	 * Increment the position *before* parsing the record so that we can
	 * skip it when there are some errors on parsing it.
	 */
	self->next_head = record[self->rec_len];
	self->used_rec_cnt++;
	ci->ci_read_cnt++;

	ExtractValuesFromFixed(self, ci, record);

	/*
	 * We should not do any operation here because ExtractValuesFromFixed
	 * could throw exceptions.
	 */
	return true;
}

/*
 * Parse typename and return the type id.
 */
static TypeId
ParseTypeName(const char *value, int n)
{
	int		i;

	/* Search from types. */
	for (i = 0; i < lengthof(TYPES); i++)
	{
		if (pg_strncasecmp(value, TYPES[i].name, n) == 0 &&
			TYPES[i].name[n] == '\0')
			return (TypeId) i;
	}

	/* Search from aliases. */
	for (i = 0; i < lengthof(ALIASES); i++)
	{
		if (pg_strncasecmp(value, ALIASES[i].name, n) == 0 &&
			ALIASES[i].name[n] == '\0')
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
static int
ParseLengthAndOffset(const char *value, Field *field)
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
			return (int) n1;
		case '+': /* TYPE(OFFSET+LEN)*/
			p1++;
			n2 = strtoul(p1, &p2, 0);
			if (p1 < p2 && CheckRightparenthesis(p2))
			{
				field->offset = (int) (n1 - 1);	/* start with 1 */
				return (int) n2;
			}
			break;
		case ':': /* TYPE(BEGIN:END) */
			p1++;
			n2 = strtoul(p1, &p2, 0);
			if (p1 < p2 && CheckRightparenthesis(p2))
			{
				field->offset = (int) (n1 - 1);	/* start with 1 */
				return (int) (n2 - n1 + 1);
			}
			break;
		}
	}

	ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
		errmsg("TYPE argument must be ( { L | B+L | B:E } )")));
	return 0;
}

static int
hex(char c)
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

/*
 * Parse field format like "TYPE(STRIDE) NULLIF { 'str' | hex }"
 */
static void
ParseFormat(const char *value, Field *field)
{
	TypeId		id;
	int			len;
	const char *nullif;
	const char *p;
	const struct TypeInfo *type;

	/* parse nullif */
	nullif = strstr(value, "NULLIF");
	if (nullif)
	{
		/* Seek to the argument of NULLIF */
		p = nullif + lengthof("NULLIF") - 1;
		while (*p && isspace((unsigned char) *p))
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
		}
		else
		{
			/* hex format */
			int		i;
			size_t	n = (1 + strlen(p)) / 2;
			field->nulllen = n;
			field->nullif = palloc(n + 1);
			field->nullif[n] = '\0';
			for (i = 0; i < n; i++)
			{
				field->nullif[i] = (char)
					((hex(p[i*2]) << 4) + hex(p[i*2+1]));
			}
		}
	}

	/* parse typename and length */
	p = strchr(value, '(');
	if (p)
	{	/* typename (N | B:E) */
		len = ParseLengthAndOffset(p + 1, field);
		if (len <= 0)
			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("COL length must be positive")));
		if (field->offset < 0)
			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("COL offset must be positive")));
	}
	else if (nullif)
	{	/* typename NULLIF ... */
		p = nullif;
		len = 0;
	}
	else
	{	/* typename */
		p = value + strlen(value);
		len = 0;
	}

	/* trim trailing spaces */
	while (p > value && isspace((unsigned char) p[-1]))
		p--;

	/* resolve the type */
	id = ParseTypeName(value, p - value);
	type = &TYPES[id];

	if (len == 0)
	{
		len = type->len;	/* use default */
	}
	else switch (id)
	{
	case T_INT2:
	case T_INT8:
	case T_UINT2:
	case T_FLOAT8:
		if (len != type->len)
			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("length of %s should be %d or default", type->name, type->len)));
		break;
	case T_INT4:
		switch (len)
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
		switch (len)
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
		switch (len)
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

	if (len <= 0)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			errmsg("length of %s should be specified", type->name)));

	field->read = type->read;
	field->len = len;

	if ((type->len == 0 && field->len < field->nulllen) ||
        (type->len > 0 && field->nulllen > 0 && field->len != field->nulllen))
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			errmsg("length of NULLIF argument should be %d bytes %s(%d bytes given) : %s",
				field->len, (type->len == 0 ? "or less " : ""),
				field->nulllen, value)));
	}
}

static bool
ParamFixed(FixedParser *self, const char *keyword, char *value)
{
	if (strcmp(keyword, "COL") == 0)
	{
		Field  *field;

		if (self->fields)
			self->fields = repalloc(self->fields, sizeof(Field) * (self->nfield + 1));
		else
			self->fields = palloc(sizeof(Field) * (self->nfield + 1));
		field = &self->fields[self->nfield];

		if (self->nfield > 0)
			field->offset = field[-1].offset + field[-1].len;
		else
			field->offset = 0;
		field->nullif = "";
		field->nulllen = 0;

		if (isdigit((unsigned char) value[0]))
		{
			/* default is CHAR or VARCHAR (compatible with 2.2.x) */
			field->read = (self->preserve_blanks ? Read_varchar : Read_char);
			field->len = ParseInteger(value, 1);
		}
		else
		{
			/* Overwrite field parameters */
			ParseFormat(value, field);
		}

		self->nfield++;
	}
	else if (strcmp(keyword, "PRESERVE_BLANKS") == 0)
	{
		self->preserve_blanks = ParseBoolean(value, true);
	}
	else if (strcmp(keyword, "STRIDE") == 0)
	{
		ASSERT_ONCE(self->rec_len == 0);
		self->rec_len = ParseInteger(value, 1);
	}
	else
		return false;	/* unknown parameter */

	return true;
}

/* Read null-terminated string and convert to internal format */
static Datum
ReadCString(ControlInfo *ci, const char *in, int idx)
{
	Form_pg_attribute *attrs = RelationGetDescr(ci->ci_rel)->attrs;

	return FunctionCall3(&ci->ci_in_functions[idx],
        CStringGetDatum(in),
        ObjectIdGetDatum(ci->ci_typeioparams[idx]),
        Int32GetDatum(attrs[idx]->atttypmod));
}

#define IsWhiteSpace(c)	((c) == ' ' || (c) == '\0')

static Datum
Read_char(ControlInfo *ci, char *in, const Field* field, int idx, bool *isnull)
{
	Datum		value;
	const int	len = field->len;
	char		head = in[len];	/* save the next char to restore '\0' */

	in[len] = '\0';
	if (in[field->nulllen] == '\0' &&
		strncmp(in, field->nullif, field->nulllen) == 0)
	{
		*isnull = true;
		value = 0;
	}
	else
	{
		int			k;

		/* Trim trailing spaces */
		for (k = len - 1; k >= 0 && IsWhiteSpace(in[k]); k--);
		in[k + 1] = '\0';

		*isnull = false;
		value = ReadCString(ci, in, idx);
	}

	in[len] = head;	/* restore '\0' */
	return value;
}

static Datum
Read_varchar(ControlInfo *ci, char *in, const Field* field, int idx, bool *isnull)
{
	Datum		value;
	const int	len = field->len;
	char		head = in[len];	/* save the next char to restore '\0' */

	in[len] = '\0';
	if (in[field->nulllen] == '\0' &&
		strncmp(in, field->nullif, field->nulllen) == 0)
	{
		*isnull = true;
		value = 0;
	}
	else
	{
		*isnull = false;
		value = ReadCString(ci, in, idx);
	}

	in[len] = head;	/* restore '\0' */
	return value;
}

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
#ifdef HAVE_LONG_INT_64
#define int64_FMT		"%ld"
#else
#define int64_FMT		"%lld"
#endif
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
Read_##T(ControlInfo *ci, char *in, const Field* field, int idx, bool *isnull) \
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
	switch (RelationGetDescr(ci->ci_rel)->attrs[idx]->atttypid) \
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
		return ReadCString(ci, str, idx); \
	} \
}

DefineRead(int16)
DefineRead(int32)
DefineRead(int64)
DefineRead(uint16)
DefineRead(uint32)
DefineRead(float4)
DefineRead(float8)

/**
 * @brief Extract internal format for each column from string data in a record
 *
 * Process flow
 * -# Loop from the head of fields and process as follow
 *	 -# Reserve the head byte of the next field
 *	 -# Make sure whether it is NUL character
 *	   - If it is NUL character:
 *		 -# Return to the caller by ereport() if it is violate to NOT NULL
 *			constraint
 *	   - If not
 *		 -# Terminate the input string by rewrite the head byte of the next
 *			field or the first white space character following string.
 *		 -# Transfer each field value to internal format by FunctionCall3()
 *	 -# Retrurn the stored value to next field
 *
 * @param ci [in/out] Controll information
 * @param record [in/out] One record data (which must be NUL terminated)
 * @return void
 * @note Memory allocated in this function is not able to be freed. So if you
 *		 call this function, you have to be in a memory context which is able
 *		 to be reseted or destroyed.
 * @note When exit, the context record is pointing may be modified.
 * @note If error occurs, return to the caller by ereport().
 */
static void
ExtractValuesFromFixed(FixedParser *self, ControlInfo *ci, char *record)
{
	int			i;
	Form_pg_attribute *attrs = RelationGetDescr(ci->ci_rel)->attrs;

	/*
	 * Loop for fields in the input file
	 */
	ci->ci_field = 1;	/* ci_field is 1 origin */
	for (i = 0; i < self->nfield; i++)
	{
		int			j = ci->ci_attnumlist[i];	/* Index of physical fields */
		bool		isnull;
		Datum		value;

		value = self->fields[i].read(ci,
			record + self->fields[i].offset, &self->fields[i], j, &isnull);

		/* Check NOT NULL constraint. */
		if (isnull && attrs[j]->attnotnull)
			ereport(ERROR,
				(errcode(ERRCODE_NOT_NULL_VIOLATION),
				 errmsg("null value in column \"%s\" violates not-null constraint",
					  NameStr(attrs[j]->attname))));

		ci->ci_isnull[j] = isnull;
		ci->ci_values[j] = value;
		ci->ci_field++;
	}
}
