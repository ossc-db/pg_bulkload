/*
 * pg_bulkload: lib/parser_binary.c
 *
 *	  Copyright (c) 2007-2024, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

/**
 * @file
 * @brief Implementation of fixed file processing module
 */
#include "pg_bulkload.h"

#include "access/heapam.h"
#include "access/htup.h"
#include "executor/executor.h"
#include "mb/pg_wchar.h"
#include "nodes/execnodes.h"
#include "utils/rel.h"

#include "binary.h"
#include "logger.h"
#include "reader.h"
#include "pg_strutil.h"
#include "pg_profile.h"

/**
 * @brief  The number of records read at one time
 */
#define READ_LINE_NUM		100

#define MAX_CONVERSION_GROWTH  4

typedef struct BinaryParser
{
	Parser	base;

	Source		   *source;
	Filter			filter;
	TupleFormer		former;

	int64	offset;				/**< lines to skip */
	int64	need_offset;		/**< lines to skip */

	size_t	rec_len;			/**< One record length */
	char   *buffer;				/**< Record buffer to keep input data */
	int		total_rec_cnt;		/**< # of records in buffer */
	int		used_rec_cnt;		/**< # of returned records in buffer */
	char	next_head;			/**< Preserved the head of next record */

	bool	preserve_blanks;	/**< preserve trailing spaces? */
	int		nfield;				/**< number of fields */
	Field  *fields;				/**< array of field descriptor */
} BinaryParser;

/*
 * Prototype declaration for local functions
 */
static void	BinaryParserInit(BinaryParser *self, Checker *checker, const char *infile, TupleDesc desc, bool multi_process, Oid collation);
static HeapTuple BinaryParserRead(BinaryParser *self, Checker *checker);
static int64	BinaryParserTerm(BinaryParser *self);
static bool BinaryParserParam(BinaryParser *self, const char *keyword, char *value);
static void BinaryParserDumpParams(BinaryParser *self);
static void BinaryParserDumpRecord(BinaryParser *self, FILE *fp, char *badfile);

static void ExtractValuesFromFixed(BinaryParser *self, char *record);

/**
 * @brief Create a new Binary parser.
 */
Parser *
CreateBinaryParser(void)
{
	BinaryParser *self = palloc0(sizeof(BinaryParser));
	self->base.init = (ParserInitProc) BinaryParserInit;
	self->base.read = (ParserReadProc) BinaryParserRead;
	self->base.term = (ParserTermProc) BinaryParserTerm;
	self->base.param = (ParserParamProc) BinaryParserParam;
	self->base.dumpParams = (ParserDumpParamsProc) BinaryParserDumpParams;
	self->base.dumpRecord = (ParserDumpRecordProc) BinaryParserDumpRecord;
	self->offset = -1;
	return (Parser *)self;
}

/**
 * @brief Initialize a module for reading fixed file
 *
 * Process flow
 *	 -# Acquire a list of valid column numbers
 *	 -# Compute a record length
 *	 -# Allocate ((record length) * READ_LINE_NUM + 1) bytes for record buffer
 * @param rd [in] Control information
 * @return void
 * @note Return to caller by ereport() if the number of fields in the table
 * definition is not correspond to the number of fields in input file.
 * @note Caller must release the resource by calling BinaryParserTerm() after calling this
 * function.
 */
static void
BinaryParserInit(BinaryParser *self, Checker *checker, const char *infile, TupleDesc desc, bool multi_process, Oid collation)
{
	int					i;
	size_t				maxlen;
	TupleCheckStatus	status;

	/*
	 * set default values
	 */
	self->need_offset = self->offset = self->offset > 0 ? self->offset : 0;

	/*
	 * checking necessary setting items for fixed length file
	 */
	if (self->nfield == 0)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("no COL specified")));

	self->source = CreateSource(infile, desc, multi_process);

	status = FilterInit(&self->filter, desc, collation);
	if (checker->tchecker)
		checker->tchecker->status = status;

	TupleFormerInit(&self->former, &self->filter, desc);

	/*
	 * Error if the number of input data fields is out of range to the number of
	 * fields
	 */
	if (self->former.minfields > self->nfield ||
		self->former.maxfields < self->nfield)
		ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION),
						errmsg("invalid field count (%d)", self->nfield)));

	/* set function default value */
	for (i = self->nfield; i < self->former.maxfields; i++)
	{
		int		index;

		index = i - self->former.minfields;
		self->former.isnull[i] = self->filter.defaultIsnull[index];
		self->former.values[i] = self->filter.defaultValues[index];
	}

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
			errmsg("STRIDE should be %ld or greater (%ld given)",
				(long) maxlen, (long) self->rec_len)));
	self->buffer = palloc(self->rec_len * READ_LINE_NUM + 1);
}

/**
 * @brief Free the resources used in the reading fixed file module.
 *
 * Process flow
 * -# Release self->buffer
 *
 * @param void
 * @return void
 */
static int64
BinaryParserTerm(BinaryParser *self)
{
	int64	skip;

	skip = self->offset;

	if (self->source)
		SourceClose(self->source);
	if (self->buffer)
		pfree(self->buffer);
	if (self->fields)
		pfree(self->fields);
	FilterTerm(&self->filter);
	TupleFormerTerm(&self->former);
	pfree(self);

	return skip;
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
 * @param rd [in/out] Control information
 * @return	Return true if there is a next record, or false if EOF.
 */
static HeapTuple
BinaryParserRead(BinaryParser *self, Checker *checker)
{
	HeapTuple	tuple;
	char	   *record;
	int			i;

	/* Skip first offset lines in the input file */
	if (unlikely(self->need_offset > 0))
	{
		int		n;

		for (n = 0; n < self->need_offset; n++)
		{
			int		len;
			len = SourceRead(self->source, self->buffer, self->rec_len);

			if (len != self->rec_len)
			{
				if (errno == 0)
					errno = EINVAL;
				ereport(ERROR, (errcode_for_file_access(),
								errmsg("could not skip " int64_FMT " lines ("
								int64_FMT " bytes) in the input file: %m",
								self->need_offset,
								self->rec_len * self->need_offset)));
			}
		}
		self->need_offset = 0;
	}

	/*
	 * If the record buffer is exhausted, read next records from file
	 * up to READ_LINE_NUM rows at once.
	 */
	if (self->used_rec_cnt >= self->total_rec_cnt)
	{
		int		len;
		div_t	v;

		BULKLOAD_PROFILE(&prof_reader_parser);
		while ((len = SourceRead(self->source, self->buffer,
						self->rec_len * READ_LINE_NUM)) < 0)
		{
			if (errno != EAGAIN && errno != EINTR)
				ereport(ERROR, (errcode_for_file_access(),
								errmsg("could not read input file: %m")));
		}
		BULKLOAD_PROFILE(&prof_reader_source);

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
			return NULL;	/* eof */

		record = self->buffer;
	}
	else
	{
		record = self->buffer + (self->rec_len * self->used_rec_cnt);
	}

	/*
	 * Increment the position *before* parsing the record so that we can
	 * skip it when there are some errors on parsing it.
	 */
	self->used_rec_cnt++;
	self->base.count++;

	for (i = 0; i < self->nfield; i++)
	{
		/* Convert it to server encoding. */
		if (self->fields[i].character)
		{
			char   *str = record + self->fields[i].offset;
			int		next_head = self->fields[i].offset + self->fields[i].len;

			self->next_head = record[next_head];
			record[next_head] = '\0';
			self->base.parsing_field = i + 1;

			self->fields[i].in = CheckerConversion(checker, str);

			record[next_head] = self->next_head;
		}
		else
		{
			self->fields[i].in = record + self->fields[i].offset;
		}
	}

	ExtractValuesFromFixed(self, record);
	self->next_head = '\0';
	self->base.parsing_field = -1;

	if (self->filter.funcstr)
		tuple = FilterTuple(&self->filter, &self->former,
							&self->base.parsing_field);
	else
		tuple = TupleFormerTuple(&self->former);

	return tuple;
}

static bool
BinaryParserParam(BinaryParser *self, const char *keyword, char *value)
{
	if (CompareKeyword(keyword, "COL"))
	{
		BinaryParam(&self->fields, &self->nfield, value, self->preserve_blanks, false);

		if (self->fields[self->nfield - 1].character)
			self->fields[self->nfield - 1].str =
				palloc(self->fields[self->nfield - 1].len * MAX_CONVERSION_GROWTH + 1);
	}
	else if (CompareKeyword(keyword, "PRESERVE_BLANKS"))
	{
		self->preserve_blanks = ParseBoolean(value);
	}
	else if (CompareKeyword(keyword, "STRIDE"))
	{
		ASSERT_ONCE(self->rec_len == 0);
		self->rec_len = ParseInt32(value, 1);
	}
	else if (CompareKeyword(keyword, "SKIP") ||
			 CompareKeyword(keyword, "OFFSET"))
	{
		ASSERT_ONCE(self->offset < 0);
		self->offset = ParseInt64(value, 0);
	}
	else if (CompareKeyword(keyword, "FILTER"))
	{
		ASSERT_ONCE(!self->filter.funcstr);
		self->filter.funcstr = pstrdup(value);
	}
	else
		return false;	/* unknown parameter */

	return true;
}

static void
BinaryParserDumpParams(BinaryParser *self)
{
	StringInfoData	buf;

	initStringInfo(&buf);
	appendStringInfoString(&buf, "TYPE = BINARY\n");
	appendStringInfo(&buf, "SKIP = " int64_FMT "\n", self->offset);
	appendStringInfo(&buf, "STRIDE = %ld\n", (long) self->rec_len);
	if (self->filter.funcstr)
		appendStringInfo(&buf, "FILTER = %s\n", self->filter.funcstr);

	BinaryDumpParams(self->fields, self->nfield, &buf, "COL");

	LoggerLog(INFO, buf.data, 0);
	pfree(buf.data);
}

static void
BinaryParserDumpRecord(BinaryParser *self, FILE *fp, char *badfile)
{
	int		len;
	char   *record = self->buffer + (self->rec_len * (self->used_rec_cnt - 1));

	if (self->base.parsing_field > 0 && self->next_head != '\0')
	{
		int	next_head;

		next_head = self->fields[self->base.parsing_field - 1].offset +
					self->fields[self->base.parsing_field - 1].len;

		record[next_head] = self->next_head;
	}

	len = fwrite(record, 1, self->rec_len, fp);
	if (len < self->rec_len || fflush(fp))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write parse badfile \"%s\": %m",
						badfile)));
}

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
 * @param rd [in/out] Controll information
 * @param record [in/out] One record data (which must be NUL terminated)
 * @return void
 * @note Memory allocated in this function is not able to be freed. So if you
 *		 call this function, you have to be in a memory context which is able
 *		 to be reseted or destroyed.
 * @note When exit, the context record is pointing may be modified.
 * @note If error occurs, return to the caller by ereport().
 */
static void
ExtractValuesFromFixed(BinaryParser *self, char *record)
{
	int			i;

	/*
	 * Loop for fields in the input file
	 */
	for (i = 0; i < self->nfield; i++)
	{
		int			j = self->former.attnum[i];	/* Index of physical fields */
		bool		isnull;
		Datum		value;
		int			next_head = self->fields[i].offset + self->fields[i].len;

		self->next_head = record[next_head];
		record[next_head] = '\0';
		self->base.parsing_field = i + 1;	/* 1 origin */

		value = self->fields[i].read(&self->former,
			self->fields[i].in, &self->fields[i], j, &isnull);

		record[next_head] = self->next_head;

		self->former.isnull[j] = isnull;
		self->former.values[j] = value;
	}
}
