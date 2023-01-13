/*
 * pg_bulkload: lib/parser_text.c
 *
 *	  Copyright (c) 2007-2021, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

/**
 * @file
 * @brief Text format file handling module implementation.
 * author: xiepeng
 */
#include "pg_bulkload.h"

#include "access/heapam.h"
#include "access/htup.h"
#include "executor/executor.h"
#include "mb/pg_wchar.h"
#include "utils/rel.h"

#include "logger.h"
#include "reader.h"
#include "pg_strutil.h"
#include "pg_profile.h"

/**
 * @brief Initial size of the record and field buffers.
 *
 * If needed more, then the size will be doubled when needed, with
 * MAX_BUF_LEN as the upper limit.
 */
#define INITIAL_BUF_LEN		(1024 * 1024)
#define ALLOC_BUF_LEN		(16*1024)
#define MAX_BUF_LEN		(1024*1024*1024-1)
#define DELIM_MAX_LEN 3
#define DELIM_INVALID_CHAR "\\.abcdefghijklmnopqrstuvwxyz0123456789"

typedef struct TextParser
{
	Parser	base;

	Source		   *source;
	Filter			filter;
	TupleFormer		former;

	int64	offset;				/**< lines to skip */
	int64	need_offset;		/**< lines to skip */

	/**
	 * @brief Record Buffer.
	 *
	 * This buffer stores the data read from the input file.
	 */
	char *rec_buf;
	
	/**
	 * @brief Field Buffer.
	 *
	 * This buffer stores character string representation of each field value,
	 * taken from the record buffer.   Quote marks and escapes have already
	 * developed.  Each field entry can be found in the link from "field".
	 */
	char *field_buf;
	
	/**
	 * @brief Contains the pointer to the character string for each field.
	 */
	char **fields;
	
	/**
	 * @brief Size of the record buffer and the field buffer.
	 * If needed more, then the size will be doubled when needed.
	 * Field buffer size does not exceed a single record size.
	 * Therefore these two has the same size. These two buffers are
	 * expanded at the same time and therefore the size is contained
	 * in this single variable.
	 */
	int	buf_len;
	
	/**
	 * @brief Actual size in the record buffer, excluding the trailing NULL.
	 */
	int	used_len;
	
	/**
	 * @brief Pointer to the current record in the record buffer.
	 */
	char *cur;
	
	/**
	 * @brief Pointer to the next record in the record buffer.
	 */
	char *next;
	
	/**
	 * @brief Flag indicating EOF has been encountered in the input file.
	 */
	bool eof;
	
	/**
	 * @brief String length for NULL string.
	 *
	 * strlen() This is actually cache variable to reduce function calls.
	 */
	int	null_len;

	char	   *delim;			/**< delimeter */
	char	   *null;			/**< NULL value string */
	List	   *fnn_name;		/**< list of NOT NULL column names */
	bool	   *fnn;			/**< array of NOT NULL column flag */
} TextParser;

static void	TextParserInit(TextParser *self, Checker *checker, const char *infile, TupleDesc desc, bool multi_process, Oid collation);
static HeapTuple	TextParserRead(TextParser *self, Checker *checker);
static int64	TextParserTerm(TextParser *self);
static bool TextParserParam(TextParser *self, const char *keyword, char *value);
static void TextParserDumpParams(TextParser *self);
static void TextParserDumpRecord(TextParser *self, FILE *fp, char *badfile);
static void	ExtractValuesFromText(TextParser *self, int parsed_field);

/*
 * check_delimiter
 */
static bool check_delimiter(const char* delim) {
	if (!delim) {
		return false;
	}

	while (*delim != '\0') {
		if (strchr(DELIM_INVALID_CHAR, *delim) != NULL) 
			return false;
		delim++;
	}

	return true;
}

/*
 * @brief Copies specified area in the record buffer to the field buffer.
 *
 * This function must be called only when one of the following (non-loadable)
 * characters is found.
 * -# Open/Close quote character.
 * -# Valid escape character surrounded by quotation marks,
 * -# Delimiter character,
 * -# Record delimiter (new line, EOF).
 *
 * Flow
 * -# If non-zero lenght is specified, copies data and shift source/destination pointer.
 * -# Increment the source pointer to skip characters not to copy.
 *
 * @param dst [in/out] Copy destination address (field buffer index)
 * @param src [in/out] Copy destination (record buffer index)
 * @param len [in] Number of byte to copy
 * @return None
 */
static void
appendToField(TextParser *self, int *dst, int *src, int len)
{
	if (len)
	{
		memcpy(self->field_buf + *dst, self->rec_buf + *src, len);
		*dst += len;
		*src += len;
		self->field_buf[*dst] = '\0';
	}
	/*
	 * Shift the source address for non-loading character.
	 */
	(*src)++;
}

/*
 * @brief Copies specified area in the record buffer to the field buffer.
 *
 * This function must be called only when one of the following (non-loadable)
 * characters is found.
 * -# Open/Close quote character.
 * -# Valid escape character surrounded by quotation marks,
 * -# Delimiter character,
 * -# Record delimiter (new line, EOF).
 *
 * Flow
 * -# If non-zero lenght is specified, copies data and shift source/destination pointer.
 * -# Increment the source pointer to skip characters not to copy.
 *
 * @param dst [in/out] Copy destination address (field buffer index)
 * @param src [in/out] Copy destination (record buffer index)
 * @param len [in] Number of byte to copy
 * @param step [in] Step length
 * @return None
 */
static void
appendToFieldByDelim(TextParser *self, int *dst, int *src, int len)
{
	if (len)
	{
		memcpy(self->field_buf + *dst, self->rec_buf + *src, len);
		*dst += len;
		*src += len;
		self->field_buf[*dst] = '\0';
	}
	/*
	 * Shift the source address for non-loading character.
	 */
	(*src) += strlen(self->delim);
}

/*
 * repallocRecBuf
 * Expand buffer size
 */
static void repallocRecBuf(TextParser *self, int field_num, int buf_size)
{
	int j;
	char *old_buf = self->field_buf;

	if (buf_size > 0)
		self->buf_len += buf_size;
	else
	{
		self->buf_len = Min(self->buf_len+ALLOC_BUF_LEN, MAX_BUF_LEN);
	}

	if (self->buf_len >= MAX_BUF_LEN) {
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg
				 ("cannot alloc buf, buf_size=%d", self->buf_len)));
		return;
	}
	
	self->field_buf = repalloc(self->field_buf, self->buf_len);
	/*
	 * After repalloc(), address of each field needs to be adjusted.
	 */
	for (j = 0; j <= field_num; j++)
	{
		if (self->fields[j])
			self->fields[j] += self->field_buf - old_buf;
	}

	self->rec_buf = repalloc(self->rec_buf, self->buf_len);
	/*
	 * Expanded buffer may be different from the original one, so we reset the
	 * record beginning.
	 */
	self->cur = self->rec_buf;
}

/*
 * readMoreData
 * Read more data
 */
static bool readMoreData(TextParser *self, int curr, int read_len)
{
	int ret = -1;
	int _read_len = read_len>0 ? read_len : (self->buf_len-self->used_len-1);

	BULKLOAD_PROFILE(&prof_reader_parser);
	ret = SourceRead(self->source, self->rec_buf + self->used_len, _read_len);
	BULKLOAD_PROFILE(&prof_reader_source);
	if (ret == 0)
	{
		self->eof = true;
		/*
		 * When no data is found in the record buffer and we encounter EOF,
		 * there're no  more input to handle and return false.
		 */
		if (self->cur[0] == '\0')
			return false;

		/*
		 * To simplify the following parsing, when the last character of the input
		 * file is not new line code, we add this.
		 */
		if (self->rec_buf[curr] == '\0')
		{
			ret++;
			self->rec_buf[curr] = '\n';
		}
	}
	else if (ret < 0)
	{
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not read input file %m")));
		return false;
	}

	self->used_len += ret;
	self->rec_buf[self->used_len] = '\0';
	return true;
}

/**
 * @brief Create a new Text parser.
 */
Parser *
CreateTextParser(void)
{
	TextParser *self = palloc0(sizeof(TextParser));
	self->base.init = (ParserInitProc) TextParserInit;
	self->base.read = (ParserReadProc) TextParserRead;
	self->base.term = (ParserTermProc) TextParserTerm;
	self->base.param = (ParserParamProc) TextParserParam;
	self->base.dumpParams = (ParserDumpParamsProc) TextParserDumpParams;
	self->base.dumpRecord = (ParserDumpRecordProc) TextParserDumpRecord;
	self->offset = -1;
	return (Parser *)self;
}

/**
 * @brief Initialize Text file reader module.
 *
 * Flow
 * -# Allocate character string pointer array for valid columns.
 * -# Cache the length of NULL-value character string.
 *
 * @param rd [in] Control Info.
 * @return None
 * @note Caller must release the resource using TextParserTerm().
 */
static void
TextParserInit(TextParser *self, Checker *checker, const char *infile, TupleDesc desc, bool multi_process, Oid collation)
{
	TupleCheckStatus	status;

	/*
	 * set default values
	 */
	self->delim = self->delim ? self->delim : ",";
	self->null = self->null ? self->null : "";
	self->need_offset = self->offset = self->offset > 0 ? self->offset : 0;

	/*
	 * validation check
	 */

	/* delimiter strings should be no more than 3 bytes. */
	if (strlen(self->delim) > DELIM_MAX_LEN) 
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg
				 ("DELIMITER must be less than %d bytes", DELIM_MAX_LEN)));

	/* Disallow end-of-line characters */
	if (strchr(self->delim, '\r') != NULL ||
		strchr(self->delim, '\n') != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg
				 ("DELIMITER cannot be newline or carriage return")));

	/*
	 * Disallow unsafe delimiter characters in non-CSV mode.  We can't allow
	 * backslash because it would be ambiguous.  We can't allow the other
	 * cases because data characters matching the delimiter must be
	 * backslashed, and certain backslash combinations are interpreted
	 * non-literally by COPY IN.  Disallowing all lower case ASCII letters is
	 * more than strictly necessary, but seems best for consistency and
	 * future-proofing.  Likewise we disallow all digits though only octal
	 * digits are actually dangerous.
	 */
	if (check_delimiter(self->delim) != true)
		ereport(ERROR,
			(errcode(ERRCODE_INTERNAL_ERROR),
			 errmsg
			 ("DELIMITER cannot be \"%s\"", self->delim)));

	/* Don't allow the delimiter to appear in the null string. */
	if ((strlen(self->null) >= 1 && strlen(self->delim) >= 1) &&
        (strstr(self->null, self->delim) != NULL || strstr(self->delim,self->null) != NULL))
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg
				 ("DELIMITER cannot be appear in the NULL parameter")));
	
	if (list_length(self->fnn_name) > 0 && self->filter.funcstr)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg
				 ("cannot use FILTER with FORCE_NOT_NULL")));

	self->source = CreateSource(infile, desc, multi_process);

	status = FilterInit(&self->filter, desc, collation);
	if (checker->tchecker)
		checker->tchecker->status = status;

	TupleFormerInit(&self->former, &self->filter, desc);

	/*
	 * set not NULL column information
	 */
	do
	{
		int			i;
		ListCell   *name;

		self->fnn = palloc0(sizeof(bool) * self->former.maxfields);
		foreach(name, self->fnn_name)
		{
			for (i = 0; i < desc->natts; i++)
			{
#if PG_VERSION_NUM >= 110000
				if (strcmp(lfirst(name), desc->attrs[i].attname.data) == 0)
#else
				if (strcmp(lfirst(name), desc->attrs[i]->attname.data) == 0)
#endif
				{
					self->fnn[i] = true;
					break;
				}
			}
			/*
			 * if not exists, error
			 */
			if (i == desc->natts)
				ereport(ERROR, (errcode(ERRCODE_UNDEFINED_COLUMN),
								errmsg("invalid column name [%s]",
									   (char *) lfirst(name))));
		}
	} while(0);

	self->buf_len = INITIAL_BUF_LEN;
	self->rec_buf = palloc(self->buf_len);
	self->rec_buf[0] = '\0';
	self->used_len = 0;
	self->field_buf = palloc(self->buf_len);
	self->next = self->rec_buf;
	self->fields = palloc(Max(self->former.maxfields, 1) * sizeof(char *));
	self->fields[0] = NULL;
	self->null_len = strlen(self->null);
	self->eof = false;
}

/**
 * @brief Release the resources used in Text file reader module.
 *
 * Flow
 * -# Release the following resources,
 *	 - self->fields,
 *	 - self->rec_buf,
 *	 - self->field_buf.
 *
 * @param None
 * @return None
 */
static int64
TextParserTerm(TextParser *self)
{
	int64	skip;

	skip = self->offset;

	if (self->source)
		SourceClose(self->source);
	if (self->fields)
		pfree(self->fields);
	if (self->rec_buf)
		pfree(self->rec_buf);
	if (self->field_buf)
		pfree(self->field_buf);
	FilterTerm(&self->filter);
	TupleFormerTerm(&self->former);
	pfree(self);

	return skip;
}

static bool
checkFieldIsNull(TextParser *self, int field_num, int len)
{
	/*
	 * We have to determine NULL value using character string before quote mark
	 * and escape character handling.	For this, we use the record buffer, not
	 * the field buffer (field buffer contains character string after these marks
	 * are handled).
	 */
	if (self->former.maxfields != 0 &&
		!self->fnn[self->former.attnum[field_num]] &&
		self->null_len == len &&
		0 == memcmp(self->null, self->fields[field_num], self->null_len))
	{
		self->fields[field_num] = NULL;
		return true;
	}
	else
		return false;
}

/**
 * @brief Reads one record from the input file, converts each field's
 * character string representation into PostgreSQL internal representation
 * and store it to the line data buffer.
 *
 * When this function is called, it is assumed that the context is in the
 * user-defined function context.	When returning from this function,
 * the memory context is switched to the tuple context.
 *
 * To cordinate the memory context in releasing memory, self->rec_buf and
  *self->field_buf are allocated only within this function.	Caller must
 * release these memory by releasing whole memory context.
 *
 * @param rd [in/out] Control Info.
 * @return Returns true when successful, false when EOF is found.
 * @note When an error is found, it returns to the caller through ereport().
 */
static HeapTuple
TextParserRead(TextParser *self, Checker *checker)
{
	HeapTuple	tuple;			/* return tuple */
	int			i = 0;			/* Index of the scanned character */
	char		c;				/* Cache for the scanned character */
	char	   *delim = self->delim;	/* Cache for the delimiter */
	bool		need_data = false;		/* Flag indicating the need to read more characters */
	bool		inCR = false;
	
	/*
	 * Field parsing info
	 */
	int			field_head;		/* Index to the current field */
	int			dst;			/* Index to the next destination */
	int			src;			/* Index to the next source */
	int			field_num = 0;	/* Number of self->fields already parsed */
	int			parsed_field;
	int			delim_len = strlen(delim);
	int			move_size = 0;

	/*
	 * If EOF found in the previous calls, returns zero.
	 */
	if (self->eof)
		return NULL;

	/* Skip first offset lines in the input file */
	if (unlikely(self->need_offset > 0))
	{
		int		len;
		int		skipped = 0;
		bool	inCR = false;

		while ((len = SourceRead(self->source, self->rec_buf, self->buf_len - 1)) > 0)
		{
			int		i;

			for (i = 0; i < len; i++)
			{
				if (self->rec_buf[i] == '\r')
				{
					if (i == len - 1)
					{
						inCR = true;
						continue;
					}
					else if (self->rec_buf[i + 1] == '\n')
						i++;
				}
				else if (!inCR && self->rec_buf[i] != '\n')
					continue;

				/* Skip the line */
				inCR = false;
				++skipped;
				if (skipped >= self->need_offset)
				{
					/* Seek to head of the next line. */
					self->next = self->rec_buf + i + 1;
					self->used_len = len;
					self->rec_buf[self->used_len] = '\0';
					goto skip_done;
				}
			}
		}
		ereport(ERROR, (errcode_for_file_access(),
			errmsg("could not skip " int64_FMT " lines in the input file: %m",
				self->need_offset)));
skip_done:
		/* done */
		self->need_offset = 0;
	}

	self->cur = self->next;

	/*
	 * Initialize variables related to fied data.
	 */
	src = self->cur - self->rec_buf;
	dst = 0;
	field_head = src;
	self->base.parsing_field = 1;
	self->field_buf[dst] = '\0';
	self->fields[field_num] = self->field_buf + dst;

	/*
	 * Loop for each input character to parse record buffer.
	 *
	 * Because errors are accumulated until specified numbers of erros are
	 * found, ereport() must no be used in this loop unless fatal error is found.
	 */
	for (i = self->cur - self->rec_buf;; i++)
	{
		/*
		 * If no record is found in the record buffer, read them from the input file.
		 */
		if (need_data)
		{
			/*
			 * When an escape character is found at the last of the buffer or no
			 * record delimiter is found in the record buffer, we extend the record
			 * buffer.
			 * - When the current line starts at the beginning of the record buffer,
			 *	 -> Buffer size is doubled and more data is read.
			 * - The current line is not at the begenning of the record buffer,
			 *	 -> Move the current line to the beginning of the record buffer and continue to read.
			 */
			if (self->cur != self->rec_buf)
			{
				move_size = self->cur - self->rec_buf;	/* Amount to move buffer. */

				memmove(self->rec_buf, self->cur, self->buf_len - move_size);
				self->used_len -= move_size;
				i -= move_size;
				field_head -= move_size;
				src -= move_size;
				self->cur = self->rec_buf;
			}
			else if (self->buf_len - self->used_len <= 1)
			{
				repallocRecBuf(self, field_num, -1);
			}

			/* start read more data */
			if (readMoreData(self, i, -1) == false)
				return NULL;
			
			need_data = false;
		}

		c = self->rec_buf[i];
		if (c == '\0')
		{
			/*
			 * If parsing has been done upto the last of the buffer, we read next data.
			 */
			need_data = true;
			i--;				/* 'i--' is needed to cancel the incrementation in the for() loop definition. */
		}
		else if (inCR)
		{
			appendToField(self, &dst, &src, i - src - 1);
			checkFieldIsNull(self, field_num, i - field_head - 1);
			self->rec_buf[i - 1] = '\0';

			if (c != '\n')
				i--;	/* re-read the char */
			self->next = self->rec_buf + i + 1;

			inCR = false;
			break;
		}
		else if (c == '\\')
		{
			if (self->rec_buf[i+1] != '\0' && self->rec_buf[i+1] == delim[0])
			{
				ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg
					 ("A backslash was detected to distinguish a field from a delimiter, not supported!")));
				return NULL;
			}
		}
		else
		{
			/*
			 * If a valid character is found at the begenning of the current
			 * line, the self->next record exists and increment the number of
			 * records to read.  The beginning of the record must not be a
			 * quote mark and we can test this here.
			 */
			if (i == self->cur - self->rec_buf)
				self->base.count++;
			else if (c == '\r')
			{
				inCR = true;
			}
			else if (c == '\n')
			{
				/*
				 * We determine the end of a field when a delimiter or line feed is found.
				 * Even if no line feed is found at the end of the input file, there will
				 * be no problem because we have already added line feed at EOF test above.
				 */
				appendToField(self, &dst, &src, i - src);

				checkFieldIsNull(self, field_num, i - field_head);

				/*
				 * Line feed other than a quote mark is the record delimiter.  Record parse
				 * terminates when the record delmiter is found.
				 */
				self->rec_buf[i] = '\0';
				self->next = self->rec_buf + i + 1;
				break;
			}
			else if (c == delim[0])
			{
				/*
				 * The delimiter cannot read part.
				 * You need to get the next piece of data immediately.
				 */
				if (strncmp(delim, &self->rec_buf[i], delim_len) != 0)
				{
					/* Read more data */
					if (strlen(&self->rec_buf[i]) < delim_len) 
					{
						/* Need to increase buf size */
						if (self->cur != self->rec_buf)
						{
							move_size = self->cur - self->rec_buf;	/* Amount to move buffer. */

							memmove(self->rec_buf, self->cur, self->buf_len - move_size);
							self->used_len -= move_size;
							i -= move_size;
							field_head -= move_size;
							src -= move_size;
							self->cur = self->rec_buf;
						}
						else if (self->buf_len - self->used_len <= 1)
						{
							repallocRecBuf(self, field_num, delim_len+1);
						}

						/* start read more data */
						if (readMoreData(self, i, delim_len+1) == false)
							return NULL;

						/* check delimter again */
						if (strncmp(delim, &self->rec_buf[i], delim_len) != 0)
						{
							continue;
						}
					}
					else
					{
						continue;
					}
				}

				appendToFieldByDelim(self, &dst, &src, i - src);
				checkFieldIsNull(self, field_num, i - field_head);

				/*
				 * If then number of columns specified in the input record exceeds the
				 * number of columns of the copy target table, then the value of the last
				 * column of the table will be overwritten by extra columns in the input
				 * data successively.
				 */
				if (field_num + 1 < self->former.maxfields)
					field_num++;
				self->base.parsing_field++;

				/* Update i location. */
				i += delim_len-1;
				
				/*
				 * The beginning of the next field is the next character from the delimiter.
				 */
				field_head = i + 1;
				
				/*
				 * Delmiter itself is not field data and skip this.
				 */
				dst++;

				/*
				 * Update the destination field
				 */
				self->field_buf[dst] = '\0';
				self->fields[field_num] = self->field_buf + dst;
			}
		}
	}

	/*
	 * We accept a record only for new lines as input of the functions without
	 * the arguments.
	 */
	if (self->former.maxfields == 0 && strlen(self->cur) == 0)
		self->base.parsing_field = 0;

	/*
	 * It's an error if the number of self->fields exceeds the number of valid column. 
	 */
	if (self->base.parsing_field > self->former.maxfields)
	{
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("extra data after last expected column")));
	}
	/*
	 * Error, if the number of self->fields is less than the number of valid columns.
	 */
	if (self->base.parsing_field < self->former.minfields)
	{
		if (self->filter.funcstr)
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
							errmsg("missing data for argument %d",
								   self->base.parsing_field + 1),
							errdetail("only %d arguments, required %d", self->base.parsing_field, self->former.maxfields)));
		else
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
							errmsg("missing data for column \"%s\"",
#if PG_VERSION_NUM >= 110000
								   NameStr(self->former.desc->attrs[self->former.attnum[self->base.parsing_field]].attname)),
#else
								   NameStr(self->former.desc->attrs[self->former.attnum[self->base.parsing_field]]->attname)),
#endif
							errdetail("only %d columns, required %d", self->base.parsing_field, self->former.maxfields)));

	}

	/* Convert it to server encoding. */
	parsed_field = self->base.parsing_field;
	for (i = 0; i < parsed_field; i++)
	{
		if (self->fields[i] == NULL)
			continue;

		self->base.parsing_field = i + 1;
		self->fields[i] = CheckerConversion(checker, self->fields[i]);
	}

	ExtractValuesFromText(self, parsed_field);
	self->base.parsing_field = -1;

	if (self->filter.funcstr)
		tuple = FilterTuple(&self->filter, &self->former,
							&self->base.parsing_field);
	else
		tuple = TupleFormerTuple(&self->former);

	return tuple;
}

static bool
TextParserParam(TextParser *self, const char *keyword, char *value)
{
	if (CompareKeyword(keyword, "DELIMITER"))
	{
		ASSERT_ONCE(!self->delim);
		self->delim = pstrdup(value);
	}
	else if (CompareKeyword(keyword, "NULL"))
	{
		ASSERT_ONCE(!self->null);
		self->null = pstrdup(value);
	}
	else if (CompareKeyword(keyword, "FORCE_NOT_NULL"))
	{
		self->fnn_name = lappend(self->fnn_name, pstrdup(value));
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
TextParserDumpParams(TextParser *self)
{
	StringInfoData	buf;
	char		   *str;
	ListCell	   *name;

	initStringInfo(&buf);

	appendStringInfoString(&buf, "TYPE = Text\n");

	appendStringInfo(&buf, "SKIP = " int64_FMT "\n", self->offset);

	str = QuoteString(self->delim);
	appendStringInfo(&buf, "DELIMITER = %s\n", str);
	pfree(str);;

	str = QuoteString(self->null);
	appendStringInfo(&buf, "NULL = %s\n", str);
	pfree(str);

	if (self->filter.funcstr)
		appendStringInfo(&buf, "FILTER = %s\n", self->filter.funcstr);

	foreach(name, self->fnn_name)
	{
		str = QuoteString(lfirst(name));
		appendStringInfo(&buf, "FORCE_NOT_NULL = %s\n", str);
		pfree(str);
	}

	LoggerLog(INFO, buf.data, 0);
	pfree(buf.data);
}

static void
TextParserDumpRecord(TextParser *self, FILE *fp, char *badfile)
{
	int	len;

	len = fprintf(fp, "%s\n", self->cur);
	if (len < strlen(self->cur) || fflush(fp))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write parse badfile \"%s\": %m",
						badfile)));
}

/**
 * @brief Obtain an internal representation of each column from field array data for a record.
 *
 * Flow
 * -# For each field array member, repeat the following.
 *	 <dl>
 *	   <dt>When either FORCE_NOT_NULL is specified or the address stored in the field array
 *		   is not NULL,</dt>
 *		 <dd>Convert the field character string into the internal representation and set
 *			 NULL value marker to false. </dd>
 *	   <dt>When the address stored in the field array is NULL,</dt>
 *		 <dd>Set NULL value marker to true.</dd>
 *	 </dl>
 *
 * This function converts each field string into corresponding internal
 * representation and stores in self->former.values and self->former.isnull.
 *
 * @param rd [in/out] Control Info.
 * @return None
 * @note Memory areas allocated in this fuhnction cannot be rleased one by onek.  So the
 * caller of this function must set the memory context which allows to reset or discard
 * them.   When an error occurs, this returns to the caller using ereport().
 * @note When error occurs, return to the caller with ereport().
 */
static void
ExtractValuesFromText(TextParser *self, int parsed_field)
{
	int					i;

	/*
	 * Converts string data in the field array into the internal representation for
	 * a destination column.
	 */
	for (i = 0; i < parsed_field; i++)
	{
		Datum	value;
		bool	isnull;
		int		index;

		self->base.parsing_field = i + 1;		/* 1 origin */

		index = self->former.attnum[i];	/* Physical column index */
		if (self->fields[i] || self->fnn[index])
		{
			value = TupleFormerValue(&self->former, self->fields[i], index);
			isnull = false;
		}
		else
		{
			value = (Datum) 0;
			isnull = true;
		}

		self->former.isnull[index] = isnull;
		self->former.values[index] = value;
	}

	/* set function default value */
	for (; i < self->former.maxfields; i++)
	{
		int		index;

		index = i - self->former.minfields;
		self->former.isnull[i] = self->filter.defaultIsnull[index];
		self->former.values[i] = self->filter.defaultValues[index];
	}
}
