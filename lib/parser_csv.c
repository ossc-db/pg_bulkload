/*
 * pg_bulkload: lib/parser_csv.c
 *
 *	  Copyright (c) 2007-2025, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

/**
 * @file
 * @brief CSV format file handling module implementation.
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
#define MAX_BUF_LEN			(16 * INITIAL_BUF_LEN)

typedef struct CsvFieldSelector
{
	/**
	 * @brief Give the field in the csv file a name.The names are separated by separators
	 * The number of names can be different from the number of fields in the csv file.
	 */
	char	   *csv_fields_name;
	
	/**
	 * @brief The name selected in the property csv_fields_name. 
	 */
	char	   *final_fields_name;
	int		   *field_index_array;			//array of index that identity location field in the csv_fields_name
	List	   *csv_field_list;
	List	   *final_field_list;
} CsvFieldSelector;

typedef struct CSVParser
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

	char		delim;			/**< delimeter */
	char		quote;			/**< quotation string */
	char		escape;			/**< escape letter */
	char	   *null;			/**< NULL value string */
	List	   *fnn_name;		/**< list of NOT NULL column names */
	bool	   *fnn;			/**< array of NOT NULL column flag */
	char       **final_fileds;				//store the final data imported into the table
	CsvFieldSelector	csv_field_selector;

} CSVParser;

static void	CSVParserInit(CSVParser *self, Checker *checker, const char *infile, TupleDesc desc, bool multi_process, Oid collation);
static HeapTuple	CSVParserRead(CSVParser *self, Checker *checker);
static int64	CSVParserTerm(CSVParser *self);
static bool CSVParserParam(CSVParser *self, const char *keyword, char *value);
static void CSVParserDumpParams(CSVParser *self);
static void CSVParserDumpRecord(CSVParser *self, FILE *fp, char *badfile);

static void	ExtractValuesFromCSV(CSVParser *self, int parsed_field);

static void InitFieldIndexArray(CSVParser *self);
static void SplitString(CSVParser *self, char *delimiter);
static int getFieldsCount(CSVParser *self);
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
appendToField(CSVParser *self, int *dst, int *src, int len)
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

/**
 * @brief Create a new CSV parser.
 */
Parser *
CreateCSVParser(void)
{
	CSVParser *self = palloc0(sizeof(CSVParser));
	self->base.init = (ParserInitProc) CSVParserInit;
	self->base.read = (ParserReadProc) CSVParserRead;
	self->base.term = (ParserTermProc) CSVParserTerm;
	self->base.param = (ParserParamProc) CSVParserParam;
	self->base.dumpParams = (ParserDumpParamsProc) CSVParserDumpParams;
	self->base.dumpRecord = (ParserDumpRecordProc) CSVParserDumpRecord;
	self->offset = -1;
	return (Parser *)self;
}

/**
 * @brief Initialize CSV file reader module.
 *
 * Flow
 * -# Allocate character string pointer array for valid columns.
 * -# Cache the length of NULL-value character string.
 *
 * @param rd [in] Control Info.
 * @return None
 * @note Caller must release the resource using CSVParserTerm().
 */
static void
CSVParserInit(CSVParser *self, Checker *checker, const char *infile, TupleDesc desc, bool multi_process, Oid collation)
{
	TupleCheckStatus	status;
	int fields_count;

	/*
	 * set default values
	 */
	self->delim = self->delim ? self->delim : ',';
	self->quote = self->quote ? self->quote : '"';
	self->escape = self->escape ? self->escape : '"';
	self->null = self->null ? self->null : "";
	self->need_offset = self->offset = self->offset > 0 ? self->offset : 0;

	/*
	 * validation check
	 */
	if (strchr(self->null, self->delim))
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg
				 ("DELIMITER cannot be appear in the NULL parameter")));
	if (strchr(self->null, self->quote))
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg
				 ("QUOTE cannot be appear in the NULL parameter")));
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
	InitFieldIndexArray(self);
	fields_count = getFieldsCount(self);
	self->fields = palloc(Max(fields_count, 1) * sizeof(char *));
	self->fields[0] = NULL;
	self->null_len = strlen(self->null);
	self->eof = false;
	self->final_fileds = palloc(Max(self->former.maxfields, 1) * sizeof(char *));
	self->final_fileds[0] = NULL;
}

/**
 * @brief Release the resources used in CSV file reader module.
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
CSVParserTerm(CSVParser *self)
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
checkFieldIsNull(CSVParser *self, int field_num, int len)
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
CSVParserRead(CSVParser *self, Checker *checker)
{
	HeapTuple	tuple;			/* return tuple */
	int			i = 0;			/* Index of the scanned character */
	int			ret;
	char		c;				/* Cache for the scanned character */
	char		quote = self->quote;	/* Cache for the quote mark */
	char		escape = self->escape; /* Cache for the escape character */
	char		delim = self->delim;	/* Cache for the delimiter */
	bool		need_data = false;		/* Flag indicating the need to read more characters */
	bool		in_quote = false;
	bool		inCR = false;

	/*
	 * Field parsing info
	 */
	int			field_head;		/* Index to the current field */
	int			dst;			/* Index to the next destination */
	int			src;			/* Index to the next source */
	int			field_num = 0;	/* Number of self->fields already parsed */
	int			parsed_field;
	int			fields_count;
	bool		enable_continue = false;
	int			total_csvfile_fields_count = 0;
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

		while ((len = SourceRead(self->source, self->rec_buf, self->buf_len - 1)) > 0)
		{
			int		n;

			for (n = 0; n < len; n++)
			{
				if (self->rec_buf[n] == '\r')
				{
					if (n == len - 1)
					{
						inCR = true;
						continue;
					}
					else if (self->rec_buf[n + 1] == '\n')
						n++;
				}
				else if (!inCR && self->rec_buf[n] != '\n')
					continue;

				/* Skip the line */
				inCR = false;
				++skipped;
				if (skipped >= self->need_offset)
				{
					/* Seek to head of the next line. */
					self->next = self->rec_buf + n + 1;
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
	
	fields_count = getFieldsCount(self);

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
				int			move_size = self->cur - self->rec_buf;	/* Amount to move buffer. */

				memmove(self->rec_buf, self->cur, self->buf_len - move_size);
				self->used_len -= move_size;
				i -= move_size;
				field_head -= move_size;
				src -= move_size;
				self->cur = self->rec_buf;
			}
			else if (self->buf_len - self->used_len <= 1)
			{
				int			j;
				char	   *old_buf = self->field_buf;

				self->buf_len = Min(self->buf_len * 2, MAX_BUF_LEN);
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

			BULKLOAD_PROFILE(&prof_reader_parser);
			ret = SourceRead(self->source, self->rec_buf + self->used_len,
								self->buf_len - self->used_len - 1);
			BULKLOAD_PROFILE(&prof_reader_source);
			if (ret == 0)
			{
				self->eof = true;
				/*
				 * When no data is found in the record buffer and we encounter EOF,
				 * there're no  more input to handle and return false.
				 */
				if (self->cur[0] == '\0')
					return NULL;

				/*
				 * When no corresponding (closing) quote mark is found and EOF is found,
				 * it's an error.   At this point, whole line has been parsed and exit from the loop.
				 */
				if (in_quote)
				{
					/* Record string does not include a new line of the end. */
					if (self->rec_buf[i - 1] == '\n')
						i--;
					if (self->rec_buf[i - 1] == '\r')
						i--;
					self->rec_buf[i] = '\0';
					break;
				}

				/*
				 * To simplify the following parsing, when the last character of the input
				 * file is not new line code, we add this.
				 */
				if (self->rec_buf[i] == '\0')
				{
					ret++;
					self->rec_buf[i] = '\n';
				}
			}
			else if (ret < 0)
				ereport(ERROR, (errcode_for_file_access(),
								errmsg("could not read input file %m")));

			self->used_len += ret;
			self->rec_buf[self->used_len] = '\0';
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
		else if (in_quote)
		{
			/*
			 * Escape character must be followed by a quote mark or an excape character.
			 * If escape character is at the end of the buffer, data is read and then
			 * we test this.
			 */
			if (c == escape)
			{
				if (self->rec_buf[i + 1] == quote || self->rec_buf[i + 1] == escape)
				{
					appendToField(self, &dst, &src, i - src);
					i++;
				}
				else if (!self->rec_buf[i + 1])
				{
					need_data = true;
					i--;		/* 'i--' is needed here to cancel increment in for statement. */
				}
				else if (c == quote)
				{
					/*
					 * A quote mark will be identical to the escape character.	So if the
					 * following character is valid character and is not escaped, test
					 * again here if it is a quote mark.
					 */
					appendToField(self, &dst, &src, i - src);
					in_quote = false;
				}
			}
			else if (c == quote)
			{
				appendToField(self, &dst, &src, i - src);
				in_quote = false;
			}
		}
		else if (inCR)
		{
			if(self->csv_field_selector.field_index_array == NULL || fields_count >= total_csvfile_fields_count+1)
			{
				appendToField(self, &dst, &src, i - src - 1);
				checkFieldIsNull(self, field_num, i - field_head - 1);
			}
			self->rec_buf[i - 1] = '\0';

			if (c != '\n')
				i--;	/* re-read the char */
			self->next = self->rec_buf + i + 1;

			inCR = false;
			break;
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

			if (c == quote)
			{
				appendToField(self, &dst, &src, i - src);
				in_quote = true;
			}
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
				if(self->csv_field_selector.field_index_array == NULL || fields_count >= total_csvfile_fields_count+1)
				{
					appendToField(self, &dst, &src, i - src);
					checkFieldIsNull(self, field_num, i - field_head);
				}

				/*
				 * Line feed other than a quote mark is the record delimiter.  Record parse
				 * terminates when the record delmiter is found.
				 */
				self->rec_buf[i] = '\0';
				self->next = self->rec_buf + i + 1;
				break;
			}
			else if (c == delim)
			{
				total_csvfile_fields_count++;
				/*
				 * If then number of columns specified in the input record exceeds the
				 * number of columns of the copy target table, then the value of the last
				 * column of the table will be overwritten by extra columns in the input
				 * data successively.
				 */
				
				if(self->csv_field_selector.field_index_array != NULL )
				{
					if(enable_continue)
						continue;		//after parse finished, It will execute other code, so break can not used;
					
					if (field_num + 1 >= fields_count)
					{
						enable_continue = true;
					}
				}
				
				appendToField(self, &dst, &src, i - src);

				//if(self->csv_field_selector.field_index_array == NULL )
				checkFieldIsNull(self, field_num, i - field_head);
				
				if(enable_continue)
					continue;
				
				if (field_num + 1 < fields_count)
					field_num++;
				
				
				self->base.parsing_field++;

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
	 * If no corresponding (closing) quote mark is found when a record parse terminates, it's an error. 
	 */
	if (in_quote)
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("unterminated CSV quoted field")));

	
	/*
	 * We accept a record only for new lines as input of the functions without
	 * the arguments.
	 */
	if (self->former.maxfields == 0 && strlen(self->cur) == 0)
		self->base.parsing_field = 0;

	/*
	 * It's an error if the number of self->fields exceeds the number of valid column. 
	 */
	if (self->csv_field_selector.field_index_array == NULL && self->base.parsing_field > self->former.maxfields)
	{
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("extra data after last expected column")));
	}
	/*
	 * Error, if the number of self->fields is less than the number of valid columns.
	 */
	if (self->csv_field_selector.field_index_array == NULL && self->base.parsing_field < self->former.minfields)
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
	
	/*
		if field_index_array is not null, then we find the value that fields by field_index_array.
		Then set the values into final_fileds. 
		Only Both of csv_fields_name and final_fields_name are not NULL. csv_field_selector is Effective. 
	 */
	if(self->csv_field_selector.csv_fields_name != NULL && self->csv_field_selector.final_fields_name != NULL)
	{
		char	   		*field_name;
		int				final_field_count;
		final_field_count = list_length(self->csv_field_selector.final_field_list);
		for(i = 0;i<self->former.maxfields; i++)
		{
			int j = self->csv_field_selector.field_index_array[i];
			if(j>=0 && j<parsed_field)
			{
				self->final_fileds[i] = self->fields[j];
			}
			else
			{
				if(i < final_field_count)
				{
					field_name =(char *) list_nth(self->csv_field_selector.final_field_list,i);
					if(strcmp(field_name,"NULL")==0 || strcmp(field_name,"")==0)
						self->final_fileds[i] = NULL;
					else
						self->final_fileds[i] = CheckerConversion(checker, field_name);
				}
				else
					self->final_fileds[i] = NULL;
			}
		}
	}
	else
	{
		self->final_fileds = self->fields;
	}
	
	//ExtractValuesFromCSV(self, parsed_field);
	/*
		The function ExtractValuesFromCSV has changed.
		Last version of bulkload handle self->fields, but this version handle self->final_fileds;
	 */
	ExtractValuesFromCSV(self, self->former.maxfields);
	self->base.parsing_field = -1;

	if (self->filter.funcstr)
		tuple = FilterTuple(&self->filter, &self->former,
							&self->base.parsing_field);
	else
		tuple = TupleFormerTuple(&self->former);

	return tuple;
}

static bool
CSVParserParam(CSVParser *self, const char *keyword, char *value)
{
	if (CompareKeyword(keyword, "DELIMITER"))
	{
		ASSERT_ONCE(!self->delim);
		self->delim = ParseSingleChar(value);
	}
	else if (CompareKeyword(keyword, "QUOTE"))
	{
		ASSERT_ONCE(!self->quote);
		self->quote = ParseSingleChar(value);
	}
	else if (CompareKeyword(keyword, "ESCAPE"))
	{
		ASSERT_ONCE(!self->escape);
		self->escape = ParseSingleChar(value);
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
	else if(CompareKeyword(keyword,"CSV_FIELDS"))
	{
		ASSERT_ONCE(!self->csv_field_selector.csv_fields_name);
		self->csv_field_selector.csv_fields_name = pstrdup(value);
	}
	else if(CompareKeyword(keyword,"FINAL_FIELDS"))
	{
		ASSERT_ONCE(!self->csv_field_selector.final_fields_name);
		self->csv_field_selector.final_fields_name = pstrdup(value);
	}
	else
		return false;	/* unknown parameter */
	
	return true;
}

static void
CSVParserDumpParams(CSVParser *self)
{
	StringInfoData	buf;
	char		   *str;
	ListCell	   *name;

	initStringInfo(&buf);

	appendStringInfoString(&buf, "TYPE = CSV\n");

	appendStringInfo(&buf, "SKIP = " int64_FMT "\n", self->offset);

	str = QuoteSingleChar(self->delim);
	appendStringInfo(&buf, "DELIMITER = %s\n", str);
	pfree(str);

	str = QuoteSingleChar(self->quote);
	appendStringInfo(&buf, "QUOTE = %s\n", str);
	pfree(str);

	str = QuoteSingleChar(self->escape);
	appendStringInfo(&buf, "ESCAPE = %s\n", str);
	pfree(str);

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

	if(self->csv_field_selector.csv_fields_name != NULL)
	{
		str = QuoteString(self->csv_field_selector.csv_fields_name);
		appendStringInfo(&buf,"CSV_FIELDS = %s\n",str);
		pfree(str);
		
		foreach(name,self->csv_field_selector.csv_field_list)
		{
			str = QuoteString(lfirst(name));
			appendStringInfo(&buf, "CSV_FIELD_LIST = %s\n", str);
			pfree(str);
		}
	}
	
	if(self->csv_field_selector.final_fields_name != NULL)
	{
		str = QuoteString(self->csv_field_selector.final_fields_name);
		appendStringInfo(&buf,"FINAL_FIELDS = %s\n",str);
		pfree(str);
		
		foreach(name, self->csv_field_selector.final_field_list)
		{
			str = QuoteString(lfirst(name));
			appendStringInfo(&buf, "FINAL_FIELD_LIST = %s\n", str);
			pfree(str);
		}
	}
	
	LoggerLog(INFO, buf.data, 0);
	pfree(buf.data);
}

static void
CSVParserDumpRecord(CSVParser *self, FILE *fp, char *badfile)
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
ExtractValuesFromCSV(CSVParser *self, int parsed_field)
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
		if (self->final_fileds[i] || self->fnn[index])
		{
			value = TupleFormerValue(&self->former, self->final_fileds[i], index);
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

/**
 * @brief 
 * This function initialize the field_index_array.
 * field_index_array store indexs that where field in final_fields_name appear in csv_fields_name.
 *
 * @param CSVParser *self
 * @return void
 */
static void 
InitFieldIndexArray(CSVParser *self)
{
	int			final_fields_index;
	int 		i;
	int			j;
	ListCell	*csv_name;
	ListCell	*final_name;

	if(self->csv_field_selector.final_fields_name == NULL || self->csv_field_selector.csv_fields_name == NULL)
		return ;

	SplitString(self,",");
	
	self->csv_field_selector.field_index_array = palloc(self->former.maxfields * sizeof(int));
	
	for(i = 0; i<self->former.maxfields; i++)
	{
		self->csv_field_selector.field_index_array[i] = -1;
	}
	
	final_fields_index = -1;
	foreach(final_name, self->csv_field_selector.final_field_list)
	{
		++final_fields_index;
		
		if(strcmp(lfirst(final_name),"NULL")==0 || lfirst(final_name) == NULL)
			continue;

		if(final_fields_index+1 > self->former.maxfields)
			break;
		
		j = -1;
		foreach(csv_name,self->csv_field_selector.csv_field_list)
		{
			++j;
			if(strcmp(lfirst(final_name),lfirst(csv_name))==0)
			{
				self->csv_field_selector.field_index_array[final_fields_index] = j;
				break;
			}
		}
	}

}

/**
 * @brief 
 * This function split a string into an array by delimeter
 */
static void
SplitString(CSVParser *self, char *delimiter)
{
	char	   		*field_name;
	char			*str;
	
	str = self->csv_field_selector.csv_fields_name;
	for (field_name = strsep(&str, delimiter); field_name != NULL; field_name = strsep(&str, delimiter)) {
		self->csv_field_selector.csv_field_list = lappend(self->csv_field_selector.csv_field_list, field_name);
	}

	str = self->csv_field_selector.final_fields_name;
	for (field_name = strsep(&str, delimiter); field_name != NULL; field_name = strsep(&str, delimiter)) {
		self->csv_field_selector.final_field_list = lappend(self->csv_field_selector.final_field_list, field_name);
	}
}

static int 
getFieldsCount(CSVParser *self)
{
	if (self->csv_field_selector.field_index_array == NULL)
	{
		return self->former.maxfields;
	}
	else
	{
		return list_length(self->csv_field_selector.csv_field_list);
	}
}
