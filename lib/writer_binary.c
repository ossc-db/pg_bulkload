/*
 * pg_bulkload: lib/writer_binary.c
 *
 *	  Copyright (c) 2011-2026, NTT, Inc.
 */

#include "pg_bulkload.h"

#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>

#include "access/heapam.h"
#include "catalog/pg_type.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "utils/memutils.h"

#include "binary.h"
#include "logger.h"
#include "pg_profile.h"
#include "pg_strutil.h"
#include "storage/fd.h"
#include "writer.h"

#ifndef UINT16_MAX
#define UINT16_MAX             (65535U)
#endif
#ifndef UINT32_MAX
#define UINT32_MAX             (4294967295U)
#endif

/**
 * @brief The number of records write at one time
 */
#define WRITE_LINE_NUM	100

/**
 * @brief output a binary format file
 */
typedef struct BinaryWriter
{
	Writer	base;

	int		bin_fd;			/**< File descriptor of binary file to output */
	int		ctl_fd;			/**< File descriptor of control file to output */
	size_t	rec_len;		/**< One record length */
	char   *buffer;			/**< record buffer to keep output data */
	int		used_rec_cnt;	/**< # of used records in buffer */
	int		nfield;			/**< number of fields */
	Field  *fields;			/**< array of field descriptor */
	Datum  *values;
	bool   *nulls;
} BinaryWriter;

static void	BinaryWriterInit(BinaryWriter *self);
static void	BinaryWriterInsert(BinaryWriter *self, HeapTuple tuple);
static WriterResult	BinaryWriterClose(BinaryWriter *self, bool onError);
static bool	BinaryWriterParam(BinaryWriter *self, const char *keyword, char *value);
static void	BinaryWriterDumpParams(BinaryWriter *self);
static int	BinaryWriterSendQuery(BinaryWriter *self, PGconn *conn, char *queueName, char *logfile, bool verbose);

/* Signature of static functions */
static int	open_output_file(char *fname, char *filetype, bool check);
static void	close_output_file(int *fd, char *filetype);
static HeapTuple BinaryWriterCheckerTuple(TupleChecker *self, HeapTuple tuple, int *parsing_field);

/* ========================================================================
 * Implementation
 * ========================================================================*/

/**
 * @brief Create a new BinaryWriter
 */
Writer *
CreateBinaryWriter(void *opt)
{
	BinaryWriter	   *self;

	self = palloc0(sizeof(BinaryWriter));
	self->base.init = (WriterInitProc) BinaryWriterInit;
	self->base.insert = (WriterInsertProc) BinaryWriterInsert;
	self->base.close = (WriterCloseProc) BinaryWriterClose;
	self->base.param = (WriterParamProc) BinaryWriterParam;
	self->base.dumpParams = (WriterDumpParamsProc) BinaryWriterDumpParams;
	self->base.sendQuery = (WriterSendQueryProc) BinaryWriterSendQuery;
	self->bin_fd = -1;
	self->ctl_fd = -1;

	return (Writer *) self;
}

/**
 * @brief Initialize a BinaryWriter
 */
static void
BinaryWriterInit(BinaryWriter *self)
{
	char		path[MAXPGPATH];
	TupleDesc	tupdesc;
	int			i;
	bool		need_check = false;

	Assert(self->base.truncate == false);

	/* exist check of output file */
	self->bin_fd = open_output_file(self->base.output,
									"binary output file", true);
	snprintf(path, MAXPGPATH, "%s.ctl", self->base.output);
	self->ctl_fd = open_output_file(path, "sample control file", true);

	/* create TupleDesc */
#if PG_VERSION_NUM >= 120000
	tupdesc = CreateTemplateTupleDesc(self->nfield);
#else
	tupdesc = CreateTemplateTupleDesc(self->nfield, false);
#endif
	for (i = 0; i < self->nfield; i++)
	{
		TupleDescInitEntry(tupdesc, i + 1, "out col", self->fields[i].typeid,
						   -1, 0);
		self->rec_len += self->fields[i].len;

		if (self->fields[i].nulllen == 0 ||
			self->fields[i].typeid == CSTRINGOID ||
			(self->fields[i].typeid == INT4OID && self->fields[i].len == 2) ||
			(self->fields[i].typeid == INT8OID && self->fields[i].len == 4) )
			need_check = true;
	}

	self->base.desc = tupdesc;

	self->base.tchecker = CreateTupleChecker(tupdesc);
	if (need_check)
	{
		self->base.tchecker->checker =
						(CheckerTupleProc) BinaryWriterCheckerTuple;
		self->base.tchecker->opt = self->fields;
	}
	else
		self->base.tchecker->checker = (CheckerTupleProc) CoercionCheckerTuple;

	self->buffer = palloc(self->rec_len * WRITE_LINE_NUM);
	self->used_rec_cnt = 0;

	self->values = (Datum *) palloc(self->nfield * sizeof(Datum));
	self->nulls = (bool *) palloc(self->nfield * sizeof(bool));

	self->base.context = AllocSetContextCreate(
							CurrentMemoryContext,
							"BinaryWriter",
#if PG_VERSION_NUM >= 90600
									ALLOCSET_DEFAULT_SIZES);
#else
									ALLOCSET_SMALL_MINSIZE,
									ALLOCSET_SMALL_INITSIZE,
									ALLOCSET_DEFAULT_MAXSIZE);
#endif
}

static void
BinaryWriterInsert(BinaryWriter *self, HeapTuple tuple)
{
	int		i;
	char   *col;

	col = self->buffer + (self->rec_len * self->used_rec_cnt);

	/* Break down the tuple into fields */
	heap_deform_tuple(tuple, self->base.desc, self->values, self->nulls);

	for (i = 0; i < self->nfield; i++)
	{
		Field  *field = self->fields + i;

		if (!self->nulls[i])
			field->write(col, field->len, self->values[i], self->nulls[i]);
		else
			field->write(col, field->len, PointerGetDatum(field->nullif), field->nulllen);

		col += field->len;
	}

	/* open file */
	if (self->bin_fd == -1)
	{
		char	path[MAXPGPATH];

		self->bin_fd = open_output_file(self->base.output,
										"binary output file", false);
		snprintf(path, MAXPGPATH, "%s.ctl", self->base.output);
		self->ctl_fd = open_output_file(path, "sample control file", false);
	}

	self->used_rec_cnt++;

	if (self->used_rec_cnt >= WRITE_LINE_NUM)
	{
		int	len = self->rec_len * self->used_rec_cnt;

		if (write(self->bin_fd, self->buffer, len) != len)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not write to binary output file: %m")));

		self->used_rec_cnt = 0;
	}

	BULKLOAD_PROFILE(&prof_writer_table);
}

/*
 * Clean up BinaryWriter
 */
static WriterResult
BinaryWriterClose(BinaryWriter *self, bool onError)
{
	WriterResult	ret = { 0 };

	Assert(self != NULL);

	if (self->used_rec_cnt > 0)
	{
		int	len = self->rec_len * self->used_rec_cnt;

		if (write(self->bin_fd, self->buffer, len) != len)
			ereport(WARNING,
					(errcode_for_file_access(),
					 errmsg("could not write to binary output file: %m")));

		self->used_rec_cnt = 0;
	}

	/* create sample of control file */
	if (self->base.count > 0)
	{
		char		   *filepath;
		char		   *filename;
		char		   *extension;
		StringInfoData	buf;

		filepath = self->base.output;

		/* remove extension of outfile */
		filename = strrchr(self->base.output, '/');
		Assert(filename);
		filename++;
		filename = pstrdup(filename);
		extension = strrchr(filename, '.');
		if (extension && filename < extension)
			*extension = '\0';

		initStringInfo(&buf);
		appendStringInfo(&buf, "INPUT = %s\n", filepath);
		appendStringInfo(&buf, "OUTPUT = %s\n", filename);
		appendStringInfo(&buf, "LOGFILE = %s.log\n", filepath);
		appendStringInfo(&buf, "PARSE_BADFILE = %s.prs\n", filepath);
		appendStringInfo(&buf, "DUPLICATE_BADFILE = %s.dup\n", filepath);
		appendStringInfoString(&buf,
							   "PARSE_ERRORS = INFINITE\n"
							   "DUPLICATE_ERRORS = 0\n"
							   "ON_DUPLICATE_KEEP = NEW\n"
							   "SKIP = 0\n"
							   "LIMIT = INFINITE\n"
							   "CHECK_CONSTRAINTS = NO\n"
							   "MULTI_PROCESS = YES\n"
							   "VERBOSE = NO\n"
							   "TRUNCATE = NO\n"
							   "WRITER = DIRECT\n"
							   "TYPE = BINARY\n");
		BinaryDumpParams(self->fields, self->nfield, &buf, "COL");
		appendStringInfo(&buf, "# ENCODING = %s\n", GetDatabaseEncodingName());

		if (write(self->ctl_fd, buf.data, buf.len) != buf.len)
			ereport(WARNING,
					(errcode_for_file_access(),
					 errmsg("could not write to sample control file: %m")));

		pfree(filename);
		pfree(buf.data);
	}

	close_output_file(&self->bin_fd, "binary output file");
	close_output_file(&self->ctl_fd, "sample control file");

	if (self->base.output)
		pfree(self->base.output);
	self->base.output = NULL;

	if (self->buffer)
		pfree(self->buffer);
	self->buffer = NULL;

	if (self->values)
		pfree(self->values);
	self->values = NULL;

	if (self->nulls)
		pfree(self->nulls);
	self->nulls = NULL;

	if (self->fields)
		pfree(self->fields);
	self->fields = NULL;

	if (!onError)
		MemoryContextDelete(self->base.context);

	ret.num_dup_new = 0;
	ret.num_dup_old = 0;

	return ret;
}

static bool
BinaryWriterParam(BinaryWriter *self, const char *keyword, char *value)
{
	if (CompareKeyword(keyword, "CHECK_CONSTRAINTS") ||
		CompareKeyword(keyword, "FORCE_NOT_NULL"))
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("does not support parameter \"%s\" in \"WRITER = BINARY\"", keyword)));
	}
	else if (CompareKeyword(keyword, "TABLE") ||
			 CompareKeyword(keyword, "OUTPUT"))
	{
		if (strlen(value) + strlen(".ctl") >= MAXPGPATH)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("binary output file name is too long")));

		if (!is_absolute_path(value))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("relative path not allowed for OUTPUT: %s", value)));

		/* must be the super user if write to file */
		if (!superuser())
			ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to use pg_bulkload to a file")));

		ASSERT_ONCE(self->base.output == NULL);
		self->base.output = pstrdup(value);
	}
	else if (CompareKeyword(keyword, "OUT_COL"))
	{
		BinaryParam(&self->fields, &self->nfield, value, false, true);
	}
	else
		return false;	/* unknown parameter */

	return true;
}

static void
BinaryWriterDumpParams(BinaryWriter *self)
{
	StringInfoData	buf;

	initStringInfo(&buf);
	appendStringInfoString(&buf, "WRITER = BINARY\n");

	BinaryDumpParams(self->fields, self->nfield, &buf, "OUT_COL");

	LoggerLog(INFO, buf.data, 0);
	pfree(buf.data);
}

static int
BinaryWriterSendQuery(BinaryWriter *self, PGconn *conn, char *queueName, char *logfile, bool verbose)
{
	int				i;
	int				nparam;
	const char	  **params;
	StringInfoData	buf;
	int				offset;
	int				result;

	nparam = self->nfield + 4;
	params = palloc0(sizeof(char *) * nparam);

	/* async query send */
	params[0] = queueName;
	params[1] = self->base.output;
	params[2] = logfile;
	params[3] = verbose ? "true" : "no";

	initStringInfo(&buf);
	appendStringInfoString(&buf, 
		"SELECT * FROM pgbulkload.pg_bulkload(ARRAY["
		"'TYPE=TUPLE',"
		"'INPUT=' || $1,"
		"'WRITER=BINARY',"
		"'OUTPUT=' || $2,"
		"'LOGFILE=' || $3,"
		"'VERBOSE=' || $4");

	offset = 0;
	for (i = 0 ; i < self->nfield; i++)
	{
		StringInfoData	param_buf;

		appendStringInfo(&buf, ",'OUT_COL=' || $%d", i + 4 + 1);

		initStringInfo(&param_buf);
		offset = BinaryDumpParam(self->fields + i, &param_buf, offset);
		params[i + 4] = param_buf.data;
	}

	appendStringInfoString(&buf, "])");

	result = PQsendQueryParams(conn, buf.data, nparam, NULL, params, NULL, NULL, 0);

	pfree(params);
	pfree(buf.data);

	return result;
}

/*
 * Open the output file and returns its descriptor.
 */
static int
open_output_file(char *fname, char *filetype, bool check)
{
	int	fd = -1;

#if PG_VERSION_NUM >= 110000
	fd = BasicOpenFilePerm(fname, O_WRONLY | O_CREAT | O_EXCL | PG_BINARY,
					   S_IRUSR | S_IWUSR);
#else
	fd = BasicOpenFile(fname, O_WRONLY | O_CREAT | O_EXCL | PG_BINARY,
					   S_IRUSR | S_IWUSR);
#endif
	if (fd == -1)
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not open %s: %m", filetype)));

	if (check)
	{
		close_output_file(&fd, filetype);
		unlink(fname);
	}

	return fd;
}

/**
 * Flush and close the output file.
 */
static void
close_output_file(int *fd, char *filetype)
{
	if (*fd == -1)
		return;

	if (pg_fsync(*fd) != 0)
		ereport(WARNING, (errcode_for_file_access(),
				errmsg("could not fsync %s: %m", filetype)));

	if (close(*fd) != 0)
		ereport(WARNING, (errcode_for_file_access(),
				errmsg("could not close %s: %m", filetype)));

	*fd = -1;
}

static HeapTuple
BinaryWriterCheckerTuple(TupleChecker *self, HeapTuple tuple, int *parsing_field)
{
	TupleDesc	desc = self->targetDesc;
	Field	   *fields = self->opt;
	int		i;

	if (self->status == NEED_COERCION_CHECK)
		UpdateTupleCheckStatus(self, tuple);

	if (self->status == NO_COERCION)
		heap_deform_tuple(tuple, desc, self->values, self->nulls);
	else
	{
		CoercionDeformTuple(self, tuple, parsing_field);
		tuple = heap_form_tuple(self->targetDesc, self->values, self->nulls);
	}

	for (i = 0; i < desc->natts; i++)
	{
		*parsing_field = i + 1;	/* 1 origin */

		if (self->nulls[i])
		{
			if (fields[i].nulllen == 0)
				ereport(ERROR,
						(errcode(ERRCODE_NOT_NULL_VIOLATION),
						 errmsg("null value violates not-null constraint")));

			continue;
		}

		switch (fields[i].typeid)
		{
			case CSTRINGOID:
				if (strlen(DatumGetCString(self->values[i])) > fields[i].len)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("value too long for type character(%d)", fields[i].len)));
				break;
			case INT4OID:
				if (fields[i].len == sizeof(uint16))
				{
					int32	value = DatumGetInt32(self->values[i]);

					if (value < 0 || value > UINT16_MAX)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("value \"%d\" is out of range for type unsigned smallint", value)));
				}
				break;
			case INT8OID:
				if (fields[i].len == sizeof(uint32))
				{
					int64	value = DatumGetInt64(self->values[i]);

					if (value < 0 || value > UINT32_MAX)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("value \"" int64_FMT "\" is out of range for type unsigned integer", value)));
				}
				break;
		}
	}

	*parsing_field = -1;

	return tuple;
}
