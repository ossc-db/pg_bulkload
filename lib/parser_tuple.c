/*
 * pg_bulkload: lib/parser_tuple.c
 *
 *	  Copyright(C) 2009, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

/**
 * @file
 * @brief Binary HeapTuple format handling module implementation.
 */
#include "postgres.h"

#include <unistd.h>

#include "access/htup.h"
#include "utils/rel.h"

#include "reader.h"
#include "pg_profile.h"

typedef struct TupleParser
{
	Parser	base;

	HeapTupleData	tuple;
	char		   *buffer;
	uint32			buflen;
} TupleParser;

static void	TupleParserInit(TupleParser *self, TupleDesc desc);
static HeapTuple TupleParserRead(TupleParser *self, Source *source);
static void	TupleParserTerm(TupleParser *self);
static bool TupleParserParam(TupleParser *self, const char *keyword, char *value);

/**
 * @brief Create a new binary parser.
 */
Parser *
CreateTupleParser(void)
{
	TupleParser *self = palloc0(sizeof(TupleParser));
	self->base.init = (ParserInitProc) TupleParserInit;
	self->base.read = (ParserReadProc) TupleParserRead;
	self->base.term = (ParserTermProc) TupleParserTerm;
	self->base.param = (ParserParamProc) TupleParserParam;

	return (Parser *)self;
}

static void
TupleParserInit(TupleParser *self, TupleDesc desc)
{
	self->buflen = BLCKSZ;
	self->buffer = palloc(self->buflen);

#if 0
	/* Skip first ci_offset lines in the input file */
	if (rd->ci_offset > 0)
	{
	}
#endif
}

static void
TupleParserTerm(TupleParser *self)
{
	if (self->buffer)
		pfree(self->buffer);
	pfree(self);
}

static HeapTuple
TupleParserRead(TupleParser *self, Source *source)
{
	uint32		len;

	BULKLOAD_PROFILE(&prof_reader_parser);
	if (SourceRead(source, &len, sizeof(uint32)) == sizeof(uint32) && len > 0)
	{
		if (self->buflen < len)
		{
			repalloc(self->buffer, len);
			self->buflen = len;
		}
		if (SourceRead(source, self->buffer, len) == len)
		{
			BULKLOAD_PROFILE(&prof_reader_source);
			self->tuple.t_len = len;
			self->tuple.t_data = (HeapTupleHeader) self->buffer;
			return &self->tuple;
		}
	}

	return NULL;
}

static bool
TupleParserParam(TupleParser *self, const char *keyword, char *value)
{
	return false;	/* no parameters supported */
}
