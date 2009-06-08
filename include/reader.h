/*
 * pg_bulkload: include/reader.h
 *
 *	  Copyright(C) 2007-2009, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

/**
 * @file
 * @brief Declaration of reader module
 *
 */
#ifndef READER_H_INCLUDED
#define READER_H_INCLUDED

#include "pg_bulkload.h"

#include "lib/stringinfo.h"
#include "nodes/primnodes.h"
#include "utils/relcache.h"

/*
 * Source
 */

typedef size_t (*SourceReadProc)(Source *self, void *buffer, size_t len);
typedef void (*SourceCloseProc)(Source *self);

struct Source
{
	SourceReadProc		read;	/** read */
	SourceCloseProc		close;	/** close */
};

extern Source *CreateRemoteSource(const char *path, TupleDesc desc);
extern Source *CreateFileSource(const char *path, TupleDesc desc);
extern Source *CreateQueueSource(const char *path, TupleDesc desc);

#define SourceRead(self, buffer, len)	((self)->read((self), (buffer), (len)))
#define SourceClose(self)				((self)->close((self)))

/*
 * Parser
 */

typedef void (*ParserInitProc)(Parser *self, TupleDesc desc);
typedef HeapTuple (*ParserReadProc)(Parser *self, Source *source);
typedef void (*ParserTermProc)(Parser *self);
typedef bool (*ParserParamProc)(Parser *self, const char *keyword, char *value);

struct Parser
{
	ParserInitProc		init;	/**< initialize */
	ParserReadProc		read;	/**< read one tuple */
	ParserTermProc		term;	/**< clean up */
	ParserParamProc		param;	/**< parse a parameter */

	int			parsing_field;	/**< field number being parsed */
	int64		count;			/**< number of records read from stream */
};

extern Parser *CreateBinaryParser(void);
extern Parser *CreateCSVParser(void);
extern Parser *CreateTupleParser(void);

#define ParserInit(self, desc)				((self)->init((self), (desc)))
#define ParserRead(self, source)			((self)->read((self), (source)))
#define ParserTerm(self)					((self)->term((self)))
#define ParserParam(self, keyword, value)	((self)->param((self), (keyword), (value)))

/**
 * @brief Reader
 */
struct Reader
{
	/*
	 * Information from control file.
	 *	XXX: writer and on_duplicate should be another place?
	 */
	Oid			relid;				/**< target relation name */
	char	   *infile;				/**< input file name */
	int64		ci_offset;			/**< lines to skip */
	int64		ci_limit;			/**< max input lines */
	int			ci_max_err_cnt;		/**< max error admissible number */

	WriterCreate	writer;			/**< writer factory */
	ON_DUPLICATE	on_duplicate;

	/*
	 * Source and Parser
	 */
	Source	   *source;				/**< input source stream */
	Parser	   *parser;				/**< source stream parser */

	/*
	 * Internal status
	 */
	int			ci_err_cnt;			/**< number of errors ignored */
};

extern void ReaderOpen(Reader *rd, const char *fname, const char *options);
extern HeapTuple ReaderNext(Reader *rd);
extern void ReaderClose(Reader *rd);

/* TupleFormer */

typedef struct TupleFormer
{
	TupleDesc	desc;		/**< descriptor */
	Datum	   *values;		/**< array[desc->natts] of values */
	bool	   *isnull;		/**< array[desc->natts] of NULL marker */
	Oid		   *typIOParam;	/**< array[desc->natts] of type information */
	FmgrInfo   *typInput;	/**< array[desc->natts] of type input functions */
	int		   *attnum;		/**< array[nfields] of attnum mapping */
	int			nfields;	/**< number of valid fields */
} TupleFormer;

extern void TupleFormerInit(TupleFormer *former, TupleDesc desc);
extern void TupleFormerTerm(TupleFormer *former);
extern HeapTuple TupleFormerForm(TupleFormer *former);

/*
 * Utilitiy functions
 */

#define ASSERT_ONCE(expr) \
	do { if (!(expr)) \
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), \
						errmsg("duplicate %s specification", keyword))); \
	} while(0)

#endif   /* READER_H_INCLUDED */
