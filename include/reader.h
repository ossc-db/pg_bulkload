/*
 * pg_bulkload: include/reader.h
 *
 *	  Copyright (c) 2007-2010, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

/**
 * @file
 * @brief Declaration of reader module
 */
#ifndef READER_H_INCLUDED
#define READER_H_INCLUDED

#include "pg_bulkload.h"

#include "access/xact.h"
#include "lib/stringinfo.h"
#include "nodes/execnodes.h"
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

extern Source *CreateSource(const char *path, TupleDesc desc);

#define SourceRead(self, buffer, len)	((self)->read((self), (buffer), (len)))
#define SourceClose(self)				((self)->close((self)))

/*
 * Parser
 */

typedef void (*ParserInitProc)(Parser *self, const char *infile, Oid relid);
typedef HeapTuple (*ParserReadProc)(Parser *self);
typedef int64 (*ParserTermProc)(Parser *self);
typedef bool (*ParserParamProc)(Parser *self, const char *keyword, char *value);
typedef void (*ParserDumpParamsProc)(Parser *self);
typedef void (*ParserDumpRecordProc)(Parser *self, FILE *fp, char *badfile);

struct Parser
{
	ParserInitProc			init;		/**< initialize */
	ParserReadProc			read;		/**< read one tuple */
	ParserTermProc			term;		/**< clean up */
	ParserParamProc			param;		/**< parse a parameter */
	ParserDumpParamsProc	dumpParams;	/**< dump parameters */
	ParserDumpRecordProc	dumpRecord;	/**< dump parse error record */

	int			parsing_field;	/**< field number being parsed */
	int64		count;			/**< number of records read from stream */
};

extern Parser *CreateBinaryParser(void);
extern Parser *CreateCSVParser(void);
extern Parser *CreateTupleParser(void);
extern Parser *CreateFunctionParser(void);

#define ParserInit(self, infile, relid)		((self)->init((self), (infile), (relid)))
#define ParserRead(self)					((self)->read((self)))
#define ParserTerm(self)					((self)->term((self)))
#define ParserParam(self, keyword, value)	((self)->param((self), (keyword), (value)))
#define ParserDumpParams(self)				((self)->dumpParams((self)))
#define ParserDumpRecord(self, fp, fname)	((self)->dumpRecord((self), (fp), (fname)))

/**
 * @brief Reader
 */
struct Reader
{
	/*
	 * Information from control file.
	 *	XXX: writer and on_duplicate should be another place?
	 */
	Oid				relid;			/**< target relation id */
	char		   *infile;			/**< input file name */
	char		   *logfile;		/**< log file name */
	char		   *parse_badfile;	/**< parse error file name */
	char		   *dup_badfile;	/**< duplicate error file name */
	int64			limit;			/**< max input lines */
	int64			max_parse_errors;	/**< max error admissible number by parse */
	int64			max_dup_errors;	/**< max error admissible number by duplicate */

	WriterCreate	writer;			/**< writer factory */
	ON_DUPLICATE	on_duplicate;	/**< writer options */
	bool			verbose;		/**< logger options */

	/*
	 * Parser
	 */
	Parser		   *parser;			/**< source stream parser */

	/*
	 * Internal status
	 */
	int64			parse_errors;	/**< number of parse errors ignored */
	FILE		   *parse_fp;
};

extern Reader *ReaderCreate(const char *fname, const char *options, time_t tm);
extern HeapTuple ReaderNext(Reader *rd);
extern void ReaderDumpParams(Reader *rd);
extern int64 ReaderClose(Reader *rd, bool onError);

/* Checker */

typedef struct Checker
{
	Relation		rel;

	/* Check the encoding */
	bool			check_encoding;	/**< encoding check needed? */
	int				encoding;		/**< input data encoding */
	int				db_encoding;	/**< database encoding */

	/* Check the constraints */
	bool			check_constraints;
	bool			has_constraints;	/**< constraints check needed? */
	bool			has_not_null;		/**< not nulls check needed? */
	ResultRelInfo  *resultRelInfo;
	EState		   *estate;
	TupleTableSlot *slot;
} Checker;

extern void CheckerInit(Checker *checker, Relation rel);
extern void CheckerTerm(Checker *checker);
extern char *CheckerConversion(Checker *checker, char *src);
extern void CheckerConstraints(Checker *checker, HeapTuple tuple);

/* TupleFormer */

typedef struct TupleFormer
{
	TupleDesc	desc;		/**< descriptor */
	Datum	   *values;		/**< array[desc->natts] of values */
	bool	   *isnull;		/**< array[desc->natts] of NULL marker */
	Oid		   *typIOParam;	/**< array[desc->natts] of type information */
	FmgrInfo   *typInput;	/**< array[desc->natts] of type input functions */
	Oid		   *typMod;		/**< array[desc->natts] of type modifiers */
	int		   *attnum;		/**< array[maxfields] of attnum mapping */
	int			minfields;	/**< min number of valid fields */
	int			maxfields;	/**< max number of valid fields */
} TupleFormer;

typedef struct Filter	Filter;
extern void TupleFormerInit(TupleFormer *former, Filter *filter, TupleDesc desc);
extern void TupleFormerTerm(TupleFormer *former);
extern HeapTuple TupleFormerTuple(TupleFormer *former);
extern Datum TupleFormerValue(TupleFormer *former, const char *str, int col);

/* Filter */

struct Filter
{
	char		   *funcstr;
	Oid				funcid;
	int				nargs;
	int				fn_ndargs;
	bool			fn_strict;
	Oid				argtypes[FUNC_MAX_ARGS];
	Datum		   *defaultValues;
	bool		   *defaultIsnull;
	ExprContext	   *econtext;
	HeapTupleData	tuple;
	bool			tupledesc_matched;
};

extern void FilterInit(Filter *filter, TupleDesc desc);
extern void FilterTerm(Filter *filter);
extern HeapTuple FilterTuple(Filter *filter, TupleFormer *former, int *parsing_field);

/*
 * Utilitiy functions
 */

#define ASSERT_ONCE(expr) \
	do { if (!(expr)) \
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), \
						errmsg("duplicate %s specification", keyword))); \
	} while(0)

#endif   /* READER_H_INCLUDED */
