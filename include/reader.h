/*
 * pg_bulkload: include/reader.h
 *
 *	  Copyright (c) 2007-2021, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
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

#if PG_VERSION_NUM >= 90204
#include "executor/functions.h"
#endif

#if PG_VERSION_NUM >= 90300
#include "access/htup_details.h"
#endif

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

extern Source *CreateSource(const char *path, TupleDesc desc, bool async_read);

#define SourceRead(self, buffer, len)	((self)->read((self), (buffer), (len)))
#define SourceClose(self)				((self)->close((self)))

typedef struct Checker	Checker;

/*
 * Parser
 */

typedef void (*ParserInitProc)(Parser *self, Checker *checker, const char *infile, TupleDesc desc, bool multi_process, Oid collation);
typedef HeapTuple (*ParserReadProc)(Parser *self, Checker *checker);
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

#define ParserInit(self, checker, infile, relid, multi_process, collation)		((self)->init((self), (checker), (infile), (relid), (multi_process), (collation)))
#define ParserRead(self, checker)					((self)->read((self), (checker)))
#define ParserTerm(self)					((self)->term((self)))
#define ParserParam(self, keyword, value)	((self)->param((self), (keyword), (value)))
#define ParserDumpParams(self)				((self)->dumpParams((self)))
#define ParserDumpRecord(self, fp, fname)	((self)->dumpRecord((self), (fp), (fname)))

/* Checker */

typedef enum
{
	NEED_COERCION_CHECK,
	NEED_COERCION,
	NO_COERCION
} TupleCheckStatus;

typedef struct TupleChecker TupleChecker;
typedef struct CoercionChecker CoercionChecker;

typedef HeapTuple (*CheckerTupleProc)(TupleChecker *self, HeapTuple tuple, int *parsing_field);
extern TupleChecker *CreateTupleChecker(TupleDesc desc);
extern void UpdateTupleCheckStatus(TupleChecker *self, HeapTuple tuple);
extern void CoercionDeformTuple(TupleChecker *self, HeapTuple tuple, int *parsing_field);
extern HeapTuple CoercionCheckerTuple(TupleChecker *self, HeapTuple tuple, int *parsing_field);

struct TupleChecker
{
	CheckerTupleProc	checker;
	TupleCheckStatus	status;
	TupleDesc			sourceDesc;
	TupleDesc			targetDesc;
	MemoryContext		context;
	Datum			   *values;
	bool			   *nulls;
	void			   *opt;
	CoercionChecker	   *coercionChecker;
	bool			   *typIsVarlena;
	FmgrInfo		   *typOutput;
	Oid				   *typIOParam;
	FmgrInfo		   *typInput;
};

#define CheckerTuple(self, tuple, parsing_field) \
	((self)->tchecker) ? \
		((self)->tchecker->checker((self)->tchecker, \
								   (tuple), \
								   (parsing_field))) : \
		(tuple)

struct Checker
{
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
	TupleDesc		desc;
	TupleChecker   *tchecker;
};

extern void CheckerInit(Checker *checker, Relation rel, TupleChecker *tchecker);
extern void CheckerTerm(Checker *checker);
extern char *CheckerConversion(Checker *checker, char *src);
extern HeapTuple CheckerConstraints(Checker *checker, HeapTuple tuple, int *parsing_field);

/**
 * @brief Reader
 */
struct Reader
{
	/*
	 * Information from control file.
	 */
	char	   *infile;				/**< input file name */
	char	   *logfile;			/**< log file name */
	char	   *parse_badfile;		/**< parse error file name */
	int64		limit;				/**< max input lines */
	int64		max_parse_errors;	/**< max ignorable errors in parse */

	/*
	 * Parser
	 */
	Parser		   *parser;			/**< source stream parser */

	/*
	 * Checker
	 */
	Checker			checker;		/**< load data checker */

	/*
	 * Internal status
	 */
	int64			parse_errors;	/**< number of parse errors ignored */
	FILE		   *parse_fp;
};

extern Reader *ReaderCreate(char *type);
extern void ReaderInit(Reader *self);
extern bool ReaderParam(Reader *rd, const char *keyword, char *value);
extern HeapTuple ReaderNext(Reader *rd);
extern void ReaderDumpParams(Reader *rd);
extern int64 ReaderClose(Reader *rd, bool onError);

/* TupleFormer */

typedef struct TupleFormer
{
	TupleDesc	desc;		/**< descriptor */
	Datum	   *values;		/**< array[desc->natts] of values */
	bool	   *isnull;		/**< array[desc->natts] of NULL marker */
	Oid		   *typId;		/**< array[desc->natts] of type oid */
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

#if PG_VERSION_NUM >= 90204
/* This struct belong to function.c
 * If future version of PG contain declaration
 * in any header file eg. executor/functions.h
 * we no need to define following structure.
 */
typedef struct
{
	char	   *fname;
	char	   *src;

	SQLFunctionParseInfoPtr pinfo;

	Oid			rettype;
	int16		typlen;
	bool		typbyval;
	bool		returnsSet;
	bool		returnsTuple;
	bool		shutdown_reg;
	bool		readonly_func;
	bool		lazyEval;

	ParamListInfo paramLI;
	Tuplestorestate *tstore;
	JunkFilter *junkFilter;
	List	   *func_state;
	MemoryContext fcontext;
	LocalTransactionId lxid;
	SubTransactionId subxid;
} SQLFunctionCache;
#endif

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
	Oid				fn_rettype;
	Oid				collation;
	bool			is_first_time_call;
	bool			is_funcid_sql;
#if PG_VERSION_NUM >= 90204
	SQLFunctionCache	fn_extra;
#endif
	MemoryContext		context;
};

extern bool tupledesc_match(TupleDesc dst_tupdesc, TupleDesc src_tupdesc);
extern TupleCheckStatus FilterInit(Filter *filter, TupleDesc desc, Oid collation);
extern void FilterTerm(Filter *filter);
extern HeapTuple FilterTuple(Filter *filter, TupleFormer *former, int *parsing_field);

/*
 * Utilitiy functions
 */

#define ASSERT_ONCE(expr) \
	do { if (!(expr)) \
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), \
						errmsg("duplicate %s specified", keyword))); \
	} while(0)

extern size_t choice(const char *name, const char *key, const char *keys[], size_t nkeys);

#endif   /* READER_H_INCLUDED */
