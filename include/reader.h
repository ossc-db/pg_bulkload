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

typedef enum CopySource
{
	COPY_FILE,		/* to/from file */
	COPY_OLD_FE,	/* to/from frontend (2.0 protocol) */
	COPY_NEW_FE		/* to/from frontend (3.0 protocol) */
} CopySource;

size_t SourceRead(Reader *rd, void *buffer, size_t len);

/*
 * Parser
 */

typedef void (*ParserInitProc)(Parser *self, Reader *rd);
typedef bool (*ParserReadProc)(Parser *self, Reader *rd);
typedef void (*ParserTermProc)(Parser *self);
typedef bool (*ParserParamProc)(Parser *self, const char *keyword, char *value);

struct Parser
{
	ParserInitProc		init;	/**< initialize */
	ParserReadProc		read;	/**< read one tuple */
	ParserTermProc		term;	/**< clean up */
	ParserParamProc		param;	/**< parse a parameter */
};

extern Parser *CreateFixedParser(void);
extern Parser *CreateCSVParser(void);

#define ParserInit(self, rd)				((self)->init((self), (rd)))
#define ParserRead(self, rd)				((self)->read((self), (rd)))
#define ParserTerm(self)					((self)->term((self)))
#define ParserParam(self, keyword, value)	((self)->param((self), (keyword), (value)))

/**
 * @brief Reader
 */
struct Reader
{
	RangeVar   *ci_rv;				/**< target relation name */
	int			ci_max_err_cnt;		/**< max error admissible number */

	char	   *ci_infname;			/**< input file name */
	int64		ci_offset;			/**< lines to skip */
	int64		ci_limit;			/**< max input lines */

	/*
	 * General status
	 */
	Relation	ci_rel;				/**< target relation */
	int			ci_err_cnt;			/**< number of errors ignored */
	int			ci_parsing_field;	/**< field number being parsed */

	/*
	 * Source
	 */

	CopySource	source;
	bool		source_eof;
	FILE	   *ci_infd;			/**< input file descriptor */
	StringInfo	fe_msgbuf;			/* used only for dest == COPY_NEW_FE */

	/*
	 * Parser parameters and status
	 *	TODO: Move some fields to parser because they are implementation.
	 */
	Parser	   *ci_parser;			/**< input file parser */
	Datum	   *ci_values;			/**< array of values of one line */
	bool	   *ci_isnull;			/**< array of NULL marker of one line */
	int64		ci_read_cnt;		/**< number of records read from input file */
	Oid		   *ci_typeioparams;	/**< type information */
	FmgrInfo   *ci_in_functions;	/**< type transformation funcdtions */
	int		   *ci_attnumlist;		/**< index arrays for valid columns */
	int			ci_attnumcnt;		/**< length of ci_attnumlist */

	/*
	 * Loader information from control file
	 */
	Loader	   *(*ci_loader)(Relation rel);	/**< loader factory */
	ON_DUPLICATE	on_duplicate;
};

extern void ReaderOpen(Reader *rd, const char *fname, const char *options);
extern HeapTuple ReaderNext(Reader *rd);
extern void ReaderClose(Reader *rd);

/* Utilitiy functions */

#define ASSERT_ONCE(expr) \
	do { if (!(expr)) \
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), \
						errmsg("duplicate %s specification", keyword))); \
	} while(0)

#endif   /* READER_H_INCLUDED */
