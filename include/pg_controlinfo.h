/*
 * pg_bulkload: include/pg_controlinfo.h
 *
 *	  Copyright(C) 2007-2009, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

/**
 * @file
 * @brief Declaration of control information process module
 *
 */
#ifndef CONTROLINFO_H
#define CONTROLINFO_H

#include <sys/param.h>
#include "postgres.h"
#include "access/nbtree.h"
#include "nodes/execnodes.h"
#include "utils/palloc.h"
#include "utils/memutils.h"

#include "pg_bulkload.h"

/*
 * Parser
 */

typedef void (*ParserInitProc)(Parser *self, ControlInfo *ci);
typedef bool (*ParserReadProc)(Parser *self, ControlInfo *ci);
typedef void (*ParserTermProc)(Parser *self, bool inError);
typedef bool (*ParserParamProc)(Parser *self, const char *keyword, char *value);

struct Parser
{
	ParserInitProc		init;	/**< initialize */
	ParserReadProc		read;	/**< read one tuple */
	ParserTermProc		term;	/**< clean up */
	ParserParamProc		param;	/**< parse a parameter */
};

/*
 * Loader
 */

typedef void (*LoaderInitProc)(Loader *self, Relation rel);
typedef bool (*LoaderInsertProc)(Loader *self, Relation rel, HeapTuple tuple);
typedef void (*LoaderTermProc)(Loader *self, bool inError);

struct Loader
{
	LoaderInitProc		init;	/**< initialize */
	LoaderInsertProc	insert;	/**< insert one tuple */
	LoaderTermProc		term;	/**< clean up */
	bool				use_wal;
};

/**
 * @brief Control information
 */
struct ControlInfo
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
	 * Parser parameters and status
	 *	TODO: Move some fields to parser because they are implementation.
	 */
	Parser	   *ci_parser;			/**< input file parser */
	Datum	   *ci_values;			/**< array of values of one line */
	bool	   *ci_isnull;			/**< array of NULL marker of one line */
	int			ci_infd;			/**< input file descriptor */
	int64		ci_read_cnt;		/**< number of records read from input file */
	Oid		   *ci_typeioparams;	/**< type information */
	FmgrInfo   *ci_in_functions;	/**< type transformation funcdtions */
	int		   *ci_attnumlist;		/**< index arrays for valid columns */
	int			ci_attnumcnt;		/**< length of ci_attnumlist */

	/*
	 * Loader parameters and status
	 */
	Loader	   *ci_loader;	/**< loader */
};

/* External declarations */

extern ControlInfo *OpenControlInfo(const char *fname, const char *options);
extern void CloseControlInfo(ControlInfo *ci, bool inError);

extern Parser *CreateFixedParser(void);
extern Parser *CreateCSVParser(void);

#define ParserInit(self, ci)				((self)->init((self), (ci)))
#define ParserRead(self, ci)				((self)->read((self), (ci)))
#define ParserTerm(self, inError)			((self)->term((self), (inError)))
#define ParserParam(self, keyword, value)	((self)->param((self), (keyword), (value)))

extern Loader *CreateDirectLoader(void);
extern Loader *CreateBufferedLoader(void);

#define LoaderInit(self, rel)			((self)->init((self), (rel)))
#define LoaderInsert(self, rel, tuple)	((self)->insert((self), (rel), (tuple)))
#define LoaderTerm(self, inError)		((self)->term((self), (inError)))

#define ASSERT_ONCE(expr) \
	do { if (!(expr)) \
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), \
						errmsg("duplicate %s specification", keyword))); \
	} while(0)

#endif   /* CONTROLINFO_H */
