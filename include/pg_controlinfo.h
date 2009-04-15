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

/** initialization functions */
typedef void (*ParserInitProc)(Parser *self, ControlInfo *ci);

/** read one line and transforms each field from string to internal representation */
typedef bool (*ParserReadProc)(Parser *self, ControlInfo *ci);

/** clean up function */
typedef void (*ParserTermProc)(Parser *self);

/** parse parser parameter */
typedef bool (*ParserParamProc)(Parser *self, const char *keyword, char *value);

struct Parser
{
	ParserInitProc		initialize;
	ParserReadProc		read_line;
	ParserTermProc		cleanup;
	ParserParamProc		param;
};

/*
 * Loader
 */

typedef void (*Loader)(ControlInfo *ci, BTSpool **spools);

extern void DirectHeapLoad(ControlInfo *ci, BTSpool **spools);
extern void BufferedHeapLoad(ControlInfo *ci, BTSpool **spools);

/**
 * @brief Control information
 */
struct ControlInfo
{
	int			ci_max_err_cnt; /**< max error admissible number */
	RangeVar   *ci_rv;			/**< table information from control file */

	/*
	 * Input file parameters
	 */

	char	   *ci_infname;		/**< input file name */
	int			ci_infd;		/**< input file descriptor */
	int			ci_offset;		/**< lines to skip */
	int			ci_limit;		/**< max input lines */
	Parser	   *ci_parser;		/**< input file parser */

	/*
	 * database information
	 */

	Relation		ci_rel;				/**< load target relation */
	EState		   *ci_estate;
	TupleTableSlot *ci_slot;
	Oid			   *ci_typeioparams;	/**< type information */
	FmgrInfo	   *ci_in_functions;	/**< type transformation funcdtions */
	int			   *ci_attnumlist;		/**< index arrays for valid columns */
	int				ci_attnumcnt;		/**< length of ci_attnumlist */

	/*
	 * buffer for line data
	 */

	Datum	   *ci_values;	/**< array of values of one line */
	bool	   *ci_isnull;	/**< array of NULL marker of one line */

	/*
	 * heap loader
	 */

	Loader	ci_loader;	/**< loader function */

	/*
	 * loading status
	 */

	int32		ci_read_cnt;	/**< number of records read from input file */
	int32		ci_load_cnt;	/**< number of tuples loaded successfully */
	int32		ci_field;		/**< field number of processing (1 origin) */
	int			ci_err_cnt;		/**< number of occurred error */
	int			ci_status;		/**< 0:running, 1:eof, -1:error */
};

/* External declarations */

extern ControlInfo *OpenControlInfo(const char *fname);
extern void CloseControlInfo(ControlInfo *ci);

extern Parser *CreateFixedParser(void);
extern Parser *CreateCSVParser(void);

#define ParserInitialize(self, ci)			((self)->initialize((self), (ci)))
#define ParserReadLine(self, ci)			((self)->read_line((self), (ci)))
#define ParserCleanUp(self)					((self)->cleanup((self)))
#define ParserParam(self, keyword, value)	((self)->param((self), (keyword), (value)))

extern HeapTuple ReadTuple(ControlInfo *ci, TransactionId xid, TransactionId cid);

#define ASSERT_ONCE(expr) \
	do { if (!(expr)) \
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), \
						errmsg("duplicate %s specification", keyword))); \
	} while(0)

#endif   /* CONTROLINFO_H */
