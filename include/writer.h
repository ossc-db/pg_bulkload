/*
 * pg_bulkload: include/writer.h
 *
 *	  Copyright (c) 2009-2024, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

/**
 * @file
 * @brief Declaration of writer module
 *
 */
#ifndef WRITER_H_INCLUDED
#define WRITER_H_INCLUDED

#include "pg_bulkload.h"

#include "libpq-fe.h"

#include "access/xact.h"
#include "access/nbtree.h"
#include "nodes/execnodes.h"

#include "reader.h"

#if PG_VERSION_NUM >= 130000
#define MAXINT8LEN		20
#else
#define MAXINT8LEN		25
#endif

#define DEFAULT_MAX_DUP_ERRORS	0

/*
 * Writer
 */

typedef struct WriterResult
{
	int64		num_dup_new;
	int64		num_dup_old;
} WriterResult;

typedef void (*WriterInitProc)(Writer *self);
typedef void (*WriterInsertProc)(Writer *self, HeapTuple tuple);
typedef WriterResult (*WriterCloseProc)(Writer *self, bool onError);
typedef bool (*WriterParamProc)(Writer *self, const char *keyword, char *value);
typedef void (*WriterDumpParamsProc)(Writer *self);
typedef int (*WriterSendQueryProc)(Writer *self, PGconn *conn, char *queueName, char *logfile, bool verbose);

struct Writer
{
	WriterInitProc			init;		/**< initialize */
	WriterInsertProc		insert;		/**< insert one tuple */
	WriterCloseProc			close;		/**< clean up */
	WriterParamProc			param;		/**< parse a parameter */
	WriterDumpParamsProc	dumpParams;	/**< dump parameters */
	WriterSendQueryProc		sendQuery;	/**< send query to parallel writer */

	MemoryContext		context;
	int64				count;

	bool			truncate;		/* truncate before load? */
	bool			verbose;		/* output error message to server log? */
	ON_DUPLICATE	on_duplicate;	/* behavior when duplicated keys found */
	int64			max_dup_errors;	/* max ignorable errors in unique indexes */
	char		   *dup_badfile;	/* duplicate error file name */
	char		   *logfile;		/* log file name */
	bool			multi_process;	/* multi process load? */

	char		   *output;			/**< output file or relation name */
	Oid				relid;			/**< target relation id */
	Relation		rel;			/**< target relation */
	TupleDesc		desc;			/**< tuple descriptor */
	TupleChecker   *tchecker;		/**< tuple format checker */
};

typedef Writer *(*CreateWriter)(void *opt);

extern Writer *CreateDirectWriter(void *opt);
extern Writer *CreateBufferedWriter(void *opt);
extern Writer *CreateParallelWriter(void *opt);
extern Writer *CreateBinaryWriter(void *opt);

extern Writer *WriterCreate(char *type, bool multi_process);
extern void WriterInit(Writer *self);
extern WriterResult WriterClose(Writer *self, bool onError);
extern bool WriterParam(Writer *self, const char *keyword, char *value);
extern void WriterDumpParams(Writer *self);

#define WriterInsert(self, tuple)	((self)->insert((self), (tuple)))

/*
 * Utilitiy functions
 */

extern void VerifyTarget(Relation rel, int64 max_dup_errors);
extern void TruncateTable(Oid relid);
char *get_relation_name(Oid relid);
extern void ValidateLSFDirectory(const char *path);

#endif   /* WRITER_H_INCLUDED */
