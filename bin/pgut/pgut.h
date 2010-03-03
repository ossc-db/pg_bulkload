/*-------------------------------------------------------------------------
 *
 * pgut.h
 *
 * Copyright (c) 2009-2010, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 *
 *-------------------------------------------------------------------------
 */

#ifndef PGUT_H
#define PGUT_H

#include "c.h"
#include <assert.h>

#ifndef WIN32
#include <sys/time.h>
#include <unistd.h>
#endif

#include "libpq-fe.h"
#include "pqexpbuffer.h"

#define INFINITE_STR		"INFINITE"

typedef enum YesNo
{
	DEFAULT,
	NO,
	YES
} YesNo;

typedef void (*pgut_atexit_callback)(bool fatal, void *userdata);

/*
 * pgut client variables and functions
 */
extern const char  *PROGRAM_NAME;
extern const char  *PROGRAM_VERSION;
extern const char  *PROGRAM_URL;
extern const char  *PROGRAM_EMAIL;

/*
 * pgut framework variables and functions
 */
extern bool			debug;
extern bool			quiet;
extern bool			interrupted;

extern void pgut_init(int argc, char **argv);
extern void pgut_atexit_push(pgut_atexit_callback callback, void *userdata);
extern void pgut_atexit_pop(pgut_atexit_callback callback, void *userdata);

/*
 * Database connections
 */
extern PGconn *pgut_connect(const char *info, YesNo prompt, int elevel);
extern void pgut_disconnect(PGconn *conn);
extern void pgut_disconnect_all(void);
extern PGresult *pgut_execute(PGconn* conn, const char *query, int nParams, const char **params, int elevel);
extern ExecStatusType pgut_command(PGconn* conn, const char *query, int nParams, const char **params, int elevel);
extern bool pgut_send(PGconn* conn, const char *query, int nParams, const char **params, int elevel);
extern int pgut_wait(int num, PGconn *connections[], struct timeval *timeout);

/*
 * memory allocators
 */
extern void *pgut_malloc(size_t size);
extern void *pgut_realloc(void *p, size_t size);
extern char *pgut_strdup(const char *str);
extern char *strdup_with_len(const char *str, size_t len);
extern char *strdup_trim(const char *str);

#define pgut_new(type)			((type *) pgut_malloc(sizeof(type)))
#define pgut_newarray(type, n)	((type *) pgut_malloc(sizeof(type) * (n)))
#define pgut_newvar(type, m, n)	((type *) pgut_malloc(offsetof(type, m) + (n)))

/*
 * file operations
 */
extern FILE *pgut_fopen(const char *path, const char *mode, bool missing_ok);
extern void pgut_mkdir(const char *path);

/*
 * elog
 */
#define LOG			(-5)
#define INFO		(-4)
#define NOTICE		(-3)
#define WARNING		(-2)
#define ALERT		(-1)
#define HELP		1
#define ERROR		2
#define FATAL		3
#define PANIC		4

#define ERROR_SYSTEM			10	/* I/O or system error */
#define ERROR_NOMEM				11	/* memory exhausted */
#define ERROR_ARGS				12	/* some configurations are invalid */
#define ERROR_INTERRUPTED		13	/* interrupted by signal */
#define ERROR_PG_COMMAND		14	/* PostgreSQL query or command error */
#define ERROR_PG_CONNECT		15	/* PostgreSQL connection error */

#undef elog
extern void
elog(int elevel, const char *fmt, ...)
__attribute__((format(printf, 2, 3)));

#undef CHECK_FOR_INTERRUPTS
extern void CHECK_FOR_INTERRUPTS(void);

/*
 * Assert
 */
#undef Assert
#undef AssertArg
#undef AssertMacro

#ifdef USE_ASSERT_CHECKING
#define Assert(x)		assert(x)
#define AssertArg(x)	assert(x)
#define AssertMacro(x)	assert(x)
#else
#define Assert(x)		((void) 0)
#define AssertArg(x)	((void) 0)
#define AssertMacro(x)	((void) 0)
#endif

/*
 * StringInfo and string operations
 */
#define STRINGINFO_H

#define StringInfoData			PQExpBufferData
#define StringInfo				PQExpBuffer
#define makeStringInfo			createPQExpBuffer
#define initStringInfo			initPQExpBuffer
#define freeStringInfo			destroyPQExpBuffer
#define termStringInfo			termPQExpBuffer
#define resetStringInfo			resetPQExpBuffer
#define enlargeStringInfo		enlargePQExpBuffer
#define printfStringInfo		printfPQExpBuffer	/* reset + append */
#define appendStringInfo		appendPQExpBuffer
#define appendStringInfoString	appendPQExpBufferStr
#define appendStringInfoChar	appendPQExpBufferChar
#define appendBinaryStringInfo	appendBinaryPQExpBuffer

extern int appendStringInfoFile(StringInfo str, FILE *fp);
extern int appendStringInfoFd(StringInfo str, int fd);

extern bool parse_bool(const char *value, bool *result);
extern bool parse_bool_with_len(const char *value, size_t len, bool *result);
extern bool parse_int32(const char *value, int32 *result);
extern bool parse_uint32(const char *value, uint32 *result);
extern bool parse_int64(const char *value, int64 *result);
extern bool parse_uint64(const char *value, uint64 *result);
extern bool parse_time(const char *value, time_t *time);

#define IsSpace(c)		(isspace((unsigned char)(c)))
#define IsAlpha(c)		(isalpha((unsigned char)(c)))
#define IsAlnum(c)		(isalnum((unsigned char)(c)))
#define IsIdentHead(c)	(IsAlpha(c) || (c) == '_')
#define IsIdentBody(c)	(IsAlnum(c) || (c) == '_')
#define ToLower(c)		(tolower((unsigned char)(c)))
#define ToUpper(c)		(toupper((unsigned char)(c)))

/*
 * socket operations
 */
extern int wait_for_socket(int sock, struct timeval *timeout);
extern int wait_for_sockets(int nfds, fd_set *fds, struct timeval *timeout);

/*
 * import from postgres.h and catalog/genbki.h in 8.4
 */
#if PG_VERSION_NUM < 80400

typedef unsigned long Datum;
typedef struct MemoryContextData *MemoryContext;

#define CATALOG(name,oid)	typedef struct CppConcat(FormData_,name)
#define BKI_BOOTSTRAP
#define BKI_SHARED_RELATION
#define BKI_WITHOUT_OIDS
#define DATA(x)   extern int no_such_variable
#define DESCR(x)  extern int no_such_variable
#define SHDESCR(x) extern int no_such_variable
typedef int aclitem;

#endif

#ifdef WIN32
extern int sleep(unsigned int seconds);
extern int usleep(unsigned int usec);
#endif

#endif   /* PGUT_H */
