/*
 * pgut.h
 *
 * Copyright (c) 2009, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

#ifndef PGUT_H
#define PGUT_H

#include "libpq-fe.h"
#include <getopt.h>

/*
 * pgut client variables and functions
 */
extern const char		   *pgut_optstring;
extern const struct option	pgut_longopts[];

extern pqbool	pgut_argument(int c, const char *arg);
extern int		pgut_help(void);
extern int		pgut_version(void);
extern void		pgut_cleanup(void);

/*
 * pgut framework variables and functions
 */
extern const char  *progname;
extern const char  *dbname;
extern char		   *host;
extern char		   *port;
extern char		   *username;
extern pqbool		password;
extern pqbool		interrupted;
extern PGconn	   *current_conn;

extern int	pgut_getopt(int argc, char **argv);

extern void reconnect(void);
extern void disconnect(void);
extern PGresult *execute_nothrow(const char *query, int nParams, const char **params);
extern PGresult *execute(const char *query, int nParams, const char **params);
extern void command(const char *query, int nParams, const char **params);

#endif   /* PGUT_H */
