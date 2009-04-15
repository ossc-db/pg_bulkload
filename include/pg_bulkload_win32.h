/*
 * pg_bulkload: include/pg_bulkload_win32.h
 *
 *	  Copyright(C) 2007-2009, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */
#ifndef BULKLOAD_WIN32_H_INCLUDED
#define BULKLOAD_WIN32_H_INCLUDED

/*
 * Windows compatibility issues.
 */

#ifdef WIN32
#ifndef PGDLLIMPORT
#define PGDLLIMPORT	DLLIMPORT
#endif
extern PGDLLIMPORT TimeLineID		ThisTimeLineID; 
extern PGDLLIMPORT TransactionId	RecentXmin;
extern PGDLLIMPORT bool				XLogArchiveMode;
extern PGDLLIMPORT char			   *XLogArchiveCommand;
extern PGDLLIMPORT volatile bool	QueryCancelPending;
extern PGDLLIMPORT volatile bool	ProcDiePending;
#endif

#endif   /* BULKLOAD_WIN32_H_INCLUDED */
