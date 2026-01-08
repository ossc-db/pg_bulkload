/*-------------------------------------------------------------------------
 *
 * pgut-list.h
 *
 * Copyright (c) 2009-2026, NTT, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef PGUT_LIST_H
#define PGUT_LIST_H

#if PG_VERSION_NUM >= 170000
#include "utils/palloc.h"
#endif
#include "nodes/pg_list.h"

/*
 * pgut-list.c : Extended list functions
 */
extern void list_walk(List *list, void (*walker)());
extern void list_destroy(List *list, void (*walker)());

#endif /* PGUT_LIST_H */
