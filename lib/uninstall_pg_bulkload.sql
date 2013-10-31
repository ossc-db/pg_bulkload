/*
 * pg_bulkload: uninstall_pg_bulkload.sql
 *
 *    Copyright (c) 2007-2011, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

SET search_path = public;

DROP FUNCTION pg_bulkload(text[]);
