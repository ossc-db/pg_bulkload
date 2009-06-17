/*
 * pg_bulkload: uninstall_pg_bulkload.sql
 *
 *    Copyright(C) 2007-2009, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

SET search_path = public;

DROP FUNCTION pg_bulkload(text);
DROP FUNCTION pg_bulkload(text, text);
