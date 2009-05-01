/*
 * pg_bulkload: lib/uninstall_pg_bulkload.sql
 *
 *    Copyright(C) 2007-2009, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

SET search_path = public;

DROP FUNCTION pg_bulkload(text);
DROP FUNCTION pg_bulkload(text, text);

DROP FUNCTION pg_bulkread(text);
DROP FUNCTION pg_bulkread(text, text);
DROP FUNCTION pg_bulkread(text, text, anyelement);

DROP AGGREGATE pg_bulkwrite(regclass, record);
DROP FUNCTION pg_bulkwrite_accum(internal, regclass, record);
DROP FUNCTION pg_bulkwrite_finish(internal);
