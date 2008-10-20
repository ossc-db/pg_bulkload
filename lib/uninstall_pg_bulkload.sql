/*
 * pg_bulkload: lib/uninstall_pg_bulkload.sql
 *
 *    Copyright(C) 2007-2008 NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

SET search_path = public;

BEGIN;

DROP FUNCTION pg_bulkload(text);
DROP FUNCTION pg_bulkload_trigger_init();
DROP FUNCTION pg_bulkload_trigger_main();
DROP FUNCTION pg_bulkload_trigger_term();

COMMIT;
