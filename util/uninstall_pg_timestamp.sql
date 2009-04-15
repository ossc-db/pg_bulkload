/*
 * pg_bulkload: util/uninstall_pg_timestamp.sql
 *
 *    Copyright(C) 2007-2009, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

SET search_path = public;

BEGIN;

UPDATE pg_type
   SET typinput = (SELECT oid FROM pg_proc WHERE proname='timestamp_in')
 WHERE typname='timestamp';

DROP FUNCTION pg_timestamp_in(cstring, oid, int4);

COMMIT;
