/*
 * pg_bulkload: util/uninstall_pg_timestamp.sql
 *
 *    Copyright (c) 2007-2011, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

BEGIN;

UPDATE pg_type
   SET typinput = (SELECT oid FROM pg_proc WHERE proname='timestamp_in')
 WHERE typname='timestamp';

DROP FUNCTION public.pg_timestamp_in(cstring, oid, int4);

COMMIT;
