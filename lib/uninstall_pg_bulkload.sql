/*
 * pg_bulkload: uninstall_pg_bulkload.sql
 *
 *    Copyright (c) 2007-2021, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

DROP FUNCTION pgbulkload.pg_bulkload(text[]);
DROP SCHEMA pgbulkload;
