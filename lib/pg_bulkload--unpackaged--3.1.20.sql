/* pg_bulkload/pg_bulkload--unpackaged--3.1.20.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_bulkload" to load this file. \quit

ALTER EXTENSION pg_bulkload ADD FUNCTION pgbulkload.pg_bulkload(text[]);
