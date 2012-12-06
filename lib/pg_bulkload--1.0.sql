/* pg_bulkload/pg_bulkload--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_bulkload" to load this file. \quit

-- Adjust this setting to control where the objects get created.
CREATE FUNCTION pg_bulkload(
	IN options text[],
	OUT skip bigint,
	OUT count bigint,
	OUT parse_errors bigint,
	OUT duplicate_new bigint,
	OUT duplicate_old bigint,
	OUT system_time float8,
	OUT user_time float8,
	OUT duration float8
)
AS '$libdir/pg_bulkload', 'pg_bulkload' LANGUAGE C VOLATILE STRICT;
