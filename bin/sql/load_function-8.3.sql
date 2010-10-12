TRUNCATE customer;
\pset null '(null)'

CREATE FUNCTION public.load_function1(int4, int4, varchar(500)) RETURNS SETOF customer AS
$$
	SELECT generate_series($1, $2), '216'::int2, '0'::int4, 'ABCDEFG         '::varchar(16), 'AA'::char(2), 'AAAAAAAAAAAAAAAA'::varchar(16), 'c_street_1          '::varchar(20), 'c_street_2          '::varchar(20), 'AAAAAAAAAAAAAAAAAAAA'::varchar(20), 'AA'::char(2), 'AAAAAAAAA'::char(9), 'AAAAAAAAAAAAAAAA'::char(16), '2006-01-01 12:34:56'::timestamp, 'AA'::char(2), '12345.6789'::numeric(16,4), '12345.6789'::numeric(16,4), '12345.6789'::numeric(16,4), '12345.6789'::numeric(16,4), '12345.6789'::float4, '12345.6789'::float8, $3;
$$ LANGUAGE SQL;

\! pg_bulkload -d contrib_regression data/function1.ctl -o "INFILE=public.load_function(1,5,'A''')" -l results/function_e.log
\! pg_bulkload -d contrib_regression data/function1.ctl -o "INFILE=public.load_function1(1,5,'A''','B')" -l results/function_e.log
\! pg_bulkload -d contrib_regression data/function1.ctl -o "INFILE=public.load_function1('A',5,'A''')" -l results/function_e.log
\! pg_bulkload -d contrib_regression data/function1.ctl -o "INFILE=public.load_function1(1,5,'A'')" -l results/function_e.log
\! pg_bulkload -d contrib_regression data/function1.ctl -o "INFILE=public.load_function1(1,5,'A'''" -l results/function_e.log
\! pg_bulkload -d contrib_regression data/function1.ctl -o "INFILE=public.load_function1(1,5 'A''')" -l results/function_e.log
\! pg_bulkload -d contrib_regression data/function1.ctl -o "INFILE=public.load_function1(1,5,'A''',1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1)" -l results/function_e.log
\! pg_bulkload -d contrib_regression data/function1.ctl -o "INFILE=public.load_function1(1,5,A)" -l results/function_e.log
\! pg_bulkload -d contrib_regression data/function1.ctl -o "INFILE=public.load_function1(1,--5,'A''')" -l results/function_e.log
\! pg_bulkload -d contrib_regression data/function1.ctl -o "INFILE=pg_catalog.to_char('1','0')" -l results/function_e.log
\! pg_bulkload -d contrib_regression data/function1.ctl -o "INFILE=pg_catalog.lower('A')" -l results/function_e.log

\! pg_bulkload -d contrib_regression data/function1.ctl -o "INFILE=public.load_function1(1,5,'A''')" -l results/function1.log -P results/function1.prs -u results/function1.dup -o LOAD=3
\! awk -f data/adjust.awk results/function1.log

SET enable_seqscan = on;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT * FROM customer ORDER BY c_id;

SET enable_seqscan = off;
SET enable_indexscan = on;
SET enable_bitmapscan = off;
SELECT * FROM customer ORDER BY c_id;

\! pg_bulkload -d contrib_regression data/function1.ctl -o "INFILE=public.load_function1( - - - 3, -+- 5,'''B')" -l results/function2.log -P results/function2.prs -u results/function2.dup -o "ON_DUPLICATE_KEEP=OLD" -o "DUPLICATE_ERRORS=50"
\! awk -f data/adjust.awk results/function2.log

SET enable_seqscan = on;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT * FROM customer ORDER BY c_id;

SET enable_seqscan = off;
SET enable_indexscan = on;
SET enable_bitmapscan = off;
SELECT * FROM customer ORDER BY c_id;

\! pg_bulkload -d contrib_regression data/function1.ctl -o "INFILE=public	.	load_function1	 (	 3	 ,	 '7'	 ,	 nUlL	 )	 " -l results/function3.log -P results/function3.prs -u results/function3.dup -o "DUPLICATE_ERRORS=50" -o "PARSE_ERRORS=3"
\! awk -f data/adjust.awk results/function3.log

SET enable_seqscan = on;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT * FROM customer ORDER BY c_id;

SET enable_seqscan = off;
SET enable_indexscan = on;
SET enable_bitmapscan = off;
SELECT * FROM customer ORDER BY c_id;

\! pg_bulkload -d contrib_regression data/csv3.ctl -i results/function3.prs -l results/function4.log -P results/function4.prs -u results/function4.dup -o "DUPLICATE_ERRORS=50"
\! awk -f data/adjust.awk results/function4.log

\! pg_bulkload -d contrib_regression data/csv3.ctl -i results/function3.prs -l results/function5.log -P results/function5.prs -u results/function5.dup -o "DUPLICATE_ERRORS=50" -o "NULL=NULL"
\! awk -f data/adjust.awk results/function5.log

SET enable_seqscan = on;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT * FROM customer ORDER BY c_id;

SET enable_seqscan = off;
SET enable_indexscan = on;
SET enable_bitmapscan = off;
SELECT * FROM customer ORDER BY c_id;

ALTER TABLE customer ALTER c_data DROP NOT NULL;
\! pg_bulkload -d contrib_regression data/function1.ctl -o "INFILE=public . load_function1	 (	 3	 ,	 '7'	 ,	 nUlL	 )	 " -l results/function6.log -P results/function6.prs -u results/function6.dup -o "DUPLICATE_ERRORS=50"
\! awk -f data/adjust.awk results/function6.log

SET enable_seqscan = on;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT * FROM customer ORDER BY c_id;

SET enable_seqscan = off;
SET enable_indexscan = on;
SET enable_bitmapscan = off;
SELECT * FROM customer ORDER BY c_id;

\! pg_bulkload -d contrib_regression data/csv3.ctl -i results/function6.dup -o "SKIP=1" -l results/function10.log -P results/function10.prs -u results/function10.dup -o "DUPLICATE_ERRORS=50"
\! awk -f data/adjust.awk results/function10.log

SET enable_seqscan = on;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT * FROM customer ORDER BY c_id;

SET enable_seqscan = off;
SET enable_indexscan = on;
SET enable_bitmapscan = off;
SELECT * FROM customer ORDER BY c_id;

TRUNCATE customer;
\! pg_bulkload -d contrib_regression data/function1.ctl -o "INFILE=public.load_function1(1, 2147483647, 'LOAD=1')" -l results/function11.log -P results/function11.prs -u results/function11.dup -o "DUPLICATE_ERRORS=50" -o LOAD=1
\set LOAD1 `awk -f data/gettime.awk results/function11.log`
TRUNCATE customer;
\! pg_bulkload -d contrib_regression data/function1.ctl -o "INFILE=public.load_function1(1, 1000, 'LOAD=1000')" -l results/function12.log -P results/function12.prs -u results/function12.dup -o "DUPLICATE_ERRORS=50" -o LOAD=1000
\set LOAD1000 `awk -f data/gettime.awk results/function12.log`
SELECT :LOAD1 < :LOAD1000 AS "LOAD1 is fast";

\! pg_bulkload -d contrib_regression data/csv3.ctl -i results/function6.dup -o "SKIP=1" -l results/function13.log -P results/function13.prs -u results/function13.dup -o "DUPLICATE_ERRORS=0"
\! awk -f data/adjust.awk results/function13.log

TRUNCATE customer;
\! pg_bulkload -d contrib_regression data/csv3.ctl -i results/function6.dup -l results/function14.log -P results/function14.prs -u results/function14.dup -o "ON_DUPLICATE_KEEP=OLD" -o "DUPLICATE_ERRORS=1"
\! awk -f data/adjust.awk results/function14.log

\! pg_bulkload -d contrib_regression data/csv3.ctl -i results/function6.dup -l results/function14.log -P results/function14.prs -u results/function14.dup -o "ON_DUPLICATE_KEEP=OLD" -o "DUPLICATE_ERRORS=3"
\! awk -f data/adjust.awk results/function14.log
SET enable_seqscan = on;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT * FROM customer ORDER BY c_id;

SET enable_seqscan = off;
SET enable_indexscan = on;
SET enable_bitmapscan = off;
SELECT * FROM customer ORDER BY c_id;

ALTER TABLE customer ALTER c_data SET NOT NULL;
