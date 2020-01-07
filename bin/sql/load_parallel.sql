SET extra_float_digits = 0;
\set BEFORE_NSHM `ipcs -m | grep -c [0-9]`
TRUNCATE customer;

\! pg_bulkload -d contrib_regression data/csv1.ctl -o"delimiter=|" -i data/data1.csv -o "MULTI_PROCESS=YES" -l results/parallel1.log -P results/parallel1.prs -u results/parallel1.dup -o "PARSE_ERRORS=50"
\! awk -f data/adjust.awk results/parallel1.log

SET enable_seqscan = on;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT * FROM customer ORDER BY c_id;

SET enable_seqscan = off;
SET enable_indexscan = on;
SET enable_bitmapscan = off;
SELECT * FROM customer ORDER BY c_id;

\! pg_bulkload -d contrib_regression data/csv1.ctl -i data/data2.csv -o "MULTI_PROCESS=YES" -l results/parallel2.log -P results/parallel2.prs -u results/parallel2.dup -o "PARSE_ERRORS=50"
\! awk -f data/adjust.awk results/parallel2.log

SET enable_seqscan = on;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT * FROM customer ORDER BY c_id;

SET enable_seqscan = off;
SET enable_indexscan = on;
SET enable_bitmapscan = off;
SELECT * FROM customer ORDER BY c_id;

\! pg_bulkload -d contrib_regression data/csv1.ctl -i data/data2.csv -o "MULTI_PROCESS=YES" -o "WRITER=BUFFERED" -o "DUPLICATE_ERRORS=50" -l results/parallel3.log -P results/parallel3.prs -u results/parallel3.dup -o "PARSE_ERRORS=50"
\! awk -f data/adjust.awk results/parallel3.log

SET enable_seqscan = on;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT * FROM customer ORDER BY c_id;

SET enable_seqscan = off;
SET enable_indexscan = on;
SET enable_bitmapscan = off;
SELECT * FROM customer ORDER BY c_id;

\! pg_bulkload -d contrib_regression data/csv1.ctl -i data/data2.csv -o "MULTI_PROCESS=YES" -o "WRITER=DIRECT" -o "PARSE_ERRORS=0" -l results/parallel4.log -P results/parallel4.prs -u results/parallel4.dup
\! awk -f data/adjust.awk results/parallel4.log

SET enable_seqscan = on;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT * FROM customer ORDER BY c_id;

SET enable_seqscan = off;
SET enable_indexscan = on;
SET enable_bitmapscan = off;
SELECT * FROM customer ORDER BY c_id;

\set AFTER_NSHM `ipcs -m | grep -c [0-9]`
SELECT :AFTER_NSHM - :BEFORE_NSHM as "not destroy shared memorys";

CREATE TABLE public.foo (
    a char(2010),
    b text
);
CREATE INDEX foo_a ON public.foo(a);
CREATE INDEX foo_b ON public.foo(b);
\copy (SELECT 'a', 'aaaaa' FROM generate_series(1,10000) t(i)) to results/data_lost.csv csv
\copy (SELECT 'a', CASE WHEN i % 8192 = 0 THEN 'aaaaaaaaa' ELSE 'aaaaa' END FROM generate_series(1,81920) t(i)) to results/mem_error.csv csv
\! pg_bulkload -d contrib_regression -i results/data_lost.csv -O public.foo -o MULTI_PROCESS=YES -o TRUNCATE=YES -l results/parallel5.log
SET enable_seqscan = on;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT count(*) FROM public.foo WHERE b IS NOT NULL;

SET enable_seqscan = off;
SET enable_indexscan = on;
SET enable_bitmapscan = off;
SELECT count(*) FROM public.foo WHERE b IS NOT NULL;

\! pg_bulkload -d contrib_regression -i results/mem_error.csv -O public.foo -o MULTI_PROCESS=YES -o TRUNCATE=YES -l results/parallel6.log
SET enable_seqscan = on;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT count(*) FROM public.foo WHERE b IS NOT NULL;

SET enable_seqscan = off;
SET enable_indexscan = on;
SET enable_bitmapscan = off;
SELECT count(*) FROM public.foo WHERE b IS NOT NULL;

CREATE TABLE public.bar (
    a char(10000000),
    b char(6777177),
    c text
);
CREATE INDEX bar_c ON public.bar(c);
\copy (SELECT 'a', 'a', 'aa' FROM generate_series(1,10) t(i)) to results/size_over.csv csv
\copy (SELECT 'a', 'a', 'a' FROM generate_series(1,10) t(i)) to results/size_limit.csv csv
\! pg_bulkload -d contrib_regression -i results/size_over.csv -O public.bar -o MULTI_PROCESS=YES -o TRUNCATE=YES -l results/parallel7.log
SET enable_seqscan = on;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT count(*) FROM public.bar WHERE b IS NOT NULL;

SET enable_seqscan = off;
SET enable_indexscan = on;
SET enable_bitmapscan = off;
SELECT count(*) FROM public.bar WHERE b IS NOT NULL;

\! pg_bulkload -d contrib_regression -i results/size_limit.csv -O public.bar -o MULTI_PROCESS=YES -o TRUNCATE=YES -l results/parallel8.log
SET enable_seqscan = on;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT count(*) FROM public.bar WHERE b IS NOT NULL;

SET enable_seqscan = off;
SET enable_indexscan = on;
SET enable_bitmapscan = off;
SELECT count(*) FROM public.bar WHERE b IS NOT NULL;
