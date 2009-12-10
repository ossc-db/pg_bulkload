\set BEFORE_NSHM `ipcs -m | grep -c [0-9]`
TRUNCATE customer;

\! pg_bulkload -d contrib_regression data/csv1.ctl -o"delimiter=|" -i data/data1.csv -o "WRITER=PARALLEL" -l results/parallel1.log -P results/parallel1.prs -u results/parallel1.dup
\! awk -f data/adjust.awk results/parallel1.log

SET enable_seqscan = on;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT * FROM customer ORDER BY c_id;

SET enable_seqscan = off;
SET enable_indexscan = on;
SET enable_bitmapscan = off;
SELECT * FROM customer ORDER BY c_id;

\! pg_bulkload -d contrib_regression data/csv1.ctl -i data/data2.csv -o "WRITER=PARALLEL" -l results/parallel2.log -P results/parallel2.prs -u results/parallel2.dup
\! awk -f data/adjust.awk results/parallel2.log

SET enable_seqscan = on;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT * FROM customer ORDER BY c_id;

SET enable_seqscan = off;
SET enable_indexscan = on;
SET enable_bitmapscan = off;
SELECT * FROM customer ORDER BY c_id;

\! pg_bulkload -d contrib_regression data/csv1.ctl -i data/data2.csv -o "WRITER=PARALLEL" -o "ON_DUPLICATE=REMOVE_OLD" -l results/parallel3.log -P results/parallel3.prs -u results/parallel3.dup
\! awk -f data/adjust.awk results/parallel3.log

SET enable_seqscan = on;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT * FROM customer ORDER BY c_id;

SET enable_seqscan = off;
SET enable_indexscan = on;
SET enable_bitmapscan = off;
SELECT * FROM customer ORDER BY c_id;

\! pg_bulkload -d contrib_regression data/csv1.ctl -i data/data2.csv -o "WRITER=PARALLEL" -o "PARSE_ERRORS=0" -l results/parallel4.log -P results/parallel4.prs -u results/parallel4.dup
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
