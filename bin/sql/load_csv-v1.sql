\! pg_bulkload -d contrib_regression data/csv1.ctl -o"delimiter=|" -i data/data1.csv -l results/csv1.log -P results/csv1.prs -u results/csv1.dup -o "PARSE_ERRORS=3" -o "VERBOSE=YES" -o "TRUNCATE=TRUE"
\! awk -f data/adjust.awk results/csv1.log

SET enable_seqscan = on;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT * FROM customer ORDER BY c_id;

SET enable_seqscan = off;
SET enable_indexscan = on;
SET enable_bitmapscan = off;
SELECT * FROM customer ORDER BY c_id;

\! pg_bulkload -d contrib_regression data/csv2.ctl -o"delimiter=|" -i data/data1.csv -l results/csv1-2.log -P results/csv1-2.prs -u results/csv1-2.dup -o "SKIP=9" -o "LOAD=2"
\! awk -f data/adjust.awk results/csv1-2.log

SET enable_seqscan = on;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT * FROM customer ORDER BY c_id;

SET enable_seqscan = off;
SET enable_indexscan = on;
SET enable_bitmapscan = off;
SELECT * FROM customer ORDER BY c_id;

\! pg_bulkload -d contrib_regression data/csv1.ctl -i data/data2.csv -l results/csv2.log -P results/csv2.prs -u results/csv2.dup -o "PARSE_ERRORS=50"
\! awk -f data/adjust.awk results/csv2.log

SET enable_seqscan = on;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT * FROM customer ORDER BY c_id;

SET enable_seqscan = off;
SET enable_indexscan = on;
SET enable_bitmapscan = off;
SELECT * FROM customer ORDER BY c_id;

\! pg_bulkload -d contrib_regression data/csv1.ctl -i data/data2.csv -o "DUPLICATE_ERRORS=50" -l results/csv3.log -P results/csv3.prs -u results/csv3.dup -o "PARSE_ERRORS=50"
\! awk -f data/adjust.awk results/csv3.log

SET enable_seqscan = on;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT * FROM customer ORDER BY c_id;

SET enable_seqscan = off;
SET enable_indexscan = on;
SET enable_bitmapscan = off;
SELECT * FROM customer ORDER BY c_id;

\! pg_bulkload -d contrib_regression data/csv2.ctl -i data/data2.csv -o "SKIP=2" -o "LOAD=4" -o "PARSE_ERRORS=0" -o "VERBOSE=YES" -l results/csv4.log -P results/csv4.prs -u results/csv4.dup
\! awk -f data/adjust.awk results/csv4.log

\! pg_bulkload -d contrib_regression data/csv2.ctl -i data/data2.csv -o "SKIP=2" -o "LOAD=4" -o "PARSE_ERRORS=10" -o "VERBOSE=YES" -l results/csv5.log -P results/csv5.prs -u results/csv5.dup
\! awk -f data/adjust.awk results/csv5.log

SET enable_seqscan = on;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT * FROM customer ORDER BY c_id;

SET enable_seqscan = off;
SET enable_indexscan = on;
SET enable_bitmapscan = off;
SELECT * FROM customer ORDER BY c_id;

-- do not error skip an error after of toast_insert_or_update.
\! pg_bulkload -d contrib_regression data/csv2.ctl -i data/data3.csv -o "SKIP=1" -o "LOAD=4" -o "VERBOSE=YES" -l results/csv6.log -P results/csv6.prs -u results/csv6.dup
\! awk -f data/adjust.awk results/csv6.log

\! pg_bulkload -d contrib_regression data/csv2.ctl -i data/data3.csv -o "SKIP=2" -o "LOAD=4" -o "VERBOSE=YES" -l results/csv7.log -P results/csv7.prs -u results/csv7.dup -o "PARSE_ERRORS=50"
\! awk -f data/adjust.awk results/csv7.log

SET enable_seqscan = on;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT * FROM customer ORDER BY c_id;

SET enable_seqscan = off;
SET enable_indexscan = on;
SET enable_bitmapscan = off;
SELECT * FROM customer ORDER BY c_id;

\! diff data/data3.csv results/csv7.prs

-- test whether multiline field columns can be loaded.
\! pg_bulkload -d contrib_regression data/csv2.ctl -i data/data8.csv -o "VERBOSE=YES" -l results/csv8.log -P results/csv8.prs -u results/csv8.dup
\! awk -f data/adjust.awk results/csv8.log

SET enable_seqscan = on;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT * FROM customer ORDER BY c_id;

SET enable_seqscan = off;
SET enable_indexscan = on;
SET enable_bitmapscan = off;
SELECT * FROM customer ORDER BY c_id;

-- test import duplicate data
\! pg_bulkload -d contrib_regression data/csv10.ctl -i data/data11.csv -o "VERBOSE=YES" -o "DELIMITER=," -l results/csv10.log -P results/csv10.prs -u results/csv10.dup
\! pg_bulkload -d contrib_regression data/csv10.ctl -i data/data11.csv -o "VERBOSE=YES" -o "DELIMITER=," -l results/csv10.log -P results/csv10.prs -u results/csv10.dup
\! pg_bulkload -d contrib_regression data/csv10.ctl -i data/data11.csv -o "VERBOSE=YES" -o "DELIMITER=," -l results/csv10.log -P results/csv10.prs -u results/csv10.dup