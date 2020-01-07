SET extra_float_digits = 0;
TRUNCATE customer;

\! pg_bulkload -d contrib_regression data/bin1.ctl -i infile_logfile -l infile_logfile -P pbfile -u dbfile
\! pg_bulkload -d contrib_regression data/bin1.ctl -i infile_pbfile -l logfile -P infile_pbfile -u dbfile
\! pg_bulkload -d contrib_regression data/bin1.ctl -i infile_dbfile -l logfile -P pbfile -u infile_dbfile
\! pg_bulkload -d contrib_regression data/bin1.ctl -i infile -l logfile_pbfile -P logfile_pbfile -u dbfile
\! pg_bulkload -d contrib_regression data/bin1.ctl -i infile -l logfile_dbfile -P pbfile -u logfile_dbfile
\! pg_bulkload -d contrib_regression data/bin1.ctl -i infile -l logfile -P pbfile_dbfile -u pbfile_dbfile

TRUNCATE customer;
SET enable_seqscan = off;
SET enable_indexscan = on;
SET enable_bitmapscan = off;
SELECT * FROM customer ORDER BY c_id;

\! pg_bulkload -d contrib_regression data/bin1.ctl -i data/data1.bin -l results/bin1.log -P results/bin1.prs -u results/bin1.dup
\! awk -f data/adjust.awk results/bin1.log

SET enable_seqscan = on;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT * FROM customer ORDER BY c_id;

SET enable_seqscan = off;
SET enable_indexscan = on;
SET enable_bitmapscan = off;
SELECT * FROM customer ORDER BY c_id;

\! pg_bulkload -d contrib_regression data/bin1.ctl -i data/data2.bin -l results/bin2.log -P results/bin2.prs -u results/bin2.dup
\! awk -f data/adjust.awk results/bin2.log

SET enable_seqscan = on;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT * FROM customer ORDER BY c_id;

SET enable_seqscan = off;
SET enable_indexscan = on;
SET enable_bitmapscan = off;
SELECT * FROM customer ORDER BY c_id;

UPDATE customer SET c_data = 'OLD1';
\! pg_bulkload -d contrib_regression data/bin2.ctl -o "SKIP=2" -o "LOAD=4" -o "VERBOSE=YES"
\! awk -f data/adjust.awk results/bin3.log

SET enable_seqscan = on;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT * FROM customer ORDER BY c_id;

SET enable_seqscan = off;
SET enable_indexscan = on;
SET enable_bitmapscan = off;
SELECT * FROM customer ORDER BY c_id;

UPDATE customer SET c_data = 'OLD2';
\! pg_bulkload -d contrib_regression data/csv3.ctl -i results/bin3.dup -o "DUPLICATE_ERRORS=50" -l results/bin4.log -P results/bin4.prs -u results/bin4.dup
\! awk -f data/adjust.awk results/bin4.log

SET enable_seqscan = on;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT * FROM customer ORDER BY c_id;

SET enable_seqscan = off;
SET enable_indexscan = on;
SET enable_bitmapscan = off;
SELECT * FROM customer ORDER BY c_id;

UPDATE customer SET c_data = 'OLD3';
\! pg_bulkload -d contrib_regression data/csv3.ctl -i results/bin4.dup -o "ON_DUPLICATE_KEEP=OLD" -o "DUPLICATE_ERRORS=50" -l results/bin5.log -P results/bin5.prs -u results/bin5.dup
\! awk -f data/adjust.awk results/bin5.log

SET enable_seqscan = on;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT * FROM customer ORDER BY c_id;

SET enable_seqscan = off;
SET enable_indexscan = on;
SET enable_bitmapscan = off;
SELECT * FROM customer ORDER BY c_id;

-- do not error skip an error after of toast_insert_or_update.
\! pg_bulkload -d contrib_regression data/bin3.ctl -i data/data3.bin -o "SKIP=3" -o "DUPLICATE_ERRORS=50" -o "LOAD=4" -o "VERBOSE=YES" -l results/bin6.log -P results/bin6.prs -u results/bin6.dup
\! awk -f data/adjust.awk results/bin6.log

\! pg_bulkload -d contrib_regression data/bin3.ctl -i data/data3.bin -o "SKIP=4" -o "DUPLICATE_ERRORS=50" -o "PARSE_ERRORS=50" -o "LOAD=4" -o "VERBOSE=YES" -l results/bin7.log -P results/bin7.prs -u results/bin7.dup
\! awk -f data/adjust.awk results/bin7.log

SET enable_seqscan = on;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT * FROM customer ORDER BY c_id;

SET enable_seqscan = off;
SET enable_indexscan = on;
SET enable_bitmapscan = off;
SELECT * FROM customer ORDER BY c_id;

\! diff data/data3.bin results/bin7.prs

\! pg_bulkload -d contrib_regression data/bin4.ctl -i data/data1.bin -l results/bin8.log -P pbfile -u dbfile -o "PARSE_ERRORS=-1" -o "DUPLICATE_ERRORS=-1"
\! awk -f data/adjust.awk results/bin8.log
\! pg_bulkload -d contrib_regression data/bin4.ctl -i data/data1.bin -l results/bin9.log -P pbfile -u dbfile -o "PARSE_ERRORS=INFINITE" -o "DUPLICATE_ERRORS=INFINITE"
\! awk -f data/adjust.awk results/bin9.log

\! echo -n "" | pg_bulkload -d contrib_regression data/bin5.ctl -l results/bin10.log -o PRESERVE_BLANKS=YES -o "COL=10"
\! awk -f data/adjust.awk results/bin10.log

\! echo -n "" | pg_bulkload -d contrib_regression data/bin5.ctl -l results/bin11.log -o PRESERVE_BLANKS=NO -o "COL=10"
\! awk -f data/adjust.awk results/bin11.log

\! echo -n "" | pg_bulkload -d contrib_regression data/bin5.ctl -l results/bin_error.log -o "COL=SHORT NULLIF abcg"
\! echo -n "" | pg_bulkload -d contrib_regression data/bin5.ctl -l results/bin_error.log -o PRESERVE_BLANKS=NO -o "COL=10 aaaa"
\! echo -n "" | pg_bulkload -d contrib_regression data/bin5.ctl -l results/bin_error.log -o "COL=SHORT (2) NULLIF abcd aaaa"
\! echo -n "" | pg_bulkload -d contrib_regression data/bin5.ctl -l results/bin_error.log -o "COL=SHORT NULLIF abcd aaaa"
\! echo -n "" | pg_bulkload -d contrib_regression data/bin5.ctl -l results/bin_error.log -o "COL=SHORT (2) aaaa"
\! echo -n "" | pg_bulkload -d contrib_regression data/bin5.ctl -l results/bin_error.log -o "COL=SHORT aaaa"

-- load from stdin
\! pg_bulkload -d contrib_regression data/bin1.ctl -i Stdin -l results/bin12.log -P results/bin12.prs -u results/bin12.dup -o TRUNCATE=YES -o ENCODING=SQLASCII < data/data1.bin
\! awk -f data/adjust.awk results/bin12.log

SET enable_seqscan = on;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT * FROM customer ORDER BY c_id;

SET enable_seqscan = off;
SET enable_indexscan = on;
SET enable_bitmapscan = off;
SELECT * FROM customer ORDER BY c_id;

\! pg_bulkload -d contrib_regression data/bin1.ctl -i Stdin -l results/bin13.log -P results/bin13.prs -u results/bin13.dup -o TRUNCATE=YES -o ENCODING=SQLASCII < data/data1.bin
\! awk -f data/adjust.awk results/bin13.log

SET enable_seqscan = on;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT * FROM customer ORDER BY c_id;

SET enable_seqscan = off;
SET enable_indexscan = on;
SET enable_bitmapscan = off;
SELECT * FROM customer ORDER BY c_id;
