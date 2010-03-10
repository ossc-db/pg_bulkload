\! pg_bulkload -d contrib_regression data/csv4.ctl -o "DUPLICATE_ERRORS=50" -i data/data5.csv -l results/check1.log -P results/check1.prs -u results/check1.dup -o "PARSE_ERRORS=-1"
\! awk -f data/adjust.awk results/check1.log

SET enable_seqscan = on;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT * FROM target ORDER BY id;

SET enable_seqscan = off;
SET enable_indexscan = on;
SET enable_bitmapscan = off;
SELECT * FROM target ORDER BY id;

\! pg_bulkload -d contrib_regression data/csv4.ctl -i data/data5.csv -l results/check2.log -P results/check2.prs -u results/check2.dup -o "PARSE_ERRORS=-1" -o CHECK_CONSTRAINTS=O
\! awk -f data/adjust.awk results/check2.log

SET enable_seqscan = on;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT * FROM target ORDER BY id;

SET enable_seqscan = off;
SET enable_indexscan = on;
SET enable_bitmapscan = off;
SELECT * FROM target ORDER BY id;

\! pg_bulkload -d contrib_regression data/csv4.ctl -o "DUPLICATE_ERRORS=50" -i data/data5.csv -l results/check3.log -P results/check3.prs -u results/check3.dup -o "PARSE_ERRORS=2" -o CHECK_CONSTRAINTS=NO
\! awk -f data/adjust.awk results/check3.log

SET enable_seqscan = on;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT * FROM target ORDER BY id;

SET enable_seqscan = off;
SET enable_indexscan = on;
SET enable_bitmapscan = off;
SELECT * FROM target ORDER BY id;

\! pg_bulkload -d contrib_regression data/csv4.ctl -o "DUPLICATE_ERRORS=50" -i data/data5.csv -l results/check4.log -P results/check4.prs -u results/check4.dup -o "PARSE_ERRORS=4" -o CHECK_CONSTRAINTS=YES
\! awk -f data/adjust.awk results/check4.log

SET enable_seqscan = on;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT * FROM target ORDER BY id;

SET enable_seqscan = off;
SET enable_indexscan = on;
SET enable_bitmapscan = off;
SELECT * FROM target ORDER BY id;

