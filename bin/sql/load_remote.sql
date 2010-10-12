TRUNCATE customer;


\! export PGCLIENTENCODING=utf-8; pg_bulkload -d contrib_regression data/csv1.ctl -o"delimiter=|" -i stdin < data/data1.csv -l results/remote1.log -P results/remote1.prs -u results/remote1.dup -o "PARSE_ERRORS=50"
\! awk -f data/adjust.awk results/remote1.log

SET enable_seqscan = on;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT * FROM customer ORDER BY c_id;

SET enable_seqscan = off;
SET enable_indexscan = on;
SET enable_bitmapscan = off;
SELECT * FROM customer ORDER BY c_id;

\! export PGCLIENTENCODING=utf-8; pg_bulkload -d contrib_regression data/csv1.ctl -i stdin -l results/remote2.log -P results/remote2.prs -u results/remote2.dup < data/data2.csv
\! awk -f data/adjust.awk results/remote2.log

SET enable_seqscan = on;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT * FROM customer ORDER BY c_id;

SET enable_seqscan = off;
SET enable_indexscan = on;
SET enable_bitmapscan = off;
SELECT * FROM customer ORDER BY c_id;

\! export PGCLIENTENCODING=utf-8; pg_bulkload -d contrib_regression data/csv1.ctl -o "DUPLICATE_ERRORS=50" -i stdin -l results/remote3.log -P results/remote3.prs -u results/remote3.dup < data/data2.csv -o "PARSE_ERRORS=50"
\! awk -f data/adjust.awk results/remote3.log

SET enable_seqscan = on;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT * FROM customer ORDER BY c_id;

SET enable_seqscan = off;
SET enable_indexscan = on;
SET enable_bitmapscan = off;
SELECT * FROM customer ORDER BY c_id;
