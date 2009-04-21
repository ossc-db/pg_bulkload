TRUNCATE customer;

\! pg_bulkload -d contrib_regression data/bin.ctl -i data/data1.bin

SET enable_seqscan = on;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT * FROM customer ORDER BY c_id;

SET enable_seqscan = off;
SET enable_indexscan = on;
SET enable_bitmapscan = off;
SELECT * FROM customer ORDER BY c_id;

\! pg_bulkload -d contrib_regression data/bin.ctl -i data/data2.bin

SET enable_seqscan = on;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT * FROM customer ORDER BY c_id;

SET enable_seqscan = off;
SET enable_indexscan = on;
SET enable_bitmapscan = off;
SELECT * FROM customer ORDER BY c_id;
