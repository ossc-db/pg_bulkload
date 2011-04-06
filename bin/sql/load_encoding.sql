\connect contrib_regression_sqlascii
SET client_encoding = 'SQL_ASCII';

\! pg_bulkload -d contrib_regression_sqlascii data/csv7.ctl -i data/data4.csv -l results/encoding1.log -P results/encoding1.prs -u results/encoding1.dup -o "PARSE_ERRORS=-1"
\! awk -f data/adjust.awk results/encoding1.log

SET enable_seqscan = on;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT id, encode(str::bytea, 'hex'), master FROM target ORDER BY id;

SET enable_seqscan = off;
SET enable_indexscan = on;
SET enable_bitmapscan = off;
SELECT id, encode(str::bytea, 'hex'), master FROM target ORDER BY id;

\! pg_bulkload -d contrib_regression_sqlascii data/csv7.ctl -i data/data4.csv -l results/encoding2.log -P results/encoding2.prs -u results/encoding2.dup -o "PARSE_ERRORS=-1" -o "ENCODING=UTF0"
\! awk -f data/adjust.awk results/encoding2.log

SET enable_seqscan = on;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT id, encode(str::bytea, 'hex'), master FROM target ORDER BY id;

SET enable_seqscan = off;
SET enable_indexscan = on;
SET enable_bitmapscan = off;
SELECT id, encode(str::bytea, 'hex'), master FROM target ORDER BY id;

\! pg_bulkload -d contrib_regression_sqlascii data/csv7.ctl -i data/data4.csv -l results/encoding3.log -P results/encoding3.prs -u results/encoding3.dup -o "PARSE_ERRORS=0" -o "ENCODING=UTF8"
\! awk -f data/adjust.awk results/encoding3.log

SET enable_seqscan = on;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT id, encode(str::bytea, 'hex'), master FROM target ORDER BY id;

SET enable_seqscan = off;
SET enable_indexscan = on;
SET enable_bitmapscan = off;
SELECT id, encode(str::bytea, 'hex'), master FROM target ORDER BY id;

\connect contrib_regression_utf8
SET client_encoding = 'SQL_ASCII';

\! pg_bulkload -d contrib_regression_utf8 data/csv7.ctl -i data/data4.csv -l results/encoding4.log -P results/encoding4.prs -u results/encoding4.dup -o "PARSE_ERRORS=1" -o "ENCODING=SQL_ASCII"
\! awk -f data/adjust.awk results/encoding4.log

SET enable_seqscan = on;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT id, encode(str::bytea, 'hex'), master FROM target ORDER BY id;

SET enable_seqscan = off;
SET enable_indexscan = on;
SET enable_bitmapscan = off;
SELECT id, encode(str::bytea, 'hex'), master FROM target ORDER BY id;

\! pg_bulkload -d contrib_regression_utf8 data/csv7.ctl -i data/data4.csv -l results/encoding5.log -P results/encoding5.prs -u results/encoding5.dup -o "PARSE_ERRORS=2" -o "ENCODING=UTF8"
\! awk -f data/adjust.awk results/encoding5.log

SET enable_seqscan = on;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT id, encode(str::bytea, 'hex'), master FROM target ORDER BY id;

SET enable_seqscan = off;
SET enable_indexscan = on;
SET enable_bitmapscan = off;
SELECT id, encode(str::bytea, 'hex'), master FROM target ORDER BY id;

\! pg_bulkload -d contrib_regression_utf8 data/csv7.ctl -i data/data4.csv -l results/encoding6.log -P results/encoding6.prs -u results/encoding6.dup -o "PARSE_ERRORS=3" -o "ENCODING=EUC-JP"
\! awk -f data/adjust.awk results/encoding6.log

SET enable_seqscan = on;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT id, encode(str::bytea, 'hex'), master FROM target ORDER BY id;

SET enable_seqscan = off;
SET enable_indexscan = on;
SET enable_bitmapscan = off;
SELECT id, encode(str::bytea, 'hex'), master FROM target ORDER BY id;

