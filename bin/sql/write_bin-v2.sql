CREATE TABLE binout1 (
  val1 smallint NOT NULL,
  val2 integer NOT NULL,
  val3 bigint NOT NULL,
  val4 integer NOT NULL,
  val5 bigint NOT NULL,
  val6 real NOT NULL,
  val7 double precision NOT NULL,
  val8 text NOT NULL
);
CREATE TABLE binout2 (LIKE binout1);
CREATE TABLE binout3 (
  val1 smallint NOT NULL,
  val2 integer NOT NULL,
  val3 bigint NOT NULL,
  val4 integer NOT NULL,
  val5 bigint NOT NULL,
  val6 real NOT NULL,
  val7 double precision NOT NULL
);
CREATE FUNCTION binout_f1() RETURNS SETOF RECORD AS $$
	VALUES (11, 12, 13, 14, 15, 1.1, 1.2)
	      ,(21, 22, 23, 24, 25, 2.1, 2.2)
	      ,(31, 32, 33, 34, 35, 3.1, 3.2)
	      ,(41, 42, 43, 44, 45, 4.1, 4.2)
	      ,(51, 52, 53, 54, 55, 5.1, 5.2)
	;
$$ LANGUAGE SQL;

/* error case */
\! pg_bulkload -d contrib_regression data/binout1.ctl -i data/binout1.csv -l results/binout1.log -P results/binout1.prs -o TYPE=CSV -O results/binout1.bin -o TRUNCATE=YES

\! touch results/binout1.bin results/binout1.bin.ctl
\! pg_bulkload -d contrib_regression data/binout1.ctl -i data/binout1.csv -l results/binout1.log -P results/binout1.prs -o TYPE=CSV -O results/binout1.bin

\! rm results/binout1.bin
\! pg_bulkload -d contrib_regression data/binout1.ctl -i data/binout1.csv -l results/binout1.log -P results/binout1.prs -o TYPE=CSV -O results/binout1.bin

\! rm results/binout1.bin.ctl
\! pg_bulkload -d contrib_regression data/binout1.ctl -i data/binout1.csv -l results/binout1.log -P results/binout1.prs -o TYPE=CSV -O results/binout1.bin -u results/binout1.dup
\! pg_bulkload -d contrib_regression data/binout1.ctl -i data/binout1.csv -l results/binout1.log -P results/binout1.prs -o TYPE=CSV -O results/binout1.bin -o DUPLICATE_BADFILE=/tmp/binout1.dup
\! pg_bulkload -d contrib_regression data/binout1.ctl -i data/binout1.csv -l results/binout1.log -P results/binout1.prs -o TYPE=CSV -O results/binout1.bin -o DUPLICATE_ERRORS=0
\! pg_bulkload -d contrib_regression data/binout1.ctl -i data/binout1.csv -l results/binout1.log -P results/binout1.prs -o TYPE=CSV -O results/binout1.bin -o ON_DUPLICATE_KEEP=NEW
\! pg_bulkload -d contrib_regression data/binout1.ctl -i data/binout1.csv -l results/binout1.log -P results/binout1.prs -o TYPE=CSV -O results/binout1.bin -o "OUT_COL=CHAR(100+10)"
\! pg_bulkload -d contrib_regression data/binout1.ctl -i data/binout1.csv -l results/binout1.log -P results/binout1.prs -o TYPE=CSV -O results/binout1.bin -o "OUT_COL=CHAR(100:110)"
\! pg_bulkload -d contrib_regression data/binout1.ctl -i data/binout1.csv -l results/binout1.log -P results/binout1.prs -o TYPE=CSV -O data/binout1.csv
\! pg_bulkload -d contrib_regression data/binout1.ctl -i data/binout1.csv -l results/binout1.log -P results/binout1.prs -o TYPE=CSV -O results/binout1.log
\! pg_bulkload -d contrib_regression data/binout1.ctl -i data/binout1.csv -l results/binout1.log -P results/binout1.prs -o TYPE=CSV -O results/binout1.prs

/* normal case */
\! pg_bulkload -d contrib_regression data/binout1.ctl -i data/binout1.csv -l results/binout1.log -P results/binout1.prs -o TYPE=CSV -O results/binout1.bin
\! awk -f data/adjust.awk results/binout1.log

\! pg_bulkload -d contrib_regression results/binout1.bin.ctl
\! awk -f data/adjust.awk results/binout1.bin.log

SELECT * FROM binout1 ORDER BY val1;

\! pg_bulkload -d contrib_regression data/binout2.ctl -i data/binout2.csv -l results/binout2.log -P results/binout2.prs -o TYPE=CSV -O results/binout2.bin
\! awk -f data/adjust.awk results/binout2.log

\! pg_bulkload -d contrib_regression results/binout2.bin.ctl
\! awk -f data/adjust.awk results/binout2.bin.log

SELECT * FROM binout2 ORDER BY val1;

\! pg_bulkload -d contrib_regression data/binout3.ctl -i "binout_f1()" -l results/binout3.log -P results/binout3.prs -o TYPE=FUNCTION -O results/binout3.bin
\! awk -f data/adjust.awk results/binout3.log

\! pg_bulkload -d contrib_regression results/binout3.bin.ctl
\! awk -f data/adjust.awk results/binout3.bin.log

SELECT * FROM binout3 ORDER BY val1;
