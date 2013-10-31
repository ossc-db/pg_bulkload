--------------------------------
-- normal case
--------------------------------
-- PL/pgSQL function
CREATE FUNCTION plpgsql_f(int4, int4, int4 DEFAULT 100, text DEFAULT 'default value') RETURNS record AS
$$
DECLARE
    ret target;
BEGIN
    IF $1 = 61 THEN
    	RETURN NULL;
    END IF;
    IF $1 = 41 THEN
        RAISE EXCEPTION 'field 1 is error value';
    END IF;
	ret.id := $2 + $3;
	ret.str := $4;
	ret.master := $1;
	RETURN ret;
END;
$$ LANGUAGE plpgsql STRICT;

-- using OUT paramator function
CREATE FUNCTION using_out_f(int4, int4, int4 DEFAULT 100, text DEFAULT 'default value', OUT int4, OUT text, OUT int4)
    RETURNS record AS
$$
    SELECT plpgsql_f($1, $2, $3, $4);
$$ LANGUAGE SQL;


TRUNCATE target;
INSERT INTO target VALUES(1, 'dummy', 1);

-- FILTER option parse error
\! pg_bulkload -d contrib_regression data/csv4.ctl -i data/data6.csv -l results/filter_e.log -P results/filter_e1.prs -u results/filter_e1.dup -o 'FILTER=variadic_f(int, text)'
\! pg_bulkload -d contrib_regression data/csv4.ctl -i data/data6.csv -l results/filter_e.log -P results/filter_e1.prs -u results/filter_e1.dup -o 'FILTER=overload_f'
\! pg_bulkload -d contrib_regression data/csv4.ctl -i data/data6.csv -l results/filter_e.log -P results/filter_e1.prs -u results/filter_e1.dup -o 'FILTER=using_out_f()'
\! pg_bulkload -d contrib_regression data/csv4.ctl -i data/data6.csv -l results/filter_e.log -P results/filter_e1.prs -u results/filter_e1.dup -o 'FILTER=using_out_f(int4, int4, text)'
\! pg_bulkload -d contrib_regression data/csv4.ctl -i data/data7.csv -l results/filter_e.log -P results/filter_e1.prs -u results/filter_e1.dup -o 'FILTER=outarg_f()'
\! pg_bulkload -d contrib_regression data/csv4.ctl -i data/data6.csv -l results/filter_e.log -P results/filter_e1.prs -u results/filter_e1.dup -o 'FILTER=setof_f()'
\! pg_bulkload -d contrib_regression data/csv4.ctl -i data/data6.csv -l results/filter_e.log -P results/filter_e1.prs -u results/filter_e1.dup -o 'FILTER=using_out_f(int4, int4, int4)' -o FORCE_NOT_NULL=id
\! pg_bulkload -d contrib_regression data/csv4.ctl -i data/data7.csv -l results/filter_e.log -P results/filter_e1.prs -u results/filter_e1.dup -o 'FILTER=type_mismatch_f()'
\! pg_bulkload -d contrib_regression data/csv4.ctl -i data/data6.csv -l results/filter_e.log -P results/filter_e1.prs -u results/filter_e1.dup -o 'FILTER=no_create_f()'
\! pg_bulkload -d contrib_regression data/csv4.ctl -i data/data7.csv -l results/filter_e.log -P results/filter_e1.prs -u results/filter_e1.dup -o 'FILTER=rec_mismatch_f()'

-- FILTER option error
\! pg_bulkload -d contrib_regression data/csv4.ctl -i data/data6.csv -l results/filter_e.log -P results/filter_e1.prs -u results/filter_e1.dup -o 'FILTER="f1'
\! pg_bulkload -d contrib_regression data/csv4.ctl -i data/data6.csv -l results/filter_e.log -P results/filter_e1.prs -u results/filter_e1.dup -o 'FILTER=  (int4)'
\! pg_bulkload -d contrib_regression data/csv4.ctl -i data/data6.csv -l results/filter_e.log -P results/filter_e1.prs -u results/filter_e1.dup -o 'FILTER="f1" int4, int4, text)'
\! pg_bulkload -d contrib_regression data/csv4.ctl -i data/data6.csv -l results/filter_e.log -P results/filter_e1.prs -u results/filter_e1.dup -o 'FILTER=f1(int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4,int4)'
\! pg_bulkload -d contrib_regression data/csv4.ctl -i data/data6.csv -l results/filter_e.log -P results/filter_e1.prs -u results/filter_e1.dup -o 'FILTER=f1(int4, int4, text)    aaa'
\! pg_bulkload -d contrib_regression data/csv4.ctl -i data/data6.csv -l results/filter_e.log -P results/filter_e1.prs -u results/filter_e1.dup -o 'FILTER=f1(numeric((1,2)))'
\! pg_bulkload -d contrib_regression data/csv4.ctl -i data/data6.csv -l results/filter_e.log -P results/filter_e1.prs -u results/filter_e1.dup -o 'FILTER=f1(numeric(1, 2) (1, 2))'
\! pg_bulkload -d contrib_regression data/csv4.ctl -i data/data6.csv -l results/filter_e.log -P results/filter_e1.prs -u results/filter_e1.dup -o 'FILTER=f1(    , text)'
\! pg_bulkload -d contrib_regression data/csv4.ctl -i data/data6.csv -l results/filter_e.log -P results/filter_e1.prs -u results/filter_e1.dup -o 'FILTER=f1(double)'

SET enable_seqscan = on;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT * FROM target ORDER BY id;

SET enable_seqscan = off;
SET enable_indexscan = on;
SET enable_bitmapscan = off;
SELECT * FROM target ORDER BY id;

-- FILTER option test
\pset null (null)
\! pg_bulkload -d contrib_regression data/csv5.ctl -o "PARSE_ERRORS=50" -i data/data6.csv -l results/filter1.log -P results/filter1.prs -u results/filter1.dup -o 'FILTER=using_out_f(int4, int4, int4, text)'
\! awk -f data/adjust.awk results/filter1.log
SET enable_seqscan = on;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT * FROM target_like ORDER BY id;

SET enable_seqscan = off;
SET enable_indexscan = on;
SET enable_bitmapscan = off;
SELECT * FROM target_like ORDER BY id;

\! pg_bulkload -d contrib_regression data/csv5.ctl -i data/data6.csv -l results/filter2.log -P results/filter2.prs -u results/filter2.dup -o "PARSE_ERRORS=4" --option FILTER=plpgsql_f
\! awk -f data/adjust.awk results/filter2.log
SET enable_seqscan = on;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT * FROM target_like ORDER BY id;

SET enable_seqscan = off;
SET enable_indexscan = on;
SET enable_bitmapscan = off;
SELECT * FROM target_like ORDER BY id;

\! pg_bulkload -d contrib_regression data/csv5.ctl -i data/data6.csv -l results/filter3.log -P results/filter3.prs -u results/filter3.dup -o "PARSE_ERRORS=3" -o 'FILTER=plpgsql_f(int4, int4, int4, text)'
\! awk -f data/adjust.awk results/filter3.log
SET enable_seqscan = on;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT * FROM target_like ORDER BY id;

SET enable_seqscan = off;
SET enable_indexscan = on;
SET enable_bitmapscan = off;
SELECT * FROM target_like ORDER BY id;

\! pg_bulkload -d contrib_regression data/csv5.ctl -i data/data7.csv -l results/filter4.log -P results/filter4.prs -u results/filter4.dup -o 'FILTER=no_arg_f()'
\! awk -f data/adjust.awk results/filter4.log
SET enable_seqscan = on;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT * FROM target_like ORDER BY id;

SET enable_seqscan = off;
SET enable_indexscan = on;
SET enable_bitmapscan = off;
SELECT * FROM target_like ORDER BY id;

\! pg_bulkload -d contrib_regression data/bin6.ctl -i data/data4.bin -l results/filter5.log -P results/filter5.prs -u results/filter5.dup -o "PARSE_ERRORS=4" -o FILTER=plpgsql_f
\! awk -f data/adjust.awk results/filter5.log
SET enable_seqscan = on;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT * FROM target_like ORDER BY id;

SET enable_seqscan = off;
SET enable_indexscan = on;
SET enable_bitmapscan = off;
SELECT * FROM target_like ORDER BY id;
