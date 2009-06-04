SET client_min_messages = warning;
\set ECHO none
\i ../lib/pg_bulkload.sql
\set ECHO all
RESET client_min_messages;

CREATE TABLE customer (
    c_id            int4 NOT NULL,
    c_d_id          int2 NOT NULL,
    c_w_id          int4 NOT NULL,
    c_first         varchar(16) NOT NULL,
    c_middle        char(2) NOT NULL,
    c_last          varchar(16) NOT NULL,
    c_street_1      varchar(20) NOT NULL,
    c_street_2      varchar(20) NOT NULL,
    c_city          varchar(20) NOT NULL,
    c_state         char(2) NOT NULL,
    c_zip           char(9) NOT NULL,
    c_phone         char(16) NOT NULL,
    c_since         timestamp NOT NULL,
    c_credit        char(2) NOT NULL,
    c_credit_lim    numeric(16,4) NOT NULL,
    c_discount      numeric(16,4) NOT NULL,
    c_balance       numeric(16,4) NOT NULL,
    c_ytd_payment   numeric(16,4) NOT NULL,
    c_payment_cnt   float4 NOT NULL,
    c_delivery_cnt  float8 NOT NULL,
    c_data          varchar(500) NOT NULL
) WITH (oids, fillfactor=20);

ALTER TABLE customer ADD PRIMARY KEY (c_w_id, c_d_id, c_id);
CREATE INDEX idx_btree ON customer USING btree (c_d_id, c_last);
CREATE INDEX idx_btree_fn ON customer USING btree ((abs(c_w_id) + c_d_id));
CREATE INDEX idx_hash ON customer USING hash (c_d_id);
CREATE INDEX idx_hash_fn ON customer USING hash ((abs(c_w_id) + c_d_id));
