-- This is just used if you want to run really simple manual tests on the CLI. Feel free to delete the whole thing and write your own manual tests
-- Command: `cargo run --bin datafusion-optd-cli -- --enable-df-logical -f datafusion-optd-cli/tpch-sf0_01/simple_manual_test.sql`
CREATE TABLE NATION  (
    N_NATIONKEY  INT NOT NULL,
    N_NAME       CHAR(25) NOT NULL,
    N_REGIONKEY  INT NOT NULL,
    N_COMMENT    VARCHAR(152)
);

CREATE EXTERNAL TABLE nation_tbl STORED AS CSV DELIMITER '|' LOCATION 'datafusion-optd-cli/tpch-sf0_01/nation.tbl';
insert into nation select column_1, column_2, column_3, column_4 from nation_tbl;

SELECT * FROM nation where nation.n_nationkey = 1 OR nation.n_nationkey = 2 OR nation.n_nationkey = 5;
