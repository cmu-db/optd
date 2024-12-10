-- A special version of DDL/DML for popluating the TPC-H tables, sf=0.01

CREATE EXTERNAL TABLE customer_tbl STORED AS CSV OPTIONS (HAS_HEADER false, DELIMITER '|') LOCATION '../datafusion-optd-cli/tpch-sf0_01/customer.csv';
insert into customer select column_1, column_2, column_3, column_4, column_5, column_6, column_7, column_8 from customer_tbl;
CREATE EXTERNAL TABLE lineitem_tbl STORED AS CSV OPTIONS (HAS_HEADER false, DELIMITER '|') LOCATION '../datafusion-optd-cli/tpch-sf0_01/lineitem.csv';
insert into lineitem select column_1, column_2, column_3, column_4, column_5, column_6, column_7, column_8, column_9, column_10, column_11, column_12, column_13, column_14, column_15, column_16 from lineitem_tbl;
CREATE EXTERNAL TABLE nation_tbl STORED AS CSV OPTIONS (HAS_HEADER false, DELIMITER '|') LOCATION '../datafusion-optd-cli/tpch-sf0_01/nation.csv';
insert into nation select column_1, column_2, column_3, column_4 from nation_tbl;
CREATE EXTERNAL TABLE orders_tbl STORED AS CSV OPTIONS (HAS_HEADER false, DELIMITER '|') LOCATION '../datafusion-optd-cli/tpch-sf0_01/orders.csv';
insert into orders select column_1, column_2, column_3, column_4, column_5, column_6, column_7, column_8, column_9 from orders_tbl;
CREATE EXTERNAL TABLE part_tbl STORED AS CSV OPTIONS (HAS_HEADER false, DELIMITER '|') LOCATION '../datafusion-optd-cli/tpch-sf0_01/part.csv';
insert into part select column_1, column_2, column_3, column_4, column_5, column_6, column_7, column_8, column_9 from part_tbl;
CREATE EXTERNAL TABLE partsupp_tbl STORED AS CSV OPTIONS (HAS_HEADER false, DELIMITER '|') LOCATION '../datafusion-optd-cli/tpch-sf0_01/partsupp.csv';
insert into partsupp select column_1, column_2, column_3, column_4, column_5 from partsupp_tbl;
CREATE EXTERNAL TABLE region_tbl STORED AS CSV OPTIONS (HAS_HEADER false, DELIMITER '|') LOCATION '../datafusion-optd-cli/tpch-sf0_01/region.csv';
insert into region select column_1, column_2, column_3 from region_tbl;
CREATE EXTERNAL TABLE supplier_tbl STORED AS CSV OPTIONS (HAS_HEADER false, DELIMITER '|') LOCATION '../datafusion-optd-cli/tpch-sf0_01/supplier.csv';
insert into supplier select column_1, column_2, column_3, column_4, column_5, column_6, column_7 from supplier_tbl;
