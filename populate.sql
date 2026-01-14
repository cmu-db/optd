CREATE EXTERNAL TABLE customer
STORED AS PARQUET
LOCATION 'data/tpch/customer.parquet';
    
CREATE EXTERNAL TABLE nation
STORED AS PARQUET
LOCATION 'data/tpch/nation.parquet';
CREATE EXTERNAL TABLE part
STORED AS PARQUET
LOCATION 'data/tpch/part.parquet';

CREATE EXTERNAL TABLE region
STORED AS PARQUET
LOCATION 'data/tpch/region.parquet';

CREATE EXTERNAL TABLE lineitem
STORED AS PARQUET
LOCATION 'data/tpch/lineitem.parquet';
CREATE EXTERNAL TABLE orders
STORED AS PARQUET
LOCATION 'data/tpch/orders.parquet';

CREATE EXTERNAL TABLE partsupp
STORED AS PARQUET
LOCATION 'data/tpch/partsupp.parquet';
CREATE EXTERNAL TABLE supplier
STORED AS PARQUET
LOCATION 'data/tpch/supplier.parquet';

