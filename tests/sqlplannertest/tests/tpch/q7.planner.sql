-- TPC-H Q7
SELECT
    supp_nation,
    cust_nation,
    l_year,
    SUM(volume) AS revenue
FROM
    (
        SELECT
            n1.n_name AS supp_nation,
            n2.n_name AS cust_nation,
            EXTRACT(YEAR FROM l_shipdate) AS l_year,
            l_extendedprice * (1 - l_discount) AS volume
        FROM
            supplier,
            lineitem,
            orders,
            customer,
            nation n1,
            nation n2
        WHERE
            s_suppkey = l_suppkey
            AND o_orderkey = l_orderkey
            AND c_custkey = o_custkey
            AND s_nationkey = n1.n_nationkey
            AND c_nationkey = n2.n_nationkey
            AND (
                (n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
                OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE')
            )
            AND l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
    ) AS shipping
GROUP BY
    supp_nation,
    cust_nation,
    l_year
ORDER BY
    supp_nation,
    cust_nation,
    l_year;

/*
logical_plan after optd-initial:
OrderBy { ordering_exprs: [ `__internal_#13`.`supp_nation`(#13.0) ASC, `__internal_#13`.`cust_nation`(#13.1) ASC, `__internal_#13`.`l_year`(#13.2) ASC ], (.output_columns): `__internal_#13`.`cust_nation`(#13.1), `__internal_#13`.`l_year`(#13.2), `__internal_#13`.`revenue`(#13.3), `__internal_#13`.`supp_nation`(#13.0), (.cardinality): 0.00 }
└── Project { .table_index: 13, .projections: [ `shipping`.`supp_nation`(#10.0), `shipping`.`cust_nation`(#10.1), `shipping`.`l_year`(#10.2), `__internal_#12`.`sum(shipping.volume)`(#12.0) ], (.output_columns): `__internal_#13`.`cust_nation`(#13.1), `__internal_#13`.`l_year`(#13.2), `__internal_#13`.`revenue`(#13.3), `__internal_#13`.`supp_nation`(#13.0), (.cardinality): 0.00 }
    └── Aggregate { .key_table_index: 11, .aggregate_table_index: 12, .implementation: None, .exprs: [ sum(`shipping`.`volume`(#10.3)) ], .keys: [ `shipping`.`supp_nation`(#10.0), `shipping`.`cust_nation`(#10.1), `shipping`.`l_year`(#10.2) ], (.output_columns): `__internal_#11`.`cust_nation`(#11.1), `__internal_#11`.`l_year`(#11.2), `__internal_#11`.`supp_nation`(#11.0), `__internal_#12`.`sum(shipping.volume)`(#12.0), (.cardinality): 0.00 }
        └── Remap { .table_index: 10, (.output_columns): `shipping`.`cust_nation`(#10.1), `shipping`.`l_year`(#10.2), `shipping`.`supp_nation`(#10.0), `shipping`.`volume`(#10.3), (.cardinality): 0.00 }
            └── Project { .table_index: 9, .projections: [ `n1`.`n_name`(#6.1), `n2`.`n_name`(#8.1), date_part(YEAR::utf8, `lineitem`.`l_shipdate`(#2.10)), `lineitem`.`l_extendedprice`(#2.5) * 1::decimal128(20, 0) - `lineitem`.`l_discount`(#2.6) ], (.output_columns): `__internal_#9`.`cust_nation`(#9.1), `__internal_#9`.`l_year`(#9.2), `__internal_#9`.`supp_nation`(#9.0), `__internal_#9`.`volume`(#9.3), (.cardinality): 0.00 }
                └── Select
                    ├── .predicate: (((`n1`.`n_name`(#6.1) = FRANCE::utf8_view) AND (`n2`.`n_name`(#8.1) = GERMANY::utf8_view)) OR ((`n1`.`n_name`(#6.1) = GERMANY::utf8_view) AND (`n2`.`n_name`(#8.1) = FRANCE::utf8_view))) AND ((`lineitem`.`l_shipdate`(#2.10) >= 1995-01-01::date32) AND (`lineitem`.`l_shipdate`(#2.10) <= 1996-12-31::date32))
                    ├── (.output_columns): `customer`.`c_acctbal`(#4.5), `customer`.`c_address`(#4.2), `customer`.`c_comment`(#4.7), `customer`.`c_custkey`(#4.0), `customer`.`c_mktsegment`(#4.6), `customer`.`c_name`(#4.1), `customer`.`c_nationkey`(#4.3), `customer`.`c_phone`(#4.4), `lineitem`.`l_comment`(#2.15), `lineitem`.`l_commitdate`(#2.11), `lineitem`.`l_discount`(#2.6), `lineitem`.`l_extendedprice`(#2.5), `lineitem`.`l_linenumber`(#2.3), `lineitem`.`l_linestatus`(#2.9), `lineitem`.`l_orderkey`(#2.0), `lineitem`.`l_partkey`(#2.1), `lineitem`.`l_quantity`(#2.4), `lineitem`.`l_receiptdate`(#2.12), `lineitem`.`l_returnflag`(#2.8), `lineitem`.`l_shipdate`(#2.10), `lineitem`.`l_shipinstruct`(#2.13), `lineitem`.`l_shipmode`(#2.14), `lineitem`.`l_suppkey`(#2.2), `lineitem`.`l_tax`(#2.7), `n1`.`n_comment`(#6.3), `n1`.`n_name`(#6.1), `n1`.`n_nationkey`(#6.0), `n1`.`n_regionkey`(#6.2), `n2`.`n_comment`(#8.3), `n2`.`n_name`(#8.1), `n2`.`n_nationkey`(#8.0), `n2`.`n_regionkey`(#8.2), `orders`.`o_clerk`(#3.6), `orders`.`o_comment`(#3.8), `orders`.`o_custkey`(#3.1), `orders`.`o_orderdate`(#3.4), `orders`.`o_orderkey`(#3.0), `orders`.`o_orderpriority`(#3.5), `orders`.`o_orderstatus`(#3.2), `orders`.`o_shippriority`(#3.7), `orders`.`o_totalprice`(#3.3), `supplier`.`s_acctbal`(#1.5), `supplier`.`s_address`(#1.2), `supplier`.`s_comment`(#1.6), `supplier`.`s_name`(#1.1), `supplier`.`s_nationkey`(#1.3), `supplier`.`s_phone`(#1.4), `supplier`.`s_suppkey`(#1.0)
                    ├── (.cardinality): 0.00
                    └── Join
                        ├── .join_type: Inner
                        ├── .implementation: None
                        ├── .join_cond: (`customer`.`c_nationkey`(#4.3) = `n2`.`n_nationkey`(#8.0))
                        ├── (.output_columns): `customer`.`c_acctbal`(#4.5), `customer`.`c_address`(#4.2), `customer`.`c_comment`(#4.7), `customer`.`c_custkey`(#4.0), `customer`.`c_mktsegment`(#4.6), `customer`.`c_name`(#4.1), `customer`.`c_nationkey`(#4.3), `customer`.`c_phone`(#4.4), `lineitem`.`l_comment`(#2.15), `lineitem`.`l_commitdate`(#2.11), `lineitem`.`l_discount`(#2.6), `lineitem`.`l_extendedprice`(#2.5), `lineitem`.`l_linenumber`(#2.3), `lineitem`.`l_linestatus`(#2.9), `lineitem`.`l_orderkey`(#2.0), `lineitem`.`l_partkey`(#2.1), `lineitem`.`l_quantity`(#2.4), `lineitem`.`l_receiptdate`(#2.12), `lineitem`.`l_returnflag`(#2.8), `lineitem`.`l_shipdate`(#2.10), `lineitem`.`l_shipinstruct`(#2.13), `lineitem`.`l_shipmode`(#2.14), `lineitem`.`l_suppkey`(#2.2), `lineitem`.`l_tax`(#2.7), `n1`.`n_comment`(#6.3), `n1`.`n_name`(#6.1), `n1`.`n_nationkey`(#6.0), `n1`.`n_regionkey`(#6.2), `n2`.`n_comment`(#8.3), `n2`.`n_name`(#8.1), `n2`.`n_nationkey`(#8.0), `n2`.`n_regionkey`(#8.2), `orders`.`o_clerk`(#3.6), `orders`.`o_comment`(#3.8), `orders`.`o_custkey`(#3.1), `orders`.`o_orderdate`(#3.4), `orders`.`o_orderkey`(#3.0), `orders`.`o_orderpriority`(#3.5), `orders`.`o_orderstatus`(#3.2), `orders`.`o_shippriority`(#3.7), `orders`.`o_totalprice`(#3.3), `supplier`.`s_acctbal`(#1.5), `supplier`.`s_address`(#1.2), `supplier`.`s_comment`(#1.6), `supplier`.`s_name`(#1.1), `supplier`.`s_nationkey`(#1.3), `supplier`.`s_phone`(#1.4), `supplier`.`s_suppkey`(#1.0)
                        ├── (.cardinality): 0.00
                        ├── Join
                        │   ├── .join_type: Inner
                        │   ├── .implementation: None
                        │   ├── .join_cond: (`supplier`.`s_nationkey`(#1.3) = `n1`.`n_nationkey`(#6.0))
                        │   ├── (.output_columns): `customer`.`c_acctbal`(#4.5), `customer`.`c_address`(#4.2), `customer`.`c_comment`(#4.7), `customer`.`c_custkey`(#4.0), `customer`.`c_mktsegment`(#4.6), `customer`.`c_name`(#4.1), `customer`.`c_nationkey`(#4.3), `customer`.`c_phone`(#4.4), `lineitem`.`l_comment`(#2.15), `lineitem`.`l_commitdate`(#2.11), `lineitem`.`l_discount`(#2.6), `lineitem`.`l_extendedprice`(#2.5), `lineitem`.`l_linenumber`(#2.3), `lineitem`.`l_linestatus`(#2.9), `lineitem`.`l_orderkey`(#2.0), `lineitem`.`l_partkey`(#2.1), `lineitem`.`l_quantity`(#2.4), `lineitem`.`l_receiptdate`(#2.12), `lineitem`.`l_returnflag`(#2.8), `lineitem`.`l_shipdate`(#2.10), `lineitem`.`l_shipinstruct`(#2.13), `lineitem`.`l_shipmode`(#2.14), `lineitem`.`l_suppkey`(#2.2), `lineitem`.`l_tax`(#2.7), `n1`.`n_comment`(#6.3), `n1`.`n_name`(#6.1), `n1`.`n_nationkey`(#6.0), `n1`.`n_regionkey`(#6.2), `orders`.`o_clerk`(#3.6), `orders`.`o_comment`(#3.8), `orders`.`o_custkey`(#3.1), `orders`.`o_orderdate`(#3.4), `orders`.`o_orderkey`(#3.0), `orders`.`o_orderpriority`(#3.5), `orders`.`o_orderstatus`(#3.2), `orders`.`o_shippriority`(#3.7), `orders`.`o_totalprice`(#3.3), `supplier`.`s_acctbal`(#1.5), `supplier`.`s_address`(#1.2), `supplier`.`s_comment`(#1.6), `supplier`.`s_name`(#1.1), `supplier`.`s_nationkey`(#1.3), `supplier`.`s_phone`(#1.4), `supplier`.`s_suppkey`(#1.0)
                        │   ├── (.cardinality): 0.00
                        │   ├── Join { .join_type: Inner, .implementation: None, .join_cond: (`orders`.`o_custkey`(#3.1) = `customer`.`c_custkey`(#4.0)), (.output_columns): `customer`.`c_acctbal`(#4.5), `customer`.`c_address`(#4.2), `customer`.`c_comment`(#4.7), `customer`.`c_custkey`(#4.0), `customer`.`c_mktsegment`(#4.6), `customer`.`c_name`(#4.1), `customer`.`c_nationkey`(#4.3), `customer`.`c_phone`(#4.4), `lineitem`.`l_comment`(#2.15), `lineitem`.`l_commitdate`(#2.11), `lineitem`.`l_discount`(#2.6), `lineitem`.`l_extendedprice`(#2.5), `lineitem`.`l_linenumber`(#2.3), `lineitem`.`l_linestatus`(#2.9), `lineitem`.`l_orderkey`(#2.0), `lineitem`.`l_partkey`(#2.1), `lineitem`.`l_quantity`(#2.4), `lineitem`.`l_receiptdate`(#2.12), `lineitem`.`l_returnflag`(#2.8), `lineitem`.`l_shipdate`(#2.10), `lineitem`.`l_shipinstruct`(#2.13), `lineitem`.`l_shipmode`(#2.14), `lineitem`.`l_suppkey`(#2.2), `lineitem`.`l_tax`(#2.7), `orders`.`o_clerk`(#3.6), `orders`.`o_comment`(#3.8), `orders`.`o_custkey`(#3.1), `orders`.`o_orderdate`(#3.4), `orders`.`o_orderkey`(#3.0), `orders`.`o_orderpriority`(#3.5), `orders`.`o_orderstatus`(#3.2), `orders`.`o_shippriority`(#3.7), `orders`.`o_totalprice`(#3.3), `supplier`.`s_acctbal`(#1.5), `supplier`.`s_address`(#1.2), `supplier`.`s_comment`(#1.6), `supplier`.`s_name`(#1.1), `supplier`.`s_nationkey`(#1.3), `supplier`.`s_phone`(#1.4), `supplier`.`s_suppkey`(#1.0), (.cardinality): 0.00 }
                        │   │   ├── Join { .join_type: Inner, .implementation: None, .join_cond: (`lineitem`.`l_orderkey`(#2.0) = `orders`.`o_orderkey`(#3.0)), (.output_columns): `lineitem`.`l_comment`(#2.15), `lineitem`.`l_commitdate`(#2.11), `lineitem`.`l_discount`(#2.6), `lineitem`.`l_extendedprice`(#2.5), `lineitem`.`l_linenumber`(#2.3), `lineitem`.`l_linestatus`(#2.9), `lineitem`.`l_orderkey`(#2.0), `lineitem`.`l_partkey`(#2.1), `lineitem`.`l_quantity`(#2.4), `lineitem`.`l_receiptdate`(#2.12), `lineitem`.`l_returnflag`(#2.8), `lineitem`.`l_shipdate`(#2.10), `lineitem`.`l_shipinstruct`(#2.13), `lineitem`.`l_shipmode`(#2.14), `lineitem`.`l_suppkey`(#2.2), `lineitem`.`l_tax`(#2.7), `orders`.`o_clerk`(#3.6), `orders`.`o_comment`(#3.8), `orders`.`o_custkey`(#3.1), `orders`.`o_orderdate`(#3.4), `orders`.`o_orderkey`(#3.0), `orders`.`o_orderpriority`(#3.5), `orders`.`o_orderstatus`(#3.2), `orders`.`o_shippriority`(#3.7), `orders`.`o_totalprice`(#3.3), `supplier`.`s_acctbal`(#1.5), `supplier`.`s_address`(#1.2), `supplier`.`s_comment`(#1.6), `supplier`.`s_name`(#1.1), `supplier`.`s_nationkey`(#1.3), `supplier`.`s_phone`(#1.4), `supplier`.`s_suppkey`(#1.0), (.cardinality): 0.00 }
                        │   │   │   ├── Join { .join_type: Inner, .implementation: None, .join_cond: (`supplier`.`s_suppkey`(#1.0) = `lineitem`.`l_suppkey`(#2.2)), (.output_columns): `lineitem`.`l_comment`(#2.15), `lineitem`.`l_commitdate`(#2.11), `lineitem`.`l_discount`(#2.6), `lineitem`.`l_extendedprice`(#2.5), `lineitem`.`l_linenumber`(#2.3), `lineitem`.`l_linestatus`(#2.9), `lineitem`.`l_orderkey`(#2.0), `lineitem`.`l_partkey`(#2.1), `lineitem`.`l_quantity`(#2.4), `lineitem`.`l_receiptdate`(#2.12), `lineitem`.`l_returnflag`(#2.8), `lineitem`.`l_shipdate`(#2.10), `lineitem`.`l_shipinstruct`(#2.13), `lineitem`.`l_shipmode`(#2.14), `lineitem`.`l_suppkey`(#2.2), `lineitem`.`l_tax`(#2.7), `supplier`.`s_acctbal`(#1.5), `supplier`.`s_address`(#1.2), `supplier`.`s_comment`(#1.6), `supplier`.`s_name`(#1.1), `supplier`.`s_nationkey`(#1.3), `supplier`.`s_phone`(#1.4), `supplier`.`s_suppkey`(#1.0), (.cardinality): 0.00 }
                        │   │   │   │   ├── Get { .data_source_id: 4, .table_index: 1, .implementation: None, (.output_columns): `supplier`.`s_acctbal`(#1.5), `supplier`.`s_address`(#1.2), `supplier`.`s_comment`(#1.6), `supplier`.`s_name`(#1.1), `supplier`.`s_nationkey`(#1.3), `supplier`.`s_phone`(#1.4), `supplier`.`s_suppkey`(#1.0), (.cardinality): 0.00 }
                        │   │   │   │   └── Get { .data_source_id: 8, .table_index: 2, .implementation: None, (.output_columns): `lineitem`.`l_comment`(#2.15), `lineitem`.`l_commitdate`(#2.11), `lineitem`.`l_discount`(#2.6), `lineitem`.`l_extendedprice`(#2.5), `lineitem`.`l_linenumber`(#2.3), `lineitem`.`l_linestatus`(#2.9), `lineitem`.`l_orderkey`(#2.0), `lineitem`.`l_partkey`(#2.1), `lineitem`.`l_quantity`(#2.4), `lineitem`.`l_receiptdate`(#2.12), `lineitem`.`l_returnflag`(#2.8), `lineitem`.`l_shipdate`(#2.10), `lineitem`.`l_shipinstruct`(#2.13), `lineitem`.`l_shipmode`(#2.14), `lineitem`.`l_suppkey`(#2.2), `lineitem`.`l_tax`(#2.7), (.cardinality): 0.00 }
                        │   │   │   └── Get { .data_source_id: 7, .table_index: 3, .implementation: None, (.output_columns): `orders`.`o_clerk`(#3.6), `orders`.`o_comment`(#3.8), `orders`.`o_custkey`(#3.1), `orders`.`o_orderdate`(#3.4), `orders`.`o_orderkey`(#3.0), `orders`.`o_orderpriority`(#3.5), `orders`.`o_orderstatus`(#3.2), `orders`.`o_shippriority`(#3.7), `orders`.`o_totalprice`(#3.3), (.cardinality): 0.00 }
                        │   │   └── Get { .data_source_id: 6, .table_index: 4, .implementation: None, (.output_columns): `customer`.`c_acctbal`(#4.5), `customer`.`c_address`(#4.2), `customer`.`c_comment`(#4.7), `customer`.`c_custkey`(#4.0), `customer`.`c_mktsegment`(#4.6), `customer`.`c_name`(#4.1), `customer`.`c_nationkey`(#4.3), `customer`.`c_phone`(#4.4), (.cardinality): 0.00 }
                        │   └── Remap { .table_index: 6, (.output_columns): `n1`.`n_comment`(#6.3), `n1`.`n_name`(#6.1), `n1`.`n_nationkey`(#6.0), `n1`.`n_regionkey`(#6.2), (.cardinality): 0.00 }
                        │       └── Get { .data_source_id: 1, .table_index: 5, .implementation: None, (.output_columns): `nation`.`n_comment`(#5.3), `nation`.`n_name`(#5.1), `nation`.`n_nationkey`(#5.0), `nation`.`n_regionkey`(#5.2), (.cardinality): 0.00 }
                        └── Remap { .table_index: 8, (.output_columns): `n2`.`n_comment`(#8.3), `n2`.`n_name`(#8.1), `n2`.`n_nationkey`(#8.0), `n2`.`n_regionkey`(#8.2), (.cardinality): 0.00 }
                            └── Get { .data_source_id: 1, .table_index: 7, .implementation: None, (.output_columns): `nation`.`n_comment`(#7.3), `nation`.`n_name`(#7.1), `nation`.`n_nationkey`(#7.0), `nation`.`n_regionkey`(#7.2), (.cardinality): 0.00 }

physical_plan after optd-finalized:
EnforcerSort { tuple_ordering: [(#13.0, Asc), (#13.1, Asc), (#13.2, Asc)], (.output_columns): `__internal_#13`.`cust_nation`(#13.1), `__internal_#13`.`l_year`(#13.2), `__internal_#13`.`revenue`(#13.3), `__internal_#13`.`supp_nation`(#13.0), (.cardinality): 0.00 }
└── Project { .table_index: 13, .projections: [ `shipping`.`supp_nation`(#10.0), `shipping`.`cust_nation`(#10.1), `shipping`.`l_year`(#10.2), `__internal_#12`.`sum(shipping.volume)`(#12.0) ], (.output_columns): `__internal_#13`.`cust_nation`(#13.1), `__internal_#13`.`l_year`(#13.2), `__internal_#13`.`revenue`(#13.3), `__internal_#13`.`supp_nation`(#13.0), (.cardinality): 0.00 }
    └── Aggregate { .key_table_index: 11, .aggregate_table_index: 12, .implementation: None, .exprs: [ sum(`shipping`.`volume`(#10.3)) ], .keys: [ `shipping`.`supp_nation`(#10.0), `shipping`.`cust_nation`(#10.1), `shipping`.`l_year`(#10.2) ], (.output_columns): `__internal_#11`.`cust_nation`(#11.1), `__internal_#11`.`l_year`(#11.2), `__internal_#11`.`supp_nation`(#11.0), `__internal_#12`.`sum(shipping.volume)`(#12.0), (.cardinality): 0.00 }
        └── Remap { .table_index: 10, (.output_columns): `shipping`.`cust_nation`(#10.1), `shipping`.`l_year`(#10.2), `shipping`.`supp_nation`(#10.0), `shipping`.`volume`(#10.3), (.cardinality): 0.00 }
            └── Project { .table_index: 9, .projections: [ `n1`.`n_name`(#6.1), `n2`.`n_name`(#8.1), date_part(YEAR::utf8, `lineitem`.`l_shipdate`(#2.10)), `lineitem`.`l_extendedprice`(#2.5) * 1::decimal128(20, 0) - `lineitem`.`l_discount`(#2.6) ], (.output_columns): `__internal_#9`.`cust_nation`(#9.1), `__internal_#9`.`l_year`(#9.2), `__internal_#9`.`supp_nation`(#9.0), `__internal_#9`.`volume`(#9.3), (.cardinality): 0.00 }
                └── Join
                    ├── .join_type: Inner
                    ├── .implementation: None
                    ├── .join_cond: (`customer`.`c_nationkey`(#4.3) = `n2`.`n_nationkey`(#8.0)) AND (((`n1`.`n_name`(#6.1) = FRANCE::utf8_view) AND (`n2`.`n_name`(#8.1) = GERMANY::utf8_view)) OR ((`n1`.`n_name`(#6.1) = GERMANY::utf8_view) AND (`n2`.`n_name`(#8.1) = FRANCE::utf8_view)))
                    ├── (.output_columns): `customer`.`c_custkey`(#4.0), `customer`.`c_nationkey`(#4.3), `lineitem`.`l_discount`(#2.6), `lineitem`.`l_extendedprice`(#2.5), `lineitem`.`l_orderkey`(#2.0), `lineitem`.`l_shipdate`(#2.10), `lineitem`.`l_suppkey`(#2.2), `n1`.`n_comment`(#6.3), `n1`.`n_name`(#6.1), `n1`.`n_nationkey`(#6.0), `n1`.`n_regionkey`(#6.2), `n2`.`n_comment`(#8.3), `n2`.`n_name`(#8.1), `n2`.`n_nationkey`(#8.0), `n2`.`n_regionkey`(#8.2), `orders`.`o_custkey`(#3.1), `orders`.`o_orderkey`(#3.0), `supplier`.`s_nationkey`(#1.3), `supplier`.`s_suppkey`(#1.0)
                    ├── (.cardinality): 0.00
                    ├── Join
                    │   ├── .join_type: Inner
                    │   ├── .implementation: None
                    │   ├── .join_cond: `supplier`.`s_nationkey`(#1.3) = `n1`.`n_nationkey`(#6.0)
                    │   ├── (.output_columns): `customer`.`c_custkey`(#4.0), `customer`.`c_nationkey`(#4.3), `lineitem`.`l_discount`(#2.6), `lineitem`.`l_extendedprice`(#2.5), `lineitem`.`l_orderkey`(#2.0), `lineitem`.`l_shipdate`(#2.10), `lineitem`.`l_suppkey`(#2.2), `n1`.`n_comment`(#6.3), `n1`.`n_name`(#6.1), `n1`.`n_nationkey`(#6.0), `n1`.`n_regionkey`(#6.2), `orders`.`o_custkey`(#3.1), `orders`.`o_orderkey`(#3.0), `supplier`.`s_nationkey`(#1.3), `supplier`.`s_suppkey`(#1.0)
                    │   ├── (.cardinality): 0.00
                    │   ├── Join { .join_type: Inner, .implementation: None, .join_cond: `orders`.`o_custkey`(#3.1) = `customer`.`c_custkey`(#4.0), (.output_columns): `customer`.`c_custkey`(#4.0), `customer`.`c_nationkey`(#4.3), `lineitem`.`l_discount`(#2.6), `lineitem`.`l_extendedprice`(#2.5), `lineitem`.`l_orderkey`(#2.0), `lineitem`.`l_shipdate`(#2.10), `lineitem`.`l_suppkey`(#2.2), `orders`.`o_custkey`(#3.1), `orders`.`o_orderkey`(#3.0), `supplier`.`s_nationkey`(#1.3), `supplier`.`s_suppkey`(#1.0), (.cardinality): 0.00 }
                    │   │   ├── Join { .join_type: Inner, .implementation: None, .join_cond: `lineitem`.`l_orderkey`(#2.0) = `orders`.`o_orderkey`(#3.0), (.output_columns): `lineitem`.`l_discount`(#2.6), `lineitem`.`l_extendedprice`(#2.5), `lineitem`.`l_orderkey`(#2.0), `lineitem`.`l_shipdate`(#2.10), `lineitem`.`l_suppkey`(#2.2), `orders`.`o_custkey`(#3.1), `orders`.`o_orderkey`(#3.0), `supplier`.`s_nationkey`(#1.3), `supplier`.`s_suppkey`(#1.0), (.cardinality): 0.00 }
                    │   │   │   ├── Join { .join_type: Inner, .implementation: None, .join_cond: `supplier`.`s_suppkey`(#1.0) = `lineitem`.`l_suppkey`(#2.2), (.output_columns): `lineitem`.`l_discount`(#2.6), `lineitem`.`l_extendedprice`(#2.5), `lineitem`.`l_orderkey`(#2.0), `lineitem`.`l_shipdate`(#2.10), `lineitem`.`l_suppkey`(#2.2), `supplier`.`s_nationkey`(#1.3), `supplier`.`s_suppkey`(#1.0), (.cardinality): 0.00 }
                    │   │   │   │   ├── Get { .data_source_id: 4, .table_index: 1, .implementation: None, (.output_columns): `supplier`.`s_nationkey`(#1.3), `supplier`.`s_suppkey`(#1.0), (.cardinality): 0.00 }
                    │   │   │   │   └── Select { .predicate: (`lineitem`.`l_shipdate`(#2.10) >= 1995-01-01::date32) AND (`lineitem`.`l_shipdate`(#2.10) <= 1996-12-31::date32), (.output_columns): `lineitem`.`l_discount`(#2.6), `lineitem`.`l_extendedprice`(#2.5), `lineitem`.`l_orderkey`(#2.0), `lineitem`.`l_shipdate`(#2.10), `lineitem`.`l_suppkey`(#2.2), (.cardinality): 0.00 }
                    │   │   │   │       └── Get { .data_source_id: 8, .table_index: 2, .implementation: None, (.output_columns): `lineitem`.`l_discount`(#2.6), `lineitem`.`l_extendedprice`(#2.5), `lineitem`.`l_orderkey`(#2.0), `lineitem`.`l_shipdate`(#2.10), `lineitem`.`l_suppkey`(#2.2), (.cardinality): 0.00 }
                    │   │   │   └── Get { .data_source_id: 7, .table_index: 3, .implementation: None, (.output_columns): `orders`.`o_custkey`(#3.1), `orders`.`o_orderkey`(#3.0), (.cardinality): 0.00 }
                    │   │   └── Get { .data_source_id: 6, .table_index: 4, .implementation: None, (.output_columns): `customer`.`c_custkey`(#4.0), `customer`.`c_nationkey`(#4.3), (.cardinality): 0.00 }
                    │   └── Remap { .table_index: 6, (.output_columns): `n1`.`n_comment`(#6.3), `n1`.`n_name`(#6.1), `n1`.`n_nationkey`(#6.0), `n1`.`n_regionkey`(#6.2), (.cardinality): 0.00 }
                    │       └── Get { .data_source_id: 1, .table_index: 5, .implementation: None, (.output_columns): `nation`.`n_comment`(#5.3), `nation`.`n_name`(#5.1), `nation`.`n_nationkey`(#5.0), `nation`.`n_regionkey`(#5.2), (.cardinality): 0.00 }
                    └── Remap { .table_index: 8, (.output_columns): `n2`.`n_comment`(#8.3), `n2`.`n_name`(#8.1), `n2`.`n_nationkey`(#8.0), `n2`.`n_regionkey`(#8.2), (.cardinality): 0.00 }
                        └── Get { .data_source_id: 1, .table_index: 7, .implementation: None, (.output_columns): `nation`.`n_comment`(#7.3), `nation`.`n_name`(#7.1), `nation`.`n_nationkey`(#7.0), `nation`.`n_regionkey`(#7.2), (.cardinality): 0.00 }
*/

