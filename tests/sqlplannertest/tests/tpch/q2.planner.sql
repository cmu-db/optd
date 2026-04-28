-- TPC-H Q2
select
        s_acctbal,
        s_name,
        n_name,
        p_partkey,
        p_mfgr,
        s_address,
        s_phone,
        s_comment
from
        part,
        supplier,
        partsupp,
        nation,
        region
where
        p_partkey = ps_partkey
        and s_suppkey = ps_suppkey
and p_size = 4
and p_type like '%TIN'
        and s_nationkey = n_nationkey
        and n_regionkey = r_regionkey
        and r_name = 'AFRICA'
        and ps_supplycost = (
                select
                        min(ps_supplycost)
                from
                        partsupp,
                        supplier,
                        nation,
                        region
                where
                        p_partkey = ps_partkey
                        and s_suppkey = ps_suppkey
                        and s_nationkey = n_nationkey
                        and n_regionkey = r_regionkey
                        and r_name = 'AFRICA'
        )
order by
    s_acctbal desc,
    n_name,
    s_name,
    p_partkey
limit 100;

/*
logical_plan after optd-initial:
Limit
├── .skip: 0::bigint
├── .fetch: 100::bigint
├── (.output_columns):
│   ┌── "__#17.n_name"(#17.2)
│   ├── "__#17.p_mfgr"(#17.4)
│   ├── "__#17.p_partkey"(#17.3)
│   ├── "__#17.s_acctbal"(#17.0)
│   ├── "__#17.s_address"(#17.5)
│   ├── "__#17.s_comment"(#17.7)
│   ├── "__#17.s_name"(#17.1)
│   └── "__#17.s_phone"(#17.6)
├── (.cardinality): 0.00
└── OrderBy
    ├── ordering_exprs: [ "__#17.s_acctbal"(#17.0) DESC, "__#17.n_name"(#17.2) ASC, "__#17.s_name"(#17.1) ASC, "__#17.p_partkey"(#17.3) ASC ]
    ├── (.output_columns):
    │   ┌── "__#17.n_name"(#17.2)
    │   ├── "__#17.p_mfgr"(#17.4)
    │   ├── "__#17.p_partkey"(#17.3)
    │   ├── "__#17.s_acctbal"(#17.0)
    │   ├── "__#17.s_address"(#17.5)
    │   ├── "__#17.s_comment"(#17.7)
    │   ├── "__#17.s_name"(#17.1)
    │   └── "__#17.s_phone"(#17.6)
    ├── (.cardinality): 0.00
    └── Project
        ├── .table_index: 17
        ├── .projections:
        │   ┌── "__#16.s_acctbal"(#16.14)
        │   ├── "__#16.s_name"(#16.10)
        │   ├── "__#16.n_name"(#16.22)
        │   ├── "__#16.p_partkey"(#16.0)
        │   ├── "__#16.p_mfgr"(#16.2)
        │   ├── "__#16.s_address"(#16.11)
        │   ├── "__#16.s_phone"(#16.13)
        │   └── "__#16.s_comment"(#16.15)
        ├── (.output_columns):
        │   ┌── "__#17.n_name"(#17.2)
        │   ├── "__#17.p_mfgr"(#17.4)
        │   ├── "__#17.p_partkey"(#17.3)
        │   ├── "__#17.s_acctbal"(#17.0)
        │   ├── "__#17.s_address"(#17.5)
        │   ├── "__#17.s_comment"(#17.7)
        │   ├── "__#17.s_name"(#17.1)
        │   └── "__#17.s_phone"(#17.6)
        ├── (.cardinality): 0.00
        └── Project
            ├── .table_index: 16
            ├── .projections:
            │   ┌── "__#15.p_partkey"(#15.0)
            │   ├── "__#15.p_name"(#15.1)
            │   ├── "__#15.p_mfgr"(#15.2)
            │   ├── "__#15.p_brand"(#15.3)
            │   ├── "__#15.p_type"(#15.4)
            │   ├── "__#15.p_size"(#15.5)
            │   ├── "__#15.p_container"(#15.6)
            │   ├── "__#15.p_retailprice"(#15.7)
            │   ├── "__#15.p_comment"(#15.8)
            │   ├── "__#15.s_suppkey"(#15.9)
            │   ├── "__#15.s_name"(#15.10)
            │   ├── "__#15.s_address"(#15.11)
            │   ├── "__#15.s_nationkey"(#15.12)
            │   ├── "__#15.s_phone"(#15.13)
            │   ├── "__#15.s_acctbal"(#15.14)
            │   ├── "__#15.s_comment"(#15.15)
            │   ├── "__#15.ps_partkey"(#15.16)
            │   ├── "__#15.ps_suppkey"(#15.17)
            │   ├── "__#15.ps_availqty"(#15.18)
            │   ├── "__#15.ps_supplycost"(#15.19)
            │   ├── "__#15.ps_comment"(#15.20)
            │   ├── "__#15.n_nationkey"(#15.21)
            │   ├── "__#15.n_name"(#15.22)
            │   ├── "__#15.n_regionkey"(#15.23)
            │   ├── "__#15.n_comment"(#15.24)
            │   ├── "__#15.r_regionkey"(#15.25)
            │   ├── "__#15.r_name"(#15.26)
            │   └── "__#15.r_comment"(#15.27)
            ├── (.output_columns):
            │   ┌── "__#16.n_comment"(#16.24)
            │   ├── "__#16.n_name"(#16.22)
            │   ├── "__#16.n_nationkey"(#16.21)
            │   ├── "__#16.n_regionkey"(#16.23)
            │   ├── "__#16.p_brand"(#16.3)
            │   ├── "__#16.p_comment"(#16.8)
            │   ├── "__#16.p_container"(#16.6)
            │   ├── "__#16.p_mfgr"(#16.2)
            │   ├── "__#16.p_name"(#16.1)
            │   ├── "__#16.p_partkey"(#16.0)
            │   ├── "__#16.p_retailprice"(#16.7)
            │   ├── "__#16.p_size"(#16.5)
            │   ├── "__#16.p_type"(#16.4)
            │   ├── "__#16.ps_availqty"(#16.18)
            │   ├── "__#16.ps_comment"(#16.20)
            │   ├── "__#16.ps_partkey"(#16.16)
            │   ├── "__#16.ps_suppkey"(#16.17)
            │   ├── "__#16.ps_supplycost"(#16.19)
            │   ├── "__#16.r_comment"(#16.27)
            │   ├── "__#16.r_name"(#16.26)
            │   ├── "__#16.r_regionkey"(#16.25)
            │   ├── "__#16.s_acctbal"(#16.14)
            │   ├── "__#16.s_address"(#16.11)
            │   ├── "__#16.s_comment"(#16.15)
            │   ├── "__#16.s_name"(#16.10)
            │   ├── "__#16.s_nationkey"(#16.12)
            │   ├── "__#16.s_phone"(#16.13)
            │   └── "__#16.s_suppkey"(#16.9)
            ├── (.cardinality): 0.00
            └── Select
                ├── .predicate: ("__#15.p_size"(#15.5) = 4::integer) AND ("__#15.p_type"(#15.4) LIKE '%TIN'::utf8_view) AND ("__#15.r_name"(#15.26) = 'AFRICA'::utf8_view)
                ├── (.output_columns):
                │   ┌── "__#15.__always_true"(#15.30)
                │   ├── "__#15.min(partsupp.ps_supplycost)"(#15.28)
                │   ├── "__#15.n_comment"(#15.24)
                │   ├── "__#15.n_name"(#15.22)
                │   ├── "__#15.n_nationkey"(#15.21)
                │   ├── "__#15.n_regionkey"(#15.23)
                │   ├── "__#15.p_brand"(#15.3)
                │   ├── "__#15.p_comment"(#15.8)
                │   ├── "__#15.p_container"(#15.6)
                │   ├── "__#15.p_mfgr"(#15.2)
                │   ├── "__#15.p_name"(#15.1)
                │   ├── "__#15.p_partkey"(#15.0)
                │   ├── "__#15.p_retailprice"(#15.7)
                │   ├── "__#15.p_size"(#15.5)
                │   ├── "__#15.p_type"(#15.4)
                │   ├── "__#15.ps_availqty"(#15.18)
                │   ├── "__#15.ps_comment"(#15.20)
                │   ├── "__#15.ps_partkey"(#15.16)
                │   ├── "__#15.ps_partkey"(#15.29)
                │   ├── "__#15.ps_suppkey"(#15.17)
                │   ├── "__#15.ps_supplycost"(#15.19)
                │   ├── "__#15.r_comment"(#15.27)
                │   ├── "__#15.r_name"(#15.26)
                │   ├── "__#15.r_regionkey"(#15.25)
                │   ├── "__#15.s_acctbal"(#15.14)
                │   ├── "__#15.s_address"(#15.11)
                │   ├── "__#15.s_comment"(#15.15)
                │   ├── "__#15.s_name"(#15.10)
                │   ├── "__#15.s_nationkey"(#15.12)
                │   ├── "__#15.s_phone"(#15.13)
                │   └── "__#15.s_suppkey"(#15.9)
                ├── (.cardinality): 0.00
                └── Project
                    ├── .table_index: 15
                    ├── .projections:
                    │   ┌── "part.p_partkey"(#1.0)
                    │   ├── "part.p_name"(#1.1)
                    │   ├── "part.p_mfgr"(#1.2)
                    │   ├── "part.p_brand"(#1.3)
                    │   ├── "part.p_type"(#1.4)
                    │   ├── "part.p_size"(#1.5)
                    │   ├── "part.p_container"(#1.6)
                    │   ├── "part.p_retailprice"(#1.7)
                    │   ├── "part.p_comment"(#1.8)
                    │   ├── "supplier.s_suppkey"(#3.0)
                    │   ├── "supplier.s_name"(#3.1)
                    │   ├── "supplier.s_address"(#3.2)
                    │   ├── "supplier.s_nationkey"(#3.3)
                    │   ├── "supplier.s_phone"(#3.4)
                    │   ├── "supplier.s_acctbal"(#3.5)
                    │   ├── "supplier.s_comment"(#3.6)
                    │   ├── "partsupp.ps_partkey"(#2.0)
                    │   ├── "partsupp.ps_suppkey"(#2.1)
                    │   ├── "partsupp.ps_availqty"(#2.2)
                    │   ├── "partsupp.ps_supplycost"(#2.3)
                    │   ├── "partsupp.ps_comment"(#2.4)
                    │   ├── "nation.n_nationkey"(#4.0)
                    │   ├── "nation.n_name"(#4.1)
                    │   ├── "nation.n_regionkey"(#4.2)
                    │   ├── "nation.n_comment"(#4.3)
                    │   ├── "region.r_regionkey"(#5.0)
                    │   ├── "region.r_name"(#5.1)
                    │   ├── "region.r_comment"(#5.2)
                    │   ├── "__scalar_sq_1.min(partsupp.ps_supplycost)"(#14.0)
                    │   ├── "__scalar_sq_1.ps_partkey"(#14.1)
                    │   └── "__scalar_sq_1.__always_true"(#14.2)
                    ├── (.output_columns):
                    │   ┌── "__#15.__always_true"(#15.30)
                    │   ├── "__#15.min(partsupp.ps_supplycost)"(#15.28)
                    │   ├── "__#15.n_comment"(#15.24)
                    │   ├── "__#15.n_name"(#15.22)
                    │   ├── "__#15.n_nationkey"(#15.21)
                    │   ├── "__#15.n_regionkey"(#15.23)
                    │   ├── "__#15.p_brand"(#15.3)
                    │   ├── "__#15.p_comment"(#15.8)
                    │   ├── "__#15.p_container"(#15.6)
                    │   ├── "__#15.p_mfgr"(#15.2)
                    │   ├── "__#15.p_name"(#15.1)
                    │   ├── "__#15.p_partkey"(#15.0)
                    │   ├── "__#15.p_retailprice"(#15.7)
                    │   ├── "__#15.p_size"(#15.5)
                    │   ├── "__#15.p_type"(#15.4)
                    │   ├── "__#15.ps_availqty"(#15.18)
                    │   ├── "__#15.ps_comment"(#15.20)
                    │   ├── "__#15.ps_partkey"(#15.16)
                    │   ├── "__#15.ps_partkey"(#15.29)
                    │   ├── "__#15.ps_suppkey"(#15.17)
                    │   ├── "__#15.ps_supplycost"(#15.19)
                    │   ├── "__#15.r_comment"(#15.27)
                    │   ├── "__#15.r_name"(#15.26)
                    │   ├── "__#15.r_regionkey"(#15.25)
                    │   ├── "__#15.s_acctbal"(#15.14)
                    │   ├── "__#15.s_address"(#15.11)
                    │   ├── "__#15.s_comment"(#15.15)
                    │   ├── "__#15.s_name"(#15.10)
                    │   ├── "__#15.s_nationkey"(#15.12)
                    │   ├── "__#15.s_phone"(#15.13)
                    │   └── "__#15.s_suppkey"(#15.9)
                    ├── (.cardinality): 0.00
                    └── Join
                        ├── .join_type: Inner
                        ├── .implementation: None
                        ├── .join_cond: ("part.p_partkey"(#1.0) = "__scalar_sq_1.ps_partkey"(#14.1)) AND ("partsupp.ps_supplycost"(#2.3) = "__scalar_sq_1.min(partsupp.ps_supplycost)"(#14.0))
                        ├── (.output_columns):
                        │   ┌── "__scalar_sq_1.__always_true"(#14.2)
                        │   ├── "__scalar_sq_1.min(partsupp.ps_supplycost)"(#14.0)
                        │   ├── "__scalar_sq_1.ps_partkey"(#14.1)
                        │   ├── "nation.n_comment"(#4.3)
                        │   ├── "nation.n_name"(#4.1)
                        │   ├── "nation.n_nationkey"(#4.0)
                        │   ├── "nation.n_regionkey"(#4.2)
                        │   ├── "part.p_brand"(#1.3)
                        │   ├── "part.p_comment"(#1.8)
                        │   ├── "part.p_container"(#1.6)
                        │   ├── "part.p_mfgr"(#1.2)
                        │   ├── "part.p_name"(#1.1)
                        │   ├── "part.p_partkey"(#1.0)
                        │   ├── "part.p_retailprice"(#1.7)
                        │   ├── "part.p_size"(#1.5)
                        │   ├── "part.p_type"(#1.4)
                        │   ├── "partsupp.ps_availqty"(#2.2)
                        │   ├── "partsupp.ps_comment"(#2.4)
                        │   ├── "partsupp.ps_partkey"(#2.0)
                        │   ├── "partsupp.ps_suppkey"(#2.1)
                        │   ├── "partsupp.ps_supplycost"(#2.3)
                        │   ├── "region.r_comment"(#5.2)
                        │   ├── "region.r_name"(#5.1)
                        │   ├── "region.r_regionkey"(#5.0)
                        │   ├── "supplier.s_acctbal"(#3.5)
                        │   ├── "supplier.s_address"(#3.2)
                        │   ├── "supplier.s_comment"(#3.6)
                        │   ├── "supplier.s_name"(#3.1)
                        │   ├── "supplier.s_nationkey"(#3.3)
                        │   ├── "supplier.s_phone"(#3.4)
                        │   └── "supplier.s_suppkey"(#3.0)
                        ├── (.cardinality): 0.00
                        ├── Join
                        │   ├── .join_type: Inner
                        │   ├── .implementation: None
                        │   ├── .join_cond: ("nation.n_regionkey"(#4.2) = "region.r_regionkey"(#5.0))
                        │   ├── (.output_columns):
                        │   │   ┌── "nation.n_comment"(#4.3)
                        │   │   ├── "nation.n_name"(#4.1)
                        │   │   ├── "nation.n_nationkey"(#4.0)
                        │   │   ├── "nation.n_regionkey"(#4.2)
                        │   │   ├── "part.p_brand"(#1.3)
                        │   │   ├── "part.p_comment"(#1.8)
                        │   │   ├── "part.p_container"(#1.6)
                        │   │   ├── "part.p_mfgr"(#1.2)
                        │   │   ├── "part.p_name"(#1.1)
                        │   │   ├── "part.p_partkey"(#1.0)
                        │   │   ├── "part.p_retailprice"(#1.7)
                        │   │   ├── "part.p_size"(#1.5)
                        │   │   ├── "part.p_type"(#1.4)
                        │   │   ├── "partsupp.ps_availqty"(#2.2)
                        │   │   ├── "partsupp.ps_comment"(#2.4)
                        │   │   ├── "partsupp.ps_partkey"(#2.0)
                        │   │   ├── "partsupp.ps_suppkey"(#2.1)
                        │   │   ├── "partsupp.ps_supplycost"(#2.3)
                        │   │   ├── "region.r_comment"(#5.2)
                        │   │   ├── "region.r_name"(#5.1)
                        │   │   ├── "region.r_regionkey"(#5.0)
                        │   │   ├── "supplier.s_acctbal"(#3.5)
                        │   │   ├── "supplier.s_address"(#3.2)
                        │   │   ├── "supplier.s_comment"(#3.6)
                        │   │   ├── "supplier.s_name"(#3.1)
                        │   │   ├── "supplier.s_nationkey"(#3.3)
                        │   │   ├── "supplier.s_phone"(#3.4)
                        │   │   └── "supplier.s_suppkey"(#3.0)
                        │   ├── (.cardinality): 0.00
                        │   ├── Join
                        │   │   ├── .join_type: Inner
                        │   │   ├── .implementation: None
                        │   │   ├── .join_cond: ("supplier.s_nationkey"(#3.3) = "nation.n_nationkey"(#4.0))
                        │   │   ├── (.output_columns):
                        │   │   │   ┌── "nation.n_comment"(#4.3)
                        │   │   │   ├── "nation.n_name"(#4.1)
                        │   │   │   ├── "nation.n_nationkey"(#4.0)
                        │   │   │   ├── "nation.n_regionkey"(#4.2)
                        │   │   │   ├── "part.p_brand"(#1.3)
                        │   │   │   ├── "part.p_comment"(#1.8)
                        │   │   │   ├── "part.p_container"(#1.6)
                        │   │   │   ├── "part.p_mfgr"(#1.2)
                        │   │   │   ├── "part.p_name"(#1.1)
                        │   │   │   ├── "part.p_partkey"(#1.0)
                        │   │   │   ├── "part.p_retailprice"(#1.7)
                        │   │   │   ├── "part.p_size"(#1.5)
                        │   │   │   ├── "part.p_type"(#1.4)
                        │   │   │   ├── "partsupp.ps_availqty"(#2.2)
                        │   │   │   ├── "partsupp.ps_comment"(#2.4)
                        │   │   │   ├── "partsupp.ps_partkey"(#2.0)
                        │   │   │   ├── "partsupp.ps_suppkey"(#2.1)
                        │   │   │   ├── "partsupp.ps_supplycost"(#2.3)
                        │   │   │   ├── "supplier.s_acctbal"(#3.5)
                        │   │   │   ├── "supplier.s_address"(#3.2)
                        │   │   │   ├── "supplier.s_comment"(#3.6)
                        │   │   │   ├── "supplier.s_name"(#3.1)
                        │   │   │   ├── "supplier.s_nationkey"(#3.3)
                        │   │   │   ├── "supplier.s_phone"(#3.4)
                        │   │   │   └── "supplier.s_suppkey"(#3.0)
                        │   │   ├── (.cardinality): 0.00
                        │   │   ├── Join
                        │   │   │   ├── .join_type: Inner
                        │   │   │   ├── .implementation: None
                        │   │   │   ├── .join_cond: ("partsupp.ps_suppkey"(#2.1) = "supplier.s_suppkey"(#3.0))
                        │   │   │   ├── (.output_columns):
                        │   │   │   │   ┌── "part.p_brand"(#1.3)
                        │   │   │   │   ├── "part.p_comment"(#1.8)
                        │   │   │   │   ├── "part.p_container"(#1.6)
                        │   │   │   │   ├── "part.p_mfgr"(#1.2)
                        │   │   │   │   ├── "part.p_name"(#1.1)
                        │   │   │   │   ├── "part.p_partkey"(#1.0)
                        │   │   │   │   ├── "part.p_retailprice"(#1.7)
                        │   │   │   │   ├── "part.p_size"(#1.5)
                        │   │   │   │   ├── "part.p_type"(#1.4)
                        │   │   │   │   ├── "partsupp.ps_availqty"(#2.2)
                        │   │   │   │   ├── "partsupp.ps_comment"(#2.4)
                        │   │   │   │   ├── "partsupp.ps_partkey"(#2.0)
                        │   │   │   │   ├── "partsupp.ps_suppkey"(#2.1)
                        │   │   │   │   ├── "partsupp.ps_supplycost"(#2.3)
                        │   │   │   │   ├── "supplier.s_acctbal"(#3.5)
                        │   │   │   │   ├── "supplier.s_address"(#3.2)
                        │   │   │   │   ├── "supplier.s_comment"(#3.6)
                        │   │   │   │   ├── "supplier.s_name"(#3.1)
                        │   │   │   │   ├── "supplier.s_nationkey"(#3.3)
                        │   │   │   │   ├── "supplier.s_phone"(#3.4)
                        │   │   │   │   └── "supplier.s_suppkey"(#3.0)
                        │   │   │   ├── (.cardinality): 0.00
                        │   │   │   ├── Join
                        │   │   │   │   ├── .join_type: Inner
                        │   │   │   │   ├── .implementation: None
                        │   │   │   │   ├── .join_cond: ("part.p_partkey"(#1.0) = "partsupp.ps_partkey"(#2.0))
                        │   │   │   │   ├── (.output_columns):
                        │   │   │   │   │   ┌── "part.p_brand"(#1.3)
                        │   │   │   │   │   ├── "part.p_comment"(#1.8)
                        │   │   │   │   │   ├── "part.p_container"(#1.6)
                        │   │   │   │   │   ├── "part.p_mfgr"(#1.2)
                        │   │   │   │   │   ├── "part.p_name"(#1.1)
                        │   │   │   │   │   ├── "part.p_partkey"(#1.0)
                        │   │   │   │   │   ├── "part.p_retailprice"(#1.7)
                        │   │   │   │   │   ├── "part.p_size"(#1.5)
                        │   │   │   │   │   ├── "part.p_type"(#1.4)
                        │   │   │   │   │   ├── "partsupp.ps_availqty"(#2.2)
                        │   │   │   │   │   ├── "partsupp.ps_comment"(#2.4)
                        │   │   │   │   │   ├── "partsupp.ps_partkey"(#2.0)
                        │   │   │   │   │   ├── "partsupp.ps_suppkey"(#2.1)
                        │   │   │   │   │   └── "partsupp.ps_supplycost"(#2.3)
                        │   │   │   │   ├── (.cardinality): 0.00
                        │   │   │   │   ├── Get
                        │   │   │   │   │   ├── .data_source_id: 3
                        │   │   │   │   │   ├── .table_index: 1
                        │   │   │   │   │   ├── .implementation: None
                        │   │   │   │   │   ├── (.output_columns):
                        │   │   │   │   │   │   ┌── "part.p_brand"(#1.3)
                        │   │   │   │   │   │   ├── "part.p_comment"(#1.8)
                        │   │   │   │   │   │   ├── "part.p_container"(#1.6)
                        │   │   │   │   │   │   ├── "part.p_mfgr"(#1.2)
                        │   │   │   │   │   │   ├── "part.p_name"(#1.1)
                        │   │   │   │   │   │   ├── "part.p_partkey"(#1.0)
                        │   │   │   │   │   │   ├── "part.p_retailprice"(#1.7)
                        │   │   │   │   │   │   ├── "part.p_size"(#1.5)
                        │   │   │   │   │   │   └── "part.p_type"(#1.4)
                        │   │   │   │   │   └── (.cardinality): 0.00
                        │   │   │   │   └── Get
                        │   │   │   │       ├── .data_source_id: 5
                        │   │   │   │       ├── .table_index: 2
                        │   │   │   │       ├── .implementation: None
                        │   │   │   │       ├── (.output_columns):
                        │   │   │   │       │   ┌── "partsupp.ps_availqty"(#2.2)
                        │   │   │   │       │   ├── "partsupp.ps_comment"(#2.4)
                        │   │   │   │       │   ├── "partsupp.ps_partkey"(#2.0)
                        │   │   │   │       │   ├── "partsupp.ps_suppkey"(#2.1)
                        │   │   │   │       │   └── "partsupp.ps_supplycost"(#2.3)
                        │   │   │   │       └── (.cardinality): 0.00
                        │   │   │   └── Get
                        │   │   │       ├── .data_source_id: 4
                        │   │   │       ├── .table_index: 3
                        │   │   │       ├── .implementation: None
                        │   │   │       ├── (.output_columns):
                        │   │   │       │   ┌── "supplier.s_acctbal"(#3.5)
                        │   │   │       │   ├── "supplier.s_address"(#3.2)
                        │   │   │       │   ├── "supplier.s_comment"(#3.6)
                        │   │   │       │   ├── "supplier.s_name"(#3.1)
                        │   │   │       │   ├── "supplier.s_nationkey"(#3.3)
                        │   │   │       │   ├── "supplier.s_phone"(#3.4)
                        │   │   │       │   └── "supplier.s_suppkey"(#3.0)
                        │   │   │       └── (.cardinality): 0.00
                        │   │   └── Get
                        │   │       ├── .data_source_id: 1
                        │   │       ├── .table_index: 4
                        │   │       ├── .implementation: None
                        │   │       ├── (.output_columns): [ "nation.n_comment"(#4.3), "nation.n_name"(#4.1), "nation.n_nationkey"(#4.0), "nation.n_regionkey"(#4.2) ]
                        │   │       └── (.cardinality): 0.00
                        │   └── Get
                        │       ├── .data_source_id: 2
                        │       ├── .table_index: 5
                        │       ├── .implementation: None
                        │       ├── (.output_columns): [ "region.r_comment"(#5.2), "region.r_name"(#5.1), "region.r_regionkey"(#5.0) ]
                        │       └── (.cardinality): 0.00
                        └── Remap
                            ├── .table_index: 14
                            ├── (.output_columns): [ "__scalar_sq_1.__always_true"(#14.2), "__scalar_sq_1.min(partsupp.ps_supplycost)"(#14.0), "__scalar_sq_1.ps_partkey"(#14.1) ]
                            ├── (.cardinality): 0.00
                            └── Project
                                ├── .table_index: 13
                                ├── .projections: [ "__#12.min(partsupp.ps_supplycost)"(#12.2), "partsupp.ps_partkey"(#2.0), "__#12.__always_true"(#12.1) ]
                                ├── (.output_columns): [ "__#13.__always_true"(#13.2), "__#13.min(partsupp.ps_supplycost)"(#13.0), "__#13.ps_partkey"(#13.1) ]
                                ├── (.cardinality): 0.00
                                └── Project
                                    ├── .table_index: 12
                                    ├── .projections: [ "partsupp.ps_partkey"(#6.0), true::boolean, "__#11.min(partsupp.ps_supplycost)"(#11.0) ]
                                    ├── (.output_columns): [ "__#12.__always_true"(#12.1), "__#12.min(partsupp.ps_supplycost)"(#12.2), "__#12.ps_partkey"(#12.0) ]
                                    ├── (.cardinality): 0.00
                                    └── Aggregate
                                        ├── .key_table_index: 10
                                        ├── .aggregate_table_index: 11
                                        ├── .implementation: None
                                        ├── .exprs: min("partsupp.ps_supplycost"(#6.3))
                                        ├── .keys: "partsupp.ps_partkey"(#6.0)
                                        ├── (.output_columns): [ "__#10.ps_partkey"(#10.0), "__#11.min(partsupp.ps_supplycost)"(#11.0) ]
                                        ├── (.cardinality): 0.00
                                        └── Select
                                            ├── .predicate: "region.r_name"(#9.1) = 'AFRICA'::utf8_view
                                            ├── (.output_columns):
                                            │   ┌── "nation.n_comment"(#8.3)
                                            │   ├── "nation.n_name"(#8.1)
                                            │   ├── "nation.n_nationkey"(#8.0)
                                            │   ├── "nation.n_regionkey"(#8.2)
                                            │   ├── "partsupp.ps_availqty"(#6.2)
                                            │   ├── "partsupp.ps_comment"(#6.4)
                                            │   ├── "partsupp.ps_partkey"(#6.0)
                                            │   ├── "partsupp.ps_suppkey"(#6.1)
                                            │   ├── "partsupp.ps_supplycost"(#6.3)
                                            │   ├── "region.r_comment"(#9.2)
                                            │   ├── "region.r_name"(#9.1)
                                            │   ├── "region.r_regionkey"(#9.0)
                                            │   ├── "supplier.s_acctbal"(#7.5)
                                            │   ├── "supplier.s_address"(#7.2)
                                            │   ├── "supplier.s_comment"(#7.6)
                                            │   ├── "supplier.s_name"(#7.1)
                                            │   ├── "supplier.s_nationkey"(#7.3)
                                            │   ├── "supplier.s_phone"(#7.4)
                                            │   └── "supplier.s_suppkey"(#7.0)
                                            ├── (.cardinality): 0.00
                                            └── Join
                                                ├── .join_type: Inner
                                                ├── .implementation: None
                                                ├── .join_cond: ("nation.n_regionkey"(#8.2) = "region.r_regionkey"(#9.0))
                                                ├── (.output_columns):
                                                │   ┌── "nation.n_comment"(#8.3)
                                                │   ├── "nation.n_name"(#8.1)
                                                │   ├── "nation.n_nationkey"(#8.0)
                                                │   ├── "nation.n_regionkey"(#8.2)
                                                │   ├── "partsupp.ps_availqty"(#6.2)
                                                │   ├── "partsupp.ps_comment"(#6.4)
                                                │   ├── "partsupp.ps_partkey"(#6.0)
                                                │   ├── "partsupp.ps_suppkey"(#6.1)
                                                │   ├── "partsupp.ps_supplycost"(#6.3)
                                                │   ├── "region.r_comment"(#9.2)
                                                │   ├── "region.r_name"(#9.1)
                                                │   ├── "region.r_regionkey"(#9.0)
                                                │   ├── "supplier.s_acctbal"(#7.5)
                                                │   ├── "supplier.s_address"(#7.2)
                                                │   ├── "supplier.s_comment"(#7.6)
                                                │   ├── "supplier.s_name"(#7.1)
                                                │   ├── "supplier.s_nationkey"(#7.3)
                                                │   ├── "supplier.s_phone"(#7.4)
                                                │   └── "supplier.s_suppkey"(#7.0)
                                                ├── (.cardinality): 0.00
                                                ├── Join
                                                │   ├── .join_type: Inner
                                                │   ├── .implementation: None
                                                │   ├── .join_cond: ("supplier.s_nationkey"(#7.3) = "nation.n_nationkey"(#8.0))
                                                │   ├── (.output_columns):
                                                │   │   ┌── "nation.n_comment"(#8.3)
                                                │   │   ├── "nation.n_name"(#8.1)
                                                │   │   ├── "nation.n_nationkey"(#8.0)
                                                │   │   ├── "nation.n_regionkey"(#8.2)
                                                │   │   ├── "partsupp.ps_availqty"(#6.2)
                                                │   │   ├── "partsupp.ps_comment"(#6.4)
                                                │   │   ├── "partsupp.ps_partkey"(#6.0)
                                                │   │   ├── "partsupp.ps_suppkey"(#6.1)
                                                │   │   ├── "partsupp.ps_supplycost"(#6.3)
                                                │   │   ├── "supplier.s_acctbal"(#7.5)
                                                │   │   ├── "supplier.s_address"(#7.2)
                                                │   │   ├── "supplier.s_comment"(#7.6)
                                                │   │   ├── "supplier.s_name"(#7.1)
                                                │   │   ├── "supplier.s_nationkey"(#7.3)
                                                │   │   ├── "supplier.s_phone"(#7.4)
                                                │   │   └── "supplier.s_suppkey"(#7.0)
                                                │   ├── (.cardinality): 0.00
                                                │   ├── Join
                                                │   │   ├── .join_type: Inner
                                                │   │   ├── .implementation: None
                                                │   │   ├── .join_cond: ("partsupp.ps_suppkey"(#6.1) = "supplier.s_suppkey"(#7.0))
                                                │   │   ├── (.output_columns):
                                                │   │   │   ┌── "partsupp.ps_availqty"(#6.2)
                                                │   │   │   ├── "partsupp.ps_comment"(#6.4)
                                                │   │   │   ├── "partsupp.ps_partkey"(#6.0)
                                                │   │   │   ├── "partsupp.ps_suppkey"(#6.1)
                                                │   │   │   ├── "partsupp.ps_supplycost"(#6.3)
                                                │   │   │   ├── "supplier.s_acctbal"(#7.5)
                                                │   │   │   ├── "supplier.s_address"(#7.2)
                                                │   │   │   ├── "supplier.s_comment"(#7.6)
                                                │   │   │   ├── "supplier.s_name"(#7.1)
                                                │   │   │   ├── "supplier.s_nationkey"(#7.3)
                                                │   │   │   ├── "supplier.s_phone"(#7.4)
                                                │   │   │   └── "supplier.s_suppkey"(#7.0)
                                                │   │   ├── (.cardinality): 0.00
                                                │   │   ├── Get
                                                │   │   │   ├── .data_source_id: 5
                                                │   │   │   ├── .table_index: 6
                                                │   │   │   ├── .implementation: None
                                                │   │   │   ├── (.output_columns):
                                                │   │   │   │   ┌── "partsupp.ps_availqty"(#6.2)
                                                │   │   │   │   ├── "partsupp.ps_comment"(#6.4)
                                                │   │   │   │   ├── "partsupp.ps_partkey"(#6.0)
                                                │   │   │   │   ├── "partsupp.ps_suppkey"(#6.1)
                                                │   │   │   │   └── "partsupp.ps_supplycost"(#6.3)
                                                │   │   │   └── (.cardinality): 0.00
                                                │   │   └── Get
                                                │   │       ├── .data_source_id: 4
                                                │   │       ├── .table_index: 7
                                                │   │       ├── .implementation: None
                                                │   │       ├── (.output_columns):
                                                │   │       │   ┌── "supplier.s_acctbal"(#7.5)
                                                │   │       │   ├── "supplier.s_address"(#7.2)
                                                │   │       │   ├── "supplier.s_comment"(#7.6)
                                                │   │       │   ├── "supplier.s_name"(#7.1)
                                                │   │       │   ├── "supplier.s_nationkey"(#7.3)
                                                │   │       │   ├── "supplier.s_phone"(#7.4)
                                                │   │       │   └── "supplier.s_suppkey"(#7.0)
                                                │   │       └── (.cardinality): 0.00
                                                │   └── Get
                                                │       ├── .data_source_id: 1
                                                │       ├── .table_index: 8
                                                │       ├── .implementation: None
                                                │       ├── (.output_columns): [ "nation.n_comment"(#8.3), "nation.n_name"(#8.1), "nation.n_nationkey"(#8.0), "nation.n_regionkey"(#8.2) ]
                                                │       └── (.cardinality): 0.00
                                                └── Get
                                                    ├── .data_source_id: 2
                                                    ├── .table_index: 9
                                                    ├── .implementation: None
                                                    ├── (.output_columns): [ "region.r_comment"(#9.2), "region.r_name"(#9.1), "region.r_regionkey"(#9.0) ]
                                                    └── (.cardinality): 0.00

logical_plan after optd-decorrelation:
SAME TEXT AS ABOVE

logical_plan after optd-simplification:
Limit
├── .skip: 0::bigint
├── .fetch: 100::bigint
├── (.output_columns):
│   ┌── "__#17.n_name"(#17.2)
│   ├── "__#17.p_mfgr"(#17.4)
│   ├── "__#17.p_partkey"(#17.3)
│   ├── "__#17.s_acctbal"(#17.0)
│   ├── "__#17.s_address"(#17.5)
│   ├── "__#17.s_comment"(#17.7)
│   ├── "__#17.s_name"(#17.1)
│   └── "__#17.s_phone"(#17.6)
├── (.cardinality): 0.00
└── OrderBy
    ├── ordering_exprs: [ "__#17.s_acctbal"(#17.0) DESC, "__#17.n_name"(#17.2) ASC, "__#17.s_name"(#17.1) ASC, "__#17.p_partkey"(#17.3) ASC ]
    ├── (.output_columns):
    │   ┌── "__#17.n_name"(#17.2)
    │   ├── "__#17.p_mfgr"(#17.4)
    │   ├── "__#17.p_partkey"(#17.3)
    │   ├── "__#17.s_acctbal"(#17.0)
    │   ├── "__#17.s_address"(#17.5)
    │   ├── "__#17.s_comment"(#17.7)
    │   ├── "__#17.s_name"(#17.1)
    │   └── "__#17.s_phone"(#17.6)
    ├── (.cardinality): 0.00
    └── Project
        ├── .table_index: 17
        ├── .projections:
        │   ┌── "supplier.s_acctbal"(#3.5)
        │   ├── "supplier.s_name"(#3.1)
        │   ├── "nation.n_name"(#4.1)
        │   ├── "part.p_partkey"(#1.0)
        │   ├── "part.p_mfgr"(#1.2)
        │   ├── "supplier.s_address"(#3.2)
        │   ├── "supplier.s_phone"(#3.4)
        │   └── "supplier.s_comment"(#3.6)
        ├── (.output_columns):
        │   ┌── "__#17.n_name"(#17.2)
        │   ├── "__#17.p_mfgr"(#17.4)
        │   ├── "__#17.p_partkey"(#17.3)
        │   ├── "__#17.s_acctbal"(#17.0)
        │   ├── "__#17.s_address"(#17.5)
        │   ├── "__#17.s_comment"(#17.7)
        │   ├── "__#17.s_name"(#17.1)
        │   └── "__#17.s_phone"(#17.6)
        ├── (.cardinality): 0.00
        └── Join
            ├── .join_type: Inner
            ├── .implementation: None
            ├── .join_cond: ("part.p_partkey"(#1.0) = "__scalar_sq_1.ps_partkey"(#14.1)) AND ("partsupp.ps_supplycost"(#2.3) = "__scalar_sq_1.min(partsupp.ps_supplycost)"(#14.0))
            ├── (.output_columns):
            │   ┌── "__scalar_sq_1.__always_true"(#14.2)
            │   ├── "__scalar_sq_1.min(partsupp.ps_supplycost)"(#14.0)
            │   ├── "__scalar_sq_1.ps_partkey"(#14.1)
            │   ├── "nation.n_name"(#4.1)
            │   ├── "nation.n_nationkey"(#4.0)
            │   ├── "nation.n_regionkey"(#4.2)
            │   ├── "part.p_mfgr"(#1.2)
            │   ├── "part.p_partkey"(#1.0)
            │   ├── "part.p_size"(#1.5)
            │   ├── "part.p_type"(#1.4)
            │   ├── "partsupp.ps_partkey"(#2.0)
            │   ├── "partsupp.ps_suppkey"(#2.1)
            │   ├── "partsupp.ps_supplycost"(#2.3)
            │   ├── "region.r_name"(#5.1)
            │   ├── "region.r_regionkey"(#5.0)
            │   ├── "supplier.s_acctbal"(#3.5)
            │   ├── "supplier.s_address"(#3.2)
            │   ├── "supplier.s_comment"(#3.6)
            │   ├── "supplier.s_name"(#3.1)
            │   ├── "supplier.s_nationkey"(#3.3)
            │   ├── "supplier.s_phone"(#3.4)
            │   └── "supplier.s_suppkey"(#3.0)
            ├── (.cardinality): 0.00
            ├── Join
            │   ├── .join_type: Inner
            │   ├── .implementation: None
            │   ├── .join_cond: "nation.n_regionkey"(#4.2) = "region.r_regionkey"(#5.0)
            │   ├── (.output_columns):
            │   │   ┌── "nation.n_name"(#4.1)
            │   │   ├── "nation.n_nationkey"(#4.0)
            │   │   ├── "nation.n_regionkey"(#4.2)
            │   │   ├── "part.p_mfgr"(#1.2)
            │   │   ├── "part.p_partkey"(#1.0)
            │   │   ├── "part.p_size"(#1.5)
            │   │   ├── "part.p_type"(#1.4)
            │   │   ├── "partsupp.ps_partkey"(#2.0)
            │   │   ├── "partsupp.ps_suppkey"(#2.1)
            │   │   ├── "partsupp.ps_supplycost"(#2.3)
            │   │   ├── "region.r_name"(#5.1)
            │   │   ├── "region.r_regionkey"(#5.0)
            │   │   ├── "supplier.s_acctbal"(#3.5)
            │   │   ├── "supplier.s_address"(#3.2)
            │   │   ├── "supplier.s_comment"(#3.6)
            │   │   ├── "supplier.s_name"(#3.1)
            │   │   ├── "supplier.s_nationkey"(#3.3)
            │   │   ├── "supplier.s_phone"(#3.4)
            │   │   └── "supplier.s_suppkey"(#3.0)
            │   ├── (.cardinality): 0.00
            │   ├── Join
            │   │   ├── .join_type: Inner
            │   │   ├── .implementation: None
            │   │   ├── .join_cond: "supplier.s_nationkey"(#3.3) = "nation.n_nationkey"(#4.0)
            │   │   ├── (.output_columns):
            │   │   │   ┌── "nation.n_name"(#4.1)
            │   │   │   ├── "nation.n_nationkey"(#4.0)
            │   │   │   ├── "nation.n_regionkey"(#4.2)
            │   │   │   ├── "part.p_mfgr"(#1.2)
            │   │   │   ├── "part.p_partkey"(#1.0)
            │   │   │   ├── "part.p_size"(#1.5)
            │   │   │   ├── "part.p_type"(#1.4)
            │   │   │   ├── "partsupp.ps_partkey"(#2.0)
            │   │   │   ├── "partsupp.ps_suppkey"(#2.1)
            │   │   │   ├── "partsupp.ps_supplycost"(#2.3)
            │   │   │   ├── "supplier.s_acctbal"(#3.5)
            │   │   │   ├── "supplier.s_address"(#3.2)
            │   │   │   ├── "supplier.s_comment"(#3.6)
            │   │   │   ├── "supplier.s_name"(#3.1)
            │   │   │   ├── "supplier.s_nationkey"(#3.3)
            │   │   │   ├── "supplier.s_phone"(#3.4)
            │   │   │   └── "supplier.s_suppkey"(#3.0)
            │   │   ├── (.cardinality): 0.00
            │   │   ├── Join
            │   │   │   ├── .join_type: Inner
            │   │   │   ├── .implementation: None
            │   │   │   ├── .join_cond: "partsupp.ps_suppkey"(#2.1) = "supplier.s_suppkey"(#3.0)
            │   │   │   ├── (.output_columns):
            │   │   │   │   ┌── "part.p_mfgr"(#1.2)
            │   │   │   │   ├── "part.p_partkey"(#1.0)
            │   │   │   │   ├── "part.p_size"(#1.5)
            │   │   │   │   ├── "part.p_type"(#1.4)
            │   │   │   │   ├── "partsupp.ps_partkey"(#2.0)
            │   │   │   │   ├── "partsupp.ps_suppkey"(#2.1)
            │   │   │   │   ├── "partsupp.ps_supplycost"(#2.3)
            │   │   │   │   ├── "supplier.s_acctbal"(#3.5)
            │   │   │   │   ├── "supplier.s_address"(#3.2)
            │   │   │   │   ├── "supplier.s_comment"(#3.6)
            │   │   │   │   ├── "supplier.s_name"(#3.1)
            │   │   │   │   ├── "supplier.s_nationkey"(#3.3)
            │   │   │   │   ├── "supplier.s_phone"(#3.4)
            │   │   │   │   └── "supplier.s_suppkey"(#3.0)
            │   │   │   ├── (.cardinality): 0.00
            │   │   │   ├── Join
            │   │   │   │   ├── .join_type: Inner
            │   │   │   │   ├── .implementation: None
            │   │   │   │   ├── .join_cond: "part.p_partkey"(#1.0) = "partsupp.ps_partkey"(#2.0)
            │   │   │   │   ├── (.output_columns):
            │   │   │   │   │   ┌── "part.p_mfgr"(#1.2)
            │   │   │   │   │   ├── "part.p_partkey"(#1.0)
            │   │   │   │   │   ├── "part.p_size"(#1.5)
            │   │   │   │   │   ├── "part.p_type"(#1.4)
            │   │   │   │   │   ├── "partsupp.ps_partkey"(#2.0)
            │   │   │   │   │   ├── "partsupp.ps_suppkey"(#2.1)
            │   │   │   │   │   └── "partsupp.ps_supplycost"(#2.3)
            │   │   │   │   ├── (.cardinality): 0.00
            │   │   │   │   ├── Select
            │   │   │   │   │   ├── .predicate: ("part.p_size"(#1.5) = 4::integer) AND ("part.p_type"(#1.4) LIKE '%TIN'::utf8_view)
            │   │   │   │   │   ├── (.output_columns): [ "part.p_mfgr"(#1.2), "part.p_partkey"(#1.0), "part.p_size"(#1.5), "part.p_type"(#1.4) ]
            │   │   │   │   │   ├── (.cardinality): 0.00
            │   │   │   │   │   └── Get
            │   │   │   │   │       ├── .data_source_id: 3
            │   │   │   │   │       ├── .table_index: 1
            │   │   │   │   │       ├── .implementation: None
            │   │   │   │   │       ├── (.output_columns): [ "part.p_mfgr"(#1.2), "part.p_partkey"(#1.0), "part.p_size"(#1.5), "part.p_type"(#1.4) ]
            │   │   │   │   │       └── (.cardinality): 0.00
            │   │   │   │   └── Get
            │   │   │   │       ├── .data_source_id: 5
            │   │   │   │       ├── .table_index: 2
            │   │   │   │       ├── .implementation: None
            │   │   │   │       ├── (.output_columns): [ "partsupp.ps_partkey"(#2.0), "partsupp.ps_suppkey"(#2.1), "partsupp.ps_supplycost"(#2.3) ]
            │   │   │   │       └── (.cardinality): 0.00
            │   │   │   └── Get
            │   │   │       ├── .data_source_id: 4
            │   │   │       ├── .table_index: 3
            │   │   │       ├── .implementation: None
            │   │   │       ├── (.output_columns):
            │   │   │       │   ┌── "supplier.s_acctbal"(#3.5)
            │   │   │       │   ├── "supplier.s_address"(#3.2)
            │   │   │       │   ├── "supplier.s_comment"(#3.6)
            │   │   │       │   ├── "supplier.s_name"(#3.1)
            │   │   │       │   ├── "supplier.s_nationkey"(#3.3)
            │   │   │       │   ├── "supplier.s_phone"(#3.4)
            │   │   │       │   └── "supplier.s_suppkey"(#3.0)
            │   │   │       └── (.cardinality): 0.00
            │   │   └── Get
            │   │       ├── .data_source_id: 1
            │   │       ├── .table_index: 4
            │   │       ├── .implementation: None
            │   │       ├── (.output_columns): [ "nation.n_name"(#4.1), "nation.n_nationkey"(#4.0), "nation.n_regionkey"(#4.2) ]
            │   │       └── (.cardinality): 0.00
            │   └── Select
            │       ├── .predicate: "region.r_name"(#5.1) = 'AFRICA'::utf8_view
            │       ├── (.output_columns): [ "region.r_name"(#5.1), "region.r_regionkey"(#5.0) ]
            │       ├── (.cardinality): 0.00
            │       └── Get
            │           ├── .data_source_id: 2
            │           ├── .table_index: 5
            │           ├── .implementation: None
            │           ├── (.output_columns): [ "region.r_name"(#5.1), "region.r_regionkey"(#5.0) ]
            │           └── (.cardinality): 0.00
            └── Remap
                ├── .table_index: 14
                ├── (.output_columns): [ "__scalar_sq_1.__always_true"(#14.2), "__scalar_sq_1.min(partsupp.ps_supplycost)"(#14.0), "__scalar_sq_1.ps_partkey"(#14.1) ]
                ├── (.cardinality): 0.00
                └── Project
                    ├── .table_index: 13
                    ├── .projections: [ "__#11.min(partsupp.ps_supplycost)"(#11.0), "partsupp.ps_partkey"(#2.0), true::boolean ]
                    ├── (.output_columns): [ "__#13.__always_true"(#13.2), "__#13.min(partsupp.ps_supplycost)"(#13.0), "__#13.ps_partkey"(#13.1) ]
                    ├── (.cardinality): 0.00
                    └── Aggregate
                        ├── .key_table_index: 10
                        ├── .aggregate_table_index: 11
                        ├── .implementation: None
                        ├── .exprs: min("partsupp.ps_supplycost"(#6.3))
                        ├── .keys: "partsupp.ps_partkey"(#6.0)
                        ├── (.output_columns): [ "__#10.ps_partkey"(#10.0), "__#11.min(partsupp.ps_supplycost)"(#11.0) ]
                        ├── (.cardinality): 0.00
                        └── Join
                            ├── .join_type: Inner
                            ├── .implementation: None
                            ├── .join_cond: "nation.n_regionkey"(#8.2) = "region.r_regionkey"(#9.0)
                            ├── (.output_columns):
                            │   ┌── "nation.n_nationkey"(#8.0)
                            │   ├── "nation.n_regionkey"(#8.2)
                            │   ├── "partsupp.ps_partkey"(#6.0)
                            │   ├── "partsupp.ps_suppkey"(#6.1)
                            │   ├── "partsupp.ps_supplycost"(#6.3)
                            │   ├── "region.r_name"(#9.1)
                            │   ├── "region.r_regionkey"(#9.0)
                            │   ├── "supplier.s_nationkey"(#7.3)
                            │   └── "supplier.s_suppkey"(#7.0)
                            ├── (.cardinality): 0.00
                            ├── Join
                            │   ├── .join_type: Inner
                            │   ├── .implementation: None
                            │   ├── .join_cond: "supplier.s_nationkey"(#7.3) = "nation.n_nationkey"(#8.0)
                            │   ├── (.output_columns):
                            │   │   ┌── "nation.n_nationkey"(#8.0)
                            │   │   ├── "nation.n_regionkey"(#8.2)
                            │   │   ├── "partsupp.ps_partkey"(#6.0)
                            │   │   ├── "partsupp.ps_suppkey"(#6.1)
                            │   │   ├── "partsupp.ps_supplycost"(#6.3)
                            │   │   ├── "supplier.s_nationkey"(#7.3)
                            │   │   └── "supplier.s_suppkey"(#7.0)
                            │   ├── (.cardinality): 0.00
                            │   ├── Join
                            │   │   ├── .join_type: Inner
                            │   │   ├── .implementation: None
                            │   │   ├── .join_cond: "partsupp.ps_suppkey"(#6.1) = "supplier.s_suppkey"(#7.0)
                            │   │   ├── (.output_columns):
                            │   │   │   ┌── "partsupp.ps_partkey"(#6.0)
                            │   │   │   ├── "partsupp.ps_suppkey"(#6.1)
                            │   │   │   ├── "partsupp.ps_supplycost"(#6.3)
                            │   │   │   ├── "supplier.s_nationkey"(#7.3)
                            │   │   │   └── "supplier.s_suppkey"(#7.0)
                            │   │   ├── (.cardinality): 0.00
                            │   │   ├── Get
                            │   │   │   ├── .data_source_id: 5
                            │   │   │   ├── .table_index: 6
                            │   │   │   ├── .implementation: None
                            │   │   │   ├── (.output_columns): [ "partsupp.ps_partkey"(#6.0), "partsupp.ps_suppkey"(#6.1), "partsupp.ps_supplycost"(#6.3) ]
                            │   │   │   └── (.cardinality): 0.00
                            │   │   └── Get
                            │   │       ├── .data_source_id: 4
                            │   │       ├── .table_index: 7
                            │   │       ├── .implementation: None
                            │   │       ├── (.output_columns): [ "supplier.s_nationkey"(#7.3), "supplier.s_suppkey"(#7.0) ]
                            │   │       └── (.cardinality): 0.00
                            │   └── Get
                            │       ├── .data_source_id: 1
                            │       ├── .table_index: 8
                            │       ├── .implementation: None
                            │       ├── (.output_columns): [ "nation.n_nationkey"(#8.0), "nation.n_regionkey"(#8.2) ]
                            │       └── (.cardinality): 0.00
                            └── Select
                                ├── .predicate: "region.r_name"(#9.1) = 'AFRICA'::utf8_view
                                ├── (.output_columns): [ "region.r_name"(#9.1), "region.r_regionkey"(#9.0) ]
                                ├── (.cardinality): 0.00
                                └── Get
                                    ├── .data_source_id: 2
                                    ├── .table_index: 9
                                    ├── .implementation: None
                                    ├── (.output_columns): [ "region.r_name"(#9.1), "region.r_regionkey"(#9.0) ]
                                    └── (.cardinality): 0.00

physical_plan after optd-finalized:
Limit
├── .skip: 0::bigint
├── .fetch: 100::bigint
├── (.output_columns):
│   ┌── "__#17.n_name"(#17.2)
│   ├── "__#17.p_mfgr"(#17.4)
│   ├── "__#17.p_partkey"(#17.3)
│   ├── "__#17.s_acctbal"(#17.0)
│   ├── "__#17.s_address"(#17.5)
│   ├── "__#17.s_comment"(#17.7)
│   ├── "__#17.s_name"(#17.1)
│   └── "__#17.s_phone"(#17.6)
├── (.cardinality): 0.00
└── EnforcerSort
    ├── tuple_ordering: [(#17.0, Desc), (#17.2, Asc), (#17.1, Asc), (#17.3, Asc)]
    ├── (.output_columns):
    │   ┌── "__#17.n_name"(#17.2)
    │   ├── "__#17.p_mfgr"(#17.4)
    │   ├── "__#17.p_partkey"(#17.3)
    │   ├── "__#17.s_acctbal"(#17.0)
    │   ├── "__#17.s_address"(#17.5)
    │   ├── "__#17.s_comment"(#17.7)
    │   ├── "__#17.s_name"(#17.1)
    │   └── "__#17.s_phone"(#17.6)
    ├── (.cardinality): 0.00
    └── Project
        ├── .table_index: 17
        ├── .projections:
        │   ┌── "supplier.s_acctbal"(#3.5)
        │   ├── "supplier.s_name"(#3.1)
        │   ├── "nation.n_name"(#4.1)
        │   ├── "part.p_partkey"(#1.0)
        │   ├── "part.p_mfgr"(#1.2)
        │   ├── "supplier.s_address"(#3.2)
        │   ├── "supplier.s_phone"(#3.4)
        │   └── "supplier.s_comment"(#3.6)
        ├── (.output_columns):
        │   ┌── "__#17.n_name"(#17.2)
        │   ├── "__#17.p_mfgr"(#17.4)
        │   ├── "__#17.p_partkey"(#17.3)
        │   ├── "__#17.s_acctbal"(#17.0)
        │   ├── "__#17.s_address"(#17.5)
        │   ├── "__#17.s_comment"(#17.7)
        │   ├── "__#17.s_name"(#17.1)
        │   └── "__#17.s_phone"(#17.6)
        ├── (.cardinality): 0.00
        └── Join
            ├── .join_type: Inner
            ├── .implementation: None
            ├── .join_cond: ("part.p_partkey"(#1.0) = "__scalar_sq_1.ps_partkey"(#14.1)) AND ("partsupp.ps_supplycost"(#2.3) = "__scalar_sq_1.min(partsupp.ps_supplycost)"(#14.0))
            ├── (.output_columns):
            │   ┌── "__scalar_sq_1.__always_true"(#14.2)
            │   ├── "__scalar_sq_1.min(partsupp.ps_supplycost)"(#14.0)
            │   ├── "__scalar_sq_1.ps_partkey"(#14.1)
            │   ├── "nation.n_name"(#4.1)
            │   ├── "nation.n_nationkey"(#4.0)
            │   ├── "nation.n_regionkey"(#4.2)
            │   ├── "part.p_mfgr"(#1.2)
            │   ├── "part.p_partkey"(#1.0)
            │   ├── "part.p_size"(#1.5)
            │   ├── "part.p_type"(#1.4)
            │   ├── "partsupp.ps_partkey"(#2.0)
            │   ├── "partsupp.ps_suppkey"(#2.1)
            │   ├── "partsupp.ps_supplycost"(#2.3)
            │   ├── "region.r_name"(#5.1)
            │   ├── "region.r_regionkey"(#5.0)
            │   ├── "supplier.s_acctbal"(#3.5)
            │   ├── "supplier.s_address"(#3.2)
            │   ├── "supplier.s_comment"(#3.6)
            │   ├── "supplier.s_name"(#3.1)
            │   ├── "supplier.s_nationkey"(#3.3)
            │   ├── "supplier.s_phone"(#3.4)
            │   └── "supplier.s_suppkey"(#3.0)
            ├── (.cardinality): 0.00
            ├── Join
            │   ├── .join_type: Inner
            │   ├── .implementation: None
            │   ├── .join_cond: "nation.n_regionkey"(#4.2) = "region.r_regionkey"(#5.0)
            │   ├── (.output_columns):
            │   │   ┌── "nation.n_name"(#4.1)
            │   │   ├── "nation.n_nationkey"(#4.0)
            │   │   ├── "nation.n_regionkey"(#4.2)
            │   │   ├── "part.p_mfgr"(#1.2)
            │   │   ├── "part.p_partkey"(#1.0)
            │   │   ├── "part.p_size"(#1.5)
            │   │   ├── "part.p_type"(#1.4)
            │   │   ├── "partsupp.ps_partkey"(#2.0)
            │   │   ├── "partsupp.ps_suppkey"(#2.1)
            │   │   ├── "partsupp.ps_supplycost"(#2.3)
            │   │   ├── "region.r_name"(#5.1)
            │   │   ├── "region.r_regionkey"(#5.0)
            │   │   ├── "supplier.s_acctbal"(#3.5)
            │   │   ├── "supplier.s_address"(#3.2)
            │   │   ├── "supplier.s_comment"(#3.6)
            │   │   ├── "supplier.s_name"(#3.1)
            │   │   ├── "supplier.s_nationkey"(#3.3)
            │   │   ├── "supplier.s_phone"(#3.4)
            │   │   └── "supplier.s_suppkey"(#3.0)
            │   ├── (.cardinality): 0.00
            │   ├── Join
            │   │   ├── .join_type: Inner
            │   │   ├── .implementation: None
            │   │   ├── .join_cond: "supplier.s_nationkey"(#3.3) = "nation.n_nationkey"(#4.0)
            │   │   ├── (.output_columns):
            │   │   │   ┌── "nation.n_name"(#4.1)
            │   │   │   ├── "nation.n_nationkey"(#4.0)
            │   │   │   ├── "nation.n_regionkey"(#4.2)
            │   │   │   ├── "part.p_mfgr"(#1.2)
            │   │   │   ├── "part.p_partkey"(#1.0)
            │   │   │   ├── "part.p_size"(#1.5)
            │   │   │   ├── "part.p_type"(#1.4)
            │   │   │   ├── "partsupp.ps_partkey"(#2.0)
            │   │   │   ├── "partsupp.ps_suppkey"(#2.1)
            │   │   │   ├── "partsupp.ps_supplycost"(#2.3)
            │   │   │   ├── "supplier.s_acctbal"(#3.5)
            │   │   │   ├── "supplier.s_address"(#3.2)
            │   │   │   ├── "supplier.s_comment"(#3.6)
            │   │   │   ├── "supplier.s_name"(#3.1)
            │   │   │   ├── "supplier.s_nationkey"(#3.3)
            │   │   │   ├── "supplier.s_phone"(#3.4)
            │   │   │   └── "supplier.s_suppkey"(#3.0)
            │   │   ├── (.cardinality): 0.00
            │   │   ├── Join
            │   │   │   ├── .join_type: Inner
            │   │   │   ├── .implementation: None
            │   │   │   ├── .join_cond: "partsupp.ps_suppkey"(#2.1) = "supplier.s_suppkey"(#3.0)
            │   │   │   ├── (.output_columns):
            │   │   │   │   ┌── "part.p_mfgr"(#1.2)
            │   │   │   │   ├── "part.p_partkey"(#1.0)
            │   │   │   │   ├── "part.p_size"(#1.5)
            │   │   │   │   ├── "part.p_type"(#1.4)
            │   │   │   │   ├── "partsupp.ps_partkey"(#2.0)
            │   │   │   │   ├── "partsupp.ps_suppkey"(#2.1)
            │   │   │   │   ├── "partsupp.ps_supplycost"(#2.3)
            │   │   │   │   ├── "supplier.s_acctbal"(#3.5)
            │   │   │   │   ├── "supplier.s_address"(#3.2)
            │   │   │   │   ├── "supplier.s_comment"(#3.6)
            │   │   │   │   ├── "supplier.s_name"(#3.1)
            │   │   │   │   ├── "supplier.s_nationkey"(#3.3)
            │   │   │   │   ├── "supplier.s_phone"(#3.4)
            │   │   │   │   └── "supplier.s_suppkey"(#3.0)
            │   │   │   ├── (.cardinality): 0.00
            │   │   │   ├── Join
            │   │   │   │   ├── .join_type: Inner
            │   │   │   │   ├── .implementation: None
            │   │   │   │   ├── .join_cond: "part.p_partkey"(#1.0) = "partsupp.ps_partkey"(#2.0)
            │   │   │   │   ├── (.output_columns):
            │   │   │   │   │   ┌── "part.p_mfgr"(#1.2)
            │   │   │   │   │   ├── "part.p_partkey"(#1.0)
            │   │   │   │   │   ├── "part.p_size"(#1.5)
            │   │   │   │   │   ├── "part.p_type"(#1.4)
            │   │   │   │   │   ├── "partsupp.ps_partkey"(#2.0)
            │   │   │   │   │   ├── "partsupp.ps_suppkey"(#2.1)
            │   │   │   │   │   └── "partsupp.ps_supplycost"(#2.3)
            │   │   │   │   ├── (.cardinality): 0.00
            │   │   │   │   ├── Select
            │   │   │   │   │   ├── .predicate: ("part.p_size"(#1.5) = 4::integer) AND ("part.p_type"(#1.4) LIKE '%TIN'::utf8_view)
            │   │   │   │   │   ├── (.output_columns): [ "part.p_mfgr"(#1.2), "part.p_partkey"(#1.0), "part.p_size"(#1.5), "part.p_type"(#1.4) ]
            │   │   │   │   │   ├── (.cardinality): 0.00
            │   │   │   │   │   └── Get
            │   │   │   │   │       ├── .data_source_id: 3
            │   │   │   │   │       ├── .table_index: 1
            │   │   │   │   │       ├── .implementation: None
            │   │   │   │   │       ├── (.output_columns): [ "part.p_mfgr"(#1.2), "part.p_partkey"(#1.0), "part.p_size"(#1.5), "part.p_type"(#1.4) ]
            │   │   │   │   │       └── (.cardinality): 0.00
            │   │   │   │   └── Get
            │   │   │   │       ├── .data_source_id: 5
            │   │   │   │       ├── .table_index: 2
            │   │   │   │       ├── .implementation: None
            │   │   │   │       ├── (.output_columns): [ "partsupp.ps_partkey"(#2.0), "partsupp.ps_suppkey"(#2.1), "partsupp.ps_supplycost"(#2.3) ]
            │   │   │   │       └── (.cardinality): 0.00
            │   │   │   └── Get
            │   │   │       ├── .data_source_id: 4
            │   │   │       ├── .table_index: 3
            │   │   │       ├── .implementation: None
            │   │   │       ├── (.output_columns):
            │   │   │       │   ┌── "supplier.s_acctbal"(#3.5)
            │   │   │       │   ├── "supplier.s_address"(#3.2)
            │   │   │       │   ├── "supplier.s_comment"(#3.6)
            │   │   │       │   ├── "supplier.s_name"(#3.1)
            │   │   │       │   ├── "supplier.s_nationkey"(#3.3)
            │   │   │       │   ├── "supplier.s_phone"(#3.4)
            │   │   │       │   └── "supplier.s_suppkey"(#3.0)
            │   │   │       └── (.cardinality): 0.00
            │   │   └── Get
            │   │       ├── .data_source_id: 1
            │   │       ├── .table_index: 4
            │   │       ├── .implementation: None
            │   │       ├── (.output_columns): [ "nation.n_name"(#4.1), "nation.n_nationkey"(#4.0), "nation.n_regionkey"(#4.2) ]
            │   │       └── (.cardinality): 0.00
            │   └── Select
            │       ├── .predicate: "region.r_name"(#5.1) = 'AFRICA'::utf8_view
            │       ├── (.output_columns): [ "region.r_name"(#5.1), "region.r_regionkey"(#5.0) ]
            │       ├── (.cardinality): 0.00
            │       └── Get
            │           ├── .data_source_id: 2
            │           ├── .table_index: 5
            │           ├── .implementation: None
            │           ├── (.output_columns): [ "region.r_name"(#5.1), "region.r_regionkey"(#5.0) ]
            │           └── (.cardinality): 0.00
            └── Remap
                ├── .table_index: 14
                ├── (.output_columns): [ "__scalar_sq_1.__always_true"(#14.2), "__scalar_sq_1.min(partsupp.ps_supplycost)"(#14.0), "__scalar_sq_1.ps_partkey"(#14.1) ]
                ├── (.cardinality): 0.00
                └── Project
                    ├── .table_index: 13
                    ├── .projections: [ "__#11.min(partsupp.ps_supplycost)"(#11.0), "partsupp.ps_partkey"(#2.0), true::boolean ]
                    ├── (.output_columns): [ "__#13.__always_true"(#13.2), "__#13.min(partsupp.ps_supplycost)"(#13.0), "__#13.ps_partkey"(#13.1) ]
                    ├── (.cardinality): 0.00
                    └── Aggregate
                        ├── .key_table_index: 10
                        ├── .aggregate_table_index: 11
                        ├── .implementation: None
                        ├── .exprs: min("partsupp.ps_supplycost"(#6.3))
                        ├── .keys: "partsupp.ps_partkey"(#6.0)
                        ├── (.output_columns): [ "__#10.ps_partkey"(#10.0), "__#11.min(partsupp.ps_supplycost)"(#11.0) ]
                        ├── (.cardinality): 0.00
                        └── Join
                            ├── .join_type: Inner
                            ├── .implementation: None
                            ├── .join_cond: "nation.n_regionkey"(#8.2) = "region.r_regionkey"(#9.0)
                            ├── (.output_columns):
                            │   ┌── "nation.n_nationkey"(#8.0)
                            │   ├── "nation.n_regionkey"(#8.2)
                            │   ├── "partsupp.ps_partkey"(#6.0)
                            │   ├── "partsupp.ps_suppkey"(#6.1)
                            │   ├── "partsupp.ps_supplycost"(#6.3)
                            │   ├── "region.r_name"(#9.1)
                            │   ├── "region.r_regionkey"(#9.0)
                            │   ├── "supplier.s_nationkey"(#7.3)
                            │   └── "supplier.s_suppkey"(#7.0)
                            ├── (.cardinality): 0.00
                            ├── Join
                            │   ├── .join_type: Inner
                            │   ├── .implementation: None
                            │   ├── .join_cond: "supplier.s_nationkey"(#7.3) = "nation.n_nationkey"(#8.0)
                            │   ├── (.output_columns):
                            │   │   ┌── "nation.n_nationkey"(#8.0)
                            │   │   ├── "nation.n_regionkey"(#8.2)
                            │   │   ├── "partsupp.ps_partkey"(#6.0)
                            │   │   ├── "partsupp.ps_suppkey"(#6.1)
                            │   │   ├── "partsupp.ps_supplycost"(#6.3)
                            │   │   ├── "supplier.s_nationkey"(#7.3)
                            │   │   └── "supplier.s_suppkey"(#7.0)
                            │   ├── (.cardinality): 0.00
                            │   ├── Join
                            │   │   ├── .join_type: Inner
                            │   │   ├── .implementation: None
                            │   │   ├── .join_cond: "partsupp.ps_suppkey"(#6.1) = "supplier.s_suppkey"(#7.0)
                            │   │   ├── (.output_columns):
                            │   │   │   ┌── "partsupp.ps_partkey"(#6.0)
                            │   │   │   ├── "partsupp.ps_suppkey"(#6.1)
                            │   │   │   ├── "partsupp.ps_supplycost"(#6.3)
                            │   │   │   ├── "supplier.s_nationkey"(#7.3)
                            │   │   │   └── "supplier.s_suppkey"(#7.0)
                            │   │   ├── (.cardinality): 0.00
                            │   │   ├── Get
                            │   │   │   ├── .data_source_id: 5
                            │   │   │   ├── .table_index: 6
                            │   │   │   ├── .implementation: None
                            │   │   │   ├── (.output_columns): [ "partsupp.ps_partkey"(#6.0), "partsupp.ps_suppkey"(#6.1), "partsupp.ps_supplycost"(#6.3) ]
                            │   │   │   └── (.cardinality): 0.00
                            │   │   └── Get
                            │   │       ├── .data_source_id: 4
                            │   │       ├── .table_index: 7
                            │   │       ├── .implementation: None
                            │   │       ├── (.output_columns): [ "supplier.s_nationkey"(#7.3), "supplier.s_suppkey"(#7.0) ]
                            │   │       └── (.cardinality): 0.00
                            │   └── Get
                            │       ├── .data_source_id: 1
                            │       ├── .table_index: 8
                            │       ├── .implementation: None
                            │       ├── (.output_columns): [ "nation.n_nationkey"(#8.0), "nation.n_regionkey"(#8.2) ]
                            │       └── (.cardinality): 0.00
                            └── Select
                                ├── .predicate: "region.r_name"(#9.1) = 'AFRICA'::utf8_view
                                ├── (.output_columns): [ "region.r_name"(#9.1), "region.r_regionkey"(#9.0) ]
                                ├── (.cardinality): 0.00
                                └── Get
                                    ├── .data_source_id: 2
                                    ├── .table_index: 9
                                    ├── .implementation: None
                                    ├── (.output_columns): [ "region.r_name"(#9.1), "region.r_regionkey"(#9.0) ]
                                    └── (.cardinality): 0.00
*/

