-- TPC-H Q14
SELECT
    100.00 * sum(case when p_type like 'PROMO%'
                    then l_extendedprice * (1 - l_discount)
                    else 0 end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
FROM
    lineitem,
    part
WHERE
    l_partkey = p_partkey
    AND l_shipdate >= DATE '1995-09-01'
    AND l_shipdate < DATE '1995-09-01' + INTERVAL '1' MONTH;

/*
Error
External error: Connector error: Unsupported df logical expr: CASE WHEN part.p_type LIKE Utf8View("PROMO%") THEN __common_expr_1 ELSE Decimal128(Some(0),38,4) END
*/

