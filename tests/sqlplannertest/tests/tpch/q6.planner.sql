-- TPC-H Q6
SELECT
    SUM(l_extendedprice * l_discount) AS revenue_loss
FROM
    lineitem
WHERE
    l_shipdate >= DATE '2023-01-01'
    AND l_shipdate < DATE '2024-01-01'
    AND l_discount BETWEEN 0.05 AND 0.07
    AND l_quantity < 24;

/*
Error
External error: Unsupported df logical expr: CAST(lineitem.l_discount AS Decimal128(30, 15)) BETWEEN CAST(Float64(0.05) AS Decimal128(30, 15)) AND CAST(Float64(0.07) AS Decimal128(30, 15))
*/

