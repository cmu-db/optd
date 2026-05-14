use std::collections::HashMap;

use arrow_schema::DataType;

use crate::{
    AggregateExpr, AggregateFunction, Aggregation, BinaryOp, Column, ColumnData, CrossProduct,
    Expr, ExprData, Join, JoinType, Limit, Map, NaryOp, NullOrdering, Operator, OperatorData,
    Output, Projection, QueryContext, ScalarFunction, ScalarValue, Scan, Selection, Sort,
    SortDirection, SortKey, TableRef, UnaryOp,
};

/// Builds the direct simple-graph IR shape for TPC-H Q2.
pub fn tpch_q2() -> QueryContext {
    let mut ctx = QueryContext::new();
    let part = scan_rel(
        &mut ctx,
        "part",
        &["p_partkey", "p_mfgr", "p_size", "p_type"],
    );
    let supplier = scan_rel(
        &mut ctx,
        "supplier",
        &[
            "s_suppkey",
            "s_name",
            "s_address",
            "s_phone",
            "s_comment",
            "s_acctbal",
            "s_nationkey",
        ],
    );
    let partsupp = scan_rel(
        &mut ctx,
        "partsupp",
        &["ps_partkey", "ps_suppkey", "ps_supplycost"],
    );
    let nation = scan_rel(
        &mut ctx,
        "nation",
        &["n_nationkey", "n_regionkey", "n_name"],
    );
    let region = scan_rel(&mut ctx, "region", &["r_regionkey", "r_name"]);
    let joined = cross_join_on_cols(&mut ctx, part, partsupp, "p_partkey", "ps_partkey");
    let joined = cross_join_on_cols(&mut ctx, joined, supplier, "ps_suppkey", "s_suppkey");
    let joined = cross_join_on_cols(&mut ctx, joined, nation, "s_nationkey", "n_nationkey");
    let joined = cross_join_on_cols(&mut ctx, joined, region, "n_regionkey", "r_regionkey");
    let region_filtered = filter_eq_str(&mut ctx, joined, "r_name", "ASIA");
    let size = col(&mut ctx, region_filtered.col("p_size"));
    let wanted_size = int_lit(&mut ctx, 48);
    let size_predicate = eq(&mut ctx, size, wanted_size);
    let part_type = col(&mut ctx, region_filtered.col("p_type"));
    let type_predicate = like(&mut ctx, part_type, "%TIN");
    let predicate = and(&mut ctx, vec![size_predicate, type_predicate]);
    let joined = select_rel(&mut ctx, region_filtered.clone(), predicate);

    let sub_partsupp = scan_rel(
        &mut ctx,
        "partsupp",
        &["ps_partkey", "ps_suppkey", "ps_supplycost"],
    );
    let sub_supplier = scan_rel(&mut ctx, "supplier", &["s_suppkey", "s_nationkey"]);
    let sub_nation = scan_rel(&mut ctx, "nation", &["n_nationkey", "n_regionkey"]);
    let sub_region = scan_rel(&mut ctx, "region", &["r_regionkey", "r_name"]);
    let subquery = cross_join_on_cols(
        &mut ctx,
        sub_partsupp,
        sub_supplier,
        "ps_suppkey",
        "s_suppkey",
    );
    let subquery = cross_join_on_cols(&mut ctx, subquery, sub_nation, "s_nationkey", "n_nationkey");
    let subquery = cross_join_on_cols(&mut ctx, subquery, sub_region, "n_regionkey", "r_regionkey");
    let subquery = filter_eq_str(&mut ctx, subquery, "r_name", "ASIA");
    let outer_partkey = col(&mut ctx, joined.col("p_partkey"));
    let inner_partkey = col(&mut ctx, subquery.col("ps_partkey"));
    let correlated = eq(&mut ctx, outer_partkey, inner_partkey);
    let subquery = select_rel(&mut ctx, subquery, correlated);
    let supplycost = col(&mut ctx, subquery.col("ps_supplycost"));
    let min_cost = aggregate_rel(
        &mut ctx,
        subquery,
        &[],
        vec![(
            "min_supplycost",
            DataType::Decimal128(15, 2),
            min_expr(supplycost),
        )],
    );
    let supplycost = col(&mut ctx, joined.col("ps_supplycost"));
    let min_cost = project_rel(&mut ctx, min_cost, &["min_supplycost"]);
    let min_supplycost = scalar_subquery(&mut ctx, min_cost.input);
    let predicate = eq(&mut ctx, supplycost, min_supplycost);
    let joined = select_rel(&mut ctx, joined, predicate);
    let sorted = sort_rel(
        &mut ctx,
        joined,
        vec![
            ("s_acctbal", SortDirection::Desc),
            ("n_name", SortDirection::Asc),
            ("s_name", SortDirection::Asc),
            ("p_partkey", SortDirection::Asc),
        ],
    );
    let limited = limit_rel(&mut ctx, sorted, 100);
    finish(
        &mut ctx,
        limited,
        &[
            "s_acctbal",
            "s_name",
            "n_name",
            "p_partkey",
            "p_mfgr",
            "s_address",
            "s_phone",
            "s_comment",
        ],
    );
    ctx
}

#[derive(Clone)]
pub struct Rel {
    pub input: Operator,
    cols: HashMap<String, Column>,
}

impl Rel {
    pub fn col(&self, name: &str) -> Column {
        *self
            .cols
            .get(name)
            .unwrap_or_else(|| panic!("missing column in TPC-H builder: {name}"))
    }

    fn merge(mut self, other: Rel) -> Rel {
        self.cols.extend(other.cols);
        self
    }
}

pub fn scan_rel(ctx: &mut QueryContext, table: &'static str, columns: &[&'static str]) -> Rel {
    scan_rel_as(ctx, table, None, columns)
}

pub fn scan_rel_as(
    ctx: &mut QueryContext,
    table: &'static str,
    alias: Option<&'static str>,
    columns: &[&'static str],
) -> Rel {
    let mut cols = HashMap::new();
    let mut scan_columns = Vec::new();

    for name in columns {
        let display_name = alias
            .map(|alias| format!("{alias}.{name}"))
            .unwrap_or_else(|| (*name).to_string());
        let column = ctx.add_column(ColumnData::new(display_name, tpch_column_type(name)));
        cols.insert((*name).to_string(), column);
        if let Some(alias) = alias {
            cols.insert(format!("{alias}.{name}"), column);
        }
        scan_columns.push(column);
    }

    let input = ctx.add_operator(OperatorData::Scan(Scan {
        table: TableRef::bare(table),
        columns: scan_columns,
    }));
    Rel { input, cols }
}

pub fn cross_rel(ctx: &mut QueryContext, left: Rel, right: Rel) -> Rel {
    let input = ctx.add_operator(OperatorData::CrossProduct(CrossProduct {
        outer: left.input,
        inner: right.input,
    }));
    Rel {
        input,
        cols: left.merge(right).cols,
    }
}

pub fn join_rel(
    ctx: &mut QueryContext,
    join_type: JoinType,
    left: Rel,
    right: Rel,
    on: Expr,
) -> Rel {
    let input = ctx.add_operator(OperatorData::Join(Join {
        join_type: join_type.clone(),
        on,
        outer: left.input,
        inner: right.input,
    }));
    let cols = match join_type {
        JoinType::LeftSemi | JoinType::LeftAnti => left.cols,
        _ => left.merge(right).cols,
    };
    Rel { input, cols }
}

pub fn cross_join_on_cols(
    ctx: &mut QueryContext,
    left: Rel,
    right: Rel,
    left_col: &str,
    right_col: &str,
) -> Rel {
    let joined = cross_rel(ctx, left, right);
    let left = col(ctx, joined.col(left_col));
    let right = col(ctx, joined.col(right_col));
    let predicate = eq(ctx, left, right);
    select_rel(ctx, joined, predicate)
}

pub fn filter_eq_str(ctx: &mut QueryContext, rel: Rel, column: &str, value: &'static str) -> Rel {
    let left = col(ctx, rel.col(column));
    let right = str_lit(ctx, value);
    let predicate = eq(ctx, left, right);
    select_rel(ctx, rel, predicate)
}

pub fn filter_date_range(
    ctx: &mut QueryContext,
    rel: Rel,
    column: &str,
    start_days: i32,
    end_days: i32,
) -> Rel {
    let value = col(ctx, rel.col(column));
    let start = date_lit(ctx, start_days);
    let after_start = bin(ctx, BinaryOp::GtEq, value, start);
    let value = col(ctx, rel.col(column));
    let end = date_lit(ctx, end_days);
    let before_end = bin(ctx, BinaryOp::Lt, value, end);
    let predicate = and(ctx, vec![after_start, before_end]);
    select_rel(ctx, rel, predicate)
}

pub fn select_rel(ctx: &mut QueryContext, rel: Rel, predicate: Expr) -> Rel {
    let input = ctx.add_operator(OperatorData::Selection(Selection {
        predicate,
        input: rel.input,
    }));
    Rel {
        input,
        cols: rel.cols,
    }
}

pub fn map_rel(
    ctx: &mut QueryContext,
    rel: Rel,
    computations: Vec<(&'static str, DataType, Expr)>,
) -> Rel {
    let mut cols = rel.cols;
    let mut map_computations = Vec::new();

    for (name, ty, expr) in computations {
        let column = ctx.add_column(ColumnData::new(name, ty));
        cols.insert(name.to_string(), column);
        map_computations.push((column, expr));
    }

    let input = ctx.add_operator(OperatorData::Map(Map {
        computations: map_computations,
        input: rel.input,
    }));
    Rel { input, cols }
}

pub fn aggregate_rel(
    ctx: &mut QueryContext,
    rel: Rel,
    key_columns: &[&str],
    aggregates: Vec<(&'static str, DataType, AggregateExpr)>,
) -> Rel {
    let mut cols = HashMap::new();
    let keys = key_columns
        .iter()
        .map(|name| {
            let column = rel.col(name);
            cols.insert((*name).to_string(), column);
            col(ctx, column)
        })
        .collect();
    let aggregates = aggregates
        .into_iter()
        .map(|(name, ty, expr)| {
            let column = ctx.add_column(ColumnData::new(name, ty));
            cols.insert(name.to_string(), column);
            (column, expr)
        })
        .collect();

    let input = ctx.add_operator(OperatorData::Aggregation(Aggregation {
        keys,
        aggregates,
        input: rel.input,
    }));
    Rel { input, cols }
}

pub fn sort_rel(ctx: &mut QueryContext, rel: Rel, keys: Vec<(&str, SortDirection)>) -> Rel {
    let sort_keys = keys
        .into_iter()
        .map(|(name, direction)| SortKey {
            expr: col(ctx, rel.col(name)),
            direction,
            nulls: NullOrdering::Last,
        })
        .collect();
    let input = ctx.add_operator(OperatorData::Sort(Sort {
        keys: sort_keys,
        input: rel.input,
    }));
    Rel {
        input,
        cols: rel.cols,
    }
}

pub fn limit_rel(ctx: &mut QueryContext, rel: Rel, fetch: usize) -> Rel {
    let input = ctx.add_operator(OperatorData::Limit(Limit {
        fetch: Some(fetch),
        offset: 0,
        input: rel.input,
    }));
    Rel {
        input,
        cols: rel.cols,
    }
}

pub fn project_rel(ctx: &mut QueryContext, rel: Rel, output_columns: &[&str]) -> Rel {
    let columns = output_columns
        .iter()
        .map(|name| rel.col(name))
        .collect::<Vec<_>>();
    let input = ctx.add_operator(OperatorData::Projection(Projection {
        columns: columns.clone(),
        input: rel.input,
    }));
    let cols = output_columns
        .iter()
        .zip(columns)
        .map(|(name, column)| ((*name).to_string(), column))
        .collect();
    Rel { input, cols }
}

pub fn finish(ctx: &mut QueryContext, rel: Rel, output_columns: &[&str]) {
    let columns = output_columns.iter().map(|name| rel.col(name)).collect();
    let projection = ctx.add_operator(OperatorData::Projection(Projection {
        columns,
        input: rel.input,
    }));
    let output = ctx.add_operator(OperatorData::Output(Output { input: projection }));
    ctx.set_root(output);
}

pub fn col(ctx: &mut QueryContext, column: Column) -> Expr {
    ctx.add_expr(ExprData::ColumnRef(column))
}

pub fn lit(ctx: &mut QueryContext, value: ScalarValue) -> Expr {
    ctx.add_expr(ExprData::Literal(value))
}

pub fn int_lit(ctx: &mut QueryContext, value: i64) -> Expr {
    lit(ctx, ScalarValue::Int64(value))
}

pub fn dec_lit(ctx: &mut QueryContext, value: i128) -> Expr {
    lit(
        ctx,
        ScalarValue::Decimal128 {
            value,
            precision: 15,
            scale: 2,
        },
    )
}

pub fn str_lit(ctx: &mut QueryContext, value: &'static str) -> Expr {
    lit(ctx, ScalarValue::Utf8(value.to_string()))
}

pub fn date_lit(ctx: &mut QueryContext, days_since_epoch: i32) -> Expr {
    lit(ctx, ScalarValue::Date32(days_since_epoch))
}

pub fn bin(ctx: &mut QueryContext, op: BinaryOp, left: Expr, right: Expr) -> Expr {
    ctx.add_expr(ExprData::Binary { op, left, right })
}

pub fn eq(ctx: &mut QueryContext, left: Expr, right: Expr) -> Expr {
    bin(ctx, BinaryOp::Eq, left, right)
}

pub fn and(ctx: &mut QueryContext, exprs: Vec<Expr>) -> Expr {
    ctx.add_expr(ExprData::Nary {
        op: NaryOp::And,
        exprs,
    })
}

pub fn or(ctx: &mut QueryContext, exprs: Vec<Expr>) -> Expr {
    ctx.add_expr(ExprData::Nary {
        op: NaryOp::Or,
        exprs,
    })
}

pub fn not(ctx: &mut QueryContext, expr: Expr) -> Expr {
    ctx.add_expr(ExprData::Unary {
        op: UnaryOp::Not,
        expr,
    })
}

pub fn scalar_fn(ctx: &mut QueryContext, name: &'static str, args: Vec<Expr>) -> Expr {
    ctx.add_expr(ExprData::ScalarFunction {
        function: ScalarFunction::extension(name),
        args,
    })
}

pub fn exists(ctx: &mut QueryContext, subquery: Operator) -> Expr {
    ctx.add_expr(ExprData::Exists {
        subquery,
        negated: false,
    })
}

pub fn not_exists(ctx: &mut QueryContext, subquery: Operator) -> Expr {
    ctx.add_expr(ExprData::Exists {
        subquery,
        negated: true,
    })
}

pub fn in_subquery(ctx: &mut QueryContext, expr: Expr, subquery: Operator) -> Expr {
    ctx.add_expr(ExprData::InSubquery {
        expr,
        subquery,
        negated: false,
    })
}

pub fn not_in_subquery(ctx: &mut QueryContext, expr: Expr, subquery: Operator) -> Expr {
    ctx.add_expr(ExprData::InSubquery {
        expr,
        subquery,
        negated: true,
    })
}

pub fn scalar_subquery(ctx: &mut QueryContext, subquery: Operator) -> Expr {
    ctx.add_expr(ExprData::ScalarSubquery { subquery })
}

pub fn like(ctx: &mut QueryContext, value: Expr, pattern: &'static str) -> Expr {
    let pattern = str_lit(ctx, pattern);
    scalar_fn(ctx, "like", vec![value, pattern])
}

pub fn in_list(ctx: &mut QueryContext, value: Expr, values: &[&'static str]) -> Expr {
    let mut args = vec![value];
    for value in values {
        args.push(str_lit(ctx, value));
    }
    scalar_fn(ctx, "in", args)
}

pub fn between(ctx: &mut QueryContext, value: Expr, low: Expr, high: Expr) -> Expr {
    let above = bin(ctx, BinaryOp::GtEq, value, low);
    let below = bin(ctx, BinaryOp::LtEq, value, high);
    and(ctx, vec![above, below])
}

pub fn disc_price(ctx: &mut QueryContext, extendedprice: Column, discount: Column) -> Expr {
    let one = dec_lit(ctx, 100);
    let discount = col(ctx, discount);
    let keep = bin(ctx, BinaryOp::Subtract, one, discount);
    let extendedprice = col(ctx, extendedprice);
    bin(ctx, BinaryOp::Multiply, extendedprice, keep)
}

pub fn case_when(
    ctx: &mut QueryContext,
    when_then: Vec<(Expr, Expr)>,
    else_expr: Option<Expr>,
) -> Expr {
    ctx.add_expr(ExprData::CaseWhen {
        when_then,
        else_expr,
    })
}

pub fn sum_expr(arg: Expr) -> AggregateExpr {
    AggregateExpr::Func {
        func: AggregateFunction::Sum,
        arg,
        distinct: false,
    }
}

pub fn count_expr(arg: Expr) -> AggregateExpr {
    AggregateExpr::Func {
        func: AggregateFunction::Count,
        arg,
        distinct: false,
    }
}

pub fn count_distinct_expr(arg: Expr) -> AggregateExpr {
    AggregateExpr::Func {
        func: AggregateFunction::Count,
        arg,
        distinct: true,
    }
}

pub fn avg_expr(arg: Expr) -> AggregateExpr {
    AggregateExpr::Func {
        func: AggregateFunction::Avg,
        arg,
        distinct: false,
    }
}

pub fn min_expr(arg: Expr) -> AggregateExpr {
    AggregateExpr::Func {
        func: AggregateFunction::Min,
        arg,
        distinct: false,
    }
}

pub fn max_expr(arg: Expr) -> AggregateExpr {
    AggregateExpr::Func {
        func: AggregateFunction::Max,
        arg,
        distinct: false,
    }
}

pub fn tpch_column_type(name: &str) -> DataType {
    match name {
        "l_linenumber" | "p_size" | "ps_availqty" | "o_shippriority" => DataType::Int32,
        "c_custkey" | "c_nationkey" | "l_orderkey" | "l_partkey" | "l_suppkey" | "n_nationkey"
        | "n_regionkey" | "o_orderkey" | "o_custkey" | "p_partkey" | "ps_partkey"
        | "ps_suppkey" | "r_regionkey" | "s_suppkey" | "s_nationkey" => DataType::Int64,
        "c_acctbal" | "l_quantity" | "l_extendedprice" | "l_discount" | "l_tax"
        | "o_totalprice" | "p_retailprice" | "ps_supplycost" | "s_acctbal" => {
            DataType::Decimal128(15, 2)
        }
        "l_shipdate" | "l_commitdate" | "l_receiptdate" | "o_orderdate" => DataType::Date32,
        _ => DataType::Utf8,
    }
}
