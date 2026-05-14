use arrow_schema::DataType;
use simple_graph::{
    AggregateExpr, AggregateFunction, Aggregation, BinaryOp, BoxDrawingRenderer, BoxRendererConfig,
    ColorMode, ColumnData, ColumnNullability, ExprData, FreeColumns, Join, JoinType, Map, NaryOp,
    OperatorData, Output, Projection, QueryContext, QueryFormatConfig, QueryFormatter,
    ScalarFunction, ScalarValue, Scan, Selection, TableFunction, TableFunctionDef, TableRef,
    UnaryOp, tpch::tpch_q2,
};

fn main() {
    let query = tpch_q2();

    println!("-- tpch_q2");
    println!("{}", pretty_with_free_columns_and_nullability(&query));
}

#[allow(dead_code)]
fn example_queries() {
    // SQL-ish:
    //
    // SELECT
    //     normalize_region(u_region) AS region_key,
    //     p_name,
    //     SUM(l_quantity * l_unit_price) AS total_gross,
    //     COUNT(*) AS order_count,
    //     APPROX_COUNT_DISTINCT(u_userkey) AS buyer_count
    // FROM users
    // JOIN read_orders('orders.parquet') AS orders
    //      ON u_userkey = o_userkey
    // JOIN line_items
    //      ON o_orderkey = l_orderkey
    // JOIN products
    //      ON l_partkey = p_partkey
    // WHERE u_age >= 18
    //   AND normalize_region(u_region) IS NOT NULL
    // GROUP BY normalize_region(u_region), p_name;
    //
    // The plan is intentionally bushy:
    //
    //     (users JOIN read_orders(...))
    //     JOIN
    //     (line_items JOIN products)
    let query = sales_rollup_query();

    #[cfg(feature = "serde")]
    {
        println!("-- sales_rollup_query_flat");
        println!("{}", query.pretty_flat());

        println!("-- sales_rollup_query_json");
        println!("{}", query.pretty_json());

        println!("-- sales_rollup_query_context");
        println!("{}", serde_json::to_string_pretty(&query).unwrap());
    }

    println!("-- sales_rollup_query");
    println!("{}", pretty_with_free_columns_and_nullability(&query));

    // SQL-ish correlated scalar subquery shape:
    //
    // SELECT
    //     u_userkey,
    //     (
    //         SELECT o_orderkey
    //         FROM orders
    //         WHERE o_userkey = users.u_userkey
    //     ) AS scalar_orderkey
    // FROM users;
    //
    // The inner side of the SingleJoin is correlated: its selection references
    // u_userkey from the outer users input, so that inner side has a free column.
    let query = join_with_free_column_query();
    println!("-- join_with_free_column_query");
    println!("{}", pretty_with_free_columns_and_nullability(&query));
}

fn pretty_with_free_columns_and_nullability(query: &QueryContext) -> String {
    let display = QueryFormatter::with_config(
        query,
        QueryFormatConfig::new()
            .with_analysis::<FreeColumns>()
            .with_analysis::<ColumnNullability>(),
    )
    .format();

    BoxDrawingRenderer::with_config(BoxRendererConfig::default().with_color_mode(ColorMode::Always))
        .render(&display)
}

fn sales_rollup_query() -> QueryContext {
    let mut ctx = QueryContext::new();

    let user_id = ctx.add_column(ColumnData::new("u_userkey", DataType::Int64));
    let user_age = ctx.add_column(ColumnData::new("u_age", DataType::Int32));
    let user_region = ctx.add_column(ColumnData::new("u_region", DataType::Utf8));

    let order_id = ctx.add_column(ColumnData::new("o_orderkey", DataType::Int64));
    let order_user_id = ctx.add_column(ColumnData::new("o_userkey", DataType::Int64));

    let line_item_order_id = ctx.add_column(ColumnData::new("l_orderkey", DataType::Int64));
    let line_item_product_id = ctx.add_column(ColumnData::new("l_partkey", DataType::Int64));
    let quantity = ctx.add_column(ColumnData::new("l_quantity", DataType::Int32));
    let unit_price = ctx.add_column(ColumnData::new("l_unit_price", DataType::Float64));

    let product_id = ctx.add_column(ColumnData::new("p_partkey", DataType::Int64));
    let product_name = ctx.add_column(ColumnData::new("p_name", DataType::Utf8));

    let normalized_region = ctx.add_column(ColumnData::new("region_key", DataType::Utf8));
    let gross_amount = ctx.add_column(ColumnData::new("gross_amount", DataType::Float64));
    let total_gross_amount = ctx.add_column(ColumnData::new("total_gross", DataType::Float64));
    let order_count = ctx.add_column(ColumnData::new("order_count", DataType::Int64));
    let buyer_count = ctx.add_column(ColumnData::new("buyer_count", DataType::Int64));

    let users = ctx.add_operator(OperatorData::Scan(Scan {
        table: TableRef::bare("users"),
        columns: vec![user_id, user_age, user_region],
    }));

    let orders_path = ctx.add_expr(ExprData::Literal(ScalarValue::Utf8(
        "orders.parquet".to_string(),
    )));
    let orders = ctx.add_operator(OperatorData::TableFunction(TableFunction {
        function: TableFunctionDef::ReadParquet,
        args: vec![orders_path],
        columns: vec![order_id, order_user_id],
    }));

    let line_items = ctx.add_operator(OperatorData::Scan(Scan {
        table: TableRef::bare("line_items"),
        columns: vec![
            line_item_order_id,
            line_item_product_id,
            quantity,
            unit_price,
        ],
    }));
    let products = ctx.add_operator(OperatorData::Scan(Scan {
        table: TableRef::bare("products"),
        columns: vec![product_id, product_name],
    }));

    let users_id_ref = ctx.add_expr(ExprData::ColumnRef(user_id));
    let orders_user_id_ref = ctx.add_expr(ExprData::ColumnRef(order_user_id));
    let users_orders_on = ctx.add_expr(ExprData::Binary {
        op: BinaryOp::Eq,
        left: users_id_ref,
        right: orders_user_id_ref,
    });
    let users_orders = ctx.add_operator(OperatorData::Join(Join {
        join_type: JoinType::Inner,
        on: users_orders_on,
        outer: users,
        inner: orders,
    }));

    let line_item_product_id_ref = ctx.add_expr(ExprData::ColumnRef(line_item_product_id));
    let product_id_ref = ctx.add_expr(ExprData::ColumnRef(product_id));
    let line_items_products_on = ctx.add_expr(ExprData::Binary {
        op: BinaryOp::Eq,
        left: line_item_product_id_ref,
        right: product_id_ref,
    });
    let line_items_products = ctx.add_operator(OperatorData::Join(Join {
        join_type: JoinType::Inner,
        on: line_items_products_on,
        outer: line_items,
        inner: products,
    }));

    let orders_id_ref = ctx.add_expr(ExprData::ColumnRef(order_id));
    let line_item_order_id_ref = ctx.add_expr(ExprData::ColumnRef(line_item_order_id));
    let top_join_on = ctx.add_expr(ExprData::Binary {
        op: BinaryOp::Eq,
        left: orders_id_ref,
        right: line_item_order_id_ref,
    });
    let joined = ctx.add_operator(OperatorData::Join(Join {
        join_type: JoinType::Inner,
        on: top_join_on,
        outer: users_orders,
        inner: line_items_products,
    }));

    let user_region_ref = ctx.add_expr(ExprData::ColumnRef(user_region));
    let normalize_region = ctx.add_expr(ExprData::ScalarFunction {
        function: ScalarFunction::extension("normalize_region"),
        args: vec![user_region_ref],
    });
    let quantity_ref = ctx.add_expr(ExprData::ColumnRef(quantity));
    let unit_price_ref = ctx.add_expr(ExprData::ColumnRef(unit_price));
    let gross_amount_expr = ctx.add_expr(ExprData::Binary {
        op: BinaryOp::Multiply,
        left: quantity_ref,
        right: unit_price_ref,
    });
    let map = ctx.add_operator(OperatorData::Map(Map {
        computations: vec![
            (normalized_region, normalize_region),
            (gross_amount, gross_amount_expr),
        ],
        input: joined,
    }));

    let user_age_ref = ctx.add_expr(ExprData::ColumnRef(user_age));
    let adult_age = ctx.add_expr(ExprData::Literal(ScalarValue::Int32(18)));
    let adult_predicate = ctx.add_expr(ExprData::Binary {
        op: BinaryOp::GtEq,
        left: user_age_ref,
        right: adult_age,
    });
    let normalized_region_ref = ctx.add_expr(ExprData::ColumnRef(normalized_region));
    let region_present = ctx.add_expr(ExprData::Unary {
        op: UnaryOp::IsNotNull,
        expr: normalized_region_ref,
    });
    let predicate = ctx.add_expr(ExprData::Nary {
        op: NaryOp::And,
        exprs: vec![adult_predicate, region_present],
    });
    let selection = ctx.add_operator(OperatorData::Selection(Selection {
        predicate,
        input: map,
    }));

    let group_region = ctx.add_expr(ExprData::ColumnRef(normalized_region));
    let group_product_name = ctx.add_expr(ExprData::ColumnRef(product_name));
    let gross_amount_ref = ctx.add_expr(ExprData::ColumnRef(gross_amount));
    let buyer_ref = ctx.add_expr(ExprData::ColumnRef(user_id));
    let aggregation = ctx.add_operator(OperatorData::Aggregation(Aggregation {
        keys: vec![group_region, group_product_name],
        aggregates: vec![
            (
                total_gross_amount,
                AggregateExpr::Func {
                    func: AggregateFunction::Sum,
                    arg: gross_amount_ref,
                    distinct: false,
                },
            ),
            (order_count, AggregateExpr::CountStar),
            (
                buyer_count,
                AggregateExpr::Func {
                    func: AggregateFunction::extension("approx_count_distinct"),
                    arg: buyer_ref,
                    distinct: true,
                },
            ),
        ],
        input: selection,
    }));

    let projection = ctx.add_operator(OperatorData::Projection(Projection {
        columns: vec![
            normalized_region,
            product_name,
            total_gross_amount,
            order_count,
            buyer_count,
        ],
        input: aggregation,
    }));
    let output = ctx.add_operator(OperatorData::Output(Output { input: projection }));
    ctx.set_root(output);

    ctx
}

fn join_with_free_column_query() -> QueryContext {
    let mut ctx = QueryContext::new();

    let user_id = ctx.add_column(ColumnData::new("u_userkey", DataType::Int64));
    let order_id = ctx.add_column(ColumnData::new("o_orderkey", DataType::Int64));
    let order_user_id = ctx.add_column(ColumnData::new("o_userkey", DataType::Int64));

    let users = ctx.add_operator(OperatorData::Scan(Scan {
        table: TableRef::bare("users"),
        columns: vec![user_id],
    }));
    let orders = ctx.add_operator(OperatorData::Scan(Scan {
        table: TableRef::bare("orders"),
        columns: vec![order_id, order_user_id],
    }));

    let order_user_id_ref = ctx.add_expr(ExprData::ColumnRef(order_user_id));
    let user_id_ref = ctx.add_expr(ExprData::ColumnRef(user_id));
    let correlated_predicate = ctx.add_expr(ExprData::Binary {
        op: BinaryOp::Eq,
        left: order_user_id_ref,
        right: user_id_ref,
    });
    let scalar_subquery = ctx.add_operator(OperatorData::Selection(Selection {
        predicate: correlated_predicate,
        input: orders,
    }));

    let on = ctx.add_expr(ExprData::Literal(ScalarValue::Boolean(true)));
    let join = ctx.add_operator(OperatorData::Join(Join {
        join_type: JoinType::Single,
        on,
        outer: users,
        inner: scalar_subquery,
    }));
    let projection = ctx.add_operator(OperatorData::Projection(Projection {
        columns: vec![user_id, order_id],
        input: join,
    }));
    let output = ctx.add_operator(OperatorData::Output(Output { input: projection }));
    ctx.set_root(output);

    ctx
}
