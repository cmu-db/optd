use arrow_schema::DataType;
use simple_graph::{
    AggregateExpr, AggregateFunction, BinaryOp, ColumnData, ExprData, JoinType, NaryOp,
    OperatorData, QueryContext, ScalarFunction, ScalarValue, TableFunction, UnaryOp,
};

fn main() {
    // SQL-ish:
    //
    // SELECT
    //     normalize_region(users.region) AS normalized_region,
    //     products.name,
    //     SUM(line_items.quantity * line_items.unit_price) AS gross_amount,
    //     COUNT(*) AS order_count,
    //     APPROX_COUNT_DISTINCT(users.id) AS buyer_count
    // FROM users
    // JOIN read_orders('orders.parquet') AS orders
    //      ON users.id = orders.user_id
    // JOIN line_items
    //      ON orders.id = line_items.order_id
    // JOIN products
    //      ON line_items.product_id = products.id
    // WHERE users.age >= 18
    //   AND normalize_region(users.region) IS NOT NULL
    // GROUP BY normalize_region(users.region), products.name;
    //
    // The plan is intentionally bushy:
    //
    //     (users JOIN read_orders(...))
    //     JOIN
    //     (line_items JOIN products)
    let query = sales_rollup_query();

    println!("-- sales_rollup_query");
    println!("{query}");
}

fn sales_rollup_query() -> QueryContext {
    let mut ctx = QueryContext::new();

    let user_id = ctx.add_column(ColumnData::new("users.id", DataType::Int64, false));
    let user_age = ctx.add_column(ColumnData::new("users.age", DataType::Int32, true));
    let user_region = ctx.add_column(ColumnData::new("users.region", DataType::Utf8, true));

    let order_id = ctx.add_column(ColumnData::new("orders.id", DataType::Int64, false));
    let order_user_id = ctx.add_column(ColumnData::new("orders.user_id", DataType::Int64, false));

    let line_item_order_id = ctx.add_column(ColumnData::new(
        "line_items.order_id",
        DataType::Int64,
        false,
    ));
    let line_item_product_id = ctx.add_column(ColumnData::new(
        "line_items.product_id",
        DataType::Int64,
        false,
    ));
    let quantity = ctx.add_column(ColumnData::new(
        "line_items.quantity",
        DataType::Int32,
        false,
    ));
    let unit_price = ctx.add_column(ColumnData::new(
        "line_items.unit_price",
        DataType::Float64,
        false,
    ));

    let product_id = ctx.add_column(ColumnData::new("products.id", DataType::Int64, false));
    let product_name = ctx.add_column(ColumnData::new("products.name", DataType::Utf8, false));

    let normalized_region =
        ctx.add_column(ColumnData::new("normalized_region", DataType::Utf8, true));
    let gross_amount = ctx.add_column(ColumnData::new("gross_amount", DataType::Float64, false));
    let total_gross_amount = ctx.add_column(ColumnData::new(
        "total_gross_amount",
        DataType::Float64,
        true,
    ));
    let order_count = ctx.add_column(ColumnData::new("order_count", DataType::Int64, false));
    let buyer_count = ctx.add_column(ColumnData::new("buyer_count", DataType::Int64, true));

    let users = ctx.add_operator(OperatorData::Scan {
        table: "users".to_string(),
        columns: vec![user_id, user_age, user_region],
    });

    let orders_path = ctx.add_expr(ExprData::Literal(ScalarValue::Utf8(
        "orders.parquet".to_string(),
    )));
    let orders = ctx.add_operator(OperatorData::TableFunction {
        function: TableFunction::ReadParquet,
        args: vec![orders_path],
        columns: vec![order_id, order_user_id],
    });

    let line_items = ctx.add_operator(OperatorData::Scan {
        table: "line_items".to_string(),
        columns: vec![
            line_item_order_id,
            line_item_product_id,
            quantity,
            unit_price,
        ],
    });
    let products = ctx.add_operator(OperatorData::Scan {
        table: "products".to_string(),
        columns: vec![product_id, product_name],
    });

    let users_id_ref = ctx.add_expr(ExprData::ColumnRef(user_id));
    let orders_user_id_ref = ctx.add_expr(ExprData::ColumnRef(order_user_id));
    let users_orders_on = ctx.add_expr(ExprData::Binary {
        op: BinaryOp::Eq,
        left: users_id_ref,
        right: orders_user_id_ref,
    });
    let users_orders = ctx.add_operator(OperatorData::Join {
        join_type: JoinType::Inner,
        on: users_orders_on,
        outer: users,
        inner: orders,
    });

    let line_item_product_id_ref = ctx.add_expr(ExprData::ColumnRef(line_item_product_id));
    let product_id_ref = ctx.add_expr(ExprData::ColumnRef(product_id));
    let line_items_products_on = ctx.add_expr(ExprData::Binary {
        op: BinaryOp::Eq,
        left: line_item_product_id_ref,
        right: product_id_ref,
    });
    let line_items_products = ctx.add_operator(OperatorData::Join {
        join_type: JoinType::Inner,
        on: line_items_products_on,
        outer: line_items,
        inner: products,
    });

    let orders_id_ref = ctx.add_expr(ExprData::ColumnRef(order_id));
    let line_item_order_id_ref = ctx.add_expr(ExprData::ColumnRef(line_item_order_id));
    let top_join_on = ctx.add_expr(ExprData::Binary {
        op: BinaryOp::Eq,
        left: orders_id_ref,
        right: line_item_order_id_ref,
    });
    let joined = ctx.add_operator(OperatorData::Join {
        join_type: JoinType::Inner,
        on: top_join_on,
        outer: users_orders,
        inner: line_items_products,
    });

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
    let map = ctx.add_operator(OperatorData::Map {
        computations: vec![
            (normalized_region, normalize_region),
            (gross_amount, gross_amount_expr),
        ],
        input: joined,
    });

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
    let selection = ctx.add_operator(OperatorData::Selection {
        predicate,
        input: map,
    });

    let group_region = ctx.add_expr(ExprData::ColumnRef(normalized_region));
    let group_product_name = ctx.add_expr(ExprData::ColumnRef(product_name));
    let gross_amount_ref = ctx.add_expr(ExprData::ColumnRef(gross_amount));
    let buyer_ref = ctx.add_expr(ExprData::ColumnRef(user_id));
    let aggregation = ctx.add_operator(OperatorData::Aggregation {
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
    });

    let projection = ctx.add_operator(OperatorData::Projection {
        columns: vec![
            normalized_region,
            product_name,
            total_gross_amount,
            order_count,
            buyer_count,
        ],
        input: aggregation,
    });
    ctx.set_root(projection);

    ctx
}
