use arrow_schema::DataType;
use optd::optimize::join_ordering::collect_join_group_roots;
use optd::{
    AggregateExpr, AggregateFunction, Aggregation, AnalysisContext, BinaryOp, BoxDrawingRenderer,
    BoxRendererConfig, ColorMode, ColumnData, ExprData, FreeColumns, Join, JoinOrdering, JoinType,
    Map, NaryOp, OperatorData, OperatorRewriteAdaptor, OptimizerContext, Output, PassManager,
    PredicatePushdown, Projection, QueryContext, QueryFormatConfig, QueryFormatter, ScalarFunction,
    ScalarValue, Scan, Selection, SubqueryToJoin, TableFunction, TableFunctionDef, TableRef,
    UnaryOp, build_hypergraph, tpch::tpch_query,
};

fn main() {
    let args = match CliArgs::parse(std::env::args().skip(1)) {
        Ok(args) => args,
        Err(error) => {
            eprintln!("{error}");
            eprintln!();
            print_usage();
            std::process::exit(2);
        }
    };

    let Some(query) = tpch_query(args.query) else {
        eprintln!(
            "unsupported TPC-H query q{}; currently available: q1-q22",
            args.query
        );
        std::process::exit(2);
    };

    if args.optimize && args.format == OutputFormat::OptimizerJson {
        #[cfg(feature = "serde")]
        {
            let initial = query.clone();
            let mut opt = OptimizerContext::new(query);
            let mut pm = PassManager::new();
            pm.add_pass(SubqueryToJoin);
            pm.add_pass(OperatorRewriteAdaptor::new(PredicatePushdown));
            pm.add_pass(JoinOrdering::new());
            match pm.run_with_trace(&mut opt) {
                Ok(trace) => {
                    println!(
                        "{}",
                        optd::optimizer_visualizer_trace_json(&initial, &trace)
                    );
                    return;
                }
                Err(error) => {
                    eprintln!("{error}");
                    std::process::exit(2);
                }
            }
        }

        #[cfg(not(feature = "serde"))]
        {
            eprintln!("--format optimizer-json requires the serde feature");
            std::process::exit(2);
        }
    }

    let query = if args.optimize {
        let initial = format_query(&query, args.format).unwrap_or_else(|e| e);
        let mut opt = OptimizerContext::new(query);
        let mut pm = PassManager::new();
        pm.add_pass(SubqueryToJoin);
        pm.add_pass(OperatorRewriteAdaptor::new(PredicatePushdown));
        pm.add_pass(JoinOrdering::new());
        let _res = pm.run(&mut opt);
        if let Some(root) = opt.query.root() {
            let resolved = opt.rewrites.resolve(root);
            opt.query.set_root(resolved);
        }
        let query = opt.into_query();
        println!("=== initial ===\n{initial}");
        // Print hypergraph for each join group root.
        if let Some(root) = query.root() {
            let mut analyses = AnalysisContext::new();
            for group_root in collect_join_group_roots(&query, root) {
                let hg = build_hypergraph(&query, &mut analyses, group_root);
                if hg.nodes.len() > 1 {
                    println!("=== hypergraph ===\n{}", hg.pretty(&query));
                }
            }
        }
        println!("=== optimized ===");
        query
    } else {
        query
    };

    match format_query(&query, args.format) {
        Ok(output) => println!("{output}"),
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(2);
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OutputFormat {
    Box,
    Flat,
    Json,
    Context,
    OptimizerJson,
}

impl OutputFormat {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "box" => Some(Self::Box),
            "flat" => Some(Self::Flat),
            "json" => Some(Self::Json),
            "context" => Some(Self::Context),
            "optimizer-json" => Some(Self::OptimizerJson),
            _ => None,
        }
    }
}

struct CliArgs {
    query: u8,
    format: OutputFormat,
    optimize: bool,
}

impl CliArgs {
    fn parse(args: impl IntoIterator<Item = String>) -> Result<Self, String> {
        let mut query = None;
        let mut format = OutputFormat::Box;
        let mut optimize = false;
        let mut args = args.into_iter();

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--help" | "-h" => {
                    print_usage();
                    std::process::exit(0);
                }
                "--optimize" => optimize = true,
                "--format" => {
                    let value = args
                        .next()
                        .ok_or_else(|| "--format requires a value".to_string())?;
                    format = OutputFormat::parse(&value).ok_or_else(|| {
                        format!("unknown format {value:?}; expected box, flat, json, context, or optimizer-json")
                    })?;
                }
                _ if arg.starts_with("--format=") => {
                    let value = arg.trim_start_matches("--format=");
                    format = OutputFormat::parse(value).ok_or_else(|| {
                        format!("unknown format {value:?}; expected box, flat, json, context, or optimizer-json")
                    })?;
                }
                _ if arg.starts_with('-') => return Err(format!("unknown option {arg:?}")),
                _ => {
                    if query.is_some() {
                        return Err("expected exactly one query number".to_string());
                    }
                    query = Some(
                        arg.parse::<u8>()
                            .map_err(|_| format!("invalid query number {arg:?}"))?,
                    );
                }
            }
        }

        let query = query.ok_or_else(|| "missing query number".to_string())?;
        Ok(Self {
            query,
            format,
            optimize,
        })
    }
}

fn print_usage() {
    eprintln!(
        "usage: optd [--format box|flat|json|context|optimizer-json] [--optimize] <tpch-query-number>"
    );
}

fn format_query(query: &QueryContext, format: OutputFormat) -> Result<String, String> {
    match format {
        OutputFormat::Box => Ok(pretty_with_free_column(query)),
        OutputFormat::Flat => format_flat(query),
        OutputFormat::Json => format_json(query),
        OutputFormat::Context => format_context(query),
        OutputFormat::OptimizerJson => format_optimizer_json(query),
    }
}

#[cfg(feature = "serde")]
fn format_flat(query: &QueryContext) -> Result<String, String> {
    Ok(query.pretty_flat())
}

#[cfg(not(feature = "serde"))]
fn format_flat(_query: &QueryContext) -> Result<String, String> {
    Err("--format flat requires the serde feature".to_string())
}

#[cfg(feature = "serde")]
fn format_json(query: &QueryContext) -> Result<String, String> {
    Ok(query.pretty_json())
}

#[cfg(not(feature = "serde"))]
fn format_json(_query: &QueryContext) -> Result<String, String> {
    Err("--format json requires the serde feature".to_string())
}

#[cfg(feature = "serde")]
fn format_context(query: &QueryContext) -> Result<String, String> {
    serde_json::to_string_pretty(query).map_err(|error| error.to_string())
}

#[cfg(not(feature = "serde"))]
fn format_context(_query: &QueryContext) -> Result<String, String> {
    Err("--format context requires the serde feature".to_string())
}

#[cfg(feature = "serde")]
fn format_optimizer_json(query: &QueryContext) -> Result<String, String> {
    Ok(query.optimizer_visualizer_json("0. Plan"))
}

#[cfg(not(feature = "serde"))]
fn format_optimizer_json(_query: &QueryContext) -> Result<String, String> {
    Err("--format optimizer-json requires the serde feature".to_string())
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
    println!("{}", pretty_with_free_column(&query));

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
    println!("{}", pretty_with_free_column(&query));
}

fn pretty_with_free_column(query: &QueryContext) -> String {
    let display = QueryFormatter::with_config(
        query,
        QueryFormatConfig::new().with_analysis::<FreeColumns>(),
    )
    .format();

    BoxDrawingRenderer::with_config(BoxRendererConfig::default().with_color_mode(ColorMode::Never))
        .render(&display)
}

fn sales_rollup_query() -> QueryContext {
    let mut ctx = QueryContext::new();

    let user_id = ColumnData::new("u_userkey", DataType::Int64).add(&mut ctx);
    let user_age = ColumnData::new("u_age", DataType::Int32).add(&mut ctx);
    let user_region = ColumnData::new("u_region", DataType::Utf8).add(&mut ctx);

    let order_id = ColumnData::new("o_orderkey", DataType::Int64).add(&mut ctx);
    let order_user_id = ColumnData::new("o_userkey", DataType::Int64).add(&mut ctx);

    let line_item_order_id = ColumnData::new("l_orderkey", DataType::Int64).add(&mut ctx);
    let line_item_product_id = ColumnData::new("l_partkey", DataType::Int64).add(&mut ctx);
    let quantity = ColumnData::new("l_quantity", DataType::Int32).add(&mut ctx);
    let unit_price = ColumnData::new("l_unit_price", DataType::Float64).add(&mut ctx);

    let product_id = ColumnData::new("p_partkey", DataType::Int64).add(&mut ctx);
    let product_name = ColumnData::new("p_name", DataType::Utf8).add(&mut ctx);

    let normalized_region = ColumnData::new("region_key", DataType::Utf8).add(&mut ctx);
    let gross_amount = ColumnData::new("gross_amount", DataType::Float64).add(&mut ctx);
    let total_gross_amount = ColumnData::new("total_gross", DataType::Float64).add(&mut ctx);
    let order_count = ColumnData::new("order_count", DataType::Int64).add(&mut ctx);
    let buyer_count = ColumnData::new("buyer_count", DataType::Int64).add(&mut ctx);

    let users = OperatorData::Scan(Scan {
        table: TableRef::bare("users"),
        columns: vec![user_id, user_age, user_region],
    })
    .add(&mut ctx);

    let orders_path =
        ExprData::Literal(ScalarValue::Utf8("orders.parquet".to_string())).add(&mut ctx);
    let orders = OperatorData::TableFunction(TableFunction {
        function: TableFunctionDef::ReadParquet,
        args: vec![orders_path],
        columns: vec![order_id, order_user_id],
    })
    .add(&mut ctx);

    let line_items = OperatorData::Scan(Scan {
        table: TableRef::bare("line_items"),
        columns: vec![
            line_item_order_id,
            line_item_product_id,
            quantity,
            unit_price,
        ],
    })
    .add(&mut ctx);
    let products = OperatorData::Scan(Scan {
        table: TableRef::bare("products"),
        columns: vec![product_id, product_name],
    })
    .add(&mut ctx);

    let users_id_ref = ExprData::ColumnRef(user_id).add(&mut ctx);
    let orders_user_id_ref = ExprData::ColumnRef(order_user_id).add(&mut ctx);
    let users_orders_on = ExprData::Binary {
        op: BinaryOp::Eq,
        left: users_id_ref,
        right: orders_user_id_ref,
    }
    .add(&mut ctx);
    let users_orders = OperatorData::Join(Join {
        join_type: JoinType::Inner,
        on: users_orders_on,
        outer: users,
        inner: orders,
    })
    .add(&mut ctx);

    let line_item_product_id_ref = ExprData::ColumnRef(line_item_product_id).add(&mut ctx);
    let product_id_ref = ExprData::ColumnRef(product_id).add(&mut ctx);
    let line_items_products_on = ExprData::Binary {
        op: BinaryOp::Eq,
        left: line_item_product_id_ref,
        right: product_id_ref,
    }
    .add(&mut ctx);
    let line_items_products = OperatorData::Join(Join {
        join_type: JoinType::Inner,
        on: line_items_products_on,
        outer: line_items,
        inner: products,
    })
    .add(&mut ctx);

    let orders_id_ref = ExprData::ColumnRef(order_id).add(&mut ctx);
    let line_item_order_id_ref = ExprData::ColumnRef(line_item_order_id).add(&mut ctx);
    let top_join_on = ExprData::Binary {
        op: BinaryOp::Eq,
        left: orders_id_ref,
        right: line_item_order_id_ref,
    }
    .add(&mut ctx);
    let joined = OperatorData::Join(Join {
        join_type: JoinType::Inner,
        on: top_join_on,
        outer: users_orders,
        inner: line_items_products,
    })
    .add(&mut ctx);

    let user_region_ref = ExprData::ColumnRef(user_region).add(&mut ctx);
    let normalize_region = ExprData::ScalarFunction {
        function: ScalarFunction::extension("normalize_region"),
        args: vec![user_region_ref],
    }
    .add(&mut ctx);
    let quantity_ref = ExprData::ColumnRef(quantity).add(&mut ctx);
    let unit_price_ref = ExprData::ColumnRef(unit_price).add(&mut ctx);
    let gross_amount_expr = ExprData::Binary {
        op: BinaryOp::Multiply,
        left: quantity_ref,
        right: unit_price_ref,
    }
    .add(&mut ctx);
    let map = OperatorData::Map(Map {
        computations: vec![
            (normalized_region, normalize_region),
            (gross_amount, gross_amount_expr),
        ],
        input: joined,
    })
    .add(&mut ctx);

    let user_age_ref = ExprData::ColumnRef(user_age).add(&mut ctx);
    let adult_age = ExprData::Literal(ScalarValue::Int32(18)).add(&mut ctx);
    let adult_predicate = ExprData::Binary {
        op: BinaryOp::GtEq,
        left: user_age_ref,
        right: adult_age,
    }
    .add(&mut ctx);
    let normalized_region_ref = ExprData::ColumnRef(normalized_region).add(&mut ctx);
    let region_present = ExprData::Unary {
        op: UnaryOp::IsNotNull,
        expr: normalized_region_ref,
    }
    .add(&mut ctx);
    let predicate = ExprData::Nary {
        op: NaryOp::And,
        exprs: vec![adult_predicate, region_present],
    }
    .add(&mut ctx);
    let selection = OperatorData::Selection(Selection {
        predicate,
        input: map,
    })
    .add(&mut ctx);

    let group_region = ExprData::ColumnRef(normalized_region).add(&mut ctx);
    let group_product_name = ExprData::ColumnRef(product_name).add(&mut ctx);
    let gross_amount_ref = ExprData::ColumnRef(gross_amount).add(&mut ctx);
    let buyer_ref = ExprData::ColumnRef(user_id).add(&mut ctx);
    let aggregation = OperatorData::Aggregation(Aggregation {
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
    })
    .add(&mut ctx);

    let projection = OperatorData::Projection(Projection {
        columns: vec![
            normalized_region,
            product_name,
            total_gross_amount,
            order_count,
            buyer_count,
        ],
        input: aggregation,
    })
    .add(&mut ctx);
    let output = OperatorData::Output(Output { input: projection }).add(&mut ctx);
    ctx.set_root(output);

    ctx
}

fn join_with_free_column_query() -> QueryContext {
    let mut ctx = QueryContext::new();

    let user_id = ColumnData::new("u_userkey", DataType::Int64).add(&mut ctx);
    let order_id = ColumnData::new("o_orderkey", DataType::Int64).add(&mut ctx);
    let order_user_id = ColumnData::new("o_userkey", DataType::Int64).add(&mut ctx);

    let users = OperatorData::Scan(Scan {
        table: TableRef::bare("users"),
        columns: vec![user_id],
    })
    .add(&mut ctx);
    let orders = OperatorData::Scan(Scan {
        table: TableRef::bare("orders"),
        columns: vec![order_id, order_user_id],
    })
    .add(&mut ctx);

    let order_user_id_ref = ExprData::ColumnRef(order_user_id).add(&mut ctx);
    let user_id_ref = ExprData::ColumnRef(user_id).add(&mut ctx);
    let correlated_predicate = ExprData::Binary {
        op: BinaryOp::Eq,
        left: order_user_id_ref,
        right: user_id_ref,
    }
    .add(&mut ctx);
    let scalar_subquery = OperatorData::Selection(Selection {
        predicate: correlated_predicate,
        input: orders,
    })
    .add(&mut ctx);

    let on = ExprData::Literal(ScalarValue::Boolean(true)).add(&mut ctx);
    let join = OperatorData::Join(Join {
        join_type: JoinType::Single,
        on,
        outer: users,
        inner: scalar_subquery,
    })
    .add(&mut ctx);
    let projection = OperatorData::Projection(Projection {
        columns: vec![user_id, order_id],
        input: join,
    })
    .add(&mut ctx);
    let output = OperatorData::Output(Output { input: projection }).add(&mut ctx);
    ctx.set_root(output);

    ctx
}
