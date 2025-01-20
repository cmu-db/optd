#![doc = include_str!("../../README.md")]

use datafusion::common::Result;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::*;

mod query_planner;

pub async fn tpch_ctx() -> Result<SessionContext> {
    let ctx = SessionContext::new();

    let tables = [
        "customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier",
    ];

    for table_name in tables {
        ctx.register_csv(
            table_name,
            &format!("../data/{}.csv", table_name),
            CsvReadOptions::new().delimiter(b'|'),
        )
        .await?;
    }

    Ok(ctx)
}

