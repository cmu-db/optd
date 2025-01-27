#![doc = include_str!("../../README.md")]
#![allow(unused)]


use datafusion::common::Result;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::*;


pub mod expression;
pub mod memo;
pub mod operator;
pub mod plan;

/// TODO make distinction between relational groups and scalar groups.
#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GroupId(u64);

/// TODO Add docs.
pub struct ExprId(u64);


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
