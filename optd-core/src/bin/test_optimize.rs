use std::sync::Arc;

use optd_core::{
    cascades::CascadesOptimizer,
    plan_nodes::{ConstantExpr, JoinType, LogicalFilter, LogicalJoin, LogicalScan, OptRelNode},
    rel_node::Value,
    rules::JoinCommuteRule,
};
use tracing::Level;

pub fn main() {
    tracing_subscriber::fmt()
        .with_max_level(Level::TRACE)
        .init();

    let mut optimizer = CascadesOptimizer::new_with_rules(vec![Arc::new(JoinCommuteRule {})]);
    let scan1 = LogicalScan::new("t1".into());
    let filter_cond = ConstantExpr::new(Value::Bool(true));
    let filter1 = LogicalFilter::new(scan1.0, filter_cond.0);
    let scan2 = LogicalScan::new("t2".into());
    let join_cond = ConstantExpr::new(Value::Bool(true));

    optimizer
        .optimize(
            LogicalJoin::new(filter1.0, scan2.0, join_cond.0, JoinType::Inner)
                .0
                .into_rel_node(),
        )
        .unwrap();
}
