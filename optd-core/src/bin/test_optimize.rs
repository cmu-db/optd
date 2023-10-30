use std::sync::Arc;

use optd_core::{
    cascades::CascadesOptimizer,
    plan_nodes::{
        BinOpExpr, BinOpType, ColumnRefExpr, ConstantExpr, JoinType, LogicalFilter, LogicalJoin,
        LogicalScan, OptRelNode, PlanNode,
    },
    rel_node::Value,
    rules::{FilterJoinRule, JoinAssocRule, JoinCommuteRule, PhysicalConversionRule},
};

use tracing::Level;

pub fn main() {
    tracing_subscriber::fmt()
        .with_max_level(Level::TRACE)
        .with_target(false)
        .init();

    let mut optimizer = CascadesOptimizer::new_with_rules(vec![
        // Arc::new(JoinCommuteRule {}),
        // Arc::new(JoinAssocRule {}),
        // Arc::new(FilterJoinRule {}),
        Arc::new(PhysicalConversionRule {}),
    ]);

    let scan1 = LogicalScan::new("t1".into());
    let filter_cond = BinOpExpr::new(
        ColumnRefExpr::new(1).0,
        ConstantExpr::new(Value::Int(2)).0,
        BinOpType::Eq,
    );
    let filter1 = LogicalFilter::new(scan1.0, filter_cond.0);
    let scan2 = LogicalScan::new("t2".into());
    let join_cond = ConstantExpr::new(Value::Bool(true));
    let scan3 = LogicalScan::new("t3".into());
    let join_filter = LogicalJoin::new(filter1.0, scan2.0, join_cond.clone().0, JoinType::Inner);
    let fnal = LogicalJoin::new(scan3.0, join_filter.0, join_cond.0, JoinType::Inner);
    let result = optimizer.optimize(fnal.0.into_rel_node()).unwrap();
    for node in result {
        println!(
            "{}",
            PlanNode::from_rel_node(node).unwrap().explain_to_string()
        );
    }
}
