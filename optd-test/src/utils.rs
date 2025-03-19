use optd_core::cir::*;
use std::sync::Arc;

pub fn create_scan(table_name: &str) -> LogicalPlan {
    LogicalPlan(Operator {
        tag: "scan".to_string(),
        data: vec![OperatorData::String(table_name.to_string())],
        children: vec![],
    })
}

pub fn create_filter(child: LogicalPlan, _predicate: Vec<()>) -> LogicalPlan {
    LogicalPlan(Operator {
        tag: "filter".to_string(),
        data: vec![], // TODO
        children: vec![Child::Singleton(Arc::new(child))],
    })
}

pub fn create_project(child: LogicalPlan, columns: Vec<String>) -> LogicalPlan {
    LogicalPlan(Operator {
        tag: "project".to_string(),
        data: columns.into_iter().map(OperatorData::String).collect(),
        children: vec![Child::Singleton(Arc::new(child))],
    })
}

pub fn create_join(left: LogicalPlan, right: LogicalPlan, _condition: Vec<()>) -> LogicalPlan {
    LogicalPlan(Operator {
        tag: "join".to_string(),
        data: vec![], // TODO
        children: vec![
            Child::Singleton(Arc::new(left)),
            Child::Singleton(Arc::new(right)),
        ],
    })
}
