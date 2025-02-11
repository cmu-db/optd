use std::sync::Arc;

use crate::{
    operators::{
        relational::{
            logical::{filter::Filter, join::Join, project::Project, scan::Scan, LogicalOperator},
            physical::{scan::table_scan::TableScan, PhysicalOperator},
        },
        scalar::{
            add::Add, and::And, column_ref::ColumnRef, constants::Constant, equal::Equal,
            ScalarOperator,
        },
    },
    plans::{
        logical::PartialLogicalPlan, physical::PartialPhysicalPlan, scalar::PartialScalarPlan,
    },
    values::OptdValue,
};

pub fn int64(value: i64) -> Arc<PartialScalarPlan> {
    Arc::new(PartialScalarPlan::PartialMaterialized {
        operator: ScalarOperator::Constant(Constant::new(OptdValue::Int64(value))),
    })
}

pub fn boolean(value: bool) -> Arc<PartialScalarPlan> {
    Arc::new(PartialScalarPlan::PartialMaterialized {
        operator: ScalarOperator::Constant(Constant::new(OptdValue::Bool(value))),
    })
}

pub fn string(value: &str) -> Arc<PartialScalarPlan> {
    Arc::new(PartialScalarPlan::PartialMaterialized {
        operator: ScalarOperator::Constant(Constant::new(OptdValue::String(value.to_string()))),
    })
}

pub fn column_ref(column_index: i64) -> Arc<PartialScalarPlan> {
    Arc::new(PartialScalarPlan::PartialMaterialized {
        operator: ScalarOperator::ColumnRef(ColumnRef::new(column_index)),
    })
}

pub fn add(left: Arc<PartialScalarPlan>, right: Arc<PartialScalarPlan>) -> Arc<PartialScalarPlan> {
    Arc::new(PartialScalarPlan::PartialMaterialized {
        operator: ScalarOperator::Add(Add::new(left, right)),
    })
}

pub fn equal(
    left: Arc<PartialScalarPlan>,
    right: Arc<PartialScalarPlan>,
) -> Arc<PartialScalarPlan> {
    Arc::new(PartialScalarPlan::PartialMaterialized {
        operator: ScalarOperator::Equal(Equal::new(left, right)),
    })
}

pub fn and(left: Arc<PartialScalarPlan>, right: Arc<PartialScalarPlan>) -> Arc<PartialScalarPlan> {
    Arc::new(PartialScalarPlan::PartialMaterialized {
        operator: ScalarOperator::And(And::new(left, right)),
    })
}

pub fn scan(table_name: &str, predicate: Arc<PartialScalarPlan>) -> Arc<PartialLogicalPlan> {
    Arc::new(PartialLogicalPlan::PartialMaterialized {
        operator: LogicalOperator::Scan(Scan::new(table_name, predicate)),
    })
}

pub fn filter(
    child: Arc<PartialLogicalPlan>,
    predicate: Arc<PartialScalarPlan>,
) -> Arc<PartialLogicalPlan> {
    Arc::new(PartialLogicalPlan::PartialMaterialized {
        operator: LogicalOperator::Filter(Filter::new(child, predicate)),
    })
}

pub fn join(
    join_type: &str,
    left: Arc<PartialLogicalPlan>,
    right: Arc<PartialLogicalPlan>,
    condition: Arc<PartialScalarPlan>,
) -> Arc<PartialLogicalPlan> {
    Arc::new(PartialLogicalPlan::PartialMaterialized {
        operator: LogicalOperator::Join(Join::new(join_type, left, right, condition)),
    })
}

pub fn project(
    child: Arc<PartialLogicalPlan>,
    fields: Vec<Arc<PartialScalarPlan>>,
) -> Arc<PartialLogicalPlan> {
    Arc::new(PartialLogicalPlan::PartialMaterialized {
        operator: LogicalOperator::Project(Project::new(child, fields)),
    })
}

pub fn table_scan(table_name: &str, predicate: Arc<PartialScalarPlan>) -> Arc<PartialPhysicalPlan> {
    Arc::new(PartialPhysicalPlan::PartialMaterialized {
        operator: PhysicalOperator::TableScan(TableScan::new(table_name, predicate)),
    })
}
