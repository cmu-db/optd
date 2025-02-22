use std::sync::Arc;

use crate::{
    cascades::ir::OperatorData, operators::{
        relational::{
            logical::{filter::Filter, join::Join, project::Project, scan::Scan, LogicalOperator},
            physical::{
                filter::filter::PhysicalFilter, join::nested_loop_join::NestedLoopJoin,
                project::PhysicalProject, scan::table_scan::TableScan, PhysicalOperator,
            },
        },
        scalar::{column_ref::ColumnRef, constants::Constant, ScalarOperator},
    }, plans::{
        logical::PartialLogicalPlan, physical::PartialPhysicalPlan, scalar::PartialScalarPlan,
    }
};

pub fn int64(value: i64) -> Arc<PartialScalarPlan> {
    Arc::new(PartialScalarPlan::PartialMaterialized {
        operator: ScalarOperator::Constant(Constant::new(OperatorData::Int64(value))),
    })
}

pub fn boolean(value: bool) -> Arc<PartialScalarPlan> {
    Arc::new(PartialScalarPlan::PartialMaterialized {
        operator: ScalarOperator::Constant(Constant::new(OperatorData::Bool(value))),
    })
}

pub fn string(value: &str) -> Arc<PartialScalarPlan> {
    Arc::new(PartialScalarPlan::PartialMaterialized {
        operator: ScalarOperator::Constant(Constant::new(OperatorData::String(value.to_string()))),
    })
}

pub fn column_ref(column_index: i64) -> Arc<PartialScalarPlan> {
    Arc::new(PartialScalarPlan::PartialMaterialized {
        operator: ScalarOperator::ColumnRef(ColumnRef::new(column_index)),
    })
}

pub fn add(left: Arc<PartialScalarPlan>, right: Arc<PartialScalarPlan>) -> Arc<PartialScalarPlan> {
    Arc::new(PartialScalarPlan::PartialMaterialized {
        operator: crate::operators::scalar::binary_op::add(left, right),
    })
}

pub fn minus(
    left: Arc<PartialScalarPlan>,
    right: Arc<PartialScalarPlan>,
) -> Arc<PartialScalarPlan> {
    Arc::new(PartialScalarPlan::PartialMaterialized {
        operator: crate::operators::scalar::binary_op::minus(left, right),
    })
}

pub fn equal(
    left: Arc<PartialScalarPlan>,
    right: Arc<PartialScalarPlan>,
) -> Arc<PartialScalarPlan> {
    Arc::new(PartialScalarPlan::PartialMaterialized {
        operator: crate::operators::scalar::binary_op::equal(left, right),
    })
}

pub fn neg(child: Arc<PartialScalarPlan>) -> Arc<PartialScalarPlan> {
    Arc::new(PartialScalarPlan::PartialMaterialized {
        operator: crate::operators::scalar::unary_op::neg(child),
    })
}

pub fn not(child: Arc<PartialScalarPlan>) -> Arc<PartialScalarPlan> {
    Arc::new(PartialScalarPlan::PartialMaterialized {
        operator: crate::operators::scalar::unary_op::not(child),
    })
}

pub fn and(children: Vec<Arc<PartialScalarPlan>>) -> Arc<PartialScalarPlan> {
    Arc::new(PartialScalarPlan::PartialMaterialized {
        operator: crate::operators::scalar::logic_op::and(children),
    })
}

pub fn or(children: Vec<Arc<PartialScalarPlan>>) -> Arc<PartialScalarPlan> {
    Arc::new(PartialScalarPlan::PartialMaterialized {
        operator: crate::operators::scalar::logic_op::or(children),
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

pub fn physical_filter(
    child: Arc<PartialPhysicalPlan>,
    predicate: Arc<PartialScalarPlan>,
) -> Arc<PartialPhysicalPlan> {
    Arc::new(PartialPhysicalPlan::PartialMaterialized {
        operator: PhysicalOperator::Filter(PhysicalFilter::new(child, predicate)),
    })
}

pub fn nested_loop_join(
    join_type: &str,
    outer: Arc<PartialPhysicalPlan>,
    inner: Arc<PartialPhysicalPlan>,
    condition: Arc<PartialScalarPlan>,
) -> Arc<PartialPhysicalPlan> {
    Arc::new(PartialPhysicalPlan::PartialMaterialized {
        operator: PhysicalOperator::NestedLoopJoin(NestedLoopJoin::new(
            join_type, outer, inner, condition,
        )),
    })
}

pub fn physical_project(
    child: Arc<PartialPhysicalPlan>,
    fields: Vec<Arc<PartialScalarPlan>>,
) -> Arc<PartialPhysicalPlan> {
    Arc::new(PartialPhysicalPlan::PartialMaterialized {
        operator: PhysicalOperator::Project(PhysicalProject::new(child, fields)),
    })
}
