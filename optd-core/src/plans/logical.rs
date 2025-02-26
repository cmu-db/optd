//! Logical plan representations for the OPTD optimizer.
//!
//! Provides three levels of plan materialization:
//! 1. Full materialization (LogicalPlan)
//! 2. Partial materialization (PartialLogicalPlan)
//! 3. Group references (LogicalGroupId)
//!
//! This allows the optimizer to work with plans at different stages
//! of materialization during the optimization process.

use crate::{
    cascades::{expressions::LogicalExpression, groups::RelationalGroupId},
    operators::relational::logical::{
        filter::Filter, join::Join, project::Project, scan::Scan, LogicalOperator,
    },
    values::OptdValue,
};

use super::{
    scalar::{PartialScalarPlan, ScalarPlan},
    PartialPlanExpr,
};
use std::sync::Arc;

/// A fully materialized logical query plan.
///
/// Contains a complete tree of logical operators where all children
/// (both logical and scalar) are fully materialized. Used for final
/// plan representation after optimization is complete.
#[derive(Clone, Debug, PartialEq)]
pub struct LogicalPlan {
    pub operator: LogicalOperator<OptdValue, Arc<LogicalPlan>, Arc<ScalarPlan>>,
}

/// A logical plan with varying levels of materialization.
///
/// During optimization, plans can be in three states:
/// - Partially materialized: Single materialized operator with group references
/// - Unmaterialized: Pure group reference
#[derive(Clone, Debug, PartialEq)]
pub enum PartialLogicalPlan {
    /// Single materialized operator with potentially unmaterialized children
    PartialMaterialized {
        operator: LogicalOperator<OptdValue, Arc<PartialLogicalPlan>, Arc<PartialScalarPlan>>,
    },

    /// Reference to an optimization group containing equivalent plans
    UnMaterialized(RelationalGroupId),
}

impl PartialLogicalPlan {
    pub fn from_expr(expr: &LogicalExpression) -> Self {
        match expr {
            LogicalOperator::Scan(scan) => PartialLogicalPlan::PartialMaterialized {
                operator: LogicalOperator::<
                    OptdValue,
                    Arc<PartialLogicalPlan>,
                    Arc<PartialScalarPlan>,
                >::Scan(Scan::<OptdValue, Arc<PartialScalarPlan>> {
                    table_name: scan.table_name.clone(),
                    predicate: Arc::new(PartialScalarPlan::UnMaterialized(scan.predicate)),
                }),
            },
            LogicalOperator::Filter(filter) => PartialLogicalPlan::PartialMaterialized {
                operator: LogicalOperator::<
                    OptdValue,
                    Arc<PartialLogicalPlan>,
                    Arc<PartialScalarPlan>,
                >::Filter(Filter::<
                    Arc<PartialLogicalPlan>,
                    Arc<PartialScalarPlan>,
                > {
                    predicate: Arc::new(PartialScalarPlan::UnMaterialized(filter.predicate)),
                    child: Arc::new(PartialLogicalPlan::UnMaterialized(filter.child)),
                }),
            },
            LogicalOperator::Join(join) => PartialLogicalPlan::PartialMaterialized {
                operator: LogicalOperator::<
                    OptdValue,
                    Arc<PartialLogicalPlan>,
                    Arc<PartialScalarPlan>,
                >::Join(Join::<
                    OptdValue,
                    Arc<PartialLogicalPlan>,
                    Arc<PartialScalarPlan>,
                > {
                    join_type: join.join_type.clone(),
                    left: Arc::new(PartialLogicalPlan::UnMaterialized(join.left)),
                    right: Arc::new(PartialLogicalPlan::UnMaterialized(join.right)),
                    condition: Arc::new(PartialScalarPlan::UnMaterialized(join.condition)),
                }),
            },
            LogicalOperator::Project(project) => PartialLogicalPlan::PartialMaterialized {
                operator: LogicalOperator::<
                    OptdValue,
                    Arc<PartialLogicalPlan>,
                    Arc<PartialScalarPlan>,
                >::Project(Project::<
                    Arc<PartialLogicalPlan>,
                    Arc<PartialScalarPlan>,
                > {
                    child: Arc::new(PartialLogicalPlan::UnMaterialized(project.child)),
                    fields: project
                        .fields
                        .iter()
                        .map(|field| Arc::new(PartialScalarPlan::UnMaterialized(*field)))
                        .collect(),
                }),
            },
        }
    }
}

/// Type alias for expressions that construct logical plans.
/// See PartialPlanExpr for the available expression constructs.
pub type PartialLogicalPlanExpr = PartialPlanExpr<PartialLogicalPlan>;
