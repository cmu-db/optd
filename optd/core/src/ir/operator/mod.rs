mod enforcer;
mod logical;
mod physical;

use std::sync::Arc;

pub use enforcer::sort::*;
pub use logical::get::*;
pub use logical::join::{LogicalJoin, LogicalJoinBorrowed, LogicalJoinMetadata};
pub use logical::order_by::*;
pub use logical::select::*;
pub use physical::filter::*;
pub use physical::nl_join::*;
pub use physical::table_scan::*;

pub mod join {
    pub use super::logical::join::JoinType;
}

#[cfg(test)]
pub use physical::mock_scan::*;
use pretty_xmlish::Pretty;

use crate::ir::explain::Explain;
use crate::ir::properties::OperatorProperties;
use crate::ir::{GroupMetadata, IRCommon, Scalar};

/// The operator type and its associated metadata.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum OperatorKind {
    Group(GroupMetadata),
    #[cfg(test)]
    MockScan(MockScanMetadata),
    LogicalGet(LogicalGetMetadata),
    LogicalJoin(LogicalJoinMetadata),
    LogicalSelect(LogicalSelectMetadata),
    EnforcerSort(EnforcerSortMetadata),
    PhysicalTableScan(PhysicalTableScanMetadata),
    PhysicalNLJoin(PhysicalNLJoinJoinMetadata),
    PhysicalFilter(PhysicalFilterMetadata),
}

#[derive(Debug, PartialEq)]
pub enum OperatorCategory {
    Logical,
    Physical,
    Enforcer,
    Placeholder,
}

impl OperatorKind {
    pub fn category(&self) -> OperatorCategory {
        match self {
            OperatorKind::Group(_) => OperatorCategory::Placeholder,
            OperatorKind::LogicalGet(_) | OperatorKind::LogicalJoin(_) => OperatorCategory::Logical,
            OperatorKind::EnforcerSort(_) => OperatorCategory::Enforcer,
            _other => OperatorCategory::Physical,
        }
    }

    pub fn maybe_produce_columns(&self) -> bool {
        match self {
            OperatorKind::LogicalGet(_) | OperatorKind::PhysicalTableScan(_) => true,
            #[cfg(test)]
            OperatorKind::MockScan(_) => true,
            _other => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Operator {
    /// The operator type and associated metadata.
    pub kind: OperatorKind,
    /// The input operators and scalars.
    pub(crate) common: IRCommon<OperatorProperties>,
}

impl Operator {
    /// Gets the slice to the input operators.
    pub fn input_operators(&self) -> &[Arc<Operator>] {
        &self.common.input_operators
    }

    /// Gets the slice to the input scalar expressions.
    pub fn input_scalars(&self) -> &[Arc<Scalar>] {
        &self.common.input_scalars
    }

    pub fn properties(&self) -> &Arc<OperatorProperties> {
        &self.common.properties
    }

    /// Clones the operator, optionally replacing the input operators and the input scalar expressions.
    pub fn clone_with_inputs(
        &self,
        input_operators: Option<Arc<[Arc<Operator>]>>,
        input_scalars: Option<Arc<[Arc<Scalar>]>>,
    ) -> Self {
        let input_operators =
            input_operators.unwrap_or_else(|| self.common.input_operators.clone());
        let input_scalars = input_scalars.unwrap_or_else(|| self.common.input_scalars.clone());
        Self {
            kind: self.kind.clone(),
            common: IRCommon::new(input_operators, input_scalars),
        }
    }
}

impl Explain for Operator {
    fn explain<'a>(
        &self,
        ctx: &super::IRContext,
        option: &super::explain::ExplainOption,
    ) -> pretty_xmlish::Pretty<'a> {
        match &self.kind {
            OperatorKind::Group(meta) => Pretty::display(&meta.group_id),
            #[cfg(test)]
            OperatorKind::MockScan(meta) => {
                MockScanBorrowed::from_raw_parts(meta, &self.common).explain(ctx, option)
            }
            OperatorKind::LogicalGet(_) => todo!(),
            OperatorKind::LogicalJoin(meta) => {
                LogicalJoinBorrowed::from_raw_parts(meta, &self.common).explain(ctx, option)
            }
            OperatorKind::LogicalSelect(meta) => {
                LogicalSelectBorrowed::from_raw_parts(meta, &self.common).explain(ctx, option)
            }
            OperatorKind::EnforcerSort(meta) => {
                EnforcerSortBorrowed::from_raw_parts(meta, &self.common).explain(ctx, option)
            }
            OperatorKind::PhysicalTableScan(_) => todo!(),
            OperatorKind::PhysicalNLJoin(meta) => {
                PhysicalNLJoinBorrowed::from_raw_parts(meta, &self.common).explain(ctx, option)
            }
            OperatorKind::PhysicalFilter(meta) => {
                PhysicalFilterBorrowed::from_raw_parts(meta, &self.common).explain(ctx, option)
            }
        }
    }
}
