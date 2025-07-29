mod enforcer;
mod logical;
mod physical;

use std::sync::Arc;

pub use enforcer::sort::{EnforcerSort, EnforcerSortMetadata};
pub use logical::get::*;
pub use logical::join::{LogicalJoin, LogicalJoinMetadata};
pub use logical::order_by::*;
pub use physical::nl_join::*;
pub use physical::table_scan::*;

pub mod join {
    pub use super::logical::join::JoinType;
}

#[cfg(test)]
pub use physical::mock_scan::*;

use crate::ir::group::OperatorGroupMetadata;

use crate::ir::properties::OperatorProperties;
use crate::ir::{IRCommon, Scalar};

/// The operator type and its associated metadata.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum OperatorKind {
    Group(OperatorGroupMetadata),
    #[cfg(test)]
    MockScan(MockScanMetadata),
    LogicalGet(LogicalGetMetadata),
    LogicalJoin(LogicalJoinMetadata),
    EnforcerSort(EnforcerSortMetadata),
    PhysicalTableScan(PhysicalTableScanMetadata),
    PhysicalNLJoin(PhysicalNLJoinJoinMetadata),
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
