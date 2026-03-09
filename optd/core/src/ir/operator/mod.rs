//! Operators are the core component of the IR. They represent operations
//! that can be performed on data, such as scans, joins, filters.
//!
//! While each operator has a specific structure and metadata, they all share
//! common characteristics, such as input operators and scalar expressions.
//!
//! This module defines the `Operator` struct, which encapsulates these common
//! characteristics, along with an enum `OperatorKind` that enumerates all
//! possible operator types and their associated metadata.
//!
//! When tree plans are constructed, operators are stored in the `Operator`
//! struct, with their specific type and metadata represented by the
//! `OperatorKind` enum. They can be "downcasted" (i.e. reconstructed) to their
//! specific types when needed.

mod enforcer;
mod logical;
mod physical;

use std::collections::HashSet;
use std::sync::Arc;

pub use enforcer::sort::*;
pub use logical::aggregate::*;
pub use logical::dependent_join::*;
pub use logical::get::*;
pub use logical::join::*;
pub use logical::order_by::*;
pub use logical::project::*;
pub use logical::remap::*;
pub use logical::select::*;
pub use logical::subquery::*;
pub use physical::filter::*;
pub use physical::hash_aggregate::*;
pub use physical::hash_join::*;
pub use physical::nl_join::*;
pub use physical::project::*;
pub use physical::table_scan::*;

pub mod join {
    pub use super::logical::join::JoinType;
}

pub use physical::mock_scan::*;

use crate::ir::explain::Explain;
use crate::ir::properties::OperatorProperties;
use crate::ir::{Column, Group, GroupId, GroupMetadata, IRCommon, Scalar};

/// The operator type and its associated metadata.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum OperatorKind {
    Group(GroupMetadata),
    MockScan(MockScanMetadata),
    LogicalGet(LogicalGetMetadata),
    LogicalJoin(LogicalJoinMetadata),
    LogicalDependentJoin(LogicalDependentJoinMetadata),
    LogicalSelect(LogicalSelectMetadata),
    LogicalProject(LogicalProjectMetadata),
    LogicalAggregate(LogicalAggregateMetadata),
    LogicalOrderBy(LogicalOrderByMetadata),
    LogicalRemap(LogicalRemapMetadata),
    LogicalSubquery(LogicalSubqueryMetadata),
    EnforcerSort(EnforcerSortMetadata),
    PhysicalTableScan(PhysicalTableScanMetadata),
    PhysicalNLJoin(PhysicalNLJoinMetadata),
    PhysicalHashJoin(PhysicalHashJoinMetadata),
    PhysicalFilter(PhysicalFilterMetadata),
    PhysicalProject(PhysicalProjectMetadata),
    PhysicalHashAggregate(PhysicalHashAggregateMetadata),
}

#[derive(Debug, PartialEq)]
pub enum OperatorCategory {
    Logical,
    Physical,
    Enforcer,
    Placeholder,
}

impl OperatorKind {
    /// Returns the category of the operator.
    pub fn category(&self) -> OperatorCategory {
        use OperatorKind::*;
        match self {
            Group(_) => OperatorCategory::Placeholder,
            LogicalGet(_) => OperatorCategory::Logical,
            LogicalJoin(_) => OperatorCategory::Logical,
            LogicalDependentJoin(_) => OperatorCategory::Logical,
            LogicalProject(_) => OperatorCategory::Logical,
            LogicalAggregate(_) => OperatorCategory::Logical,
            LogicalOrderBy(_) => OperatorCategory::Logical,
            LogicalRemap(_) => OperatorCategory::Logical,
            LogicalSelect(_) => OperatorCategory::Logical,
            LogicalSubquery(_) => OperatorCategory::Logical,
            EnforcerSort(_) => OperatorCategory::Enforcer,
            PhysicalFilter(_) => OperatorCategory::Physical,
            PhysicalProject(_) => OperatorCategory::Physical,
            PhysicalHashJoin(_) => OperatorCategory::Physical,
            PhysicalNLJoin(_) => OperatorCategory::Physical,
            PhysicalTableScan(_) => OperatorCategory::Physical,
            PhysicalHashAggregate(_) => OperatorCategory::Physical,
            MockScan(_) => OperatorCategory::Physical,
        }
    }

    /// Returns true if the operator may produce columns as output.
    pub fn maybe_produce_columns(&self) -> bool {
        match self {
            OperatorKind::LogicalGet(_) | OperatorKind::PhysicalTableScan(_) => true,
            OperatorKind::LogicalProject(_) | OperatorKind::PhysicalProject(_) => true,
            OperatorKind::LogicalAggregate(_) | OperatorKind::PhysicalHashAggregate(_) => true,
            OperatorKind::MockScan(_) => true,
            _other => false,
        }
    }
}

/// The operator struct that is able to represent any operator type.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Operator {
    /// The group ID if this operator is a placeholder for a group.
    pub group_id: Option<GroupId>,
    /// The operator type and associated metadata.
    pub kind: OperatorKind,
    /// The input operators and scalars.
    pub common: IRCommon<OperatorProperties>,
}

impl Operator {
    pub fn from_raw_parts(
        group_id: Option<GroupId>,
        kind: OperatorKind,
        common: IRCommon<OperatorProperties>,
    ) -> Self {
        Self {
            group_id,
            kind,
            common,
        }
    }

    /// Gets the slice to the input operators.
    pub fn input_operators(&self) -> &[Arc<Operator>] {
        &self.common.input_operators
    }

    /// Gets the slice to the input scalar expressions.
    pub fn input_scalars(&self) -> &[Arc<Scalar>] {
        &self.common.input_scalars
    }

    /// Gests the operator properties.
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
            group_id: None,
            kind: self.kind.clone(),
            common: IRCommon::new(input_operators, input_scalars),
        }
    }

    /// Gets the set of columns used by this operator and its children
    /// TODO: Are all columns used by an operator always stored in its scalar
    /// set? Can we guarantee used columns will not be part of the metadata
    /// set? If this is not guaranteed, should operators implement some sort of
    /// used_columns property (similar to output_schema), so we don't have this
    /// fragile assumption?
    pub fn collect_used_columns(&self) -> HashSet<Column> {
        let mut used = HashSet::new();
        self.collect_used_columns_recursive(&mut used);
        used
    }

    /// Recursive subcall used in collceted_used_columns to get child uses
    fn collect_used_columns_recursive(&self, used: &mut HashSet<Column>) {
        for scalar in self.input_scalars() {
            for col in scalar.used_columns().iter() {
                used.insert(*col);
            }
        }
        for child in self.input_operators() {
            child.collect_used_columns_recursive(used);
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
            OperatorKind::Group(meta) => {
                Group::borrow_raw_parts(meta, &self.common).explain(ctx, option)
            }
            OperatorKind::MockScan(meta) => {
                MockScan::borrow_raw_parts(meta, &self.common).explain(ctx, option)
            }
            OperatorKind::LogicalGet(meta) => {
                LogicalGet::borrow_raw_parts(meta, &self.common).explain(ctx, option)
            }
            OperatorKind::LogicalJoin(meta) => {
                LogicalJoin::borrow_raw_parts(meta, &self.common).explain(ctx, option)
            }
            OperatorKind::LogicalDependentJoin(meta) => {
                LogicalDependentJoin::borrow_raw_parts(meta, &self.common).explain(ctx, option)
            }
            OperatorKind::LogicalSelect(meta) => {
                LogicalSelect::borrow_raw_parts(meta, &self.common).explain(ctx, option)
            }
            OperatorKind::LogicalOrderBy(meta) => {
                LogicalOrderBy::borrow_raw_parts(meta, &self.common).explain(ctx, option)
            }
            OperatorKind::EnforcerSort(meta) => {
                EnforcerSort::borrow_raw_parts(meta, &self.common).explain(ctx, option)
            }
            OperatorKind::PhysicalTableScan(meta) => {
                PhysicalTableScan::borrow_raw_parts(meta, &self.common).explain(ctx, option)
            }
            OperatorKind::PhysicalNLJoin(meta) => {
                PhysicalNLJoin::borrow_raw_parts(meta, &self.common).explain(ctx, option)
            }
            OperatorKind::PhysicalHashJoin(meta) => {
                PhysicalHashJoin::borrow_raw_parts(meta, &self.common).explain(ctx, option)
            }
            OperatorKind::PhysicalFilter(meta) => {
                PhysicalFilter::borrow_raw_parts(meta, &self.common).explain(ctx, option)
            }
            OperatorKind::LogicalProject(meta) => {
                LogicalProject::borrow_raw_parts(meta, &self.common).explain(ctx, option)
            }
            OperatorKind::PhysicalProject(meta) => {
                PhysicalProject::borrow_raw_parts(meta, &self.common).explain(ctx, option)
            }
            OperatorKind::LogicalAggregate(meta) => {
                LogicalAggregate::borrow_raw_parts(meta, &self.common).explain(ctx, option)
            }
            OperatorKind::PhysicalHashAggregate(meta) => {
                PhysicalHashAggregate::borrow_raw_parts(meta, &self.common).explain(ctx, option)
            }
            OperatorKind::LogicalRemap(meta) => {
                LogicalRemap::borrow_raw_parts(meta, &self.common).explain(ctx, option)
            }
            OperatorKind::LogicalSubquery(meta) => {
                LogicalSubquery::borrow_raw_parts(meta, &self.common).explain(ctx, option)
            }
        }
    }
}
