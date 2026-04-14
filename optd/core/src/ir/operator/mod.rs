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

use std::collections::HashSet;
use std::sync::Arc;

pub use enforcer::sort::*;
pub use logical::aggregate::*;
pub use logical::dependent_join::*;
pub use logical::get::*;
pub use logical::join::*;
pub use logical::limit::*;
pub use logical::order_by::*;
pub use logical::project::*;
pub use logical::remap::*;
pub use logical::select::*;
pub use logical::subquery::*;

pub mod join {
    pub use super::logical::join::JoinType;
}

use crate::ir::explain::Explain;
use crate::ir::properties::OperatorProperties;
use crate::ir::{Column, Group, GroupId, GroupMetadata, IRCommon, Scalar};

/// The operator type and its associated metadata.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum OperatorKind {
    Group(GroupMetadata),
    Get(GetMetadata),
    Join(JoinMetadata),
    DependentJoin(DependentJoinMetadata),
    Select(SelectMetadata),
    Project(ProjectMetadata),
    Aggregate(AggregateMetadata),
    Limit(LimitMetadata),
    OrderBy(OrderByMetadata),
    Remap(RemapMetadata),
    Subquery(SubqueryMetadata),
    EnforcerSort(EnforcerSortMetadata),
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
            Get(meta) => {
                if meta.implementation.is_some() {
                    OperatorCategory::Physical
                } else {
                    OperatorCategory::Logical
                }
            }
            Join(meta) => {
                if meta.implementation.is_some() {
                    OperatorCategory::Physical
                } else {
                    OperatorCategory::Logical
                }
            }
            DependentJoin(_) => OperatorCategory::Logical,
            Project(_) => OperatorCategory::Logical,
            Aggregate(meta) => {
                if meta.implementation.is_some() {
                    OperatorCategory::Physical
                } else {
                    OperatorCategory::Logical
                }
            }
            Limit(_) => OperatorCategory::Logical,
            OrderBy(_) => OperatorCategory::Logical,
            Remap(_) => OperatorCategory::Logical,
            Select(_) => OperatorCategory::Logical,
            Subquery(_) => OperatorCategory::Logical,
            EnforcerSort(_) => OperatorCategory::Enforcer,
        }
    }

    /// Returns true if the operator may produce columns as output.
    pub fn maybe_produce_columns(&self) -> bool {
        match self {
            OperatorKind::Get(_) => true,
            OperatorKind::Project(_) => true,
            OperatorKind::Aggregate(_) => true,
            _other => false,
        }
    }
}

/// The operator struct that is able to represent any operator type.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Operator {
    /// The group ID if this operator is a placeholder for a group.
    pub group_id: Option<GroupId>,
    /// The operator type and associated metadata.
    pub kind: OperatorKind,
    /// The input operators and scalars.
    #[cfg_attr(feature = "serde", serde(rename = "_common"))]
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
            OperatorKind::Get(meta) => {
                Get::borrow_raw_parts(meta, &self.common).explain(ctx, option)
            }
            OperatorKind::Join(meta) => {
                Join::borrow_raw_parts(meta, &self.common).explain(ctx, option)
            }
            OperatorKind::DependentJoin(meta) => {
                DependentJoin::borrow_raw_parts(meta, &self.common).explain(ctx, option)
            }
            OperatorKind::Select(meta) => {
                Select::borrow_raw_parts(meta, &self.common).explain(ctx, option)
            }
            OperatorKind::Limit(meta) => {
                Limit::borrow_raw_parts(meta, &self.common).explain(ctx, option)
            }
            OperatorKind::OrderBy(meta) => {
                OrderBy::borrow_raw_parts(meta, &self.common).explain(ctx, option)
            }
            OperatorKind::EnforcerSort(meta) => {
                EnforcerSort::borrow_raw_parts(meta, &self.common).explain(ctx, option)
            }
            OperatorKind::Project(meta) => {
                Project::borrow_raw_parts(meta, &self.common).explain(ctx, option)
            }
            OperatorKind::Aggregate(meta) => {
                Aggregate::borrow_raw_parts(meta, &self.common).explain(ctx, option)
            }
            OperatorKind::Remap(meta) => {
                Remap::borrow_raw_parts(meta, &self.common).explain(ctx, option)
            }
            OperatorKind::Subquery(meta) => {
                Subquery::borrow_raw_parts(meta, &self.common).explain(ctx, option)
            }
        }
    }
}

#[cfg(test)]
#[cfg(feature = "serde")]
mod tests {
    use std::sync::Arc;

    use crate::ir::{
        Column, ScalarValue,
        catalog::DataSourceId,
        convert::{IntoOperator, IntoScalar},
        properties::{TupleOrdering, TupleOrderingDirection},
        scalar::{BinaryOp, BinaryOpKind, ColumnRef, Literal},
    };

    use super::{EnforcerSort, Get, Operator, Select};

    #[test]
    fn query_plan_round_trips_through_serde() {
        let input = Get::logical(DataSourceId(1), 1, Arc::from([0_usize, 1])).into_operator();
        let predicate = BinaryOp::new(
            BinaryOpKind::Eq,
            ColumnRef::new(Column(1, 0)).into_scalar(),
            Literal::new(ScalarValue::Int32(Some(42))).into_scalar(),
        )
        .into_scalar();
        let select = Select::new(input, predicate).into_operator();
        let ordering = TupleOrdering::from_iter([(Column(1, 0), TupleOrderingDirection::Asc)]);
        let plan = EnforcerSort::new(ordering, select).into_operator();

        let serialized = serde_json::to_string(&plan).unwrap();
        let round_trip = serde_json::from_str::<Operator>(&serialized).unwrap();

        println!("serialized: {}", serialized);

        assert_eq!(round_trip, *plan);
    }
}
