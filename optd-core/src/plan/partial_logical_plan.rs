//! This module contains the [`PartialLogicalPlan`] type, which is the representation of a partially
//! materialized logical query plan that is a mix of materialized logical operators and
//! unmaterialized group ID references to memo table groups of expressions.
//!
//! See the documentation for [`PartialLogicalPlan`] for more information.

use crate::memo::{GroupId, ScalarGroupId};
use crate::operator::relational::logical::LogicalOperator;
use crate::operator::scalar::ScalarOperator;
use std::sync::Arc;

/// A partially materialized logical query plan represented as a DAG (directed acyclic graph).
///
/// While a [`LogicalPlan`] contains fully materialized operator nodes, a [`PartialLogicalPlan`]
/// can contain both materialized nodes and references to unmaterialized memo groups. This enables
/// efficient plan exploration and transformation during query optimization.
///
/// # Structure
///
/// - Nodes can be either materialized operators or group references.
/// - Relational nodes can have both relational and scalar children.
/// - Scalar nodes can only have scalar children.
/// - The root must be a relational operator.
///
/// # Type Parameters
///
/// The plan uses [`Relation`] and [`Scalar`] to represent its node connections,
/// allowing mixing of materialized nodes and group references.
///
/// [`LogicalPlan`]: crate::plan::logical_plan::LogicalPlan
#[derive(Clone)]
pub struct PartialLogicalPlan {
    /// Represents the current logical operator that is the root of the current partially
    /// materialized subplan.
    ///
    /// Note that the children of the operator are either a [`Relation`] or a [`Scalar`], both of
    /// which are defined in this module. See their documentation for more information.
    pub node: Arc<LogicalOperator<Relation, Scalar>>,
}

/// A link to a relational node in a [`PartialLogicalPlan`].
///
/// This link (which denotes what kind of relational children the operators of a
/// [`PartialLogicalPlan`] can have) can be either:
/// - A materialized logical operator node.
/// - A reference (identifier) to an unmaterialized memo group.
#[derive(Clone)]
pub enum Relation {
    /// A materialized logical operator node.
    Operator(Arc<LogicalOperator<Relation, Scalar>>),
    /// A reference (identifier) to an unmaterialized memo group.
    GroupId(GroupId),
}

/// A link to a scalar node in a [`PartialLogicalPlan`].
///
/// This link (which denotes what kind of scalar children the operators of a [`PartialLogicalPlan`]
/// can have) can be either:
/// - A materialized scalar operator node.
/// - A reference to an unmaterialized memo group.
#[derive(Clone)]
pub enum Scalar {
    /// A materialized scalar operator node.
    Operator(Arc<ScalarOperator<Scalar>>),
    /// A reference to an unmaterialized memo group.
    ScalarGroupId(ScalarGroupId),
}
