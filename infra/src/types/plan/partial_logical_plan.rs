use crate::types::memo::GroupId;
use crate::types::operator::relational::logical::LogicalOperator;
use crate::types::operator::scalar::ScalarOperator;
use std::sync::Arc;

/// A partially materialized logical query plan represented as a DAG (directed acyclic graph).
///
/// While a [`LogicalPlan`] contains fully materialized operator nodes, a `PartialLogicalPlan`
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
    pub node: Arc<LogicalOperator<Relation, Scalar>>,
}

/// A link to a relational node in a [`PartialLogicalPlan`].
///
/// Can be either:
/// - A materialized logical operator node
/// - A reference to an unmaterialized memo group
#[derive(Clone)]
pub enum Relation {
    Operator(Arc<LogicalOperator<Relation, Scalar>>),
    GroupId(GroupId),
}

/// A link to a scalar node in a [`PartialLogicalPlan`].
///
/// Can be either:
/// - A materialized scalar operator node
/// - A reference to an unmaterialized memo group
#[derive(Clone)]
pub enum Scalar {
    Operator(Arc<ScalarOperator<Scalar>>),
    GroupId(GroupId),
}
