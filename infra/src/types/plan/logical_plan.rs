use crate::types::operator::logical::LogicalOperator;
use std::sync::Arc;

/// A representation of a logical query plan DAG (directed acyclic graph).
///
/// The `LogicalPlan` DAG consists of [`LogicalOperator<LogicalLink>`] and
/// `ScalarOperator<ScalarLink>` nodes, where the generic links denote the kinds of children each of
/// those operators are allowed to have. For example, logical operators are allowed to have both
/// logical and scalar operators as children, but scalar operators are only allowed to have other
/// scalar operators as children.
///
/// The root of the plan DAG _cannot_ be a scalar operator (and thus for now can only be a logical
/// operator).
#[derive(Clone)]
pub struct LogicalPlan {
    pub root: Arc<LogicalOperator<LogicalLink>>,
}

/// A link in a [`LogicalPlan`] to a node.
///
/// A `LogicalLink` can either be a `LogicalNode` that points to a `LogicalOperator` or a
/// `ScalarNode` that points to a `ScalarOperator`.
///
/// Note that this `LogicalLink` is _**different**_ from the `LogicalLink`s defined in the sibling
/// module [`super::partial_logical_plan`]. [`super::logical_plan::LogicalLink`] only allows a
/// logical operator to have other logical operators or scalar operators as children (since that is
/// all [`LogicalPlan`] needs, and it is incorrect to have anything else in the tree).
///
/// [`LogicalPlan`]: crate::plan::logical_plan::LogicalPlan
#[derive(Clone)]
pub enum LogicalLink {
    LogicalNode(Arc<LogicalOperator<LogicalLink>>),
}
