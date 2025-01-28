use crate::types::operator::logical::LogicalOperator;
use crate::GroupId;
use std::sync::Arc;

/// A representation of a partially materialized logical query plan DAG.
///
/// Similar to a [`LogicalPlan`], the `PartialLogicalPlan` DAG consists of both logical and scalar
/// operator nodes, but it can also have unmaterialized [`GroupId`]s representing entire memo groups
/// as children.
///
/// Note that while logical nodes can have both logical and scalar nodes as children, scalar nodes can only have other scalar
/// nodes as children.
///
/// Note that the root of the plan _cannot_ be a scalar operator (and thus can only be a logical
/// operator).
///
/// [`LogicalPlan`]: crate::plan::logical_plan::LogicalPlan
#[derive(Clone)]
pub enum PartialLogicalPlan {
    LogicalRoot(Arc<LogicalOperator<LogicalLink>>),
}

/// A link in a [`PartialLogicalPlan`] to a node.
///
/// A `LogicalLink` can be one of three things: it can be a `LogicalNode` that points to a
/// `LogicalOperator`, it can be a `ScalarNode` that points to a `ScalarOperator`, or it can be a
/// [`GroupId`], denoting the unamterialized part of the plan (thus the name `PartialLogicalPlan`).
///
/// Note that this `LogicalLink` is _**different**_ from the `LogicalLink`s defined in the sibling
/// module [`super::logical_plan`]. [`LogicalPlan`] does not need to have [`GroupId`]s as children,
/// and so its type representation does not allow that.
///
/// [`GroupId`]: crate::GroupId
/// [`LogicalPlan`]: crate::plan::logical_plan::LogicalPlan
#[derive(Clone)]
pub enum LogicalLink {
    LogicalNode(Arc<LogicalOperator<LogicalLink>>),
    Group(GroupId),
}
