use crate::operator::{logical::LogicalOperator, ScalarOperator};
use std::sync::Arc;

/// A representation of a logical query plan DAG (directed acyclic graph).
///
/// The `LogicalPlan` DAG consists of [`LogicalOperator<LogicalLink>`] and
/// [`ScalarOperator<ScalarLink>`] nodes, where the generic links denote the kinds of children each
/// of those operators are allowed to have. For example, logical operators are allowed to have both
/// logical and scalar operators as children, but scalar operators are only allowed to have other
/// scalar operators as children.
///
/// The root of the plan DAG _cannot_ be a scalar operator (and thus for now can only be a logical
/// operator).
pub struct LogicalPlan(LogicalOperator<LogicalLink>);

/// A link in a [`LogicalPlan`] to a node.
///
/// A `LogicalLink` can either be a `LogicalNode` that points to a `LogicalOperator` or a
/// `ScalarNode` that points to a `ScalarOperator`.
///
/// Note that this `LogicalLink` is _**different**_ from the `LogicalLink`s defined in the sibling
/// modules:
///
/// - [`super::logical_plan::LogicalLink`] only allows a logical operator to have other logical
///   operators or scalar operators as children since that is all [`LogicalPlan`] needs
/// - [`super::physical_plan::LogicalLink`] allows a logical operator to also have a physical
///   operator as a child (since [`PhysicalPlan`] needs to encode both logical and physical
///   operators).
/// - [`super::partial_logical_plan::LogicalLink`] allows a logical operator to also have a
///   [`GroupId`] as a child (since the [`PartialLogicalPlan`] is a partially materialized query
///   plan).
/// - [`super::partial_physical_plan::LogicalLink`] allows a logical operator to have both physical
///   operators _and_ [`GroupId`] as a child (since [`PartialPhysicalPlan`] needs everything).
///
/// [`GroupId`]: crate::GroupId
/// [`LogicalPlan`]: crate::plan::logical_plan::LogicalPlan
/// [`PhysicalPlan`]: crate::plan::physical_plan::PhysicalPlan
/// [`PartialLogicalPlan`]: crate::plan::partial_logical_plan::PartialLogicalPlan
/// [`PartialPhysicalPlan`]: crate::plan::partial_physical_plan::PartialPhysicalPlan
pub enum LogicalLink {
    LogicalNode(LogicalOperator<LogicalLink>),
    ScalarNode(ScalarOperator<ScalarLink>),
}

/// A link in a [`LogicalPlan`] to a scalar node. A `ScalarLink` can only be a `ScalarNode` that
/// points to a `ScalarOperator` (for now).
///
/// Note that this `ScalarLink` is _**different**_ from the `ScalarLink`s defined in the sibling
/// modules.
///
/// TODO Add detailed docs here.
pub enum ScalarLink {
    ScalarNode(ScalarOperator<ScalarLink>),
}
