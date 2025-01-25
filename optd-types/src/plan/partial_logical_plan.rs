use crate::operator::{logical::LogicalOperator, ScalarOperator};
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
pub enum PartialLogicalPlan {
    LogicalRoot(LogicalOperator<LogicalLink>),
}

/// A link in a [`PartialLogicalPlan`] to a node.
///
///
/// A `LogicalLink` can be one of three things: it can be a `LogicalNode` that points to a
/// `LogicalOperator`, it can be a `ScalarNode` that points to a `ScalarOperator`, or it can be a
/// [`GroupId`], denoting the unamterialized part of the plan (thus the name `PartialLogicalPlan`).
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
    Group(GroupId),
}

/// A link in a [`LogicalPlan`] to a scalar node. A `ScalarLink` can be either a `ScalarNode` that
/// points to a `ScalarOperator` or a `GroupId`.
///
/// Note that this `ScalarLink` is _**different**_ from the `ScalarLink`s defined in the sibling
/// modules.
///
/// TODO Add detailed docs here.
pub enum ScalarLink {
    ScalarNode(ScalarOperator<ScalarLink>),
    Group(GroupId),
}
