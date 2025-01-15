use relation::{LogicalExpression, Relation};
use scalar::Scalar;

use crate::{GroupId, Rule};

mod relation;
mod scalar;

/// A type representing an optimization node / object in the memo table.
pub enum MemoNode {
    Relation(Relation),
    Scalar(Scalar),
}

pub struct LogicalOperator;

pub struct PhysicalOperator;

/// A type representing a tree of logical nodes, scalar nodes, and group IDs.
///
/// Note that group IDs must be leaves of this tree.
///
/// TODO Make this an actual tree with the correct modeling of types.
pub enum PartialLogicalExpression {
    LogicalOperator(LogicalOperator),
    Scalar(Scalar),
    GroupId(GroupId),
}

/// A type representing a tree of logical nodes, physical nodes, scalar nodes, and group IDs.
///
/// Note that group IDs must be leaves of this tree, and that physical nodes cannot have children
/// that are logical nodes.
///
/// TODO Make this an actual tree with the correct modeling of types.
pub enum PartialPhysicalExpression {
    LogicalOperator(LogicalOperator),
    PhysicalOperator(PhysicalOperator),
    Scalar(Scalar),
    GroupId(GroupId),
}

pub struct Memo;

impl Memo {
    /// Checks if the transformation rule matches the current expression and any of it's children.
    /// Returns a vector of partially materialized plans.
    ///
    /// This returns a vector because the rule matching the input root expression could have matched
    /// with multiple child expressions.
    ///
    /// For example, let's say the input expression is `Filter(G1)`, and the group G1 has two
    /// expressions `e1 = Join(Join(A, B), C)` and `e2 = Join(A, Join(B, C))`.
    ///
    /// If the rule wants to match against `Filter(Join(?L, ?R))`, then this function will partially
    /// materialize two expressions `Filter(e1)` and `Filter(e2)`.
     pub async fn check_transformation(
        &self,
        expr: LogicalExpression,
        rule: Rule,
    ) -> Vec<PartialLogicalExpression> {
        todo!()
    }

    pub async fn check_implementation(
        &self,
        expr: LogicalExpression,
        rule: Rule,
    ) -> Vec<PartialPhysicalExpression> {
        todo!()
    }

    pub fn apply_transformation(
        &mut self,
        expr: PartialLogicalExpression,
        rule: Rule,
    ) -> Vec<MemoNode> {
        todo!()
    }

    pub fn apply_implementation(
        &mut self,
        expr: PartialPhysicalExpression,
        rule: Rule,
    ) -> Vec<MemoNode> {
        todo!()
    }

    pub fn add_expressions(&mut self, new_exprs: Vec<MemoNode>) {
        todo!()
    }
}
