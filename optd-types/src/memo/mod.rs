use relation::{LogicalExpression, Relation};
use scalar::Scalar;

use crate::Rule;

mod relation;
mod scalar;

/// A type representing an optimization node / object in the memo table.
pub enum MemoNode {
    Relation(Relation),
    Scalar(Scalar),
}

/// A type representing a tree of logical nodes, scalar nodes, and group IDs.
///
/// Note that group IDs must be leaves of this tree.
///
/// TODO Make this an actual tree with the correct modeling of types.
pub struct PartialLogicalExpression;

/// A type representing a tree of logical nodes, physical nodes, scalar nodes, and group IDs.
///
/// Note that group IDs must be leaves of this tree, and that physical nodes cannot have children
/// that are logical nodes.
///
/// TODO Make this an actual tree with the correct modeling of types.
pub struct PartialPhysicalExpression;

pub struct Memo;

impl Memo {
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

    pub fn apply_transformation(&mut self, expr: PartialLogicalExpression, rule: Rule) -> Vec<MemoNode> {
        todo!()
    }

    pub fn apply_implementation(&mut self, expr: PartialPhysicalExpression, rule: Rule) -> Vec<MemoNode> {
        todo!()
    }

    pub fn add_expressions(&mut self, new_exprs: Vec<MemoNode>) {
        todo!()
    }
}
