use super::expr::ExprList;
use super::macros::define_plan_node;

use super::{ColumnRefExpr, Expr, OptRelNode, OptRelNodeRef, OptRelNodeTyp, PlanNode};

#[derive(Clone, Debug)]
pub struct LogicalProjection(pub PlanNode);

define_plan_node!(
    LogicalProjection : PlanNode,
    Projection, [
        { 0, child: PlanNode }
    ], [
        { 1, exprs: ExprList }
    ]
);

#[derive(Clone, Debug)]
pub struct PhysicalProjection(pub PlanNode);

define_plan_node!(
    PhysicalProjection : PlanNode,
    PhysicalProjection, [
        { 0, child: PlanNode }
    ], [
        { 1, exprs: ExprList }
    ]
);

/// This struct holds the mapping from original columns to projected columns.
///
/// # Example
/// With the following plan:
///  | Filter (#0 < 5)
///  |
///  |-| Projection [#2, #3]
///    |- Scan [#0, #1, #2, #3]
///
/// The computed projection mapping is:
/// #2 -> #0
/// #3 -> #1
pub struct ProjectionMapping {
    forward: Vec<usize>,
    _backward: Vec<Option<usize>>,
}

impl ProjectionMapping {
    pub fn build(mapping: Vec<usize>) -> Option<Self> {
        let mut backward = vec![];
        for (i, &x) in mapping.iter().enumerate() {
            if x >= backward.len() {
                backward.resize(x + 1, None);
            }
            backward[x] = Some(i);
        }
        Some(Self {
            forward: mapping,
            _backward: backward,
        })
    }

    pub fn projection_col_refers_to(&self, col: usize) -> usize {
        self.forward[col]
    }

    pub fn _original_col_maps_to(&self, col: usize) -> Option<usize> {
        self._backward[col]
    }

    /// Recursively rewrites all ColumnRefs in an Expr to *undo* the projection
    /// condition. You might want to do this if you are pushing something
    /// through a projection, or pulling a projection up.
    ///
    /// # Example
    /// If we have a projection node, mapping column A to column B (A -> B)
    /// All B's in `cond` will be rewritten as A.
    pub fn rewrite_condition(&self, cond: Expr, child_schema_len: usize) -> Expr {
        let proj_schema_size = self.forward.len();
        cond.rewrite_column_refs(&|idx| {
            Some(if idx < proj_schema_size {
                self.projection_col_refers_to(idx)
            } else {
                idx - proj_schema_size + child_schema_len
            })
        })
        .unwrap()
    }
}

impl LogicalProjection {
    pub fn compute_column_mapping(exprs: &ExprList) -> Option<ProjectionMapping> {
        let mut mapping = vec![];
        for expr in exprs.to_vec() {
            let col_expr = ColumnRefExpr::from_rel_node(expr.into_rel_node())?;
            mapping.push(col_expr.index());
        }
        ProjectionMapping::build(mapping)
    }
}
