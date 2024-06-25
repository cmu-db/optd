use crate::plan_nodes::{ColumnRefExpr, Expr, ExprList, OptRelNode};

pub fn merge_exprs(first: ExprList, second: ExprList) -> ExprList {
    let mut res_vec = first.to_vec();
    res_vec.extend(second.to_vec());
    ExprList::new(res_vec)
}

pub fn split_exprs(exprs: ExprList, left_schema_len: usize) -> (ExprList, ExprList, bool) {
    let mut left_vec = vec![];
    let mut right_vec = vec![];
    let mut reached_right = false;
    let mut is_left_right_ordered = true;
    for expr in exprs.to_vec() {
        let col_ref = ColumnRefExpr::from_rel_node(expr.into_rel_node()).unwrap();
        if col_ref.index() < left_schema_len {
            // left expr
            left_vec.push(col_ref.into_expr());
            if reached_right {
                is_left_right_ordered = false;
            }
        } else {
            // right expr
            let right_col_ref = ColumnRefExpr::new(col_ref.index() - left_schema_len);
            right_vec.push(right_col_ref.into_expr());
            reached_right = true;
        }
    }
    (ExprList::new(left_vec), ExprList::new(right_vec), is_left_right_ordered)
}

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
#[derive(Clone, Debug)]
pub struct ProjectionMapping {
    forward: Vec<usize>,
    backward: Vec<Option<usize>>,
}

impl ProjectionMapping {
    // forward vec is mapped output schema -> col refs
    // backward vec is mapped col refs -> output schema
    pub fn build(exprs: &ExprList) -> Option<Self> {
        let mut forward = vec![];
        let mut backward = vec![];
        for (i, expr) in exprs.to_vec().iter().enumerate() {
            let col_expr = ColumnRefExpr::from_rel_node(expr.clone().into_rel_node())?;
            let col_idx = col_expr.index();
            forward.push(col_idx);
            if col_idx >= backward.len() {
                backward.resize(col_idx + 1, None);
            }
            backward[col_idx] = Some(i);
        }
        Some(Self { forward, backward })
    }

    pub fn projection_col_maps_to(&self, col_idx: usize) -> Option<usize> {
        self.forward.get(col_idx).copied()
    }

    pub fn original_col_maps_to(&self, col_idx: usize) -> Option<usize> {
        self.backward.get(col_idx).copied().flatten()
    }

    /// Remaps all column refs in the join condition based on a
    /// removed bottom projection node on the left child
    ///
    /// removed node:
    /// Join { cond: #0=#5 }
    ///      Projection { exprs: [#1, #0, #3, #5, #4] } --> has mapping
    ///         Scan
    ///      Scan
    /// ---->
    /// Join { cond: #1=#4 }
    ///     Scan
    ///     Scan
    pub fn rewrite_join_cond(&self, cond: Expr, left_schema_len: usize, is_added: bool, is_left_child: bool, new_left_schema_len: usize) -> Expr {
        if is_added {
            cond.rewrite_column_refs(&|col_idx| {
                if is_left_child && col_idx < left_schema_len {
                    self.original_col_maps_to(col_idx)
                } else if !is_left_child && col_idx >= left_schema_len {
                    Some(self.original_col_maps_to(col_idx - left_schema_len).unwrap() + new_left_schema_len)
                } else {
                    Some(col_idx)
                }
            })
            .unwrap()
        } else {
            let schema_size = self.forward.len();
            cond.rewrite_column_refs(&|col_idx| {
                if col_idx < schema_size {
                    self.projection_col_maps_to(col_idx)
                } else {
                    Some(col_idx - schema_size + left_schema_len)
                }
            })
            .unwrap()    
        }
    }

    /// Remaps all column refs in the filter condition based on an added or
    /// removed bottom projection node
    ///
    /// added node:
    /// Filter { cond: #1=0 and #4=1 }
    /// ---->
    /// Filter { cond: #0=0 and #5=1 }
    ///      Projection { exprs: [#1, #0, #3, #5, #4] } --> has mapping
    ///
    /// removed node:
    /// Filter { cond: #0=0 and #5=1 }
    ///      Projection { exprs: [#1, #0, #3, #5, #4] } --> has mapping
    /// ---->
    /// Filter { cond: #1=0 and #4=1 }
    pub fn rewrite_filter_cond(&self, cond: Expr, is_added: bool) -> Expr {
        cond.rewrite_column_refs(&|col_idx| {
            if is_added {
                self.original_col_maps_to(col_idx)
            } else {
                self.projection_col_maps_to(col_idx)
            }
        })
        .unwrap()
    }

    /// If the top projection node is mapped, rewrites the bottom projection's
    /// exprs based on the top projection's mapped col refs.
    ///
    /// If the bottom projection node is mapped, rewrites the top projection's
    /// exprs based on the bottom projection's mapped col refs.
    ///
    /// Projection { exprs: [#1, #0] }
    ///     Projection { exprs: [#0, #2] }
    /// ---->
    /// Projection { exprs: [#2, #0] }
    pub fn rewrite_projection(&self, exprs: &ExprList, is_top_mapped: bool) -> Option<ExprList> {
        if exprs.is_empty() {
            return None;
        }
        let mut new_projection_exprs = Vec::new();
        if is_top_mapped {
            let exprs = exprs.to_vec();
            for i in 0..self.forward.len() {
                let col_idx = self.projection_col_maps_to(i).unwrap();
                new_projection_exprs.push(exprs[col_idx].clone());
            }
        } else {
            for i in exprs.to_vec() {
                let col_ref = ColumnRefExpr::from_rel_node(i.into_rel_node()).unwrap();
                let col_idx = self.original_col_maps_to(col_ref.index()).unwrap();
                let col: Expr = ColumnRefExpr::new(col_idx).into_expr();
                new_projection_exprs.push(col);
            }
        }
        Some(ExprList::new(new_projection_exprs))
    }
}
