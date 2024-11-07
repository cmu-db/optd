// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

use crate::plan_nodes::{ArcDfPredNode, ColumnRefPred, DfReprPredNode, ListPred, PredExt};

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
    pub fn build(exprs: &ListPred) -> Option<Self> {
        let mut forward = vec![];
        let mut backward = vec![];
        for (i, expr) in exprs.to_vec().iter().enumerate() {
            let col_expr = ColumnRefPred::from_pred_node(expr.clone())?;
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
    /// removed bottom projection node
    ///
    /// removed node:
    /// Join { cond: #0=#5 }
    ///      Projection { exprs: [#1, #0, #3, #5, #4] } --> has mapping
    /// ---->
    /// Join { cond: #1=#4 }
    pub fn rewrite_join_cond(&self, cond: ArcDfPredNode, child_schema_len: usize) -> ArcDfPredNode {
        let schema_size = self.forward.len();
        cond.rewrite_column_refs(|col_idx| {
            if col_idx < schema_size {
                self.projection_col_maps_to(col_idx)
            } else {
                Some(col_idx - schema_size + child_schema_len)
            }
        })
        .unwrap()
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
    pub fn rewrite_filter_cond(&self, cond: ArcDfPredNode, is_added: bool) -> ArcDfPredNode {
        cond.rewrite_column_refs(&mut |col_idx| {
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
    pub fn rewrite_projection(&self, exprs: &ListPred, is_top_mapped: bool) -> Option<ListPred> {
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
                let col_ref = ColumnRefPred::from_pred_node(i).unwrap();
                let col_idx = self.original_col_maps_to(col_ref.index()).unwrap();
                let col = ColumnRefPred::new(col_idx);
                new_projection_exprs.push(col.into_pred_node());
            }
        }
        Some(ListPred::new(new_projection_exprs))
    }
}
