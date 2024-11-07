// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

mod between_pred;
mod bin_op_pred;
mod cast_pred;
mod column_ref_pred;
mod constant_pred;
mod data_type_pred;
mod extern_column_ref_pred;
mod func_pred;
mod in_list_pred;
mod like_pred;
mod list_pred;
mod log_op_pred;
mod sort_order_pred;
mod un_op_pred;

use std::sync::Arc;

pub use between_pred::BetweenPred;
pub use bin_op_pred::{BinOpPred, BinOpType};
pub use cast_pred::CastPred;
pub use column_ref_pred::ColumnRefPred;
pub use constant_pred::{ConstantPred, ConstantType};
pub use data_type_pred::DataTypePred;
pub use extern_column_ref_pred::ExternColumnRefPred;
pub use func_pred::{FuncPred, FuncType};
pub use in_list_pred::InListPred;
use itertools::Itertools;
pub use like_pred::LikePred;
pub use list_pred::ListPred;
pub use log_op_pred::{LogOpPred, LogOpType};
use optd_core::nodes::PredNode;
pub use sort_order_pred::{SortOrderPred, SortOrderType};
pub use un_op_pred::{UnOpPred, UnOpType};

use super::DfReprPredNode;

pub trait PredExt {
    /// Recursively rewrite all column references in the expression.using a provided
    /// function that replaces a column index.
    /// The provided function will, given a ColumnRefExpr's index,
    /// return either Some(usize) or None.
    /// - If it is Some, the column index can be rewritten with the value.
    /// - If any of the columns is None, we will return None all the way up
    ///
    /// the call stack, and no expression will be returned.
    fn rewrite_column_refs(&self, rewrite_fn: impl FnMut(usize) -> Option<usize>) -> Option<Self>
    where
        Self: Sized;

    fn get_column_refs(&self) -> Vec<ColumnRefPred>;
}

impl<P: DfReprPredNode> PredExt for P {
    fn rewrite_column_refs(
        &self,
        mut rewrite_fn: impl FnMut(usize) -> Option<usize>,
    ) -> Option<Self> {
        rewrite_column_refs_inner(self, &mut rewrite_fn)
    }

    fn get_column_refs(&self) -> Vec<ColumnRefPred> {
        if let Some(col_ref) = ColumnRefPred::from_pred_node(self.clone().into_pred_node()) {
            return vec![col_ref];
        }
        let node = self.clone().into_pred_node();
        let children = node.children.iter().map(|child| child.get_column_refs());
        children.collect_vec().concat()
    }
}

fn rewrite_column_refs_inner<P: DfReprPredNode>(
    expr: &P,
    rewrite_fn: &mut impl FnMut(usize) -> Option<usize>,
) -> Option<P> {
    let expr = expr.clone().into_pred_node();
    if let Some(col_ref) = ColumnRefPred::from_pred_node(expr.clone()) {
        let rewritten = rewrite_fn(col_ref.index())?;
        let new_col_ref = ColumnRefPred::new(rewritten);
        return Some(P::from_pred_node(new_col_ref.into_pred_node()).expect("unmatched type"));
    }
    let children = expr
        .children
        .iter()
        .map(|child| rewrite_column_refs_inner(child, rewrite_fn))
        .collect::<Option<Vec<_>>>()?;
    Some(
        P::from_pred_node(Arc::new(PredNode {
            typ: expr.typ.clone(),
            children,
            data: expr.data.clone(),
        }))
        .expect("unmatched type"),
    )
}
