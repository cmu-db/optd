//! The logical order by operator sorts incoming data based on specified 
//! expressions and directions.

use std::sync::Arc;
use bitvec::{boxed::BitBox, vec::BitVec};
use itertools::Itertools;
use pretty_xmlish::Pretty;
use crate::ir::{
    Column, IRCommon, Operator, Scalar,
    explain::Explain,
    macros::{define_node, impl_operator_conversion},
    properties::{OperatorProperties, TupleOrdering, TupleOrderingDirection},
    scalar::ColumnRef,
};

define_node!(
    LogicalOrderBy, LogicalOrderByBorrowed {
        properties: OperatorProperties,
        metadata: LogicalOrderByMetadata {
            directions: BitBox,
        },
        inputs: {
            operators: [input],
            scalars: exprs[],
        }
    }
);
impl_operator_conversion!(LogicalOrderBy, LogicalOrderByBorrowed);

/// Metadata:
/// - directions: A bit vector indicating the ordering direction for each 
///               expression (true for ascending, false for descending).
/// Scalars: 
/// - exprs: The expression array to order by.
impl LogicalOrderBy {
    pub fn new(
        input: Arc<Operator>,
        ordered_exprs: Vec<(Arc<Scalar>, TupleOrderingDirection)>,
    ) -> Self {
        let (exprs, directions): (Vec<Arc<Scalar>>, BitVec) = ordered_exprs
            .into_iter()
            .map(|(expr, direction)| (expr, direction == TupleOrderingDirection::Asc))
            .unzip();
        Self {
            meta: LogicalOrderByMetadata {
                directions: directions.into_boxed_bitslice(),
            },
            common: IRCommon::new(Arc::new([input]), exprs.into()),
        }
    }
}

impl LogicalOrderByBorrowed<'_> {
    /// Try extracts the associated tuple ordering if all the ordered exprs are of type [`ColumnRef`].
    /// On error, returns a list of all scalar expressions that are not [`ColumnRef`].
    pub fn try_extract_tuple_ordering(&self) -> Result<TupleOrdering, Vec<Arc<Scalar>>> {
        let (columns, non_column_refs): (Vec<Column>, Vec<Arc<Scalar>>) = self
            .exprs()
            .iter()
            .map(|expr| {
                expr.try_borrow::<ColumnRef>()
                    .map(|column_ref| *column_ref.column())
                    .map_err(|_| expr.clone())
            })
            .partition_result();

        if non_column_refs.is_empty() {
            Ok(TupleOrdering::new(
                columns.into_boxed_slice(),
                self.directions().clone(),
            ))
        } else {
            Err(non_column_refs)
        }
    }
}

impl Explain for LogicalOrderByBorrowed<'_> {
    fn explain<'a>(
        &self,
        ctx: &crate::ir::IRContext,
        option: &crate::ir::explain::ExplainOption,
    ) -> Pretty<'a> {
        let mut fields = Vec::with_capacity(1);

        let ordering_exprs = self
            .exprs()
            .iter()
            .zip(self.directions().iter())
            .map(|(expr, is_asc)| {
                let expr = expr.explain(ctx, option).to_one_line_string(true);
                Pretty::Text(
                    format!("{expr} {}", TupleOrderingDirection::from_bool(*is_asc)).into(),
                )
            })
            .collect();
        fields.push(("ordering_exprs", Pretty::Array(ordering_exprs)));

        let metadata = self.common.explain_operator_properties(ctx, option);
        fields.extend(metadata);
        let children = self.common.explain_input_operators(ctx, option);

        Pretty::simple_record("LogicalOrderBy", fields, children)
    }
}

#[cfg(test)]
mod tests {
    use crate::ir::{
        ColumnSet, IRContext, ScalarValue,
        convert::{IntoOperator, IntoScalar},
        scalar::*,
    };

    use super::*;

    #[test]
    fn try_extract_tuple_ordering_success() {
        let ctx = IRContext::with_empty_magic();
        let ordered_exprs = vec![
            (
                ColumnRef::new(Column(0)).into_scalar(),
                TupleOrderingDirection::Asc,
            ),
            (
                ColumnRef::new(Column(2)).into_scalar(),
                TupleOrderingDirection::Asc,
            ),
        ];
        let order_by = LogicalOrderBy::new(ctx.mock_scan(1, vec![0, 1, 2], 100.), ordered_exprs)
            .into_operator();

        let res = order_by
            .borrow::<LogicalOrderBy>()
            .try_extract_tuple_ordering()
            .unwrap()
            .iter_columns()
            .cloned()
            .collect::<ColumnSet>();

        assert!(res.contains(&Column(0)));
        assert!(res.contains(&Column(2)));
        assert!(!res.contains(&Column(1)));
    }

    #[test]
    fn try_extract_tuple_ordering_error() {
        let ctx = IRContext::with_empty_magic();
        let ordered_exprs = vec![
            (
                ColumnRef::new(Column(0)).into_scalar(),
                TupleOrderingDirection::Asc,
            ),
            (
                BinaryOp::new(
                    BinaryOpKind::Plus,
                    ColumnRef::new(Column(2)).into_scalar(),
                    Literal::new(ScalarValue::Int32(Some(1))).into_scalar(),
                )
                .into_scalar(),
                TupleOrderingDirection::Asc,
            ),
        ];

        let order_by = LogicalOrderBy::new(ctx.mock_scan(1, vec![0, 1, 2], 100.), ordered_exprs)
            .into_operator();

        let res = order_by
            .borrow::<LogicalOrderBy>()
            .try_extract_tuple_ordering()
            .unwrap_err();

        assert_eq!(res.len(), 1);
        assert!(res[0].try_borrow::<BinaryOp>().is_ok())
    }
}
