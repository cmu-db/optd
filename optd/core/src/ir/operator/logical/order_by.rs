//! The logical order by operator sorts incoming data based on specified
//! expressions and directions.

use crate::ir::{
    Column, IRCommon, Operator, Scalar,
    explain::Explain,
    macros::{define_node, impl_operator_conversion},
    properties::{OperatorProperties, TupleOrdering, TupleOrderingDirection},
    scalar::ColumnRef,
};
use bitvec::{boxed::BitBox, vec::BitVec};
use itertools::Itertools;
use pretty_xmlish::Pretty;
use std::sync::Arc;

define_node!(
    /// Metadata:
    /// - directions: A bit vector indicating the ordering direction for each
    ///               expression (true for ascending, false for descending).
    /// Scalars:
    /// - exprs: The expression array to order by.
    OrderBy, OrderByBorrowed {
        properties: OperatorProperties,
        metadata: OrderByMetadata {
            directions: BitBox,
        },
        inputs: {
            operators: [input],
            scalars: exprs[],
        }
    }
);
impl_operator_conversion!(OrderBy, OrderByBorrowed);

impl OrderBy {
    pub fn new(
        input: Arc<Operator>,
        ordered_exprs: Vec<(Arc<Scalar>, TupleOrderingDirection)>,
    ) -> Self {
        let (exprs, directions): (Vec<Arc<Scalar>>, BitVec) = ordered_exprs
            .into_iter()
            .map(|(expr, direction)| (expr, direction == TupleOrderingDirection::Asc))
            .unzip();
        Self {
            meta: OrderByMetadata {
                directions: directions.into_boxed_bitslice(),
            },
            common: IRCommon::new(Arc::new([input]), exprs.into()),
        }
    }
}

impl OrderByBorrowed<'_> {
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

impl Explain for OrderByBorrowed<'_> {
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

        Pretty::simple_record("OrderBy", fields, children)
    }
}

#[cfg(test)]
mod tests {
    use crate::ir::{
        ColumnSet, ScalarValue,
        convert::{IntoOperator, IntoScalar},
        scalar::*,
        table_ref::TableRef,
        test_utils::{test_col, test_ctx_with_tables},
    };

    use super::*;

    #[test]
    fn try_extract_tuple_ordering_success() -> crate::error::Result<()> {
        let ctx = test_ctx_with_tables(&[("t1", 3)])?;
        let input = ctx.logical_get(TableRef::bare("t1"), None)?.build();
        let c0 = test_col(&ctx, "t1", "c0")?;
        let c1 = test_col(&ctx, "t1", "c1")?;
        let c2 = test_col(&ctx, "t1", "c2")?;
        let ordered_exprs = vec![
            (
                ColumnRef::new(c0).into_scalar(),
                TupleOrderingDirection::Asc,
            ),
            (
                ColumnRef::new(c2).into_scalar(),
                TupleOrderingDirection::Asc,
            ),
        ];
        let order_by = OrderBy::new(input, ordered_exprs).into_operator();

        let res = order_by
            .borrow::<OrderBy>()
            .try_extract_tuple_ordering()
            .unwrap()
            .iter_columns()
            .cloned()
            .collect::<ColumnSet>();

        assert!(res.contains(&c0));
        assert!(res.contains(&c2));
        assert!(!res.contains(&c1));
        Ok(())
    }

    #[test]
    fn try_extract_tuple_ordering_error() -> crate::error::Result<()> {
        let ctx = test_ctx_with_tables(&[("t1", 3)])?;
        let input = ctx.logical_get(TableRef::bare("t1"), None)?.build();
        let c0 = test_col(&ctx, "t1", "c0")?;
        let c2 = test_col(&ctx, "t1", "c2")?;
        let ordered_exprs = vec![
            (
                ColumnRef::new(c0).into_scalar(),
                TupleOrderingDirection::Asc,
            ),
            (
                BinaryOp::new(
                    BinaryOpKind::Plus,
                    ColumnRef::new(c2).into_scalar(),
                    Literal::new(ScalarValue::Int32(Some(1))).into_scalar(),
                )
                .into_scalar(),
                TupleOrderingDirection::Asc,
            ),
        ];

        let order_by = OrderBy::new(input, ordered_exprs).into_operator();

        let res = order_by
            .borrow::<OrderBy>()
            .try_extract_tuple_ordering()
            .unwrap_err();

        assert_eq!(res.len(), 1);
        assert!(res[0].try_borrow::<BinaryOp>().is_ok());
        Ok(())
    }
}
