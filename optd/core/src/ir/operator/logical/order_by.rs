use std::sync::Arc;

use bitvec::{boxed::BitBox, vec::BitVec};
use itertools::Itertools;

use crate::ir::{
    Column, IRCommon, Operator, Scalar,
    macros::define_node,
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

#[cfg(test)]
mod tests {
    use crate::ir::{
        ColumnSet, ScalarValue,
        convert::{IntoOperator, IntoScalar},
        operator::{MockScan, MockSpec},
        scalar::*,
    };

    use super::*;

    #[test]
    fn try_extract_tuple_ordering_success() {
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
        let order_by = LogicalOrderBy::new(
            MockScan::with_mock_spec(1, MockSpec::new_test_only(vec![0, 1, 2], 100.))
                .into_operator(),
            ordered_exprs,
        );

        let res = order_by
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
        let order_by = LogicalOrderBy::new(
            MockScan::with_mock_spec(1, MockSpec::new_test_only(vec![0, 1, 2], 100.))
                .into_operator(),
            ordered_exprs,
        );

        let res = order_by.try_extract_tuple_ordering().unwrap_err();

        assert_eq!(res.len(), 1);
        assert!(res[0].try_borrow::<BinaryOp>().is_ok())
    }
}
