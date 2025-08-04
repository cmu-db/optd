use std::sync::Arc;

use bitvec::{boxed::BitBox, vec::BitVec};

use crate::ir::operator::*;
use crate::ir::{
    Column, Operator, OperatorCategory, OperatorKind,
    properties::{GetProperty, OutputColumns},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TupleOrderingDirection {
    Asc,
    Desc,
}

impl std::fmt::Display for TupleOrderingDirection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TupleOrderingDirection::Asc => write!(f, "ASC"),
            TupleOrderingDirection::Desc => write!(f, "DESC"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct TupleOrdering(Arc<TupleOrderingInner>);

impl PartialOrd for TupleOrdering {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        let self_len = self.0.columns.len();
        let other_len = other.0.columns.len();
        let min_len = self_len.min(other_len);

        // Check if the first min_len elements are the same
        if self.0.columns[..min_len].eq(&other.0.columns[..min_len])
            && self.0.directions[..min_len].eq(&other.0.directions[..min_len])
        {
            // One is a prefix of the other
            self_len.partial_cmp(&other_len)
        } else {
            // Neither is a prefix of the other, they're incomparable
            None
        }
    }
}

impl std::fmt::Display for TupleOrdering {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
struct TupleOrderingInner {
    /// The specified columns to be ordered on.
    columns: Box<[Column]>,
    /// The ordering directions (asc/desc).
    /// ASC is marked as `true`, DESC is marked as `false`.
    /// TODO(yuchen): consider NULL FIRST/LAST.
    directions: BitBox,
}

impl FromIterator<(Column, TupleOrderingDirection)> for TupleOrdering {
    fn from_iter<T: IntoIterator<Item = (Column, TupleOrderingDirection)>>(iter: T) -> Self {
        let mut columns = Vec::new();
        let mut directions = BitVec::new();
        iter.into_iter().for_each(|(column, direction)| {
            columns.push(column);
            directions.push(direction == TupleOrderingDirection::Asc);
        });
        let inner = TupleOrderingInner {
            columns: columns.into_boxed_slice(),
            directions: directions.into_boxed_bitslice(),
        };

        Self(Arc::new(inner))
    }
}

impl TupleOrdering {
    pub fn new(columns: Box<[Column]>, directions: BitBox) -> Self {
        assert_eq!(columns.len(), directions.len());
        Self(Arc::new(TupleOrderingInner {
            columns,
            directions,
        }))
    }

    /// Gets an ordered iterator over [`Column`], [`TupleOrderingDirection`] pairs.
    pub fn iter<'a>(&'a self) -> Iter<'a> {
        Iter {
            index: 0,
            ordering: self,
        }
    }

    /// Gets an ordered iterator over [`Column`] specified in the ordering.
    pub fn iter_columns<'a>(&'a self) -> impl Iterator<Item = &'a Column> {
        self.0.columns.iter()
    }

    pub fn is_empty(&self) -> bool {
        self.0.columns.is_empty()
    }
}

pub struct Iter<'a> {
    index: usize,
    ordering: &'a TupleOrdering,
}

impl<'a> Iterator for Iter<'a> {
    type Item = (&'a Column, TupleOrderingDirection);

    fn next(&mut self) -> Option<Self::Item> {
        let column = self.ordering.0.columns.get(self.index)?;
        let is_asc = self
            .ordering
            .0
            .directions
            .get(self.index)
            .expect("directions is same length as columns");

        let direction = is_asc
            .then(|| TupleOrderingDirection::Asc)
            .unwrap_or(TupleOrderingDirection::Desc);

        Some((column, direction))
    }
}

impl crate::ir::properties::PropertyMarker for TupleOrdering {}

impl crate::ir::properties::TrySatisfy<TupleOrdering> for Operator {
    fn try_satisfy(
        &self,
        ordering: &TupleOrdering,
        ctx: &crate::ir::context::IRContext,
    ) -> Option<std::sync::Arc<[TupleOrdering]>> {
        match &self.kind {
            OperatorKind::Group(_) => None,
            OperatorKind::LogicalGet(_)
            | OperatorKind::LogicalJoin(_)
            | OperatorKind::LogicalSelect(_) => {
                assert_eq!(self.kind.category(), OperatorCategory::Logical);
                None
            }
            OperatorKind::EnforcerSort(meta) => {
                (&meta.tuple_ordering >= ordering).then(|| vec![TupleOrdering::default(); 1].into())
            }
            OperatorKind::PhysicalTableScan(_) => {
                // TODO(yuchen): if we can obtain ordering information from catalog, then we can use that.
                ordering.is_empty().then_some(Arc::new([]))
            }
            OperatorKind::PhysicalNLJoin(meta) => {
                let join = PhysicalNLJoinBorrowed::from_raw_parts(meta, &self.common);

                let output_from_outer = join.outer().get_property::<OutputColumns>(ctx);
                ordering
                    .iter_columns()
                    .all(|col| output_from_outer.set().contains(col))
                    .then(|| vec![ordering.clone(), TupleOrdering::default()].into())
            }
            OperatorKind::PhysicalFilter(meta) => {
                let filter = PhysicalFilterBorrowed::from_raw_parts(meta, &self.common);
                let output_from_input = filter.input().get_property::<OutputColumns>(ctx);
                ordering
                    .iter_columns()
                    .all(|col| output_from_input.set().contains(col))
                    .then(|| vec![ordering.clone()].into())
            }
            #[cfg(test)]
            OperatorKind::MockScan(meta) => {
                (&meta.spec.mocked_provided_ordering >= ordering).then_some(Arc::new([]))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::{
        IRContext, ScalarValue,
        convert::{IntoOperator, IntoScalar},
        operator::join::JoinType,
        properties::TrySatisfy,
        scalar::*,
    };

    #[test]
    fn enforcer_sort_try_satisify_ordering() {
        let ctx = IRContext::with_empty_magic();
        let required = TupleOrdering::from_iter([
            (Column(0), TupleOrderingDirection::Asc),
            (Column(1), TupleOrderingDirection::Desc),
        ]);

        let t1 = MockScan::with_mock_spec(1, MockSpec::new_test_only(vec![0, 1, 2], 100.))
            .into_operator();
        let exact_enforcer = EnforcerSort::new(required.clone(), t1.clone()).into_operator();
        let res = exact_enforcer.try_satisfy(&required, &ctx).unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res[0], TupleOrdering::default());

        let stronger_provided = TupleOrdering::from_iter([
            (Column(0), TupleOrderingDirection::Asc),
            (Column(1), TupleOrderingDirection::Desc),
            (Column(2), TupleOrderingDirection::Asc),
        ]);
        let stronger_enforcer = EnforcerSort::new(stronger_provided, t1.clone()).into_operator();
        let res = stronger_enforcer.try_satisfy(&required, &ctx).unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res[0], TupleOrdering::default());

        let weaker_provided = TupleOrdering::from_iter([(Column(0), TupleOrderingDirection::Asc)]);
        let weaker_enforcer = EnforcerSort::new(weaker_provided, t1.clone()).into_operator();
        assert_eq!(None, weaker_enforcer.try_satisfy(&required, &ctx));
    }

    #[test]
    fn physical_nljoin_try_satisfy_ordering() {
        let ctx = IRContext::with_empty_magic();
        let m1 =
            MockScan::with_mock_spec(1, MockSpec::new_test_only(vec![0, 1], 100.)).into_operator();
        let m2 =
            MockScan::with_mock_spec(2, MockSpec::new_test_only(vec![2, 3], 100.)).into_operator();
        let join_cond = Literal::new(ScalarValue::Boolean(Some(true))).into_scalar();
        let join1 = PhysicalNLJoin::new(JoinType::Inner, m1.clone(), m2.clone(), join_cond.clone())
            .into_operator();
        let join2 = join1.clone_with_inputs(Some(Arc::new([m2, m1])), None);

        // Both satisfy the empty ordering.
        let empty = TupleOrdering::default();
        let res = join1.try_satisfy(&empty, &ctx).unwrap();
        assert_eq!(res[0], empty);
        assert_eq!(res[1], empty);

        let res = join1.try_satisfy(&empty, &ctx).unwrap();
        assert_eq!(res[0], empty);
        assert_eq!(res[1], empty);

        let ordering_from_t1 = TupleOrdering::from_iter([
            (Column(0), TupleOrderingDirection::Asc),
            (Column(1), TupleOrderingDirection::Desc),
        ]);

        // Only satisifies when t1 is the outer side.
        let res = join1.try_satisfy(&ordering_from_t1, &ctx).unwrap();
        assert_eq!(res[0], ordering_from_t1);
        assert_eq!(res[1], empty);

        assert_eq!(None, join2.try_satisfy(&ordering_from_t1, &ctx));

        // Both should not satisfy.
        let columns_from_both_sides = TupleOrdering::from_iter([
            (Column(0), TupleOrderingDirection::Asc),
            (Column(2), TupleOrderingDirection::Desc),
        ]);
        assert_eq!(None, join1.try_satisfy(&columns_from_both_sides, &ctx));
        assert_eq!(None, join2.try_satisfy(&columns_from_both_sides, &ctx));
    }

    #[test]
    fn physical_table_scan_try_satisfy_ordering() {
        let ctx = IRContext::with_course_tables();
        let t1 = PhysicalTableScan::mock(vec![0, 1]).into_operator();

        // Satisfies empty ordering
        let ordering = TupleOrdering::default();
        let res = t1.try_satisfy(&ordering, &ctx).unwrap();
        assert_eq!(res.len(), 0);

        // Does not satisfies the ordering [0, 1]. (for detecting regressions).
        let ordering = TupleOrdering::from_iter([
            (Column(0), TupleOrderingDirection::Asc),
            (Column(1), TupleOrderingDirection::Asc),
        ]);
        assert_eq!(None, t1.try_satisfy(&ordering, &ctx));
    }
}
