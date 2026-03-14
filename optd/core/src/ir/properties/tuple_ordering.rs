//! This module defines the `TupleOrdering` property for operators in the IR,
//! which specifies the ordering of tuples based on specified columns and their
//! directions (ascending/descending).

use crate::error::Result;
use crate::ir::operator::*;
use crate::ir::{Column, Operator, OperatorCategory, OperatorKind};
use bitvec::{boxed::BitBox, vec::BitVec};
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TupleOrderingDirection {
    Asc,
    Desc,
}

impl TupleOrderingDirection {
    pub const fn from_bool(is_asc: bool) -> Self {
        match is_asc {
            true => TupleOrderingDirection::Asc,
            false => TupleOrderingDirection::Desc,
        }
    }

    pub fn is_asc(&self) -> bool {
        matches!(self, TupleOrderingDirection::Asc)
    }
}

impl std::fmt::Display for TupleOrderingDirection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TupleOrderingDirection::Asc => write!(f, "ASC"),
            TupleOrderingDirection::Desc => write!(f, "DESC"),
        }
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Default)]
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

impl std::fmt::Debug for TupleOrdering {
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
    pub fn iter_columns(&self) -> impl Iterator<Item = &Column> {
        self.0.columns.iter()
    }

    pub fn is_empty(&self) -> bool {
        self.0.columns.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.columns.len()
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

        self.index += 1;
        Some((column, direction))
    }
}

impl crate::ir::properties::PropertyMarker for TupleOrdering {
    type Output = Self;
}

fn satisfy_scan_ordering(ordering: &TupleOrdering) -> Option<Arc<[TupleOrdering]>> {
    // TODO(yuchen): if we can obtain ordering information from catalog, then we can use that.
    ordering
        .is_empty()
        .then_some(Arc::<[TupleOrdering]>::from(Vec::new()))
}

fn satisfy_passthrough_ordering(
    input: &Operator,
    ordering: &TupleOrdering,
    ctx: &crate::ir::context::IRContext,
) -> Result<Option<Arc<[TupleOrdering]>>> {
    let output_from_input = input.output_columns(ctx)?;
    Ok(ordering
        .iter_columns()
        .all(|col| output_from_input.contains(col))
        .then(|| Arc::<[TupleOrdering]>::from(vec![ordering.clone()])))
}

fn satisfy_nl_join_ordering(
    outer: &Operator,
    ordering: &TupleOrdering,
    ctx: &crate::ir::context::IRContext,
) -> Result<Option<Arc<[TupleOrdering]>>> {
    let output_from_outer = outer.output_columns(ctx)?;
    Ok(ordering
        .iter_columns()
        .all(|col| output_from_outer.contains(col))
        .then(|| Arc::<[TupleOrdering]>::from(vec![ordering.clone(), TupleOrdering::default()])))
}

fn satisfy_aggregate_ordering(ordering: &TupleOrdering) -> Option<Arc<[TupleOrdering]>> {
    // Hash aggregate does not maintain tuple ordering.
    ordering
        .is_empty()
        .then(|| Arc::<[TupleOrdering]>::from(vec![ordering.clone()]))
}

impl crate::ir::properties::TrySatisfy<TupleOrdering> for Operator {
    fn try_satisfy(
        &self,
        ordering: &TupleOrdering,
        ctx: &crate::ir::context::IRContext,
    ) -> Result<Option<std::sync::Arc<[TupleOrdering]>>> {
        let satisfied = match &self.kind {
            OperatorKind::Group(_) => None,
            OperatorKind::Get(_) => satisfy_scan_ordering(ordering),
            OperatorKind::Join(meta) => {
                let join = Join::borrow_raw_parts(meta, &self.common);
                match join.implementation() {
                    Some(JoinImplementation::Hash(_)) => ordering
                        .is_empty()
                        .then(|| Arc::<[TupleOrdering]>::from(vec![ordering.clone(); 2])),
                    _ => satisfy_nl_join_ordering(join.outer(), ordering, ctx)?,
                }
            }
            OperatorKind::LogicalDependentJoin(meta) => {
                let join = LogicalDependentJoin::borrow_raw_parts(meta, &self.common);
                satisfy_nl_join_ordering(join.outer(), ordering, ctx)?
            }
            OperatorKind::LogicalProject(meta) => {
                let project = LogicalProject::borrow_raw_parts(meta, &self.common);
                satisfy_passthrough_ordering(project.input(), ordering, ctx)?
            }
            OperatorKind::LogicalSelect(meta) => {
                let select = LogicalSelect::borrow_raw_parts(meta, &self.common);
                satisfy_passthrough_ordering(select.input(), ordering, ctx)?
            }
            OperatorKind::LogicalLimit(_) => {
                todo!("try_satisfy for LogicalLimit")
            }
            OperatorKind::LogicalOrderBy(meta) => {
                let order_by = LogicalOrderBy::borrow_raw_parts(meta, &self.common);
                order_by
                    .try_extract_tuple_ordering()
                    .ok()
                    .and_then(|provided_ordering| {
                        (&provided_ordering >= ordering).then(|| {
                            Arc::<[TupleOrdering]>::from(vec![TupleOrdering::default(); 1])
                        })
                    })
            }
            OperatorKind::LogicalAggregate(_) => satisfy_aggregate_ordering(ordering),
            OperatorKind::LogicalSubquery(_) => {
                assert_eq!(self.kind.category(), OperatorCategory::Logical);
                todo!("try_satisfy for LogicalSubquery")
            }
            OperatorKind::EnforcerSort(meta) => (&meta.tuple_ordering >= ordering)
                .then(|| Arc::<[TupleOrdering]>::from(vec![TupleOrdering::default(); 1])),
            OperatorKind::PhysicalFilter(meta) => {
                let filter = PhysicalFilter::borrow_raw_parts(meta, &self.common);
                satisfy_passthrough_ordering(filter.input(), ordering, ctx)?
            }
            OperatorKind::PhysicalProject(meta) => {
                let project = PhysicalProject::borrow_raw_parts(meta, &self.common);
                satisfy_passthrough_ordering(project.input(), ordering, ctx)?
            }
            OperatorKind::MockScan(meta) => (&meta.spec.mocked_provided_ordering >= ordering)
                .then_some(Arc::<[TupleOrdering]>::from(Vec::new())),
            OperatorKind::PhysicalHashAggregate(_meta) => satisfy_aggregate_ordering(ordering),
            OperatorKind::LogicalRemap(_) => ordering
                .is_empty()
                .then(|| Arc::<[TupleOrdering]>::from(vec![ordering.clone()])),
        };
        Ok(satisfied)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::{
        DataType, IRContext, ScalarValue,
        catalog::{Field, Schema},
        convert::{IntoOperator, IntoScalar},
        operator::join::JoinType,
        properties::TrySatisfy,
        scalar::*,
    };

    #[test]
    fn enforcer_sort_try_satisify_ordering() {
        let ctx = IRContext::with_empty_magic();
        let required = TupleOrdering::from_iter([
            (Column(1, 0), TupleOrderingDirection::Asc),
            (Column(1, 1), TupleOrderingDirection::Desc),
        ]);

        let t1 = ctx.mock_scan(1, 3, 100.);
        let exact_enforcer = EnforcerSort::new(required.clone(), t1.clone()).into_operator();
        let res = exact_enforcer
            .try_satisfy(&required, &ctx)
            .unwrap()
            .unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res[0], TupleOrdering::default());

        let stronger_provided = TupleOrdering::from_iter([
            (Column(1, 0), TupleOrderingDirection::Asc),
            (Column(1, 1), TupleOrderingDirection::Desc),
            (Column(1, 2), TupleOrderingDirection::Asc),
        ]);
        let stronger_enforcer = EnforcerSort::new(stronger_provided, t1.clone()).into_operator();
        let res = stronger_enforcer
            .try_satisfy(&required, &ctx)
            .unwrap()
            .unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res[0], TupleOrdering::default());

        let weaker_provided =
            TupleOrdering::from_iter([(Column(1, 0), TupleOrderingDirection::Asc)]);
        let weaker_enforcer = EnforcerSort::new(weaker_provided, t1.clone()).into_operator();
        assert_eq!(None, weaker_enforcer.try_satisfy(&required, &ctx).unwrap());
    }

    #[test]
    fn physical_nljoin_try_satisfy_ordering() {
        let ctx = IRContext::with_empty_magic();

        let m1 = ctx.mock_scan(1, 2, 100.);
        let m2 = ctx.mock_scan(2, 2, 100.);
        let join_cond = Literal::new(ScalarValue::Boolean(Some(true))).into_scalar();
        let join1 = Join::nested_loop(JoinType::Inner, m1.clone(), m2.clone(), join_cond.clone())
            .into_operator();
        let join2 = join1.clone_with_inputs(Some(Arc::new([m2, m1])), None);

        // Both satisfy the empty ordering.
        let empty = TupleOrdering::default();
        let res = join1.try_satisfy(&empty, &ctx).unwrap().unwrap();
        assert_eq!(res[0], empty);
        assert_eq!(res[1], empty);

        let res = join1.try_satisfy(&empty, &ctx).unwrap().unwrap();
        assert_eq!(res[0], empty);
        assert_eq!(res[1], empty);

        let ordering_from_t1 = TupleOrdering::from_iter([
            (Column(1, 0), TupleOrderingDirection::Asc),
            (Column(1, 1), TupleOrderingDirection::Desc),
        ]);

        // Only satisifies when t1 is the outer side.
        let res = join1.try_satisfy(&ordering_from_t1, &ctx).unwrap().unwrap();
        assert_eq!(res[0], ordering_from_t1);
        assert_eq!(res[1], empty);

        assert_eq!(None, join2.try_satisfy(&ordering_from_t1, &ctx).unwrap());

        // Both should not satisfy.
        let columns_from_both_sides = TupleOrdering::from_iter([
            (Column(1, 0), TupleOrderingDirection::Asc),
            (Column(2, 0), TupleOrderingDirection::Desc),
        ]);
        assert_eq!(
            None,
            join1.try_satisfy(&columns_from_both_sides, &ctx).unwrap()
        );
        assert_eq!(
            None,
            join2.try_satisfy(&columns_from_both_sides, &ctx).unwrap()
        );
    }

    #[test]
    fn physical_table_scan_try_satisfy_ordering() {
        let ctx = IRContext::with_course_tables();
        let t1 = ctx.mock_scan(1, 2, 10.);

        // Satisfies empty ordering
        let ordering = TupleOrdering::default();
        let res = t1.try_satisfy(&ordering, &ctx).unwrap().unwrap();
        assert_eq!(res.len(), 0);

        // Does not satisfies the ordering [0, 1]. (for detecting regressions).
        let ordering = TupleOrdering::from_iter([
            (Column(1, 0), TupleOrderingDirection::Asc),
            (Column(1, 1), TupleOrderingDirection::Asc),
        ]);
        assert_eq!(None, t1.try_satisfy(&ordering, &ctx).unwrap());
    }

    #[test]
    fn logical_get_try_satisfy_ordering() {
        let ctx = IRContext::with_empty_magic();
        let schema = Schema::new(vec![
            Field::new("t1.c0", DataType::Int32, false),
            Field::new("t1.c1", DataType::Int32, false),
        ]);
        let t1 = ctx.logical_get(1, &schema, None);

        let empty = TupleOrdering::default();
        let res = t1.try_satisfy(&empty, &ctx).unwrap().unwrap();
        assert_eq!(res.len(), 0);

        let ordering = TupleOrdering::from_iter([
            (Column(1, 0), TupleOrderingDirection::Asc),
            (Column(1, 1), TupleOrderingDirection::Asc),
        ]);
        assert_eq!(None, t1.try_satisfy(&ordering, &ctx).unwrap());
    }

    #[test]
    fn logical_join_try_satisfy_ordering() {
        let ctx = IRContext::with_empty_magic();

        let m1 = ctx.mock_scan(1, 2, 100.);
        let m2 = ctx.mock_scan(2, 2, 100.);
        let join_cond = Literal::new(ScalarValue::Boolean(Some(true))).into_scalar();
        let join1 = Join::logical(JoinType::Inner, m1.clone(), m2.clone(), join_cond.clone())
            .into_operator();
        let join2 = join1.clone_with_inputs(Some(Arc::new([m2, m1])), None);

        let empty = TupleOrdering::default();
        let res = join1.try_satisfy(&empty, &ctx).unwrap().unwrap();
        assert_eq!(res[0], empty);
        assert_eq!(res[1], empty);

        let ordering_from_t1 = TupleOrdering::from_iter([
            (Column(1, 0), TupleOrderingDirection::Asc),
            (Column(1, 1), TupleOrderingDirection::Desc),
        ]);
        let res = join1.try_satisfy(&ordering_from_t1, &ctx).unwrap().unwrap();
        assert_eq!(res[0], ordering_from_t1);
        assert_eq!(res[1], empty);

        assert_eq!(None, join2.try_satisfy(&ordering_from_t1, &ctx).unwrap());
    }

    #[test]
    fn logical_select_try_satisfy_ordering() {
        let ctx = IRContext::with_empty_magic();
        let input = ctx.mock_scan(1, 2, 100.);
        let predicate = Literal::new(ScalarValue::Boolean(Some(true))).into_scalar();
        let select = LogicalSelect::new(input, predicate).into_operator();

        let ordering = TupleOrdering::from_iter([
            (Column(1, 0), TupleOrderingDirection::Asc),
            (Column(1, 1), TupleOrderingDirection::Desc),
        ]);
        let res = select.try_satisfy(&ordering, &ctx).unwrap().unwrap();
        assert_eq!(res[0], ordering);

        let invalid = TupleOrdering::from_iter([(Column(2, 0), TupleOrderingDirection::Asc)]);
        assert_eq!(None, select.try_satisfy(&invalid, &ctx).unwrap());
    }

    #[test]
    fn logical_project_try_satisfy_ordering() {
        let ctx = IRContext::with_empty_magic();
        let input = ctx.mock_scan(1, 2, 100.);
        let project = LogicalProject::new(
            3,
            input,
            List::new(
                vec![
                    ColumnRef::new(Column(1, 0)).into_scalar(),
                    ColumnRef::new(Column(1, 1)).into_scalar(),
                ]
                .into(),
            )
            .into_scalar(),
        )
        .into_operator();

        let ordering = TupleOrdering::from_iter([(Column(1, 0), TupleOrderingDirection::Asc)]);
        let res = project.try_satisfy(&ordering, &ctx).unwrap().unwrap();
        assert_eq!(res[0], ordering);

        let invalid = TupleOrdering::from_iter([(Column(3, 0), TupleOrderingDirection::Asc)]);
        assert_eq!(None, project.try_satisfy(&invalid, &ctx).unwrap());
    }

    #[test]
    fn logical_aggregate_try_satisfy_ordering() {
        let ctx = IRContext::with_empty_magic();
        let input = ctx.mock_scan(1, 2, 100.);
        let aggregate = LogicalAggregate::new(
            2,
            input,
            List::new(Vec::new().into()).into_scalar(),
            List::new(vec![ColumnRef::new(Column(1, 0)).into_scalar()].into()).into_scalar(),
        )
        .into_operator();

        let empty = TupleOrdering::default();
        let res = aggregate.try_satisfy(&empty, &ctx).unwrap().unwrap();
        assert_eq!(res[0], empty);

        let ordering = TupleOrdering::from_iter([(Column(1, 0), TupleOrderingDirection::Asc)]);
        assert_eq!(None, aggregate.try_satisfy(&ordering, &ctx).unwrap());
    }

    #[test]
    fn logical_order_by_try_satisfy_ordering() {
        let ctx = IRContext::with_empty_magic();
        let order_by = LogicalOrderBy::new(
            ctx.mock_scan(1, 3, 100.),
            vec![
                (
                    ColumnRef::new(Column(1, 0)).into_scalar(),
                    TupleOrderingDirection::Asc,
                ),
                (
                    ColumnRef::new(Column(1, 1)).into_scalar(),
                    TupleOrderingDirection::Desc,
                ),
            ],
        )
        .into_operator();

        let required = TupleOrdering::from_iter([(Column(1, 0), TupleOrderingDirection::Asc)]);
        let res = order_by.try_satisfy(&required, &ctx).unwrap().unwrap();
        assert_eq!(res[0], TupleOrdering::default());

        let invalid = TupleOrdering::from_iter([(Column(1, 1), TupleOrderingDirection::Asc)]);
        assert_eq!(None, order_by.try_satisfy(&invalid, &ctx).unwrap());
    }
}
