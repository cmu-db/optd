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
                    Some(implementation) if implementation.is_hash() => ordering
                        .is_empty()
                        .then(|| Arc::<[TupleOrdering]>::from(vec![ordering.clone(); 2])),
                    _ => satisfy_nl_join_ordering(join.outer(), ordering, ctx)?,
                }
            }
            OperatorKind::DependentJoin(meta) => {
                let join = DependentJoin::borrow_raw_parts(meta, &self.common);
                satisfy_nl_join_ordering(join.outer(), ordering, ctx)?
            }
            OperatorKind::Project(meta) => {
                let project = Project::borrow_raw_parts(meta, &self.common);
                satisfy_passthrough_ordering(project.input(), ordering, ctx)?
            }
            OperatorKind::Select(meta) => {
                let select = Select::borrow_raw_parts(meta, &self.common);
                satisfy_passthrough_ordering(select.input(), ordering, ctx)?
            }
            OperatorKind::Limit(_) => {
                todo!("try_satisfy for LogicalLimit")
            }
            OperatorKind::OrderBy(meta) => {
                let order_by = OrderBy::borrow_raw_parts(meta, &self.common);
                order_by
                    .try_extract_tuple_ordering()
                    .ok()
                    .and_then(|provided_ordering| {
                        (&provided_ordering >= ordering).then(|| {
                            Arc::<[TupleOrdering]>::from(vec![TupleOrdering::default(); 1])
                        })
                    })
            }
            OperatorKind::Aggregate(_) => satisfy_aggregate_ordering(ordering),
            OperatorKind::Subquery(_) => {
                assert_eq!(self.kind.category(), OperatorCategory::Logical);
                todo!("try_satisfy for LogicalSubquery")
            }
            OperatorKind::EnforcerSort(meta) => (&meta.tuple_ordering >= ordering)
                .then(|| Arc::<[TupleOrdering]>::from(vec![TupleOrdering::default(); 1])),
            OperatorKind::MockScan(meta) => (&meta.spec.mocked_provided_ordering >= ordering)
                .then_some(Arc::<[TupleOrdering]>::from(Vec::new())),
            OperatorKind::Remap(_) => ordering
                .is_empty()
                .then(|| Arc::<[TupleOrdering]>::from(vec![ordering.clone()])),
        };
        Ok(satisfied)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::builder_v2::column_ref;
    use crate::ir::{
        ScalarValue,
        convert::{IntoOperator, IntoScalar},
        operator::join::JoinType,
        properties::TrySatisfy,
        scalar::*,
        table_ref::TableRef,
        test_utils::{test_col, test_ctx_with_tables},
    };

    #[test]
    fn enforcer_sort_try_satisify_ordering() -> crate::error::Result<()> {
        let ctx = test_ctx_with_tables(&[("t1", 3)])?;
        let t1 = ctx.logical_get_v2(TableRef::bare("t1"), None)?.build();
        let required = TupleOrdering::from_iter([
            (test_col(&ctx, "t1", "c0")?, TupleOrderingDirection::Asc),
            (test_col(&ctx, "t1", "c1")?, TupleOrderingDirection::Desc),
        ]);

        let exact_enforcer = EnforcerSort::new(required.clone(), t1.clone()).into_operator();
        let res = exact_enforcer.try_satisfy(&required, &ctx)?.unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res[0], TupleOrdering::default());

        let stronger_provided = TupleOrdering::from_iter([
            (test_col(&ctx, "t1", "c0")?, TupleOrderingDirection::Asc),
            (test_col(&ctx, "t1", "c1")?, TupleOrderingDirection::Desc),
            (test_col(&ctx, "t1", "c2")?, TupleOrderingDirection::Asc),
        ]);
        let stronger_enforcer = EnforcerSort::new(stronger_provided, t1.clone()).into_operator();
        let res = stronger_enforcer.try_satisfy(&required, &ctx)?.unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res[0], TupleOrdering::default());

        let weaker_provided =
            TupleOrdering::from_iter([(test_col(&ctx, "t1", "c0")?, TupleOrderingDirection::Asc)]);
        let weaker_enforcer = EnforcerSort::new(weaker_provided, t1).into_operator();
        assert_eq!(None, weaker_enforcer.try_satisfy(&required, &ctx)?);
        Ok(())
    }

    #[test]
    fn physical_nljoin_try_satisfy_ordering() -> crate::error::Result<()> {
        let ctx = test_ctx_with_tables(&[("t1", 2), ("t2", 2)])?;

        let m1 = ctx.table_scan_v2(TableRef::bare("t1"), None)?.build();
        let m2 = ctx.table_scan_v2(TableRef::bare("t2"), None)?.build();
        let join_cond = Literal::new(ScalarValue::Boolean(Some(true))).into_scalar();
        let join1 = m1
            .clone()
            .with_ctx(&ctx)
            .nl_join(m2.clone(), join_cond.clone(), JoinType::Inner)
            .build();
        let join2 = m2
            .clone()
            .with_ctx(&ctx)
            .nl_join(m1, join_cond, JoinType::Inner)
            .build();

        // Both satisfy the empty ordering.
        let empty = TupleOrdering::default();
        let res = join1.try_satisfy(&empty, &ctx)?.unwrap();
        assert_eq!(res[0], empty);
        assert_eq!(res[1], empty);

        let res = join1.try_satisfy(&empty, &ctx)?.unwrap();
        assert_eq!(res[0], empty);
        assert_eq!(res[1], empty);

        let ordering_from_t1 = TupleOrdering::from_iter([
            (test_col(&ctx, "t1", "c0")?, TupleOrderingDirection::Asc),
            (test_col(&ctx, "t1", "c1")?, TupleOrderingDirection::Desc),
        ]);

        // Only satisifies when t1 is the outer side.
        let res = join1.try_satisfy(&ordering_from_t1, &ctx)?.unwrap();
        assert_eq!(res[0], ordering_from_t1);
        assert_eq!(res[1], empty);

        assert_eq!(None, join2.try_satisfy(&ordering_from_t1, &ctx)?);

        // Both should not satisfy.
        let columns_from_both_sides = TupleOrdering::from_iter([
            (test_col(&ctx, "t1", "c0")?, TupleOrderingDirection::Asc),
            (test_col(&ctx, "t2", "c0")?, TupleOrderingDirection::Desc),
        ]);
        assert_eq!(None, join1.try_satisfy(&columns_from_both_sides, &ctx)?);
        assert_eq!(None, join2.try_satisfy(&columns_from_both_sides, &ctx)?);
        Ok(())
    }

    #[test]
    fn physical_table_scan_try_satisfy_ordering() -> crate::error::Result<()> {
        let ctx = test_ctx_with_tables(&[("t1", 2)])?;
        let t1 = ctx.table_scan_v2(TableRef::bare("t1"), None)?.build();

        let ordering = TupleOrdering::default();
        let res = t1.try_satisfy(&ordering, &ctx)?.unwrap();
        assert_eq!(res.len(), 0);

        let ordering = TupleOrdering::from_iter([
            (test_col(&ctx, "t1", "c0")?, TupleOrderingDirection::Asc),
            (test_col(&ctx, "t1", "c1")?, TupleOrderingDirection::Asc),
        ]);
        assert_eq!(None, t1.try_satisfy(&ordering, &ctx)?);
        Ok(())
    }

    #[test]
    fn logical_get_try_satisfy_ordering() -> crate::error::Result<()> {
        let ctx = test_ctx_with_tables(&[("t1", 2)])?;
        let t1 = ctx.logical_get_v2(TableRef::bare("t1"), None)?.build();

        let empty = TupleOrdering::default();
        let res = t1.try_satisfy(&empty, &ctx)?.unwrap();
        assert_eq!(res.len(), 0);

        let ordering = TupleOrdering::from_iter([
            (test_col(&ctx, "t1", "c0")?, TupleOrderingDirection::Asc),
            (test_col(&ctx, "t1", "c1")?, TupleOrderingDirection::Asc),
        ]);
        assert_eq!(None, t1.try_satisfy(&ordering, &ctx)?);
        Ok(())
    }

    #[test]
    fn logical_join_try_satisfy_ordering() -> crate::error::Result<()> {
        let ctx = test_ctx_with_tables(&[("t1", 2), ("t2", 2)])?;

        let m1 = ctx.logical_get_v2(TableRef::bare("t1"), None)?.build();
        let m2 = ctx.logical_get_v2(TableRef::bare("t2"), None)?.build();
        let join_cond = Literal::new(ScalarValue::Boolean(Some(true))).into_scalar();
        let join1 = m1
            .clone()
            .with_ctx(&ctx)
            .logical_join(m2.clone(), join_cond.clone(), JoinType::Inner)
            .build();
        let join2 = m2
            .clone()
            .with_ctx(&ctx)
            .logical_join(m1, join_cond, JoinType::Inner)
            .build();

        let empty = TupleOrdering::default();
        let res = join1.try_satisfy(&empty, &ctx)?.unwrap();
        assert_eq!(res[0], empty);
        assert_eq!(res[1], empty);

        let ordering_from_t1 = TupleOrdering::from_iter([
            (test_col(&ctx, "t1", "c0")?, TupleOrderingDirection::Asc),
            (test_col(&ctx, "t1", "c1")?, TupleOrderingDirection::Desc),
        ]);
        let res = join1.try_satisfy(&ordering_from_t1, &ctx)?.unwrap();
        assert_eq!(res[0], ordering_from_t1);
        assert_eq!(res[1], empty);

        assert_eq!(None, join2.try_satisfy(&ordering_from_t1, &ctx)?);
        Ok(())
    }

    #[test]
    fn logical_select_try_satisfy_ordering() -> crate::error::Result<()> {
        let ctx = test_ctx_with_tables(&[("t1", 2), ("t2", 1)])?;
        let input = ctx.logical_get_v2(TableRef::bare("t1"), None)?.build();
        let _other = ctx.logical_get_v2(TableRef::bare("t2"), None)?.build();
        let predicate = Literal::new(ScalarValue::Boolean(Some(true))).into_scalar();
        let select = input.with_ctx(&ctx).select(predicate).build();

        let ordering = TupleOrdering::from_iter([
            (test_col(&ctx, "t1", "c0")?, TupleOrderingDirection::Asc),
            (test_col(&ctx, "t1", "c1")?, TupleOrderingDirection::Desc),
        ]);
        let res = select.try_satisfy(&ordering, &ctx)?.unwrap();
        assert_eq!(res[0], ordering);

        let invalid =
            TupleOrdering::from_iter([(test_col(&ctx, "t2", "c0")?, TupleOrderingDirection::Asc)]);
        assert_eq!(None, select.try_satisfy(&invalid, &ctx)?);
        Ok(())
    }

    #[test]
    fn logical_project_try_satisfy_ordering() -> crate::error::Result<()> {
        let ctx = test_ctx_with_tables(&[("t1", 2)])?;
        let input = ctx.logical_get_v2(TableRef::bare("t1"), None)?.build();
        let c0 = test_col(&ctx, "t1", "c0")?;
        let c1 = test_col(&ctx, "t1", "c1")?;
        let project = ctx
            .project(input, [column_ref(c0), column_ref(c1)])?
            .build();

        let ordering = TupleOrdering::from_iter([(c0, TupleOrderingDirection::Asc)]);
        let res = project.try_satisfy(&ordering, &ctx)?.unwrap();
        assert_eq!(res[0], ordering);

        let project_node = project.try_borrow::<Project>().unwrap();
        let project_table_ref = ctx
            .get_binding(project_node.table_index())?
            .table_ref()
            .clone();
        let invalid = TupleOrdering::from_iter([(
            ctx.col(Some(&project_table_ref), "c0")?,
            TupleOrderingDirection::Asc,
        )]);
        assert_eq!(None, project.try_satisfy(&invalid, &ctx)?);
        Ok(())
    }

    #[test]
    fn logical_aggregate_try_satisfy_ordering() -> crate::error::Result<()> {
        let ctx = test_ctx_with_tables(&[("t1", 2)])?;
        let input = ctx.logical_get_v2(TableRef::bare("t1"), None)?.build();
        let c0 = test_col(&ctx, "t1", "c0")?;
        let aggregate = input
            .with_ctx(&ctx)
            .logical_aggregate(Vec::new(), [column_ref(c0)])?
            .build();

        let empty = TupleOrdering::default();
        let res = aggregate.try_satisfy(&empty, &ctx)?.unwrap();
        assert_eq!(res[0], empty);

        let ordering = TupleOrdering::from_iter([(c0, TupleOrderingDirection::Asc)]);
        assert_eq!(None, aggregate.try_satisfy(&ordering, &ctx)?);
        Ok(())
    }

    #[test]
    fn logical_order_by_try_satisfy_ordering() -> crate::error::Result<()> {
        let ctx = test_ctx_with_tables(&[("t1", 3)])?;
        let input = ctx.logical_get_v2(TableRef::bare("t1"), None)?.build();
        let c0 = test_col(&ctx, "t1", "c0")?;
        let c1 = test_col(&ctx, "t1", "c1")?;
        let order_by = OrderBy::new(
            input,
            vec![
                (column_ref(c0), TupleOrderingDirection::Asc),
                (column_ref(c1), TupleOrderingDirection::Desc),
            ],
        )
        .into_operator();

        let required = TupleOrdering::from_iter([(c0, TupleOrderingDirection::Asc)]);
        let res = order_by.try_satisfy(&required, &ctx)?.unwrap();
        assert_eq!(res[0], TupleOrdering::default());

        let invalid = TupleOrdering::from_iter([(c1, TupleOrderingDirection::Asc)]);
        assert_eq!(None, order_by.try_satisfy(&invalid, &ctx)?);
        Ok(())
    }
}
