//! This module defines the `OutputColumns` property for operators in the IR,
//! representing the set of columns produced by an operator.

use snafu::whatever;

use crate::{
    error::Result,
    ir::{
        Column, ColumnSet, OperatorKind,
        operator::{
            Aggregate, DependentJoin, EnforcerSort, Get, Join, Limit, OrderBy, Project, Remap,
            Select, Subquery, join::JoinType,
        },
        properties::{Derive, GetProperty, PropertyMarker},
        scalar::List,
    },
};
use std::sync::Arc;

pub struct OutputColumns;

impl PropertyMarker for OutputColumns {
    type Output = Result<Arc<ColumnSet>>;
}

impl Derive<OutputColumns> for crate::ir::Operator {
    fn derive_by_compute(
        &self,
        ctx: &crate::ir::IRContext,
    ) -> <OutputColumns as PropertyMarker>::Output {
        match &self.kind {
            OperatorKind::Group(_) => {
                whatever!("Right now group's properties should always be set.")
            }
            OperatorKind::Get(meta) => {
                let node = Get::borrow_raw_parts(meta, &self.common);
                Ok(Arc::new(
                    node.projections()
                        .iter()
                        .map(|x| Column(*node.table_index(), *x))
                        .collect(),
                ))
            }
            OperatorKind::Join(meta) => {
                let join = Join::borrow_raw_parts(meta, &self.common);
                derive_join_output_columns(join.outer(), join.inner(), join.join_type(), ctx)
            }
            OperatorKind::DependentJoin(meta) => {
                let join = DependentJoin::borrow_raw_parts(meta, &self.common);
                derive_join_output_columns(join.outer(), join.inner(), join.join_type(), ctx)
            }
            OperatorKind::Select(_)
            | OperatorKind::Limit(_)
            | OperatorKind::OrderBy(_)
            | OperatorKind::EnforcerSort(_)
            | OperatorKind::Subquery(_) => {
                let set = self.input_operators().iter().try_fold(
                    ColumnSet::default(),
                    |mut set, op| {
                        let output = op.output_columns(ctx)?;
                        set |= &output;
                        Ok(set)
                    },
                )?;
                Ok(Arc::new(set))
            }
            OperatorKind::Project(meta) => {
                let project = Project::borrow_raw_parts(meta, &self.common);
                let projections = project.projections().try_borrow::<List>().unwrap();
                let set = (0..projections.members().len())
                    .map(|i| Column(*project.table_index(), i))
                    .collect();
                Ok(Arc::new(set))
            }
            OperatorKind::Aggregate(meta) => {
                let agg = Aggregate::borrow_raw_parts(meta, &self.common);
                let exprs = agg.exprs().borrow::<List>();
                let keys = agg.keys().borrow::<List>();

                let set = (0..exprs.members().len())
                    .map(|i| Column(*agg.aggregate_table_index(), i))
                    .chain((0..keys.members().len()).map(|i| Column(*agg.key_table_index(), i)))
                    .collect();

                Ok(Arc::new(set))
            }
            OperatorKind::Remap(meta) => {
                let remap = Remap::borrow_raw_parts(meta, &self.common);
                let input_columns = remap.input().output_columns(ctx)?;
                let set = (0..input_columns.len())
                    .map(|i| Column(*remap.table_index(), i))
                    .collect();
                Ok(Arc::new(set))
            }
        }
    }

    fn derive(
        &self,
        ctx: &crate::ir::context::IRContext,
    ) -> <OutputColumns as PropertyMarker>::Output {
        if let Some(set) = self.common.properties.output_columns.get() {
            return Ok(set.clone());
        }

        let set = <Self as Derive<OutputColumns>>::derive_by_compute(self, ctx)?;
        Ok(self
            .common
            .properties
            .output_columns
            .get_or_init(|| set)
            .clone())
    }
}

impl crate::ir::Operator {
    pub fn output_columns(&self, ctx: &crate::ir::context::IRContext) -> Result<Arc<ColumnSet>> {
        self.get_property::<OutputColumns>(ctx)
    }

    /// Returns output columns in schema order rather than set order.
    ///
    /// This is useful when rebuilding operators that allocate a fresh
    /// `table_index`, because callers need to know which old column lands at
    /// which ordinal position in the new binding.
    pub fn output_columns_in_order(
        &self,
        ctx: &crate::ir::context::IRContext,
    ) -> Result<Vec<Column>> {
        derive_output_columns_in_order(self, ctx)
    }
}

fn derive_join_output_columns(
    outer: &crate::ir::Operator,
    inner: &crate::ir::Operator,
    join_type: &JoinType,
    ctx: &crate::ir::IRContext,
) -> Result<Arc<ColumnSet>> {
    let outer_columns = outer.output_columns(ctx)?;
    match join_type {
        JoinType::LeftSemi | JoinType::LeftAnti => Ok(outer_columns),
        JoinType::Mark(mark_column) => {
            let set = outer_columns
                .iter()
                .cloned()
                .chain(std::iter::once(*mark_column))
                .collect();
            Ok(Arc::new(set))
        }
        JoinType::Inner | JoinType::LeftOuter | JoinType::Single => {
            let inner_columns = inner.output_columns(ctx)?;
            Ok(Arc::new(outer_columns.as_ref() | inner_columns.as_ref()))
        }
    }
}

fn derive_join_output_columns_in_order(
    outer: &crate::ir::Operator,
    inner: &crate::ir::Operator,
    join_type: &JoinType,
    ctx: &crate::ir::IRContext,
) -> Result<Vec<Column>> {
    let mut cols = outer.output_columns_in_order(ctx)?;
    match join_type {
        JoinType::LeftSemi | JoinType::LeftAnti => {}
        JoinType::Mark(mark_column) => cols.push(*mark_column),
        JoinType::Inner | JoinType::LeftOuter | JoinType::Single => {
            cols.extend(inner.output_columns_in_order(ctx)?);
        }
    }
    Ok(cols)
}

fn derive_output_columns_in_order(
    op: &crate::ir::Operator,
    ctx: &crate::ir::IRContext,
) -> Result<Vec<Column>> {
    match &op.kind {
        OperatorKind::Get(meta) => {
            let get = Get::borrow_raw_parts(meta, &op.common);
            Ok(get
                .projections()
                .iter()
                .copied()
                .map(|idx| Column(*get.table_index(), idx))
                .collect())
        }
        OperatorKind::Project(meta) => {
            let project = Project::borrow_raw_parts(meta, &op.common);
            let len = project.projections().borrow::<List>().members().len();
            Ok((0..len)
                .map(|idx| Column(*project.table_index(), idx))
                .collect())
        }
        OperatorKind::Aggregate(meta) => {
            let aggregate = Aggregate::borrow_raw_parts(meta, &op.common);
            let key_len = aggregate.keys().borrow::<List>().members().len();
            let expr_len = aggregate.exprs().borrow::<List>().members().len();
            Ok((0..key_len)
                .map(|idx| Column(*aggregate.key_table_index(), idx))
                .chain((0..expr_len).map(|idx| Column(*aggregate.aggregate_table_index(), idx)))
                .collect())
        }
        OperatorKind::Remap(meta) => {
            let remap = Remap::borrow_raw_parts(meta, &op.common);
            let len = remap.input().output_columns_in_order(ctx)?.len();
            Ok((0..len)
                .map(|idx| Column(*remap.table_index(), idx))
                .collect())
        }
        OperatorKind::Select(meta) => {
            let select = Select::borrow_raw_parts(meta, &op.common);
            select.input().output_columns_in_order(ctx)
        }
        OperatorKind::Limit(meta) => {
            let limit = Limit::borrow_raw_parts(meta, &op.common);
            limit.input().output_columns_in_order(ctx)
        }
        OperatorKind::OrderBy(meta) => {
            let order_by = OrderBy::borrow_raw_parts(meta, &op.common);
            order_by.input().output_columns_in_order(ctx)
        }
        OperatorKind::Subquery(meta) => {
            let subquery = Subquery::borrow_raw_parts(meta, &op.common);
            subquery.input().output_columns_in_order(ctx)
        }
        OperatorKind::EnforcerSort(meta) => {
            let sort = EnforcerSort::borrow_raw_parts(meta, &op.common);
            sort.input().output_columns_in_order(ctx)
        }
        OperatorKind::Join(meta) => {
            let join = Join::borrow_raw_parts(meta, &op.common);
            derive_join_output_columns_in_order(join.outer(), join.inner(), join.join_type(), ctx)
        }
        OperatorKind::DependentJoin(meta) => {
            let join = DependentJoin::borrow_raw_parts(meta, &op.common);
            derive_join_output_columns_in_order(join.outer(), join.inner(), join.join_type(), ctx)
        }
        OperatorKind::Group(_) => {
            let mut cols: Vec<_> = op.output_columns(ctx)?.iter().copied().collect();
            cols.sort();
            Ok(cols)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::ir::{
        Group, GroupId, IRContext, builder::boolean, convert::IntoOperator,
        operator::join::JoinType, properties::OperatorProperties, table_ref::TableRef,
        test_utils::test_ctx_with_tables,
    };
    use std::sync::Arc;

    #[test]
    fn group_output_columns_uses_cached_properties() -> crate::error::Result<()> {
        let ctx = test_ctx_with_tables(&[("t1", 2)])?;
        let input = ctx.logical_get(TableRef::bare("t1"), None)?.build();
        let expected = input.output_columns(&ctx).unwrap();

        let group = Group::new(GroupId(42), input.properties().clone()).into_operator();

        assert_eq!(
            group.output_columns(&ctx).unwrap().as_ref(),
            expected.as_ref()
        );
        Ok(())
    }

    #[test]
    fn group_output_columns_errors_without_cached_properties() {
        let ctx = IRContext::with_empty_magic();
        let group =
            Group::new(GroupId(42), Arc::new(OperatorProperties::default())).into_operator();

        assert!(group.output_columns(&ctx).is_err());
    }

    #[test]
    fn left_semi_join_output_columns_use_only_outer_columns() -> crate::error::Result<()> {
        let ctx = test_ctx_with_tables(&[("t1", 2), ("t2", 3)])?;
        let outer = ctx.logical_get(TableRef::bare("t1"), None)?.build();
        let inner = ctx.logical_get(TableRef::bare("t2"), None)?.build();
        let expected = outer.output_columns(&ctx)?;

        let join = outer
            .with_ctx(&ctx)
            .logical_join(inner, boolean(true), JoinType::LeftSemi)
            .build();

        let output_columns = join.output_columns(&ctx)?;

        assert_eq!(output_columns.as_ref(), expected.as_ref());

        Ok(())
    }

    #[test]
    fn left_anti_join_output_columns_use_only_outer_columns() -> crate::error::Result<()> {
        let ctx = test_ctx_with_tables(&[("t1", 2), ("t2", 3)])?;
        let outer = ctx.logical_get(TableRef::bare("t1"), None)?.build();
        let inner = ctx.logical_get(TableRef::bare("t2"), None)?.build();
        let expected = outer.output_columns(&ctx)?;

        let join = outer
            .with_ctx(&ctx)
            .logical_join(inner, boolean(true), JoinType::LeftAnti)
            .build();

        let output_columns = join.output_columns(&ctx)?;

        assert_eq!(output_columns.as_ref(), expected.as_ref());

        Ok(())
    }
}
