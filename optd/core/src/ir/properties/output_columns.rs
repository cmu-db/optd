//! This module defines the `OutputColumns` property for operators in the IR,
//! representing the set of columns produced by an operator.

use snafu::whatever;

use crate::{
    error::Result,
    ir::{
        Column, ColumnSet, OperatorKind,
        operator::{
            Get, Join, LogicalAggregate, LogicalDependentJoin, LogicalProject, LogicalRemap,
            PhysicalHashAggregate, PhysicalProject, join::JoinType,
        },
        properties::{Derive, GetProperty, PropertyMarker},
        scalar::{ColumnRef, List},
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
                match join.join_type() {
                    JoinType::Mark(mark_column) => {
                        let outer_columns = join.outer().output_columns(ctx)?;
                        let set = outer_columns
                            .iter()
                            .cloned()
                            .chain(std::iter::once(*mark_column))
                            .collect();
                        Ok(Arc::new(set))
                    }
                    _ => {
                        let outer_columns = join.outer().output_columns(ctx)?;
                        let inner_columns = join.inner().output_columns(ctx)?;
                        Ok(Arc::new(outer_columns.as_ref() | inner_columns.as_ref()))
                    }
                }
            }
            OperatorKind::LogicalDependentJoin(meta) => {
                let join = LogicalDependentJoin::borrow_raw_parts(meta, &self.common);
                match join.join_type() {
                    JoinType::Mark(mark_column) => {
                        let outer_columns = join.outer().output_columns(ctx)?;
                        let set = outer_columns
                            .iter()
                            .cloned()
                            .chain(std::iter::once(*mark_column))
                            .collect();
                        Ok(Arc::new(set))
                    }
                    _ => {
                        let outer_columns = join.outer().output_columns(ctx)?;
                        let inner_columns = join.inner().output_columns(ctx)?;
                        Ok(Arc::new(outer_columns.as_ref() | inner_columns.as_ref()))
                    }
                }
            }
            OperatorKind::LogicalSelect(_)
            | OperatorKind::LogicalLimit(_)
            | OperatorKind::PhysicalFilter(_)
            | OperatorKind::LogicalOrderBy(_)
            | OperatorKind::EnforcerSort(_)
            | OperatorKind::LogicalSubquery(_) => {
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
            OperatorKind::MockScan(meta) => Ok(meta.spec.mocked_output_columns.clone()),
            OperatorKind::LogicalProject(meta) => {
                let project = LogicalProject::borrow_raw_parts(meta, &self.common);
                let projections = project.projections().try_borrow::<List>().unwrap();
                let set = (0..projections.members().len())
                    .map(|i| Column(*project.table_index(), i))
                    .collect();
                Ok(Arc::new(set))
            }
            OperatorKind::PhysicalProject(meta) => {
                let project = PhysicalProject::borrow_raw_parts(meta, &self.common);
                let projections = project.projections().try_borrow::<List>().unwrap();
                let set = (0..projections.members().len())
                    .map(|i| Column(*project.table_index(), i))
                    .collect();
                Ok(Arc::new(set))
            }
            OperatorKind::LogicalAggregate(meta) => {
                let agg = LogicalAggregate::borrow_raw_parts(meta, &self.common);
                let exprs = agg.exprs().borrow::<List>();
                let keys = agg.keys().borrow::<List>();

                let set = (0..exprs.members().len())
                    .map(|i| Column(*agg.aggregate_table_index(), i))
                    .chain(
                        keys.members()
                            .iter()
                            .map(|e| e.borrow::<ColumnRef>().column().clone()),
                    )
                    .collect();

                Ok(Arc::new(set))
            }
            OperatorKind::PhysicalHashAggregate(meta) => {
                let agg = PhysicalHashAggregate::borrow_raw_parts(meta, &self.common);
                let exprs = agg.exprs().borrow::<List>();
                let keys = agg.keys().borrow::<List>();

                let set = (0..exprs.members().len())
                    .map(|i| Column(*agg.aggregate_table_index(), i))
                    .chain(
                        keys.members()
                            .iter()
                            .map(|e| e.borrow::<ColumnRef>().column().clone()),
                    )
                    .collect();

                Ok(Arc::new(set))
            }
            OperatorKind::LogicalRemap(meta) => {
                let remap = LogicalRemap::borrow_raw_parts(meta, &self.common);
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
}
