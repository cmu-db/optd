//! This module defines the `OutputColumns` property for operators in the IR,
//! representing the set of columns produced by an operator.

use crate::ir::{
    Column, ColumnSet, OperatorKind,
    operator::{
        LogicalAggregate, LogicalDependentJoin, LogicalGet, LogicalJoin, LogicalProject,
        LogicalRemap, PhysicalHashAggregate, PhysicalProject, PhysicalTableScan, join::JoinType,
    },
    properties::{Derive, GetProperty, PropertyMarker},
    scalar::{ColumnAssign, ColumnRef, List},
};
use std::sync::Arc;

pub struct OutputColumns;

impl PropertyMarker for OutputColumns {
    type Output = Arc<ColumnSet>;
}

impl Derive<OutputColumns> for crate::ir::Operator {
    fn derive_by_compute(
        &self,
        ctx: &crate::ir::IRContext,
    ) -> <OutputColumns as PropertyMarker>::Output {
        match &self.kind {
            OperatorKind::Group(_) => {
                panic!("Right now group's properties should always be set.")
            }
            OperatorKind::LogicalGet(meta) => {
                let node = LogicalGet::borrow_raw_parts(meta, &self.common);
                Arc::new(
                    node.projections()
                        .iter()
                        .map(|x| Column(node.first_column().0 + (*x)))
                        .collect(),
                )
            }
            OperatorKind::PhysicalTableScan(meta) => {
                let node = PhysicalTableScan::borrow_raw_parts(meta, &self.common);
                Arc::new(
                    node.projections()
                        .iter()
                        .map(|x| Column(node.first_column().0 + (*x)))
                        .collect(),
                )
            }
            OperatorKind::LogicalJoin(meta) => {
                let join = LogicalJoin::borrow_raw_parts(meta, &self.common);
                match join.join_type() {
                    JoinType::Mark(mark_column) => {
                        let outer_columns = join.outer().output_columns(ctx);
                        let set = outer_columns
                            .iter()
                            .cloned()
                            .chain(std::iter::once(*mark_column))
                            .collect();
                        Arc::new(set)
                    }
                    _ => {
                        let outer_columns = join.outer().output_columns(ctx);
                        let inner_columns = join.inner().output_columns(ctx);
                        Arc::new(outer_columns.as_ref() | inner_columns.as_ref())
                    }
                }
            }
            OperatorKind::LogicalDependentJoin(meta) => {
                let join = LogicalDependentJoin::borrow_raw_parts(meta, &self.common);
                match join.join_type() {
                    JoinType::Mark(mark_column) => {
                        let outer_columns = join.outer().output_columns(ctx);
                        let set = outer_columns
                            .iter()
                            .cloned()
                            .chain(std::iter::once(*mark_column))
                            .collect();
                        Arc::new(set)
                    }
                    _ => {
                        let outer_columns = join.outer().output_columns(ctx);
                        let inner_columns = join.inner().output_columns(ctx);
                        Arc::new(outer_columns.as_ref() | inner_columns.as_ref())
                    }
                }
            }
            OperatorKind::PhysicalNLJoin(_)
            | OperatorKind::PhysicalHashJoin(_)
            | OperatorKind::LogicalSelect(_)
            | OperatorKind::PhysicalFilter(_)
            | OperatorKind::LogicalOrderBy(_)
            | OperatorKind::EnforcerSort(_)
            | OperatorKind::LogicalSubquery(_) => {
                let set =
                    self.input_operators()
                        .iter()
                        .fold(ColumnSet::default(), |mut set, op| {
                            set |= &op.output_columns(ctx);
                            set
                        });
                Arc::new(set)
            }
            OperatorKind::MockScan(meta) => meta.spec.mocked_output_columns.clone(),
            OperatorKind::LogicalProject(meta) => {
                let project = LogicalProject::borrow_raw_parts(meta, &self.common);
                let projections = project.projections().try_borrow::<List>().unwrap();
                let set = projections
                    .members()
                    .iter()
                    .map(|member| {
                        if let Ok(column_assign) = member.try_borrow::<ColumnAssign>() {
                            *column_assign.column()
                        } else if let Ok(column_ref) = member.try_borrow::<ColumnRef>() {
                            *column_ref.column()
                        } else {
                            unreachable!()
                        }
                    })
                    .collect();
                Arc::new(set)
            }
            OperatorKind::PhysicalProject(meta) => {
                let project = PhysicalProject::borrow_raw_parts(meta, &self.common);
                let projections = project.projections().try_borrow::<List>().unwrap();
                let set = projections
                    .members()
                    .iter()
                    .map(|member| {
                        let column_assign = member.try_borrow::<ColumnAssign>().unwrap();
                        *column_assign.column()
                    })
                    .collect();
                Arc::new(set)
            }
            OperatorKind::LogicalAggregate(meta) => {
                let agg = LogicalAggregate::borrow_raw_parts(meta, &self.common);
                let exprs = agg.exprs().try_borrow::<List>().unwrap();
                let keys = agg.exprs().try_borrow::<List>().unwrap();
                let set = exprs
                    .members()
                    .iter()
                    .chain(keys.members().iter())
                    .map(|member| {
                        let column_assign = member.try_borrow::<ColumnAssign>().unwrap();
                        *column_assign.column()
                    })
                    .collect();
                Arc::new(set)
            }
            OperatorKind::PhysicalHashAggregate(meta) => {
                let agg = PhysicalHashAggregate::borrow_raw_parts(meta, &self.common);
                let exprs = agg.exprs().try_borrow::<List>().unwrap();
                let keys = agg.exprs().try_borrow::<List>().unwrap();
                let set = exprs
                    .members()
                    .iter()
                    .chain(keys.members().iter())
                    .map(|member| {
                        let column_assign = member.try_borrow::<ColumnAssign>().unwrap();
                        *column_assign.column()
                    })
                    .collect();
                Arc::new(set)
            }
            OperatorKind::LogicalRemap(meta) => {
                let remap = LogicalRemap::borrow_raw_parts(meta, &self.common);
                let projections = remap.mappings().try_borrow::<List>().unwrap();
                let set = projections
                    .members()
                    .iter()
                    .map(|member| {
                        let column_assign = member.try_borrow::<ColumnAssign>().unwrap();
                        *column_assign.column()
                    })
                    .collect();
                Arc::new(set)
            }
        }
    }

    fn derive(
        &self,
        ctx: &crate::ir::context::IRContext,
    ) -> <OutputColumns as PropertyMarker>::Output {
        self.common
            .properties
            .output_columns
            .get_or_init(|| <Self as Derive<OutputColumns>>::derive_by_compute(self, ctx))
            .clone()
    }
}

impl crate::ir::Operator {
    pub fn output_columns(&self, ctx: &crate::ir::context::IRContext) -> Arc<ColumnSet> {
        self.get_property::<OutputColumns>(ctx)
    }
}
