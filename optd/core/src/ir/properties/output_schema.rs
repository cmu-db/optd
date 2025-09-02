use std::sync::Arc;

use itertools::Itertools;

use crate::ir::{
    OperatorKind,
    catalog::{Field, Schema},
    operator::{
        EnforcerSort, LogicalAggregate, LogicalGet, LogicalJoin, LogicalOrderBy, LogicalProject,
        LogicalSelect, PhysicalFilter, PhysicalHashAggregate, PhysicalHashJoin, PhysicalNLJoin,
        PhysicalProject, PhysicalTableScan,
    },
    properties::{Derive, GetProperty, PropertyMarker},
    scalar::{ColumnAssign, List},
};

pub struct OutputSchema;

impl PropertyMarker for OutputSchema {
    type Output = Option<crate::ir::catalog::Schema>;
}

impl Derive<OutputSchema> for crate::ir::Operator {
    fn derive_by_compute(
        &self,
        ctx: &crate::ir::IRContext,
    ) -> <OutputSchema as PropertyMarker>::Output {
        match &self.kind {
            OperatorKind::Group(_) => None,
            OperatorKind::MockScan(_) => None,
            OperatorKind::LogicalGet(meta) => {
                let get = LogicalGet::borrow_raw_parts(meta, &self.common);
                let meta = ctx.cat.describe_table(*get.source());
                Some(
                    get.projections()
                        .iter()
                        .map(|i| meta.schema.columns()[*i].clone())
                        .collect(),
                )
            }
            OperatorKind::PhysicalTableScan(meta) => {
                let scan = PhysicalTableScan::borrow_raw_parts(meta, &self.common);
                let meta = ctx.cat.describe_table(*scan.source());
                Some(
                    scan.projections()
                        .iter()
                        .map(|i| meta.schema.columns()[*i].clone())
                        .collect(),
                )
            }
            OperatorKind::LogicalSelect(meta) => {
                let select = LogicalSelect::borrow_raw_parts(meta, &self.common);
                select.input().output_schema(ctx)
            }
            OperatorKind::PhysicalFilter(meta) => {
                let filter = PhysicalFilter::borrow_raw_parts(meta, &self.common);
                filter.input().output_schema(ctx)
            }
            OperatorKind::LogicalJoin(meta) => {
                let join = LogicalJoin::borrow_raw_parts(meta, &self.common);
                let columns = join
                    .outer()
                    .output_schema(ctx)?
                    .columns()
                    .iter()
                    .chain(join.inner().output_schema(ctx)?.columns().iter())
                    .cloned()
                    .collect_vec();
                Some(Schema::new(columns))
            }
            OperatorKind::PhysicalNLJoin(meta) => {
                let join = PhysicalNLJoin::borrow_raw_parts(meta, &self.common);
                let columns = join
                    .outer()
                    .output_schema(ctx)?
                    .columns()
                    .iter()
                    .chain(join.inner().output_schema(ctx)?.columns().iter())
                    .cloned()
                    .collect_vec();
                Some(Schema::new(columns))
            }
            OperatorKind::PhysicalHashJoin(meta) => {
                let join = PhysicalHashJoin::borrow_raw_parts(meta, &self.common);
                let columns = join
                    .build_side()
                    .output_schema(ctx)?
                    .columns()
                    .iter()
                    .chain(join.probe_side().output_schema(ctx)?.columns().iter())
                    .cloned()
                    .collect_vec();
                Some(Schema::new(columns))
            }
            OperatorKind::EnforcerSort(meta) => {
                let sort = EnforcerSort::borrow_raw_parts(meta, &self.common);
                sort.input().output_schema(ctx)
            }
            OperatorKind::LogicalOrderBy(meta) => {
                let order_by = LogicalOrderBy::borrow_raw_parts(meta, &self.common);
                order_by.input().output_schema(ctx)
            }
            OperatorKind::LogicalProject(meta) => {
                let project = LogicalProject::borrow_raw_parts(meta, &self.common);
                let columns = project
                    .projections()
                    .borrow::<List>()
                    .members()
                    .iter()
                    .map(|e| {
                        let column = *e.borrow::<ColumnAssign>().column();
                        let column_meta = ctx.get_column_meta(&column);
                        Arc::new(Field::new(
                            column_meta.name.clone(),
                            column_meta.data_type,
                            true,
                        ))
                    })
                    .collect_vec();
                Some(Schema::new(columns))
            }
            OperatorKind::PhysicalProject(meta) => {
                let project = PhysicalProject::borrow_raw_parts(meta, &self.common);
                let columns = project
                    .projections()
                    .borrow::<List>()
                    .members()
                    .iter()
                    .map(|e| {
                        let column = *e.borrow::<ColumnAssign>().column();
                        let column_meta = ctx.get_column_meta(&column);
                        Arc::new(Field::new(
                            column_meta.name.clone(),
                            column_meta.data_type,
                            true,
                        ))
                    })
                    .collect_vec();
                Some(Schema::new(columns))
            }
            OperatorKind::LogicalAggregate(meta) => {
                let agg = LogicalAggregate::borrow_raw_parts(meta, &self.common);
                let columns = agg
                    .exprs()
                    .borrow::<List>()
                    .members()
                    .iter()
                    .chain(agg.keys().borrow::<List>().members())
                    .map(|e| {
                        let column = *e.borrow::<ColumnAssign>().column();
                        let column_meta = ctx.get_column_meta(&column);
                        Arc::new(Field::new(
                            column_meta.name.clone(),
                            column_meta.data_type,
                            true,
                        ))
                    })
                    .collect();

                Some(Schema::new(columns))
            }
            OperatorKind::PhysicalHashAggregate(meta) => {
                let agg = PhysicalHashAggregate::borrow_raw_parts(meta, &self.common);
                let columns = agg
                    .keys()
                    .borrow::<List>()
                    .members()
                    .iter()
                    .chain(agg.exprs().borrow::<List>().members())
                    .map(|e| {
                        let column = *e.borrow::<ColumnAssign>().column();
                        let column_meta = ctx.get_column_meta(&column);
                        Arc::new(Field::new(
                            column_meta.name.clone(),
                            column_meta.data_type,
                            true,
                        ))
                    })
                    .collect();

                Some(Schema::new(columns))
            }
        }
    }
}

impl crate::ir::Operator {
    pub fn output_schema(&self, ctx: &crate::ir::context::IRContext) -> Option<Schema> {
        self.get_property::<OutputSchema>(ctx)
    }
}
