//! This module defines the `OutputSchema` property for operators in the IR,
//! allowing retrieval of the output schema based on the operator type and its
//! metadata.

use crate::error::{Result, whatever};
use crate::ir::Operator;
use crate::ir::{
    OperatorKind,
    catalog::{Field, Schema},
    operator::{
        EnforcerSort, LogicalAggregate, LogicalDependentJoin, LogicalGet, LogicalJoin,
        LogicalOrderBy, LogicalProject, LogicalRemap, LogicalSelect, LogicalSubquery,
        PhysicalFilter, PhysicalHashAggregate, PhysicalHashJoin, PhysicalNLJoin, PhysicalProject,
        PhysicalTableScan, join::JoinType,
    },
    properties::{Derive, GetProperty, PropertyMarker},
    scalar::{ColumnAssign, List},
};
use itertools::Itertools;
use std::sync::Arc;

pub struct OutputSchema;

impl PropertyMarker for OutputSchema {
    type Output = Result<Schema>;
}

impl Derive<OutputSchema> for Operator {
    fn derive_by_compute(
        &self,
        ctx: &crate::ir::IRContext,
    ) -> <OutputSchema as PropertyMarker>::Output {
        match &self.kind {
            OperatorKind::Group(_) => {
                whatever!("should not derive output schema for Group operator")
            }
            OperatorKind::MockScan(_) => {
                whatever!("should not derive output schema for MockScan operator")
            }
            OperatorKind::LogicalGet(meta) => {
                let get = LogicalGet::borrow_raw_parts(meta, &self.common);
                let meta = ctx.cat.describe_table(*get.source());
                Ok(Schema::new(
                    get.projections()
                        .iter()
                        .map(|i| meta.schema.field(*i).clone())
                        .collect_vec(),
                ))
            }
            OperatorKind::PhysicalTableScan(meta) => {
                let scan = PhysicalTableScan::borrow_raw_parts(meta, &self.common);
                let meta = ctx.cat.describe_table(*scan.source());
                Ok(Schema::new(
                    scan.projections()
                        .iter()
                        .map(|i| meta.schema.field(*i).clone())
                        .collect_vec(),
                ))
            }
            OperatorKind::LogicalSelect(meta) => {
                let select = LogicalSelect::borrow_raw_parts(meta, &self.common);
                select.input().output_schema(ctx)
            }
            OperatorKind::LogicalSubquery(meta) => {
                let subquery = LogicalSubquery::borrow_raw_parts(meta, &self.common);
                subquery.input().output_schema(ctx)
            }
            OperatorKind::PhysicalFilter(meta) => {
                let filter = PhysicalFilter::borrow_raw_parts(meta, &self.common);
                filter.input().output_schema(ctx)
            }
            OperatorKind::LogicalJoin(meta) => {
                let join = LogicalJoin::borrow_raw_parts(meta, &self.common);
                compute_join_schema(join.outer(), join.inner(), join.join_type(), ctx)
            }
            OperatorKind::LogicalDependentJoin(meta) => {
                let join = LogicalDependentJoin::borrow_raw_parts(meta, &self.common);
                compute_join_schema(join.outer(), join.inner(), join.join_type(), ctx)
            }
            OperatorKind::PhysicalNLJoin(meta) => {
                let join = PhysicalNLJoin::borrow_raw_parts(meta, &self.common);
                compute_join_schema(join.outer(), join.inner(), join.join_type(), ctx)
            }
            OperatorKind::PhysicalHashJoin(meta) => {
                let join = PhysicalHashJoin::borrow_raw_parts(meta, &self.common);
                compute_join_schema(join.build_side(), join.probe_side(), join.join_type(), ctx)
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
                            column_meta.data_type.clone(),
                            true,
                        ))
                    })
                    .collect_vec();
                Ok(Schema::new(columns))
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
                            column_meta.data_type.clone(),
                            true,
                        ))
                    })
                    .collect_vec();
                Ok(Schema::new(columns))
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
                            column_meta.data_type.clone(),
                            true,
                        ))
                    })
                    .collect_vec();

                Ok(Schema::new(columns))
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
                            column_meta.data_type.clone(),
                            true,
                        ))
                    })
                    .collect_vec();

                Ok(Schema::new(columns))
            }
            OperatorKind::LogicalRemap(meta) => {
                let remap = LogicalRemap::borrow_raw_parts(meta, &self.common);
                let columns = remap
                    .mappings()
                    .borrow::<List>()
                    .members()
                    .iter()
                    .map(|e| {
                        let column = *e.borrow::<ColumnAssign>().column();
                        let column_meta = ctx.get_column_meta(&column);
                        Arc::new(Field::new(
                            column_meta.name.clone(),
                            column_meta.data_type.clone(),
                            true,
                        ))
                    })
                    .collect_vec();
                Ok(Schema::new(columns))
            }
        }
    }
}

impl Operator {
    pub fn output_schema(&self, ctx: &crate::ir::context::IRContext) -> Result<Schema> {
        self.get_property::<OutputSchema>(ctx)
    }
}

/// Computes the output schema for join operators based on their join type.
/// This function handles different join types, adjusting the nullability
/// of the fields accordingly and adding a mark field if necessary.
fn compute_join_schema(
    outer: &Operator,
    inner: &Operator,
    join_type: &JoinType,
    ctx: &crate::ir::IRContext,
) -> Result<Schema> {
    let (outer_nullable, inner_nullable, mark_field) = match join_type {
        JoinType::Inner => (false, false, None),
        JoinType::Left => (false, true, None),
        JoinType::Single => (false, true, None),
        JoinType::Mark(mark_column) => {
            let mark_meta = ctx.get_column_meta(mark_column);
            (
                false,
                true,
                Some(Arc::new(Field::new(
                    mark_meta.name.clone(),
                    mark_meta.data_type.clone(),
                    true,
                ))),
            )
        }
    };

    let inner_schema = inner.output_schema(ctx)?;
    let outer_schema = outer.output_schema(ctx)?;

    let map_all_fields_to_nullable = |schema: &Schema| {
        Schema::new(
            schema
                .fields()
                .iter()
                .map(|x| Arc::new(Field::new(x.name().clone(), x.data_type().clone(), true)))
                .collect_vec(),
        )
    };

    let outer_schema = if outer_nullable {
        map_all_fields_to_nullable(&outer_schema)
    } else {
        outer_schema
    };
    let inner_schema = if inner_nullable {
        map_all_fields_to_nullable(&inner_schema)
    } else {
        inner_schema
    };

    Ok(Schema::new(
        outer_schema
            .fields()
            .iter()
            .cloned()
            .chain(inner_schema.fields().iter().cloned())
            .chain(mark_field)
            .collect_vec(),
    ))
}
