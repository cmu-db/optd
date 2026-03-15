//! This module defines the `OutputSchema` property for operators in the IR,
//! allowing retrieval of the output schema based on the operator type and its
//! metadata.

use crate::error::{CatalogSnafu, Result, SchemaSnafu, whatever};
use crate::ir::Operator;
use crate::ir::{
    OperatorKind,
    catalog::Field,
    operator::{
        Aggregate, DependentJoin, EnforcerSort, Get, Join, LogicalLimit, OrderBy, Project, Remap,
        Select, Subquery, join::JoinType,
    },
    properties::{Derive, GetProperty, PropertyMarker},
    scalar::{ColumnRef, List},
    schema::OptdSchema,
    table_ref::TableRef,
};
use itertools::Itertools;
use snafu::ResultExt;
use std::sync::Arc;

pub struct OutputSchema;

impl PropertyMarker for OutputSchema {
    type Output = Result<OptdSchema>;
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
            OperatorKind::Get(meta) => {
                let scan = Get::borrow_raw_parts(meta, &self.common);
                compute_scan_schema(
                    *scan.data_source_id(),
                    scan.table_index(),
                    scan.projections(),
                    ctx,
                )
            }
            OperatorKind::Select(meta) => {
                let select = Select::borrow_raw_parts(meta, &self.common);
                select.input().output_schema(ctx)
            }
            OperatorKind::LogicalLimit(meta) => {
                let limit = LogicalLimit::borrow_raw_parts(meta, &self.common);
                limit.input().output_schema(ctx)
            }
            OperatorKind::Subquery(meta) => {
                let subquery = Subquery::borrow_raw_parts(meta, &self.common);
                subquery.input().output_schema(ctx)
            }
            OperatorKind::Join(meta) => {
                let join = Join::borrow_raw_parts(meta, &self.common);
                compute_join_schema(join.outer(), join.inner(), join.join_type(), ctx)
            }
            OperatorKind::DependentJoin(meta) => {
                let join = DependentJoin::borrow_raw_parts(meta, &self.common);
                compute_join_schema(join.outer(), join.inner(), join.join_type(), ctx)
            }
            OperatorKind::EnforcerSort(meta) => {
                let sort = EnforcerSort::borrow_raw_parts(meta, &self.common);
                sort.input().output_schema(ctx)
            }
            OperatorKind::OrderBy(meta) => {
                let order_by = OrderBy::borrow_raw_parts(meta, &self.common);
                order_by.input().output_schema(ctx)
            }
            OperatorKind::Project(meta) => {
                let project = Project::borrow_raw_parts(meta, &self.common);
                compute_binding_schema(project.table_index(), ctx)
            }
            OperatorKind::Aggregate(meta) => {
                let agg = Aggregate::borrow_raw_parts(meta, &self.common);
                compute_aggregate_schema(agg.keys(), agg.aggregate_table_index(), ctx)
            }
            OperatorKind::Remap(meta) => {
                let remap = Remap::borrow_raw_parts(meta, &self.common);
                compute_binding_schema(remap.table_index(), ctx)
            }
        }
    }
}

impl Operator {
    pub fn output_schema(&self, ctx: &crate::ir::context::IRContext) -> Result<OptdSchema> {
        self.get_property::<OutputSchema>(ctx)
    }
}

fn new_optd_schema(
    qualified_fields: Vec<(TableRef, Arc<Field>)>,
    metadata: std::collections::HashMap<String, String>,
) -> Result<OptdSchema> {
    OptdSchema::new_with_metadata(qualified_fields, metadata).context(SchemaSnafu)
}

fn compute_binding_schema(
    table_index: &i64,
    ctx: &crate::ir::context::IRContext,
) -> Result<OptdSchema> {
    let binding = ctx.get_binding(table_index)?;
    let table_ref = binding.table_ref().clone();
    let metadata = binding.schema().metadata().clone();
    let qualified_fields = binding
        .schema()
        .fields()
        .iter()
        .cloned()
        .map(|field| (table_ref.clone(), field))
        .collect_vec();
    new_optd_schema(qualified_fields, metadata)
}

fn compute_scan_schema(
    data_source_id: crate::ir::catalog::DataSourceId,
    table_index: &i64,
    projections: &[usize],
    ctx: &crate::ir::context::IRContext,
) -> Result<OptdSchema> {
    let meta = ctx.catalog.table(data_source_id).context(CatalogSnafu)?;
    let table_ref = ctx
        .get_binding(table_index)
        .ok()
        .map(|binding| binding.table_ref().clone())
        .unwrap_or_else(|| TableRef::from(&meta.table));
    let metadata = meta.schema.metadata().clone();
    let qualified_fields = projections
        .iter()
        .map(|i| (table_ref.clone(), Arc::new(meta.schema.field(*i).clone())))
        .collect_vec();
    new_optd_schema(qualified_fields, metadata)
}

/// Computes the output schema for join operators based on their join type.
/// This function handles different join types, adjusting the nullability
/// of the fields accordingly and adding a mark field if necessary.
fn compute_join_schema(
    outer: &Operator,
    inner: &Operator,
    join_type: &JoinType,
    ctx: &crate::ir::IRContext,
) -> Result<OptdSchema> {
    let (outer_nullable, inner_nullable, mark_field) = match join_type {
        JoinType::Inner => (false, false, None),
        JoinType::Left => (false, true, None),
        JoinType::Single => (false, true, None),
        JoinType::Mark(mark_column) => {
            let (mark_table_ref, mark_meta) = {
                let (table_ref, field) = ctx.get_column_name(mark_column)?;
                (table_ref, field)
            };
            (
                false,
                true,
                Some((
                    mark_table_ref,
                    Arc::new(Field::new(
                        mark_meta.name().clone(),
                        mark_meta.data_type().clone(),
                        true,
                    )),
                )),
            )
        }
    };

    let inner_schema = inner.output_schema(ctx)?;
    let outer_schema = outer.output_schema(ctx)?;

    let map_all_fields_to_nullable = |schema: &OptdSchema| {
        let metadata = schema.inner().metadata().clone();
        let qualified_fields = schema
            .iter()
            .map(|(table_ref, field)| {
                (
                    table_ref.clone(),
                    Arc::new(Field::new(
                        field.name().clone(),
                        field.data_type().clone(),
                        true,
                    )),
                )
            })
            .collect_vec();
        new_optd_schema(qualified_fields, metadata)
    };

    let outer_schema = if outer_nullable {
        map_all_fields_to_nullable(&outer_schema)?
    } else {
        outer_schema
    };
    let inner_schema = if inner_nullable {
        map_all_fields_to_nullable(&inner_schema)?
    } else {
        inner_schema
    };

    let mut metadata = outer_schema.inner().metadata().clone();
    metadata.extend(inner_schema.inner().metadata().clone());

    let qualified_fields = outer_schema
        .iter()
        .map(|(table_ref, field)| (table_ref.clone(), field.clone()))
        .chain(
            inner_schema
                .iter()
                .map(|(table_ref, field)| (table_ref.clone(), field.clone())),
        )
        .chain(mark_field)
        .collect_vec();

    new_optd_schema(qualified_fields, metadata)
}

fn compute_aggregate_schema(
    keys: &crate::ir::Scalar,
    aggregate_table_index: &i64,
    ctx: &crate::ir::IRContext,
) -> Result<OptdSchema> {
    let key_fields = keys
        .borrow::<List>()
        .members()
        .iter()
        .map(|key| {
            let key = match key.try_borrow::<ColumnRef>() {
                Ok(key) => key,
                Err(_) => whatever!("aggregate key must be a column reference"),
            };
            ctx.get_column_name(key.column())
        })
        .collect::<Result<Vec<_>>>()?;

    let aggregate_binding = ctx.get_binding(aggregate_table_index)?;
    let aggregate_table_ref = aggregate_binding.table_ref().clone();
    let metadata = aggregate_binding.schema().metadata().clone();
    let qualified_fields = key_fields
        .into_iter()
        .chain(
            aggregate_binding
                .schema()
                .fields()
                .iter()
                .cloned()
                .map(|field| (aggregate_table_ref.clone(), field)),
        )
        .collect_vec();

    new_optd_schema(qualified_fields, metadata)
}
