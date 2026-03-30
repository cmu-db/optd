//! This module defines the `OutputSchema` property for operators in the IR,
//! allowing retrieval of the output schema based on the operator type and its
//! metadata.

use crate::error::{CatalogSnafu, Result, SchemaSnafu, whatever};
use crate::ir::Operator;
use crate::ir::{
    OperatorKind,
    catalog::Field,
    operator::{
        Aggregate, DependentJoin, EnforcerSort, Get, Join, Limit, OrderBy, Project, Remap, Select,
        Subquery, join::JoinType,
    },
    properties::{Derive, GetProperty, PropertyMarker},
    scalar::{ColumnRef, List, ScalarKind},
    schema::OptdSchema,
    table_ref::TableRef,
};
use itertools::Itertools;
use snafu::ResultExt;
use std::sync::Arc;

type QualifiedField = (TableRef, Arc<Field>);

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
            OperatorKind::Limit(meta) => {
                let limit = Limit::borrow_raw_parts(meta, &self.common);
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
                compute_aggregate_schema(
                    agg.key_table_index(),
                    agg.aggregate_table_index(),
                    agg.keys(),
                    ctx,
                )
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
    let outer_schema = outer.output_schema(ctx)?;
    let inner_schema = inner.output_schema(ctx)?;

    match join_schema_shape(join_type, ctx)? {
        JoinSchemaShape::OuterOnly => Ok(outer_schema),
        JoinSchemaShape::Combined {
            outer_nullable,
            inner_nullable,
            mark_field,
        } => {
            let outer_schema = if outer_nullable {
                make_schema_nullable(&outer_schema)?
            } else {
                outer_schema
            };
            let inner_schema = if inner_nullable {
                make_schema_nullable(&inner_schema)?
            } else {
                inner_schema
            };

            let mut metadata = outer_schema.inner().metadata().clone();
            metadata.extend(inner_schema.inner().metadata().clone());

            let qualified_fields = collect_qualified_fields(&outer_schema)
                .chain(collect_qualified_fields(&inner_schema))
                .chain(mark_field)
                .collect_vec();

            new_optd_schema(qualified_fields, metadata)
        }
    }
}

enum JoinSchemaShape {
    OuterOnly,
    Combined {
        outer_nullable: bool,
        inner_nullable: bool,
        mark_field: Option<QualifiedField>,
    },
}

fn join_schema_shape(join_type: &JoinType, ctx: &crate::ir::IRContext) -> Result<JoinSchemaShape> {
    match join_type {
        JoinType::Inner => Ok(JoinSchemaShape::Combined {
            outer_nullable: false,
            inner_nullable: false,
            mark_field: None,
        }),
        JoinType::LeftOuter | JoinType::Single => Ok(JoinSchemaShape::Combined {
            outer_nullable: false,
            inner_nullable: true,
            mark_field: None,
        }),
        JoinType::LeftSemi | JoinType::LeftAnti => Ok(JoinSchemaShape::OuterOnly),
        JoinType::Mark(mark_column) => {
            let (mark_table_ref, mark_meta) = ctx.get_column_name(mark_column)?;
            Ok(JoinSchemaShape::Combined {
                outer_nullable: false,
                inner_nullable: true,
                mark_field: Some((
                    mark_table_ref,
                    Arc::new(Field::new(
                        mark_meta.name().clone(),
                        mark_meta.data_type().clone(),
                        true,
                    )),
                )),
            })
        }
    }
}

fn make_schema_nullable(schema: &OptdSchema) -> Result<OptdSchema> {
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
}

fn collect_qualified_fields(schema: &OptdSchema) -> impl Iterator<Item = QualifiedField> + '_ {
    schema
        .iter()
        .map(|(table_ref, field)| (table_ref.clone(), field.clone()))
}

fn compute_aggregate_schema(
    key_table_index: &i64,
    aggregate_table_index: &i64,
    keys: &crate::ir::Scalar,
    ctx: &crate::ir::IRContext,
) -> Result<OptdSchema> {
    let key_binding = ctx.get_binding(key_table_index)?;
    let key_table_ref = key_binding.table_ref().clone();
    let key_exprs = keys.borrow::<List>();
    let metadata = key_binding.schema().metadata().clone();
    let qualified_fields = key_binding
        .schema()
        .fields()
        .iter()
        .cloned()
        .zip_eq(key_exprs.members().iter())
        .map(|(field, key_expr)| {
            Ok((
                aggregate_key_qualifier(&key_table_ref, &field, key_expr, ctx)?,
                field,
            ))
        })
        .collect::<Result<Vec<_>>>()?;

    let key_schema = new_optd_schema(qualified_fields, metadata)?;

    let aggregate_binding = ctx.get_binding(aggregate_table_index)?;
    let aggregate_table_ref = aggregate_binding.table_ref().clone();
    let metadata = aggregate_binding.schema().metadata().clone();
    let qualified_fields = aggregate_binding
        .schema()
        .fields()
        .iter()
        .cloned()
        .map(|field| (aggregate_table_ref.clone(), field))
        .collect_vec();

    let aggregate_schema = new_optd_schema(qualified_fields, metadata)?;

    key_schema.join(&aggregate_schema).context(SchemaSnafu)
}

fn aggregate_key_qualifier(
    default_table_ref: &TableRef,
    bound_field: &Arc<Field>,
    key_expr: &crate::ir::Scalar,
    ctx: &crate::ir::IRContext,
) -> Result<TableRef> {
    match &key_expr.kind {
        ScalarKind::ColumnRef(meta) => {
            let column = ColumnRef::borrow_raw_parts(meta, &key_expr.common);
            let (source_table_ref, source_field) = ctx.get_column_name(column.column())?;
            if source_field.name() == bound_field.name() {
                Ok(source_table_ref)
            } else {
                Ok(default_table_ref.clone())
            }
        }
        _ => Ok(default_table_ref.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::{
        builder::{boolean, column_ref, list},
        catalog::{Field as CatalogField, Schema as CatalogSchema},
        convert::IntoOperator,
        operator::{Aggregate, join::JoinType},
        table_ref::TableRef,
        test_utils::test_ctx_with_tables,
    };
    use std::sync::Arc;

    #[test]
    fn left_semi_join_output_schema_uses_only_outer_columns() -> Result<()> {
        let ctx = test_ctx_with_tables(&[("t1", 2), ("t2", 3)])?;
        let outer = ctx.logical_get(TableRef::bare("t1"), None)?.build();
        let inner = ctx.logical_get(TableRef::bare("t2"), None)?.build();

        let join = outer
            .with_ctx(&ctx)
            .logical_join(inner, boolean(true), JoinType::LeftSemi)
            .build();

        let schema = join.output_schema(&ctx)?;
        let fields = schema.iter().collect_vec();

        assert_eq!(fields.len(), 2);
        assert!(
            fields
                .iter()
                .all(|(table_ref, _)| **table_ref == TableRef::bare("t1"))
        );
        assert_eq!(fields[0].1.name(), "c0");
        assert_eq!(fields[1].1.name(), "c1");

        Ok(())
    }

    #[test]
    fn left_anti_join_output_schema_uses_only_outer_columns() -> Result<()> {
        let ctx = test_ctx_with_tables(&[("t1", 2), ("t2", 3)])?;
        let outer = ctx.logical_get(TableRef::bare("t1"), None)?.build();
        let inner = ctx.logical_get(TableRef::bare("t2"), None)?.build();

        let join = outer
            .with_ctx(&ctx)
            .logical_join(inner, boolean(true), JoinType::LeftAnti)
            .build();

        let schema = join.output_schema(&ctx)?;
        let fields = schema.iter().collect_vec();

        assert_eq!(fields.len(), 2);
        assert!(
            fields
                .iter()
                .all(|(table_ref, _)| **table_ref == TableRef::bare("t1"))
        );
        assert_eq!(fields[0].1.name(), "c0");
        assert_eq!(fields[1].1.name(), "c1");

        Ok(())
    }

    #[test]
    fn aggregate_output_schema_keeps_passthrough_key_qualifier() -> Result<()> {
        let ctx = test_ctx_with_tables(&[("part", 2)])?;
        let input = ctx.logical_get(TableRef::bare("part"), None)?.build();
        let key_table_index = ctx.add_binding(
            None,
            Arc::new(CatalogSchema::new(vec![CatalogField::new(
                "c0",
                crate::ir::DataType::Int32,
                false,
            )])),
        )?;
        let aggregate_table_index = ctx.add_binding(
            None,
            Arc::new(CatalogSchema::new(Vec::<CatalogField>::new())),
        )?;

        let aggregate = Aggregate::new(
            key_table_index,
            aggregate_table_index,
            input,
            list([]),
            list([column_ref(crate::ir::test_utils::test_col(
                &ctx, "part", "c0",
            )?)]),
            None,
        )
        .into_operator();

        let schema = aggregate.output_schema(&ctx)?;
        let fields = schema.iter().collect_vec();

        assert_eq!(fields.len(), 1);
        assert_eq!(*fields[0].0, TableRef::bare("part"));
        assert_eq!(fields[0].1.name(), "c0");

        Ok(())
    }

    #[test]
    fn aggregate_output_schema_uses_internal_qualifier_for_renamed_key() -> Result<()> {
        let ctx = test_ctx_with_tables(&[("part", 2)])?;
        let input = ctx.logical_get(TableRef::bare("part"), None)?.build();
        let key_table_index = ctx.add_binding(
            None,
            Arc::new(CatalogSchema::new(vec![CatalogField::new(
                "alias1",
                crate::ir::DataType::Int32,
                false,
            )])),
        )?;
        let aggregate_table_index = ctx.add_binding(
            None,
            Arc::new(CatalogSchema::new(Vec::<CatalogField>::new())),
        )?;
        let key_binding = ctx.get_binding(&key_table_index)?;

        let aggregate = Aggregate::new(
            key_table_index,
            aggregate_table_index,
            input,
            list([]),
            list([column_ref(crate::ir::test_utils::test_col(
                &ctx, "part", "c0",
            )?)]),
            None,
        )
        .into_operator();

        let schema = aggregate.output_schema(&ctx)?;
        let fields = schema.iter().collect_vec();

        assert_eq!(fields.len(), 1);
        assert_eq!(*fields[0].0, key_binding.table_ref);
        assert_eq!(fields[0].1.name(), "alias1");

        Ok(())
    }
}
