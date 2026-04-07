//! Context-aware builders for constructing IR nodes while hiding binding
//! registration details from callers.

use std::sync::Arc;

use arrow_schema::{Field, Schema, SchemaRef};
use snafu::{OptionExt, ResultExt};

use crate::{
    error::{CatalogSnafu, Result, whatever},
    ir::{
        Column, DataType, Group, GroupId, IRContext, Operator, Scalar, ScalarKind, ScalarValue,
        convert::{IntoOperator, IntoScalar},
        operator::{
            Aggregate, AggregateImplementation, DependentJoin, Get, GetImplementation, Join,
            JoinImplementation, JoinSide, Project, Remap, Select, join::JoinType,
        },
        properties::OperatorProperties,
        scalar::{
            BinaryOp, BinaryOpKind, Case, Cast, ColumnRef, Function, InList, Like, List, Literal,
            NaryOp, NaryOpKind,
        },
        table_ref::TableRef,
    },
};

pub struct OptdOperatorBuilder<'a> {
    ctx: &'a IRContext,
    operator: Arc<Operator>,
}

pub fn group(group_id: GroupId, properties: Arc<OperatorProperties>) -> Arc<Operator> {
    Group::new(group_id, properties).into_operator()
}

impl IRContext {
    pub fn logical_get<'a>(
        &'a self,
        table_ref: impl Into<TableRef>,
        projections: Option<Arc<[usize]>>,
    ) -> Result<OptdOperatorBuilder<'a>> {
        self.build_get_inner(table_ref.into(), projections, None)
            .map(|operator| OptdOperatorBuilder {
                ctx: self,
                operator,
            })
    }

    pub fn table_scan<'a>(
        &'a self,
        table_ref: impl Into<TableRef>,
        projections: Option<Arc<[usize]>>,
    ) -> Result<OptdOperatorBuilder<'a>> {
        self.build_get_inner(
            table_ref.into(),
            projections,
            Some(GetImplementation::TableScan),
        )
        .map(|operator| OptdOperatorBuilder {
            ctx: self,
            operator,
        })
    }

    pub fn project(
        &self,
        input: Arc<Operator>,
        projections: impl IntoIterator<Item = Arc<Scalar>>,
    ) -> Result<OptdOperatorBuilder<'_>> {
        let projections: Vec<_> = projections.into_iter().collect();
        let schema = derive_projection_schema(self, input.as_ref(), &projections)?;
        let table_index = self.add_binding(None, schema)?;
        let operator = Project::new(table_index, input, list(projections)).into_operator();
        Ok(OptdOperatorBuilder {
            ctx: self,
            operator,
        })
    }

    pub fn remap(
        &self,
        input: Arc<Operator>,
        table_ref: impl Into<TableRef>,
    ) -> Result<OptdOperatorBuilder<'_>> {
        let schema = input.output_schema(self)?.inner().clone();
        let table_index = self.add_binding(Some(table_ref.into()), schema)?;
        let operator = Remap::new(table_index, input).into_operator();
        Ok(OptdOperatorBuilder {
            ctx: self,
            operator,
        })
    }

    /// Helper function for building get.
    fn build_get_inner(
        &self,
        table_ref: TableRef,
        projections: Option<Arc<[usize]>>,
        implementation: Option<GetImplementation>,
    ) -> Result<Arc<Operator>> {
        let table = self
            .catalog
            .table_by_ref(&table_ref)
            .context(CatalogSnafu)?;
        let projections = projections
            .unwrap_or_else(|| (0..table.schema.fields().len()).collect::<Arc<[usize]>>());
        let schema = projected_schema(table.schema.clone(), &projections)?;
        let table_index = self.add_binding(Some(table_ref), schema)?;
        Ok(Get::new(table.id, table_index, projections, implementation).into_operator())
    }
}

impl Operator {
    pub fn with_ctx<'a>(self: Arc<Self>, ctx: &'a IRContext) -> OptdOperatorBuilder<'a> {
        OptdOperatorBuilder {
            ctx,
            operator: self,
        }
    }
}

impl<'a> OptdOperatorBuilder<'a> {
    pub fn build(self) -> Arc<Operator> {
        self.operator
    }

    pub fn logical_join(
        self,
        inner: Arc<Operator>,
        join_cond: Arc<Scalar>,
        join_type: JoinType,
    ) -> Self {
        Self {
            ctx: self.ctx,
            operator: Join::new(join_type, self.operator, inner, join_cond, None).into_operator(),
        }
    }

    pub fn logical_dependent_join(
        self,
        inner: Arc<Operator>,
        join_cond: Arc<Scalar>,
        join_type: JoinType,
    ) -> Self {
        Self {
            ctx: self.ctx,
            operator: DependentJoin::new(join_type, self.operator, inner, join_cond)
                .into_operator(),
        }
    }

    pub fn nested_loop(
        self,
        inner: Arc<Operator>,
        join_cond: Arc<Scalar>,
        join_type: JoinType,
    ) -> Self {
        Self {
            ctx: self.ctx,
            operator: Join::new(
                join_type,
                self.operator,
                inner,
                join_cond,
                Some(JoinImplementation::nested_loop()),
            )
            .into_operator(),
        }
    }

    pub fn hash_join(
        self,
        probe_side: Arc<Operator>,
        keys: Arc<[(Column, Column)]>,
        non_equi_conds: Arc<Scalar>,
        join_type: JoinType,
    ) -> Self {
        let join_cond = NaryOp::new(
            NaryOpKind::And,
            keys.iter()
                .map(|(l, r)| column_ref(*l).eq(column_ref(*r)))
                .chain(std::iter::once(non_equi_conds))
                .collect(),
        )
        .into_scalar();

        Self {
            ctx: self.ctx,
            operator: Join::new(
                join_type,
                self.operator,
                probe_side,
                join_cond,
                Some(JoinImplementation::hash(JoinSide::Outer, keys)),
            )
            .into_operator(),
        }
    }

    pub fn select(self, predicate: Arc<Scalar>) -> Self {
        Self {
            ctx: self.ctx,
            operator: Select::new(self.operator, predicate).into_operator(),
        }
    }

    pub fn logical_aggregate(
        self,
        exprs: impl IntoIterator<Item = Arc<Scalar>>,
        keys: impl IntoIterator<Item = Arc<Scalar>>,
    ) -> Result<Self> {
        self.aggregate(exprs, keys, None)
    }

    pub fn hash_aggregate(
        self,
        exprs: impl IntoIterator<Item = Arc<Scalar>>,
        keys: impl IntoIterator<Item = Arc<Scalar>>,
    ) -> Result<Self> {
        self.aggregate(exprs, keys, Some(AggregateImplementation::Hash))
    }

    fn aggregate(
        self,
        exprs: impl IntoIterator<Item = Arc<Scalar>>,
        keys: impl IntoIterator<Item = Arc<Scalar>>,
        implementation: Option<AggregateImplementation>,
    ) -> Result<Self> {
        let exprs: Vec<_> = exprs.into_iter().collect();
        let keys: Vec<_> = keys.into_iter().collect();
        let aggregate_schema = derive_aggregate_schema(self.ctx, self.operator.as_ref(), &exprs)?;
        let key_schema = derive_aggregate_schema(self.ctx, self.operator.as_ref(), &keys)?;
        let key_table_index = self.ctx.add_binding(None, key_schema)?;
        let aggregate_table_index = self.ctx.add_binding(None, aggregate_schema)?;

        Ok(Self {
            ctx: self.ctx,
            operator: Aggregate::new(
                key_table_index,
                aggregate_table_index,
                self.operator,
                list(exprs),
                list(keys),
                implementation,
            )
            .into_operator(),
        })
    }
}

/// Creates a raw column reference.
pub fn column_ref(column: Column) -> Arc<Scalar> {
    ColumnRef::new(column).into_scalar()
}

/// Creates a literal of type boolean.
pub fn boolean(v: impl Into<Option<bool>>) -> Arc<Scalar> {
    Literal::new(ScalarValue::Boolean(v.into())).into_scalar()
}

pub fn utf8<'a>(v: impl Into<Option<&'a str>>) -> Arc<Scalar> {
    Literal::new(ScalarValue::Utf8(v.into().map(|x| x.to_string()))).into_scalar()
}

pub fn utf8_view<'a>(v: impl Into<Option<&'a str>>) -> Arc<Scalar> {
    Literal::new(ScalarValue::Utf8View(v.into().map(|x| x.to_string()))).into_scalar()
}

pub fn literal<T: Into<ScalarValue>>(v: T) -> Arc<Scalar> {
    Literal::new(v.into()).into_scalar()
}

/// Creates a literal of type integer (i32).
pub fn int32(v: impl Into<Option<i32>>) -> Arc<Scalar> {
    Literal::new(ScalarValue::Int32(v.into())).into_scalar()
}

pub fn int64(v: impl Into<Option<i64>>) -> Arc<Scalar> {
    Literal::new(ScalarValue::Int64(v.into())).into_scalar()
}

pub fn cast(expr: Arc<Scalar>, data_type: DataType) -> Arc<Scalar> {
    Cast::new(data_type, expr).into_scalar()
}

pub fn like(
    expr: Arc<Scalar>,
    pattern: Arc<Scalar>,
    negated: bool,
    case_insensative: bool,
    escape_char: Option<char>,
) -> Arc<Scalar> {
    Like::new(expr, pattern, negated, case_insensative, escape_char).into_scalar()
}

pub fn in_list(expr: Arc<Scalar>, list: Arc<Scalar>, negated: bool) -> Arc<Scalar> {
    InList::new(expr, list, negated).into_scalar()
}

pub fn list(members: impl IntoIterator<Item = Arc<Scalar>>) -> Arc<Scalar> {
    List::new(members.into_iter().collect()).into_scalar()
}

impl Scalar {
    pub fn binary_op(self: Arc<Self>, rhs: Arc<Self>, op_kind: BinaryOpKind) -> Arc<Self> {
        BinaryOp::new(op_kind, self, rhs).into_scalar()
    }

    pub fn plus(self: Arc<Self>, rhs: Arc<Self>) -> Arc<Self> {
        self.binary_op(rhs, BinaryOpKind::Plus)
    }

    pub fn eq(self: Arc<Self>, rhs: Arc<Self>) -> Arc<Self> {
        self.binary_op(rhs, BinaryOpKind::Eq)
    }

    pub fn lt(self: Arc<Self>, rhs: Arc<Self>) -> Arc<Self> {
        self.binary_op(rhs, BinaryOpKind::Lt)
    }

    pub fn le(self: Arc<Self>, rhs: Arc<Self>) -> Arc<Self> {
        self.binary_op(rhs, BinaryOpKind::Le)
    }

    pub fn gt(self: Arc<Self>, rhs: Arc<Self>) -> Arc<Self> {
        self.binary_op(rhs, BinaryOpKind::Gt)
    }

    pub fn ge(self: Arc<Self>, rhs: Arc<Self>) -> Arc<Self> {
        self.binary_op(rhs, BinaryOpKind::Ge)
    }

    pub fn nary_op(self: Arc<Self>, rhs: Arc<Self>, op_kind: NaryOpKind) -> Arc<Self> {
        if let Ok(nary_op) = self.try_borrow::<NaryOp>()
            && nary_op.op_kind() == &op_kind
        {
            let terms = nary_op
                .terms()
                .iter()
                .cloned()
                .chain(std::iter::once(rhs))
                .collect();
            NaryOp::new(op_kind, terms).into_scalar()
        } else {
            NaryOp::new(op_kind, Arc::new([self, rhs])).into_scalar()
        }
    }

    pub fn and(self: Arc<Self>, rhs: Arc<Self>) -> Arc<Self> {
        self.nary_op(rhs, NaryOpKind::And)
    }

    pub fn or(self: Arc<Self>, rhs: Arc<Self>) -> Arc<Self> {
        self.nary_op(rhs, NaryOpKind::Or)
    }
}

fn projected_schema(schema: SchemaRef, projections: &[usize]) -> Result<SchemaRef> {
    let fields = projections
        .iter()
        .map(|index| {
            schema
                .fields()
                .get(*index)
                .cloned()
                .whatever_context(format!("projection index {index} out of range"))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(Schema::new_with_metadata(
        fields,
        schema.metadata().clone(),
    )))
}

fn derive_projection_schema(
    ctx: &IRContext,
    input: &Operator,
    projections: &[Arc<Scalar>],
) -> Result<SchemaRef> {
    let input_schema = input.output_schema(ctx)?;
    let fields = projections
        .iter()
        .enumerate()
        .map(|(idx, scalar)| derive_scalar_field(ctx, scalar.as_ref(), idx))
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(Schema::new_with_metadata(
        fields,
        input_schema.inner().metadata().clone(),
    )))
}

fn derive_aggregate_schema(
    ctx: &IRContext,
    input: &Operator,
    exprs: &[Arc<Scalar>],
) -> Result<SchemaRef> {
    let input_schema = input.output_schema(ctx)?;
    let fields = exprs
        .iter()
        .enumerate()
        .map(|(idx, scalar)| derive_scalar_field(ctx, scalar.as_ref(), idx))
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(Schema::new_with_metadata(
        fields,
        input_schema.inner().metadata().clone(),
    )))
}

fn derive_scalar_field(ctx: &IRContext, scalar: &Scalar, position: usize) -> Result<Field> {
    let derived = derive_scalar_descriptor(ctx, scalar, position)?;
    Ok(Field::new(
        derived.name,
        derived.data_type,
        derived.nullable,
    ))
}

struct DerivedScalarDescriptor {
    name: String,
    data_type: DataType,
    nullable: bool,
}

fn derive_scalar_descriptor(
    ctx: &IRContext,
    scalar: &Scalar,
    position: usize,
) -> Result<DerivedScalarDescriptor> {
    match &scalar.kind {
        ScalarKind::ColumnRef(meta) => {
            let column = ColumnRef::borrow_raw_parts(meta, &scalar.common);
            let (_, field) = ctx.get_column_name(column.column())?;
            Ok(DerivedScalarDescriptor {
                name: field.name().clone(),
                data_type: field.data_type().clone(),
                nullable: field.is_nullable(),
            })
        }
        ScalarKind::Literal(meta) => {
            let literal = Literal::borrow_raw_parts(meta, &scalar.common);
            Ok(DerivedScalarDescriptor {
                name: format!("lit{position}"),
                data_type: literal.value().data_type(),
                nullable: literal.value().is_null(),
            })
        }
        ScalarKind::BinaryOp(meta) => {
            let binary_op = BinaryOp::borrow_raw_parts(meta, &scalar.common);
            let lhs = derive_scalar_descriptor(ctx, binary_op.lhs(), position)?;
            let rhs = derive_scalar_descriptor(ctx, binary_op.rhs(), position)?;
            let data_type = match binary_op.op_kind() {
                BinaryOpKind::Plus
                | BinaryOpKind::Minus
                | BinaryOpKind::Multiply
                | BinaryOpKind::Divide
                | BinaryOpKind::Modulo => lhs.data_type.clone(),
                BinaryOpKind::Eq
                | BinaryOpKind::Ne
                | BinaryOpKind::IsNotDistinctFrom
                | BinaryOpKind::Lt
                | BinaryOpKind::Le
                | BinaryOpKind::Gt
                | BinaryOpKind::Ge => DataType::Boolean,
            };

            Ok(DerivedScalarDescriptor {
                name: format!("expr{position}"),
                data_type,
                nullable: lhs.nullable || rhs.nullable,
            })
        }
        ScalarKind::NaryOp(meta) => {
            let nary_op = NaryOp::borrow_raw_parts(meta, &scalar.common);
            Ok(DerivedScalarDescriptor {
                name: format!("expr{position}"),
                data_type: match nary_op.op_kind() {
                    NaryOpKind::And | NaryOpKind::Or => DataType::Boolean,
                },
                nullable: nary_op
                    .terms()
                    .iter()
                    .map(|term| derive_scalar_descriptor(ctx, term.as_ref(), position))
                    .collect::<Result<Vec<_>>>()?
                    .into_iter()
                    .any(|term| term.nullable),
            })
        }
        ScalarKind::Function(meta) => {
            let function = Function::borrow_raw_parts(meta, &scalar.common);
            Ok(DerivedScalarDescriptor {
                name: function.id().to_string(),
                data_type: function.return_type().clone(),
                nullable: true,
            })
        }
        ScalarKind::Cast(meta) => {
            let cast = Cast::borrow_raw_parts(meta, &scalar.common);
            let expr = derive_scalar_descriptor(ctx, cast.expr(), position)?;
            Ok(DerivedScalarDescriptor {
                name: expr.name,
                data_type: cast.data_type().clone(),
                nullable: expr.nullable,
            })
        }
        ScalarKind::InList(meta) => {
            let in_expr = InList::borrow_raw_parts(meta, &scalar.common);
            let expr = derive_scalar_descriptor(ctx, in_expr.expr(), position)?;
            let list = in_expr.list().borrow::<List>();
            let list_nullable = list
                .members()
                .iter()
                .map(|term| derive_scalar_descriptor(ctx, term.as_ref(), position))
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .any(|term| term.nullable);

            Ok(DerivedScalarDescriptor {
                name: format!("in{position}"),
                data_type: DataType::Boolean,
                nullable: expr.nullable || list_nullable,
            })
        }
        ScalarKind::Like(meta) => {
            let like = Like::borrow_raw_parts(meta, &scalar.common);
            Ok(DerivedScalarDescriptor {
                name: format!("like{position}"),
                data_type: DataType::Boolean,
                nullable: [like.expr(), like.pattern()]
                    .into_iter()
                    .map(|term| derive_scalar_descriptor(ctx, term.as_ref(), position))
                    .collect::<Result<Vec<_>>>()?
                    .into_iter()
                    .any(|term| term.nullable),
            })
        }
        ScalarKind::Case(meta) => {
            let case = Case::borrow_raw_parts(meta, &scalar.common);
            let result = case
                .when_then_expr()
                .next()
                .map(|(_, then)| then)
                .or_else(|| case.else_expr())
                .whatever_context("case expression should have at least one THEN or ELSE branch")
                .and_then(|expr| derive_scalar_descriptor(ctx, expr.as_ref(), position))?;

            let then_nullable = case
                .when_then_expr()
                .map(|(_, then)| derive_scalar_descriptor(ctx, then.as_ref(), position))
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .any(|term| term.nullable);
            let else_nullable = case
                .else_expr()
                .map(|expr| derive_scalar_descriptor(ctx, expr.as_ref(), position))
                .transpose()?
                .map(|term| term.nullable)
                .unwrap_or(true);

            Ok(DerivedScalarDescriptor {
                name: format!("expr{position}"),
                data_type: result.data_type,
                nullable: then_nullable || else_nullable,
            })
        }
        ScalarKind::List(_) => whatever!("cannot derive a field for a scalar list expression"),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};

    use super::*;
    use crate::{
        ir::{
            catalog::Catalog,
            convert::IntoScalar,
            operator::{Aggregate, Get, Project, Remap},
            scalar::Function,
        },
        magic::MemoryCatalog,
    };

    fn test_ctx() -> IRContext {
        let catalog = MemoryCatalog::new("optd", "public");
        catalog
            .create_table(
                TableRef::bare("t1"),
                Arc::new(Schema::new(vec![
                    Field::new("v1", DataType::Int64, true),
                    Field::new("v2", DataType::Int32, false),
                    Field::new("v3", DataType::Boolean, true),
                ])),
                None,
            )
            .unwrap();
        IRContext::with_memory_catalog(catalog)
    }

    #[test]
    fn logical_get_builder_registers_projected_binding() -> Result<()> {
        let ctx = test_ctx();

        let get = ctx
            .logical_get(TableRef::bare("t1"), Some(Arc::from([1_usize, 2_usize])))?
            .build();
        let get = get.borrow::<Get>();
        let binding = ctx.get_binding(get.table_index())?;

        assert_eq!(binding.table_ref(), &TableRef::bare("t1"));
        assert_eq!(binding.schema().fields().len(), 2);
        assert_eq!(binding.schema().field(0).name(), "v2");
        assert_eq!(binding.schema().field(1).name(), "v3");
        Ok(())
    }

    #[test]
    fn project_and_remap_use_ctx_registered_schemas() -> Result<()> {
        let ctx = test_ctx();
        let get = ctx.logical_get(TableRef::bare("t1"), None)?.build();

        let projected = ctx
            .project(
                get,
                [
                    column_ref(ctx.col(Some(&TableRef::bare("t1")), "v1")?),
                    int32(7),
                ],
            )?
            .build();
        let project = projected.borrow::<Project>();
        let project_binding = ctx.get_binding(project.table_index())?;
        assert_eq!(project_binding.schema().fields().len(), 2);
        assert_eq!(project_binding.schema().field(0).name(), "v1");
        assert_eq!(project_binding.schema().field(1).name(), "lit1");

        let remapped = ctx.remap(projected, TableRef::bare("alias"))?.build();
        let remap = remapped.borrow::<Remap>();
        let remap_binding = ctx.get_binding(remap.table_index())?;

        assert_eq!(remap_binding.table_ref(), &TableRef::bare("alias"));
        assert_eq!(remap_binding.schema().fields().len(), 2);
        assert_eq!(remap_binding.schema().field(0).name(), "v1");
        Ok(())
    }

    #[test]
    fn aggregate_builder_registers_output_binding() -> Result<()> {
        let ctx = test_ctx();
        let get = ctx.logical_get(TableRef::bare("t1"), None)?.build();
        let table_index = *get.borrow::<Get>().table_index();

        let aggregate = get
            .with_ctx(&ctx)
            .logical_aggregate(
                [Function::new_aggregate("count", Arc::new([]), DataType::Int64).into_scalar()],
                [column_ref(Column(table_index, 1))],
            )?
            .build();
        let aggregate = aggregate.borrow::<Aggregate>();
        let binding = ctx.get_binding(aggregate.aggregate_table_index())?;

        assert_eq!(binding.schema().fields().len(), 1);
        assert_eq!(binding.schema().field(0).name(), "count");
        assert_eq!(binding.schema().field(0).data_type(), &DataType::Int64);
        Ok(())
    }

    #[test]
    fn project_builder_derives_in_scalar_as_boolean() -> Result<()> {
        let ctx = test_ctx();
        let get = ctx.logical_get(TableRef::bare("t1"), None)?.build();

        let projected = ctx
            .project(
                get,
                [in_list(
                    column_ref(ctx.col(Some(&TableRef::bare("t1")), "v2")?),
                    list([int32(7), int32(8)]),
                    false,
                )],
            )?
            .build();
        let project = projected.borrow::<Project>();
        let binding = ctx.get_binding(project.table_index())?;

        assert_eq!(binding.schema().field(0).name(), "in0");
        assert_eq!(binding.schema().field(0).data_type(), &DataType::Boolean);
        assert!(!binding.schema().field(0).is_nullable());
        Ok(())
    }

    #[test]
    fn project_builder_marks_in_nullable_when_list_contains_null() -> Result<()> {
        let ctx = test_ctx();
        let get = ctx.logical_get(TableRef::bare("t1"), None)?.build();

        let projected = ctx
            .project(
                get,
                [in_list(
                    column_ref(ctx.col(Some(&TableRef::bare("t1")), "v2")?),
                    list([int32(7), int32(None)]),
                    false,
                )],
            )?
            .build();
        let project = projected.borrow::<Project>();
        let binding = ctx.get_binding(project.table_index())?;

        assert!(binding.schema().field(0).is_nullable());
        Ok(())
    }
}
