use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use datafusion::{
    catalog::memory::DataSourceExec,
    common::{
        DFSchema,
        tree_node::{TransformedResult, TreeNode},
    },
    datasource::{physical_plan::ParquetSource, source_as_provider},
    error::DataFusionError,
    execution::{SessionState, context::QueryPlanner},
    logical_expr::{self, ExprSchemable, LogicalPlan, PlanType, StringifiedPlan, logical_plan},
    physical_expr::{LexOrdering, aggregate::AggregateExprBuilder, create_physical_sort_expr},
    physical_plan::{
        ExecutionPlan,
        aggregates::PhysicalGroupBy,
        displayable,
        explain::ExplainExec,
        filter::FilterExec,
        joins::{
            HashJoinExec, NestedLoopJoinExec, PartitionMode, SortMergeJoinExec, utils::JoinFilter,
        },
        projection::ProjectionExec,
        sorts::sort::SortExec,
        udaf::AggregateFunctionExpr,
    },
    physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner},
    sql::TableReference,
};
use itertools::{Either, Itertools};
use optd_core::{
    cascades::Cascades,
    connector_err,
    error::Result as OptdResult,
    ir::{
        IRContext, Scalar,
        builder::{self as optd_builder, column_assign, column_ref, list, literal},
        catalog::{Field, Schema},
        convert::{IntoOperator, IntoScalar},
        explain::quick_explain,
        operator::{
            EnforcerSort, LogicalOrderBy, LogicalRemap, PhysicalFilter, PhysicalHashAggregate,
            PhysicalHashJoin, PhysicalNLJoin, PhysicalProject, PhysicalTableScan, join,
        },
        properties::TupleOrderingDirection,
        rule::RuleSet,
        scalar::{
            BinaryOp, Cast, ColumnAssign, ColumnRef, Function, FunctionKind, Like, List, NaryOp,
            NaryOpKind,
        },
    },
    rules,
};
use snafu::{OptionExt, ResultExt};
use tracing::{info, warn};

use crate::{
    OptdExtensionConfig,
    value::{from_optd_value, try_into_optd_value},
};

#[derive(Default)]
pub struct OptdQueryPlanner {
    default: DefaultPhysicalPlanner,
}

impl std::fmt::Debug for OptdQueryPlanner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OptdQueryPlanner").finish_non_exhaustive()
    }
}

fn tuple_err<T, R>(
    value: (datafusion::common::Result<T>, datafusion::common::Result<R>),
) -> datafusion::common::Result<(T, R)> {
    match value {
        (Ok(e), Ok(e1)) => Ok((e, e1)),
        (Err(e), Ok(_)) => Err(e),
        (Ok(_), Err(e1)) => Err(e1),
        (Err(e), Err(_)) => Err(e),
    }
}

impl OptdQueryPlanner {
    pub async fn try_into_optd_logical_plan(
        &self,
        df_logical: &LogicalPlan,
        ctx: &IRContext,
        session_state: &SessionState,
    ) -> OptdResult<Arc<optd_core::ir::Operator>> {
        match df_logical {
            LogicalPlan::TableScan(table_scan) => {
                self.try_into_optd_logical_get(table_scan, ctx, session_state)
                    .await
            }
            LogicalPlan::Filter(filter) => {
                self.try_into_optd_logical_select(filter, ctx, session_state)
                    .await
            }
            LogicalPlan::Join(join) => {
                self.try_into_optd_logical_join(join, ctx, session_state)
                    .await
            }
            LogicalPlan::Projection(project) => {
                self.try_into_optd_logical_project(project, ctx, session_state)
                    .await
            }
            LogicalPlan::Aggregate(agg) => {
                self.try_into_optd_logical_aggregate(agg, ctx, session_state)
                    .await
            }
            LogicalPlan::Sort(sort) => {
                self.try_into_optd_logical_order_by(sort, ctx, session_state)
                    .await
            }
            LogicalPlan::SubqueryAlias(alias) => {
                self.try_into_optd_logical_remap(alias, ctx, session_state)
                    .await
            }
            plan => {
                connector_err!("Unsupported DataFusion logical plan: {:?}", plan);
            }
        }
    }

    pub async fn try_into_optd_logical_order_by(
        &self,
        node: &logical_plan::Sort,
        ctx: &IRContext,
        session_state: &SessionState,
    ) -> OptdResult<Arc<optd_core::ir::Operator>> {
        let input =
            Box::pin(self.try_into_optd_logical_plan(&node.input, ctx, session_state)).await?;
        let ordering_exprs = Box::pin(self.try_into_optd_ordering_exprs(
            &node.expr,
            node.input.schema(),
            ctx,
            session_state,
        ))
        .await?;

        Ok(LogicalOrderBy::new(input, ordering_exprs).into_operator())
    }

    pub async fn try_into_optd_logical_remap(
        &self,
        node: &logical_plan::SubqueryAlias,
        ctx: &IRContext,
        session_state: &SessionState,
    ) -> OptdResult<Arc<optd_core::ir::Operator>> {
        let input =
            Box::pin(self.try_into_optd_logical_plan(&node.input, ctx, session_state)).await?;

        let input_schema = input.output_schema(ctx)?;

        let mut mappings = Vec::new();
        for (field, remapped_column_name) in
            node.schema.fields().iter().zip(node.schema.field_names())
        {
            let (index, _) = node
                .input
                .schema()
                .fields()
                .find(field.name())
                .whatever_context("cannot find field name in the input schema")?;
            let optd_field = &input_schema.fields[index];
            let column = ctx
                .column_by_name(optd_field.name())
                .whatever_context("cannot find column by name in ctx")?;
            let mapped =
                ctx.define_column(optd_field.data_type().clone(), Some(remapped_column_name));
            mappings.push(column_assign(mapped, column_ref(column)));
        }
        Ok(LogicalRemap::new(input, list(mappings)).into_operator())
    }

    pub async fn try_into_optd_ordering_exprs(
        &self,
        sort_exprs: &[logical_expr::SortExpr],
        input_schema: &DFSchema,
        ctx: &IRContext,
        session_state: &SessionState,
    ) -> OptdResult<Vec<(Arc<Scalar>, TupleOrderingDirection)>> {
        // TODO(yuchen) support NULL first / last.
        let mut ordering_exprs = Vec::with_capacity(sort_exprs.len());
        for sort_expr in sort_exprs {
            let expr = Box::pin(self.try_into_optd_scalar(
                &sort_expr.expr,
                input_schema,
                ctx,
                session_state,
            ))
            .await?;
            let direction = if sort_expr.asc {
                TupleOrderingDirection::Asc
            } else {
                TupleOrderingDirection::Desc
            };
            ordering_exprs.push((expr, direction))
        }
        Ok(ordering_exprs)
    }

    pub async fn try_into_optd_logical_aggregate(
        &self,
        node: &logical_plan::Aggregate,
        ctx: &IRContext,
        session_state: &SessionState,
    ) -> OptdResult<Arc<optd_core::ir::Operator>> {
        let input =
            Box::pin(self.try_into_optd_logical_plan(&node.input, ctx, session_state)).await?;

        let keys = Box::pin(self.try_into_optd_expr_list(
            &node.group_expr,
            node.input.schema(),
            true,
            ctx,
            session_state,
        ))
        .await?;

        let exprs = Box::pin(self.try_into_optd_expr_list(
            &node.aggr_expr,
            node.input.schema(),
            true,
            ctx,
            session_state,
        ))
        .await?;

        Ok(input.logical_aggregate(exprs, keys))
    }

    pub async fn try_into_optd_expr_list(
        &self,
        exprs: &[logical_expr::Expr],
        input_schema: &DFSchema,
        should_project: bool,
        ctx: &IRContext,
        session_state: &SessionState,
    ) -> OptdResult<Vec<Arc<Scalar>>> {
        let mut expr_list = Vec::with_capacity(exprs.len());
        for maybe_aliased in exprs {
            let unaliased = match maybe_aliased {
                logical_expr::Expr::Alias(alias) => &alias.expr,
                other => other,
            };

            let optd_expr = self
                .try_into_optd_scalar(unaliased, input_schema, ctx, session_state)
                .await?;

            let expr = if should_project {
                let name = maybe_aliased
                    .name_for_alias()
                    .whatever_context("failed to get DataFusion name for alias")?;
                let data_type = maybe_aliased.get_type(input_schema).unwrap();
                let column = if let Ok(column_ref) = optd_expr.try_borrow::<ColumnRef>() {
                    let column = *column_ref.column();
                    ctx.rename_column_alias(column, name);
                    column
                } else {
                    ctx.define_column(data_type.clone(), Some(name))
                };
                column_assign(column, optd_expr)
            } else {
                optd_expr
            };

            expr_list.push(expr);
        }
        Ok(expr_list)
    }

    pub async fn try_into_optd_logical_project(
        &self,
        node: &logical_plan::Projection,
        ctx: &IRContext,
        session_state: &SessionState,
    ) -> OptdResult<Arc<optd_core::ir::Operator>> {
        let input =
            Box::pin(self.try_into_optd_logical_plan(&node.input, ctx, session_state)).await?;

        let projections = Box::pin(self.try_into_optd_expr_list(
            &node.expr,
            node.input.schema(),
            true,
            ctx,
            session_state,
        ))
        .await?;

        Ok(input.logical_project(projections))
    }

    pub async fn try_into_optd_logical_join(
        &self,
        node: &logical_plan::Join,
        ctx: &IRContext,
        session_state: &SessionState,
    ) -> OptdResult<Arc<optd_core::ir::Operator>> {
        let join_type = match node.join_type {
            logical_expr::JoinType::Inner => optd_core::ir::operator::join::JoinType::Inner,
            logical_expr::JoinType::Left => optd_core::ir::operator::join::JoinType::Left,
            v => connector_err!("Unsupported join type: {:?}", v),
        };
        let left =
            Box::pin(self.try_into_optd_logical_plan(&node.left, ctx, session_state)).await?;
        let right =
            Box::pin(self.try_into_optd_logical_plan(&node.right, ctx, session_state)).await?;

        let mut terms = Vec::with_capacity(node.on.len());
        for (left_key, right_key) in node.on.iter() {
            let lhs = Box::pin(self.try_into_optd_scalar(
                left_key,
                node.left.schema(),
                ctx,
                session_state,
            ))
            .await?;
            let rhs = Box::pin(self.try_into_optd_scalar(
                right_key,
                node.right.schema(),
                ctx,
                session_state,
            ))
            .await?;
            terms.push(lhs.eq(rhs));
        }

        if let Some(filter) = &node.filter {
            let non_equi_conds =
                Box::pin(self.try_into_optd_scalar(filter, &node.schema, ctx, session_state))
                    .await?;
            terms.push(non_equi_conds);
        }

        let join_cond = NaryOp::new(NaryOpKind::And, terms.into()).into_scalar();

        Ok(left.logical_join(right, join_cond, join_type))
    }

    pub async fn try_into_optd_logical_select(
        &self,
        node: &logical_plan::Filter,
        ctx: &IRContext,
        session_state: &SessionState,
    ) -> OptdResult<Arc<optd_core::ir::Operator>> {
        let input =
            Box::pin(self.try_into_optd_logical_plan(&node.input, ctx, session_state)).await?;
        let predicate = Box::pin(self.try_into_optd_scalar(
            &node.predicate,
            node.input.schema(),
            ctx,
            session_state,
        ))
        .await?;

        Ok(input.logical_select(predicate))
    }

    pub async fn try_from_optd_expr_list(
        &self,
        exprs: &[Arc<Scalar>],
        should_project: bool,
        ctx: &IRContext,
        session_state: &SessionState,
    ) -> datafusion::error::Result<Vec<logical_expr::Expr>> {
        let mut df_exprs = Vec::with_capacity(exprs.len());

        for expr in exprs {
            if should_project {
                let column_assign = expr.borrow::<ColumnAssign>();
                let column_meta = ctx.get_column_meta(column_assign.column());
                let e =
                    Box::pin(self.try_from_optd_scalar(column_assign.expr(), ctx, session_state))
                        .await?;
                let split = column_meta.name.split(".").collect_vec();
                df_exprs.push(e.alias_if_changed(split.last().unwrap().to_string())?);
            } else {
                let e = Box::pin(self.try_from_optd_scalar(expr, ctx, session_state)).await?;
                df_exprs.push(e);
            }
        }
        Ok(df_exprs)
    }

    pub async fn try_from_optd_agg_exprs(
        &self,
        exprs: &[Arc<Scalar>],
        input_dfschema: &DFSchema,
        ctx: &IRContext,
        session_state: &SessionState,
    ) -> datafusion::error::Result<Vec<Arc<AggregateFunctionExpr>>> {
        let mut df_exprs = Vec::with_capacity(exprs.len());

        for expr in exprs {
            let column_assign = expr.borrow::<ColumnAssign>();
            let column_meta = ctx.get_column_meta(column_assign.column());
            let func = column_assign.expr().borrow::<Function>();
            assert_eq!(func.kind(), &FunctionKind::Aggregate);
            let udf = session_state
                .aggregate_functions()
                .get(func.id().as_ref())
                .ok_or_else(|| {
                    DataFusionError::External(format!("unknown agg UDF: {}", func.id()).into())
                })?
                .clone();
            let args = self
                .try_from_optd_expr_list(func.params(), false, ctx, session_state)
                .await?
                .iter()
                .map(|arg| {
                    self.default
                        .create_physical_expr(arg, input_dfschema, session_state)
                })
                .collect::<Result<_, _>>()
                .unwrap();
            let agg_func = AggregateExprBuilder::new(udf, args)
                .schema(input_dfschema.inner().clone())
                .alias(column_meta.name.clone())
                .build()
                .map(Arc::new)?;
            df_exprs.push(agg_func);
        }
        Ok(df_exprs)
    }

    pub async fn try_from_optd_scalar(
        &self,
        optd_scalar: &Scalar,
        ctx: &IRContext,
        session_state: &SessionState,
    ) -> datafusion::common::Result<logical_expr::Expr> {
        match &optd_scalar.kind {
            optd_core::ir::ScalarKind::Literal(meta) => {
                let literal =
                    optd_core::ir::scalar::Literal::borrow_raw_parts(meta, &optd_scalar.common);
                let value = from_optd_value(literal.value().clone());
                Ok(logical_expr::Expr::Literal(value, None))
            }
            optd_core::ir::ScalarKind::ColumnRef(meta) => {
                let column_ref = ColumnRef::borrow_raw_parts(meta, &optd_scalar.common);
                let column = column_ref.column();
                let meta = ctx.get_column_meta(column);
                Ok(logical_expr::Expr::Column(
                    datafusion::common::Column::from_qualified_name(&meta.name),
                ))
            }
            optd_core::ir::ScalarKind::BinaryOp(meta) => {
                let binary_op = BinaryOp::borrow_raw_parts(meta, &optd_scalar.common);
                let op = match binary_op.op_kind() {
                    optd_core::ir::scalar::BinaryOpKind::Plus => logical_expr::Operator::Plus,
                    optd_core::ir::scalar::BinaryOpKind::Minus => logical_expr::Operator::Minus,
                    optd_core::ir::scalar::BinaryOpKind::Multiply => {
                        logical_expr::Operator::Multiply
                    }
                    optd_core::ir::scalar::BinaryOpKind::Divide => logical_expr::Operator::Divide,
                    optd_core::ir::scalar::BinaryOpKind::Modulo => logical_expr::Operator::Modulo,
                    optd_core::ir::scalar::BinaryOpKind::Eq => logical_expr::Operator::Eq,
                    optd_core::ir::scalar::BinaryOpKind::Lt => logical_expr::Operator::Lt,
                    optd_core::ir::scalar::BinaryOpKind::Le => logical_expr::Operator::LtEq,
                    optd_core::ir::scalar::BinaryOpKind::Gt => logical_expr::Operator::Gt,
                    optd_core::ir::scalar::BinaryOpKind::Ge => logical_expr::Operator::GtEq,
                };
                let left = Box::pin(self.try_from_optd_scalar(binary_op.lhs(), ctx, session_state))
                    .await?;
                let right =
                    Box::pin(self.try_from_optd_scalar(binary_op.rhs(), ctx, session_state))
                        .await?;
                Ok(logical_expr::binary_expr(left, op, right))
            }
            optd_core::ir::ScalarKind::NaryOp(meta) => {
                let nary_op = NaryOp::borrow_raw_parts(meta, &optd_scalar.common);
                let op = match nary_op.op_kind() {
                    NaryOpKind::And => logical_expr::Operator::And,
                    NaryOpKind::Or => logical_expr::Operator::Or,
                };

                let mut exprs = Vec::with_capacity(nary_op.terms().len());
                for term in nary_op.terms().iter() {
                    let expr =
                        Box::pin(self.try_from_optd_scalar(term, ctx, session_state)).await?;
                    exprs.push(expr);
                }

                let x = exprs
                    .into_iter()
                    .reduce(|l, r| logical_expr::binary_expr(l, op, r))
                    .unwrap();

                Ok(x)
            }
            optd_core::ir::ScalarKind::Cast(meta) => {
                let cast = Cast::borrow_raw_parts(meta, &optd_scalar.common);
                let expr =
                    Box::pin(self.try_from_optd_scalar(cast.expr(), ctx, session_state)).await?;
                Ok(logical_expr::cast(expr, cast.data_type().clone()))
            }
            optd_core::ir::ScalarKind::Function(_) => todo!(),
            optd_core::ir::ScalarKind::ColumnAssign(_) => todo!(),
            optd_core::ir::ScalarKind::List(_) => todo!(),
            optd_core::ir::ScalarKind::Like(meta) => {
                let like = Like::borrow_raw_parts(meta, &optd_scalar.common);
                let expr =
                    Box::pin(self.try_from_optd_scalar(like.expr(), ctx, session_state)).await?;
                let pattern =
                    Box::pin(self.try_from_optd_scalar(like.pattern(), ctx, session_state)).await?;
                Ok(logical_expr::Expr::Like(logical_expr::Like::new(
                    *like.negated(),
                    Box::new(expr),
                    Box::new(pattern),
                    *like.escape_char(),
                    *like.case_insensative(),
                )))
            }
        }
    }

    pub async fn try_into_optd_scalar(
        &self,
        node: &logical_expr::Expr,
        input_schema: &DFSchema,
        ctx: &IRContext,
        session_state: &SessionState,
    ) -> OptdResult<Arc<optd_core::ir::Scalar>> {
        match node {
            logical_expr::Expr::Column(column) => Ok(column_ref({
                let column_name = column.flat_name();
                ctx.column_by_name(column_name.as_str())
                    .unwrap_or_else(|| panic!("{column_name} not found"))
            })),

            logical_expr::Expr::Literal(v, _) => Ok(literal(try_into_optd_value(v.clone())?)),
            logical_expr::Expr::BinaryExpr(binary_expr) => {
                let op_kind = match &binary_expr.op {
                    logical_expr::Operator::Eq => {
                        Either::Left(optd_core::ir::scalar::BinaryOpKind::Eq)
                    }
                    logical_expr::Operator::Plus => {
                        Either::Left(optd_core::ir::scalar::BinaryOpKind::Plus)
                    }
                    logical_expr::Operator::Minus => {
                        Either::Left(optd_core::ir::scalar::BinaryOpKind::Minus)
                    }
                    logical_expr::Operator::Multiply => {
                        Either::Left(optd_core::ir::scalar::BinaryOpKind::Multiply)
                    }
                    logical_expr::Operator::Divide => {
                        Either::Left(optd_core::ir::scalar::BinaryOpKind::Divide)
                    }
                    logical_expr::Operator::Modulo => {
                        Either::Left(optd_core::ir::scalar::BinaryOpKind::Modulo)
                    }
                    logical_expr::Operator::Lt => {
                        Either::Left(optd_core::ir::scalar::BinaryOpKind::Lt)
                    }
                    logical_expr::Operator::LtEq => {
                        Either::Left(optd_core::ir::scalar::BinaryOpKind::Le)
                    }
                    logical_expr::Operator::Gt => {
                        Either::Left(optd_core::ir::scalar::BinaryOpKind::Gt)
                    }
                    logical_expr::Operator::GtEq => {
                        Either::Left(optd_core::ir::scalar::BinaryOpKind::Ge)
                    }
                    logical_expr::Operator::And => {
                        Either::Right(optd_core::ir::scalar::NaryOpKind::And)
                    }
                    logical_expr::Operator::Or => {
                        Either::Right(optd_core::ir::scalar::NaryOpKind::Or)
                    }
                    op => {
                        connector_err!("Unsupported binary expr op: {:?}", op);
                    }
                };

                let left = Box::pin(self.try_into_optd_scalar(
                    &binary_expr.left,
                    input_schema,
                    ctx,
                    session_state,
                ))
                .await?;
                let right = Box::pin(self.try_into_optd_scalar(
                    &binary_expr.right,
                    input_schema,
                    ctx,
                    session_state,
                ))
                .await?;

                match op_kind {
                    Either::Left(op_kind) => Ok(left.binary_op(right, op_kind)),
                    Either::Right(op_kind) => Ok(left.nary_op(right, op_kind)),
                }
            }
            logical_expr::Expr::Alias(alias) => {
                Box::pin(self.try_into_optd_scalar(&alias.expr, input_schema, ctx, session_state))
                    .await
            }
            logical_expr::Expr::AggregateFunction(agg_func) => {
                let func_name = agg_func.func.name();
                let params = Box::pin(self.try_into_optd_expr_list(
                    &agg_func.params.args,
                    input_schema,
                    false,
                    ctx,
                    session_state,
                ))
                .await?;
                let input_types = agg_func
                    .params
                    .args
                    .iter()
                    .map(|x| x.to_field(input_schema).unwrap().1.data_type().clone())
                    .collect_vec();
                let return_type = agg_func.func.return_type(&input_types).unwrap();
                Ok(
                    Function::new_aggregate(func_name.to_string(), params.into(), return_type)
                        .into_scalar(),
                )
            }
            logical_expr::Expr::Cast(cast) => {
                let input = Box::pin(self.try_into_optd_scalar(
                    &cast.expr,
                    input_schema,
                    ctx,
                    session_state,
                ))
                .await?;
                Ok(optd_builder::cast(input, cast.data_type.clone()))
            }
            logical_expr::Expr::Like(like) => {
                let expr = Box::pin(self.try_into_optd_scalar(
                    &like.expr,
                    input_schema,
                    ctx,
                    session_state,
                ))
                .await?;
                let pattern = Box::pin(self.try_into_optd_scalar(
                    &like.pattern,
                    input_schema,
                    ctx,
                    session_state,
                ))
                .await?;

                Ok(optd_builder::like(
                    expr,
                    pattern,
                    like.negated,
                    like.case_insensitive,
                    like.escape_char,
                ))
            }
            expr => {
                connector_err!("Unsupported df logical expr: {:?}", expr);
            }
        }
    }

    pub async fn try_into_optd_logical_get(
        &self,
        node: &logical_plan::TableScan,
        ctx: &IRContext,
        session_state: &SessionState,
    ) -> OptdResult<Arc<optd_core::ir::Operator>> {
        let table_name = node
            .table_name
            .clone()
            .resolve("datafusion", "default")
            .to_string();
        let df_schema =
            DFSchema::try_from_qualified_schema(node.table_name.clone(), &node.source.schema())
                .unwrap();
        let schema = into_optd_schema(&df_schema)?;

        let source = ctx
            .cat
            .try_create_table(table_name, schema.clone())
            .unwrap_or_else(|existing| existing);
        ctx.add_base_table_columns(source, &schema);

        let provider = source_as_provider(&node.source).unwrap();
        let exec = provider
            .scan(session_state, node.projection.as_ref(), &[], None)
            .await
            .unwrap();

        ctx.cat.set_table_row_count(
            source,
            exec.partition_statistics(None)
                .map(|x| match x.num_rows {
                    datafusion::common::stats::Precision::Exact(x) => x,
                    datafusion::common::stats::Precision::Inexact(x) => x,
                    datafusion::common::stats::Precision::Absent => 1000,
                })
                .unwrap_or(1000),
        );
        let mut projections = node
            .projection
            .as_ref()
            .map(|x| x.iter().cloned().collect::<HashSet<_>>())
            .unwrap_or((0..schema.fields().len()).collect());

        let predicate = {
            let filters = node.filters.iter().collect_vec();
            let supports_filter_pushdown = provider.supports_filters_pushdown(&filters).unwrap();
            if supports_filter_pushdown
                .iter()
                .all(|x| x.ne(&logical_expr::TableProviderFilterPushDown::Exact))
            {
                None
            } else {
                let mut terms = Vec::with_capacity(node.filters.len());
                for filter in node.filters.iter() {
                    filter.column_refs().iter().for_each(|column| {
                        if let Ok(index) = node.source.schema().index_of(column.name()) {
                            projections.insert(index);
                        } else {
                            panic!()
                        }
                    });
                    let term = self
                        .try_into_optd_scalar(filter, &df_schema, ctx, session_state)
                        .await
                        .unwrap();

                    terms.push(term);
                }
                terms.into_iter().reduce(Scalar::and)
            }
        };

        let x = ctx.logical_get(
            source,
            &schema,
            Some(projections.into_iter().sorted().collect()),
        );
        let x = match predicate {
            Some(p) => x.logical_select(p),
            None => x,
        };

        Ok(x)
    }

    pub async fn try_from_optd_physical_plan(
        &self,
        optd_physical: &optd_core::ir::Operator,
        ctx: &IRContext,
        session_state: &SessionState,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        use optd_core::ir::OperatorKind;

        match &optd_physical.kind {
            OperatorKind::PhysicalTableScan(meta) => {
                let physical = PhysicalTableScan::borrow_raw_parts(meta, &optd_physical.common);
                let table_metadata = ctx.cat.describe_table(*physical.source());
                let table_ref = TableReference::from(table_metadata.name.clone());
                let schema = session_state.schema_for_ref(table_metadata.name.clone())?;
                let table = schema
                    .table(table_ref.table())
                    .await?
                    .ok_or_else(|| DataFusionError::Internal("Bad".to_string()))?;
                let projections = physical.projections().iter().cloned().collect();
                info!("converted from optd physical table scan");
                Ok(table
                    .scan(session_state, Some(&projections), &[], None)
                    .await
                    .unwrap())
            }
            OperatorKind::PhysicalFilter(meta) => {
                let physical = PhysicalFilter::borrow_raw_parts(meta, &optd_physical.common);
                let input = Box::pin(self.try_from_optd_physical_plan(
                    physical.input(),
                    ctx,
                    session_state,
                ))
                .await?;
                let input_schema = from_optd_schema(&physical.input().output_schema(ctx).unwrap());
                let predicate = self
                    .try_from_optd_scalar(physical.predicate(), ctx, session_state)
                    .await?;
                let predicate =
                    self.default
                        .create_physical_expr(&predicate, &input_schema, session_state)?;

                let plan = Arc::new(FilterExec::try_new(predicate, input)?);
                Ok(plan as Arc<dyn ExecutionPlan>)
            }
            OperatorKind::PhysicalNLJoin(meta) => {
                let join = PhysicalNLJoin::borrow_raw_parts(meta, &optd_physical.common);
                let left_exec =
                    Box::pin(self.try_from_optd_physical_plan(join.outer(), ctx, session_state))
                        .await?;
                let right_exec =
                    Box::pin(self.try_from_optd_physical_plan(join.inner(), ctx, session_state))
                        .await?;
                let join_type = match join.join_type() {
                    join::JoinType::Inner => logical_expr::JoinType::Inner,
                    join::JoinType::Left => logical_expr::JoinType::Left,
                };
                let left_dfschema = from_optd_schema(&join.outer().output_schema(ctx).unwrap());
                let right_dfschema = from_optd_schema(&join.inner().output_schema(ctx).unwrap());

                let join_cond =
                    Box::pin(self.try_from_optd_scalar(join.join_cond(), ctx, session_state))
                        .await?;

                let cols = join_cond.column_refs();

                // Collect left & right field indices, the field indices are sorted in ascending order
                let left_field_indices = cols
                    .iter()
                    .filter_map(|c| left_dfschema.index_of_column(c).ok())
                    .sorted()
                    .collect::<Vec<_>>();
                let right_field_indices = cols
                    .iter()
                    .filter_map(|c| right_dfschema.index_of_column(c).ok())
                    .sorted()
                    .collect::<Vec<_>>();

                // Collect DFFields and Fields required for intermediate schemas
                let (filter_df_fields, filter_fields): (Vec<_>, Vec<_>) = left_field_indices
                    .clone()
                    .into_iter()
                    .map(|i| {
                        (
                            left_dfschema.qualified_field(i),
                            left_exec.schema().field(i).clone(),
                        )
                    })
                    .chain(right_field_indices.clone().into_iter().map(|i| {
                        (
                            right_dfschema.qualified_field(i),
                            right_exec.schema().field(i).clone(),
                        )
                    }))
                    .unzip();
                let filter_df_fields = filter_df_fields
                    .into_iter()
                    .map(|(qualifier, field)| (qualifier.cloned(), field.clone()))
                    .collect();

                let metadata: HashMap<_, _> = left_dfschema
                    .metadata()
                    .clone()
                    .into_iter()
                    .chain(right_dfschema.metadata().clone())
                    .collect();

                // Construct intermediate schemas used for filtering data and
                // convert logical expression to physical according to filter schema
                let filter_dfschema =
                    DFSchema::new_with_metadata(filter_df_fields, metadata.clone())?;
                let filter_schema = datafusion::arrow::datatypes::Schema::new_with_metadata(
                    filter_fields,
                    metadata,
                );
                let join_cond = self.default.create_physical_expr(
                    &join_cond,
                    &filter_dfschema,
                    session_state,
                )?;

                let column_indices =
                    JoinFilter::build_column_indices(left_field_indices, right_field_indices);

                let join_filter =
                    JoinFilter::new(join_cond, column_indices, Arc::new(filter_schema));

                let plan = Arc::new(NestedLoopJoinExec::try_new(
                    left_exec,
                    right_exec,
                    Some(join_filter),
                    &join_type,
                    None,
                )?);
                Ok(plan as Arc<dyn ExecutionPlan>)
            }
            OperatorKind::PhysicalHashJoin(meta) => {
                let join = PhysicalHashJoin::borrow_raw_parts(meta, &optd_physical.common);
                let build_side_exec = Box::pin(self.try_from_optd_physical_plan(
                    join.build_side(),
                    ctx,
                    session_state,
                ))
                .await?;
                let probe_side_exec = Box::pin(self.try_from_optd_physical_plan(
                    join.probe_side(),
                    ctx,
                    session_state,
                ))
                .await?;
                let join_type = match join.join_type() {
                    join::JoinType::Inner => logical_expr::JoinType::Inner,
                    join::JoinType::Left => logical_expr::JoinType::Left,
                };
                let build_side_dfschema =
                    from_optd_schema(&join.build_side().output_schema(ctx).unwrap());
                let probe_side_dfschema =
                    from_optd_schema(&join.probe_side().output_schema(ctx).unwrap());

                let mut join_on = Vec::with_capacity(join.keys().len());
                for (l_col, r_col) in join.keys().iter() {
                    let left_key = Box::pin(self.try_from_optd_scalar(
                        &column_ref(*l_col),
                        ctx,
                        session_state,
                    ))
                    .await
                    .and_then(|expr| {
                        self.default.create_physical_expr(
                            &expr,
                            &build_side_dfschema,
                            session_state,
                        )
                    })?;

                    let right_key = Box::pin(self.try_from_optd_scalar(
                        &column_ref(*r_col),
                        ctx,
                        session_state,
                    ))
                    .await
                    .and_then(|expr| {
                        self.default.create_physical_expr(
                            &expr,
                            &probe_side_dfschema,
                            session_state,
                        )
                    })?;
                    join_on.push((left_key, right_key));
                }

                let post_join_cond = self
                    .try_from_optd_scalar(join.non_equi_conds(), ctx, session_state)
                    .await?;

                let cols = post_join_cond.column_refs();

                // Collect build side & probe side field indices, the field indices are sorted in ascending order
                let build_field_indices = cols
                    .iter()
                    .filter_map(|c| build_side_dfschema.index_of_column(c).ok())
                    .sorted()
                    .collect::<Vec<_>>();
                let probe_side_field_indices = cols
                    .iter()
                    .filter_map(|c| probe_side_dfschema.index_of_column(c).ok())
                    .sorted()
                    .collect::<Vec<_>>();

                // Collect DFFields and Fields required for intermediate schemas
                let (filter_df_fields, filter_fields): (Vec<_>, Vec<_>) = build_field_indices
                    .clone()
                    .into_iter()
                    .map(|i| {
                        (
                            build_side_dfschema.qualified_field(i),
                            build_side_exec.schema().field(i).clone(),
                        )
                    })
                    .chain(probe_side_field_indices.clone().into_iter().map(|i| {
                        (
                            probe_side_dfschema.qualified_field(i),
                            probe_side_exec.schema().field(i).clone(),
                        )
                    }))
                    .unzip();
                let filter_df_fields = filter_df_fields
                    .into_iter()
                    .map(|(qualifier, field)| (qualifier.cloned(), field.clone()))
                    .collect();

                let metadata: HashMap<_, _> = build_side_dfschema
                    .metadata()
                    .clone()
                    .into_iter()
                    .chain(probe_side_dfschema.metadata().clone())
                    .collect();

                // Construct intermediate schemas used for filtering data and
                // convert logical expression to physical according to filter schema
                let filter_dfschema =
                    DFSchema::new_with_metadata(filter_df_fields, metadata.clone())?;
                let filter_schema = datafusion::arrow::datatypes::Schema::new_with_metadata(
                    filter_fields,
                    metadata,
                );
                let post_join_cond = self.default.create_physical_expr(
                    &post_join_cond,
                    &filter_dfschema,
                    session_state,
                )?;

                let column_indices =
                    JoinFilter::build_column_indices(build_field_indices, probe_side_field_indices);

                let join_filter =
                    JoinFilter::new(post_join_cond, column_indices, Arc::new(filter_schema));
                Ok(Arc::new(HashJoinExec::try_new(
                    build_side_exec,
                    probe_side_exec,
                    join_on,
                    Some(join_filter),
                    &join_type,
                    None,
                    PartitionMode::CollectLeft,
                    datafusion::common::NullEquality::NullEqualsNothing,
                )?) as Arc<dyn ExecutionPlan>)
            }
            OperatorKind::PhysicalProject(meta) => {
                let physical_project =
                    PhysicalProject::borrow_raw_parts(meta, &optd_physical.common);

                let input_exec = Box::pin(self.try_from_optd_physical_plan(
                    physical_project.input(),
                    ctx,
                    session_state,
                ))
                .await?;

                let input_logical_schema =
                    from_optd_schema(&physical_project.input().output_schema(ctx).unwrap());
                let projections = physical_project.projections().borrow::<List>();

                let mut expr = Vec::with_capacity(projections.members().len());

                for member in projections.members() {
                    let column_assign = member.borrow::<ColumnAssign>();
                    let column_meta = ctx.get_column_meta(column_assign.column());
                    let e = self
                        .try_from_optd_scalar(column_assign.expr(), ctx, session_state)
                        .await?;

                    let split = column_meta.name.split(".").collect_vec();
                    expr.push(e.alias_if_changed(split.last().unwrap().to_string())?);
                }

                let input_physical_schema = input_exec.schema();
                let physical_exprs = expr
                    .iter()
                    .map(|e| {
                        // For projections, SQL planner and logical plan builder may convert user
                        // provided expressions into logical Column expressions if their results
                        // are already provided from the input plans. Because we work with
                        // qualified columns in logical plane, derived columns involve operators or
                        // functions will contain qualifiers as well. This will result in logical
                        // columns with names like `SUM(t1.c1)`, `t1.c1 + t1.c2`, etc.
                        //
                        // If we run these logical columns through physical_name function, we will
                        // get physical names with column qualifiers, which violates DataFusion's
                        // field name semantics. To account for this, we need to derive the
                        // physical name from physical input instead.
                        //
                        // This depends on the invariant that logical schema field index MUST match
                        // with physical schema field index.
                        let physical_name = if let logical_expr::Expr::Column(col) = e {
                            match input_logical_schema.index_of_column(col) {
                                Ok(idx) => {
                                    // index physical field using logical field index
                                    Ok(input_exec.schema().field(idx).name().to_string())
                                }
                                // logical column is not a derived column, safe to pass along to
                                // physical_name
                                Err(_) => logical_expr::expr::physical_name(e),
                            }
                        } else {
                            logical_expr::expr::physical_name(e)
                        };

                        let physical_expr = self.default.create_physical_expr(
                            e,
                            &input_logical_schema,
                            session_state,
                        );

                        // Check for possible column name mismatches
                        let final_physical_expr =
                            maybe_fix_physical_column_name(physical_expr, &input_physical_schema);

                        tuple_err((final_physical_expr, physical_name))
                    })
                    .collect::<datafusion::common::Result<Vec<_>>>()
                    .unwrap();

                warn!(name= %input_exec.name(), schema = ?input_exec.schema());
                Ok(
                    Arc::new(ProjectionExec::try_new(physical_exprs, input_exec).unwrap())
                        as Arc<dyn ExecutionPlan>,
                )
            }
            OperatorKind::PhysicalHashAggregate(meta) => {
                let agg = PhysicalHashAggregate::borrow_raw_parts(meta, &optd_physical.common);
                let input_exec =
                    Box::pin(self.try_from_optd_physical_plan(agg.input(), ctx, session_state))
                        .await?;

                let input_dfschema = from_optd_schema(&agg.input().output_schema(ctx).unwrap());
                let agg_exprs = Box::pin(self.try_from_optd_agg_exprs(
                    agg.exprs().borrow::<List>().members(),
                    &input_dfschema,
                    ctx,
                    session_state,
                ))
                .await?;

                let keys = Box::pin(self.try_from_optd_expr_list(
                    agg.keys().borrow::<List>().members(),
                    true,
                    ctx,
                    session_state,
                ))
                .await?
                .iter()
                .map(|expr| {
                    self.default
                        .create_physical_expr(expr, &input_dfschema, session_state)
                        .map(|e| (e, expr.name_for_alias().unwrap()))
                })
                .collect::<Result<Vec<_>, _>>()?;
                let group_exprs = PhysicalGroupBy::new_single(keys);

                let agg_num = agg_exprs.len();
                let schema = input_exec.schema().clone();

                Ok(Arc::new(
                    datafusion::physical_plan::aggregates::AggregateExec::try_new(
                        datafusion::physical_plan::aggregates::AggregateMode::Single,
                        group_exprs,
                        agg_exprs,
                        vec![None; agg_num],
                        input_exec,
                        schema,
                    )
                    .unwrap(),
                ) as Arc<dyn ExecutionPlan + 'static>)
            }
            OperatorKind::EnforcerSort(meta) => {
                let sort = EnforcerSort::borrow_raw_parts(meta, &optd_physical.common);
                let input_exec =
                    Box::pin(self.try_from_optd_physical_plan(sort.input(), ctx, session_state))
                        .await?;
                let input_dfschema = from_optd_schema(&sort.input().output_schema(ctx).unwrap());
                let mut physical_sort_exprs = Vec::with_capacity(sort.tuple_ordering().len());

                for (column, direction) in sort.tuple_ordering().iter() {
                    let expr = Box::pin(self.try_from_optd_scalar(
                        &column_ref(*column),
                        ctx,
                        session_state,
                    ))
                    .await?;
                    let (asc, nulls_first) = (direction.is_asc(), !direction.is_asc());

                    let e = logical_expr::SortExpr::new(expr, asc, nulls_first);
                    let physical_sort_expr = create_physical_sort_expr(
                        &e,
                        &input_dfschema,
                        session_state.execution_props(),
                    )?;
                    physical_sort_exprs.push(physical_sort_expr);
                }
                Ok(Arc::new(SortExec::new(
                    LexOrdering::new(physical_sort_exprs).unwrap(),
                    input_exec,
                )) as Arc<dyn ExecutionPlan>)
            }
            OperatorKind::LogicalRemap(meta) => {
                let remap = LogicalRemap::borrow_raw_parts(meta, &optd_physical.common);

                let input_exec =
                    Box::pin(self.try_from_optd_physical_plan(remap.input(), ctx, session_state))
                        .await?;

                let input_logical_schema =
                    from_optd_schema(&remap.input().output_schema(ctx).unwrap());
                let mappings = remap.mappings().borrow::<List>();

                let mut expr = Vec::with_capacity(mappings.members().len());

                for member in mappings.members() {
                    let column_assign = member.borrow::<ColumnAssign>();
                    let column_meta = ctx.get_column_meta(column_assign.column());
                    let e = self
                        .try_from_optd_scalar(column_assign.expr(), ctx, session_state)
                        .await?;

                    let split = column_meta.name.split(".").collect_vec();
                    expr.push(e.alias_if_changed(split.last().unwrap().to_string())?);
                }

                let input_physical_schema = input_exec.schema();
                let physical_exprs = expr
                    .iter()
                    .map(|e| {
                        // For projections, SQL planner and logical plan builder may convert user
                        // provided expressions into logical Column expressions if their results
                        // are already provided from the input plans. Because we work with
                        // qualified columns in logical plane, derived columns involve operators or
                        // functions will contain qualifiers as well. This will result in logical
                        // columns with names like `SUM(t1.c1)`, `t1.c1 + t1.c2`, etc.
                        //
                        // If we run these logical columns through physical_name function, we will
                        // get physical names with column qualifiers, which violates DataFusion's
                        // field name semantics. To account for this, we need to derive the
                        // physical name from physical input instead.
                        //
                        // This depends on the invariant that logical schema field index MUST match
                        // with physical schema field index.
                        let physical_name = if let logical_expr::Expr::Column(col) = e {
                            match input_logical_schema.index_of_column(col) {
                                Ok(idx) => {
                                    // index physical field using logical field index
                                    Ok(input_exec.schema().field(idx).name().to_string())
                                }
                                // logical column is not a derived column, safe to pass along to
                                // physical_name
                                Err(_) => logical_expr::expr::physical_name(e),
                            }
                        } else {
                            logical_expr::expr::physical_name(e)
                        };

                        let physical_expr = self.default.create_physical_expr(
                            e,
                            &input_logical_schema,
                            session_state,
                        );

                        // Check for possible column name mismatches
                        let final_physical_expr =
                            maybe_fix_physical_column_name(physical_expr, &input_physical_schema);

                        tuple_err((final_physical_expr, physical_name))
                    })
                    .collect::<datafusion::common::Result<Vec<_>>>()
                    .unwrap();

                // warn!(?input_exec);
                Ok(
                    Arc::new(ProjectionExec::try_new(physical_exprs, input_exec).unwrap())
                        as Arc<dyn ExecutionPlan>,
                )
            }
            x => Err(DataFusionError::External(
                format!("{x:?} cannot be executed on df yet").into(),
            )),
        }
    }
}

fn into_optd_schema(df_schema: &DFSchema) -> OptdResult<Arc<optd_core::ir::catalog::Schema>> {
    let x = df_schema
        .columns()
        .iter()
        .zip(df_schema.as_arrow().fields())
        .map(|(column, f)| {
            Ok(Arc::new(Field::new(
                column.flat_name(),
                f.data_type().clone(),
                f.is_nullable(),
            )))
        })
        .collect::<OptdResult<Vec<_>>>()?;
    Ok(Arc::new(Schema::new(x)))
}

fn from_optd_schema(schema: &optd_core::ir::catalog::Schema) -> Arc<DFSchema> {
    let mut builder = datafusion::arrow::datatypes::SchemaBuilder::new();
    let qualifiers = schema
        .fields()
        .iter()
        .map(|f| {
            let column = datafusion::prelude::Column::from_qualified_name(f.name().clone());

            builder.push(Arc::new(datafusion::arrow::datatypes::Field::new(
                column.name,
                f.data_type().clone(),
                f.is_nullable(),
            )));
            column.relation
        })
        .collect::<Vec<_>>();

    Arc::new(
        DFSchema::from_field_specific_qualified_schema(qualifiers, &Arc::new(builder.finish()))
            .unwrap(),
    )
}

// Handle the case where the name of a physical column expression does not match the corresponding physical input fields names.
// Physical column names are derived from the physical schema, whereas physical column expressions are derived from the logical column names.
//
// This is a special case that applies only to column expressions. Logical plans may slightly modify column names by appending a suffix (e.g., using ':'),
// to avoid duplicates—since DFSchemas do not allow duplicate names. For example: `count(Int64(1)):1`.
fn maybe_fix_physical_column_name(
    expr: datafusion::common::Result<Arc<dyn datafusion::physical_plan::PhysicalExpr>>,
    input_physical_schema: &datafusion::arrow::datatypes::SchemaRef,
) -> datafusion::common::Result<Arc<dyn datafusion::physical_plan::PhysicalExpr>> {
    use datafusion::common::tree_node::Transformed;
    use datafusion::physical_plan::expressions::Column;
    let Ok(expr) = expr else { return expr };
    expr.transform_down(|node| {
        if let Some(column) = node.as_any().downcast_ref::<Column>() {
            let idx = column.index();
            let physical_field = input_physical_schema.field(idx);
            let expr_col_name = column.name();
            let physical_name = physical_field.name();

            if expr_col_name != physical_name {
                // handle edge cases where the physical_name contains ':'.
                let colon_count = physical_name.matches(':').count();
                let mut splits = expr_col_name.match_indices(':');
                let split_pos = splits.nth(colon_count);

                if let Some((i, _)) = split_pos {
                    let base_name = &expr_col_name[..i];
                    if base_name == physical_name {
                        let updated_column =
                            datafusion::physical_plan::expressions::Column::new(physical_name, idx);
                        return Ok(datafusion::common::tree_node::Transformed::yes(Arc::new(
                            updated_column,
                        )));
                    }
                }
            }

            // If names already match or fix is not possible, just leave it as it is
            Ok(Transformed::no(node))
        } else {
            Ok(Transformed::no(node))
        }
    })
    .data()
}

impl OptdQueryPlanner {
    /// optd's actual implementation of [`QueryPlanner::create_physical_plan`].
    async fn create_physical_plan_inner(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        match session_state
            .config_options()
            .extensions
            .get::<OptdExtensionConfig>()
        {
            Some(config) if config.optd_enabled => (),
            _ => {
                return self
                    .default
                    .create_physical_plan(logical_plan, session_state)
                    .await;
            }
        }
        let ctx = IRContext::with_empty_magic();

        let (actual_logical_plan, mut explain) = match logical_plan {
            LogicalPlan::Explain(explain) => (explain.plan.as_ref(), Some(explain.clone())),
            _ => (logical_plan, None),
        };

        let optd_logical = self
            .try_into_optd_logical_plan(actual_logical_plan, &ctx, session_state)
            .await
            .map_err(|e| DataFusionError::External(e.into()))?;

        // let Ok(optd_logical) = res else {
        //     return self
        //         .create_physical_plan_default(logical_plan, session_state)
        //         .await;
        // };

        let rule_set = RuleSet::builder()
            .add_rule(rules::LogicalGetAsPhysicalTableScanRule::new())
            .add_rule(rules::LogicalAggregateAsPhysicalHashAggregateRule::new())
            .add_rule(rules::LogicalJoinAsPhysicalHashJoinRule::new())
            .add_rule(rules::LogicalJoinAsPhysicalNLJoinRule::new())
            .add_rule(rules::LogicalProjectAsPhysicalProjectRule::new())
            .add_rule(rules::LogicalSelectAsPhysicalFilterRule::new())
            .add_rule(rules::LogicalJoinInnerCommuteRule::new())
            .add_rule(rules::LogicalJoinInnerAssocRule::new())
            .build();
        let opt = Arc::new(Cascades::new(ctx, rule_set));
        let Some(optd_physical) = opt.optimize(&optd_logical, Arc::default()).await else {
            {
                opt.memo.read().await.dump();
            }
            warn!("optimization failed");
            return self
                .create_physical_plan_default(logical_plan, session_state)
                .await;
        };

        {
            opt.memo.read().await.dump();
        }
        info!("got a plan:\n{}", quick_explain(&optd_physical, &opt.ctx));

        let physical_plan = self
            .try_from_optd_physical_plan(&optd_physical, &opt.ctx, session_state)
            .await?;

        info!("Converted into df");

        if let Some(x) = explain.as_mut() {
            let s = quick_explain(&optd_logical, &opt.ctx);
            x.stringified_plans.push(StringifiedPlan::new(
                PlanType::OptimizedLogicalPlan {
                    optimizer_name: "optd-conversion".to_string(),
                },
                s,
            ));
        }

        if let Some(x) = explain.as_mut() {
            let s = quick_explain(&optd_physical, &opt.ctx);
            x.stringified_plans.push(StringifiedPlan::new(
                PlanType::OptimizedPhysicalPlan {
                    optimizer_name: "optd-finalized".to_string(),
                },
                s.clone(),
            ));
            x.stringified_plans
                .push(StringifiedPlan::new(PlanType::FinalPhysicalPlan, s));
        }

        if let Some(x) = explain.as_mut() {
            let config = &session_state.config_options().explain;
            x.stringified_plans.push(StringifiedPlan::new(
                PlanType::FinalPhysicalPlan,
                displayable(physical_plan.as_ref())
                    .set_show_statistics(config.show_statistics)
                    .set_show_schema(config.show_schema)
                    .indent(x.verbose)
                    .to_string(),
            ));

            // Show statistics + schema in verbose output even if not
            // explicitly requested
            if x.verbose {
                if !config.show_statistics {
                    x.stringified_plans.push(StringifiedPlan::new(
                        PlanType::FinalPhysicalPlanWithStats,
                        displayable(physical_plan.as_ref())
                            .set_show_statistics(true)
                            .indent(x.verbose)
                            .to_string(),
                    ));
                }
                if !config.show_schema {
                    x.stringified_plans.push(StringifiedPlan::new(
                        PlanType::FinalPhysicalPlanWithSchema,
                        // This will include schema if show_schema is on
                        // and will be set to true if verbose is on
                        displayable(physical_plan.as_ref())
                            .set_show_schema(true)
                            .indent(x.verbose)
                            .to_string(),
                    ));
                }
            }
        }

        Ok(explain
            .map(|x| {
                Arc::new(ExplainExec::new(
                    Arc::clone(x.schema.inner()),
                    x.stringified_plans,
                    x.verbose,
                )) as Arc<dyn ExecutionPlan>
            })
            .unwrap_or(physical_plan))
    }

    async fn create_physical_plan_default(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        return self
            .default
            .create_physical_plan(logical_plan, session_state)
            .await;
    }
}

impl QueryPlanner for OptdQueryPlanner {
    fn create_physical_plan<'a, 'b, 'c, 'ret>(
        &'a self,
        logical_plan: &'b LogicalPlan,
        session_state: &'c SessionState,
    ) -> ::core::pin::Pin<
        Box<dyn Future<Output = datafusion::common::Result<Arc<dyn ExecutionPlan>>> + Send + 'ret>,
    >
    where
        'a: 'ret,
        'b: 'ret,
        'c: 'ret,
        Self: 'ret,
    {
        Box::pin(async {
            let res = self
                .create_physical_plan_inner(logical_plan, session_state)
                .await;

            match res {
                Err(e) => {
                    warn!(error = %e, "optd planner failed, fallback to default planner");
                    return self
                        .create_physical_plan_default(logical_plan, session_state)
                        .await;
                }
                Ok(plan) => Ok(plan),
            }
        })
    }
}

#[derive(Debug, Eq, PartialEq, Hash, PartialOrd, Ord)]
#[allow(dead_code)]
enum JoinOrder {
    Table(String),
    HashJoin(Box<Self>, Box<Self>),
    MergeJoin(Box<Self>, Box<Self>),
    NestedLoopJoin(Box<Self>, Box<Self>),
    Other(Box<Self>),
}

#[allow(dead_code)]
impl JoinOrder {
    fn write_graphviz(&self, graphviz: &mut String) {
        graphviz.push_str("digraph G {\n");
        graphviz.push_str("\trankdir = BT\n");

        let mut counter = 0;
        self.visit(graphviz, &mut counter);
        graphviz.push('}');
    }

    fn add_base_table(label: String, graphviz: &mut String, counter: &mut usize) -> usize {
        let id = *counter;
        *counter += 1;
        graphviz.push_str(&format!("\tnode{id} [label=\"{label}\"]\n"));
        id
    }

    fn add_edge(from: usize, to: usize, graphviz: &mut String) {
        graphviz.push_str(&format!("\tnode{from} -> node{to}\n"));
    }

    fn add_join(
        join_method: &str,
        _joined_tables: &Vec<&str>,
        left_id: usize,
        right_id: usize,
        graphviz: &mut String,
        counter: &mut usize,
    ) -> usize {
        let id = *counter;
        *counter += 1;
        // let label = format!(
        //     "{join_method} (joined=[{}])",
        //     joined_tables.iter().join(",")
        // );

        graphviz.push_str(&format!("\tnode{id} [label=\"{join_method}\"]\n"));
        Self::add_edge(left_id, id, graphviz);
        Self::add_edge(right_id, id, graphviz);
        id
    }

    fn visit(&self, graphviz: &mut String, counter: &mut usize) -> (usize, Vec<&str>) {
        match self {
            JoinOrder::Table(name) => {
                let id = Self::add_base_table(name.clone(), graphviz, counter);
                (id, vec![name])
            }
            JoinOrder::HashJoin(left, right) => {
                let (left_id, mut joined_tables) = left.visit(graphviz, counter);
                let (right_id, mut right_joined) = right.visit(graphviz, counter);
                joined_tables.append(&mut right_joined);
                let id = Self::add_join("HJ", &joined_tables, left_id, right_id, graphviz, counter);
                (id, joined_tables)
            }
            JoinOrder::MergeJoin(left, right) => {
                let (left_id, mut joined_tables) = left.visit(graphviz, counter);
                let (right_id, mut right_joined) = right.visit(graphviz, counter);
                joined_tables.append(&mut right_joined);
                let id = Self::add_join("MJ", &joined_tables, left_id, right_id, graphviz, counter);
                (id, joined_tables)
            }
            JoinOrder::NestedLoopJoin(left, right) => {
                let (left_id, mut joined_tables) = left.visit(graphviz, counter);
                let (right_id, mut right_joined) = right.visit(graphviz, counter);
                joined_tables.append(&mut right_joined);
                let id =
                    Self::add_join("NLJ", &joined_tables, left_id, right_id, graphviz, counter);
                (id, joined_tables)
            }
            JoinOrder::Other(child) => child.visit(graphviz, counter),
        }
    }
}

impl std::fmt::Display for JoinOrder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JoinOrder::Table(name) => write!(f, "{}", name),
            JoinOrder::HashJoin(left, right) => {
                write!(f, "(HJ {} {})", left, right)
            }
            JoinOrder::MergeJoin(left, right) => {
                write!(f, "(MJ {} {})", left, right)
            }
            JoinOrder::NestedLoopJoin(left, right) => {
                write!(f, "(NLJ {} {})", left, right)
            }
            JoinOrder::Other(child) => {
                write!(f, "<{}>", child)
            }
        }
    }
}

#[allow(dead_code)]
fn get_join_order_from_df_exec(rel_node: &Arc<dyn ExecutionPlan>) -> Option<JoinOrder> {
    if let Some(x) = rel_node.as_any().downcast_ref::<DataSourceExec>() {
        let (config, _) = x.downcast_to_file_source::<ParquetSource>()?;
        let location = config.file_groups[0].files()[0]
            .object_meta
            .location
            .to_string();
        let maybe_table_name = location.split('/').rev().nth(1)?;
        return Some(JoinOrder::Table(maybe_table_name.to_string()));
    }
    if let Some(x) = rel_node.as_any().downcast_ref::<HashJoinExec>() {
        let left = get_join_order_from_df_exec(x.left())?;
        let right = get_join_order_from_df_exec(x.right())?;
        return Some(JoinOrder::HashJoin(Box::new(left), Box::new(right)));
    }

    if let Some(x) = rel_node.as_any().downcast_ref::<SortMergeJoinExec>() {
        let left = get_join_order_from_df_exec(x.left())?;
        let right = get_join_order_from_df_exec(x.right())?;
        return Some(JoinOrder::MergeJoin(Box::new(left), Box::new(right)));
    }

    if let Some(x) = rel_node.as_any().downcast_ref::<NestedLoopJoinExec>() {
        let left = get_join_order_from_df_exec(x.left())?;
        let right = get_join_order_from_df_exec(x.right())?;
        return Some(JoinOrder::NestedLoopJoin(Box::new(left), Box::new(right)));
    }

    if rel_node.children().len() == 1 {
        let child = get_join_order_from_df_exec(rel_node.children()[0])?;
        if matches!(child, JoinOrder::Other(_) | JoinOrder::Table(_)) {
            return Some(child);
        }
        return Some(JoinOrder::Other(Box::new(child)));
    }
    None
}
