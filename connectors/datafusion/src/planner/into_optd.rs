use std::sync::Arc;

use datafusion::{
    common::DFSchema,
    logical_expr::{
        self, Cast as DFCast, Expr as DFExpr, ExprSchemable, Like as DFLike,
        LogicalPlan as DFLogicalPlan, expr::AggregateFunction, logical_plan,
    },
    scalar::ScalarValue as DFScalarValue,
};
use itertools::{Either, Itertools};
use optd_core::{
    error::CatalogSnafu,
    ir::{
        Scalar, ScalarValue, builder as optd_builder,
        catalog::Schema,
        convert::{IntoOperator, IntoScalar},
        operator::{Aggregate, Get, Join, Limit, OrderBy, Project, Remap, Select},
        properties::TupleOrderingDirection,
        scalar::{Cast, ColumnRef, Function, Like, List, NaryOp, NaryOpKind},
    },
};
use snafu::{ResultExt, whatever};

use crate::planner::{DataFusionSnafu, OptdQueryPlannerContext, OptdSnafu, Result};

impl OptdQueryPlannerContext<'_> {
    pub fn try_into_optd_plan(
        &mut self,
        df_logical_plan: &DFLogicalPlan,
    ) -> Result<Arc<optd_core::ir::Operator>> {
        match df_logical_plan {
            DFLogicalPlan::TableScan(table_scan) => self.try_into_optd_get(table_scan),
            DFLogicalPlan::Filter(filter) => self.try_into_optd_select(filter),
            DFLogicalPlan::Join(join) => self.try_into_optd_join(join),
            DFLogicalPlan::Projection(project) => self.try_into_optd_project(project),
            DFLogicalPlan::Aggregate(aggregate) => self.try_into_optd_aggregate(aggregate),
            DFLogicalPlan::Sort(sort) => self.try_into_optd_order_by(sort),
            DFLogicalPlan::SubqueryAlias(alias) => self.try_into_optd_remap(alias),
            DFLogicalPlan::Limit(limit) => self.try_into_optd_limit(limit),

            plan => {
                whatever!("Unsupported DataFusion logical plan: {}", plan);
            }
        }
    }

    pub fn try_into_optd_limit(
        &mut self,
        node: &logical_plan::Limit,
    ) -> Result<Arc<optd_core::ir::Operator>> {
        let input = self.try_into_optd_plan(&node.input)?;
        let skip = match &node.skip {
            Some(skip) => self.try_into_optd_scalar_expr(skip, node.input.schema())?,
            None => optd_builder::literal(0_i64),
        };
        let fetch = match &node.fetch {
            Some(fetch) => self.try_into_optd_scalar_expr(fetch, node.input.schema())?,
            None => optd_builder::literal(ScalarValue::Int64(None)),
        };

        Ok(Limit::new(input, skip, fetch).into_operator())
    }

    pub fn try_into_optd_order_by(
        &mut self,
        node: &logical_plan::Sort,
    ) -> Result<Arc<optd_core::ir::Operator>> {
        if node.fetch.is_some() {
            whatever!("Does not handle fetch in sort yet")
        }
        let input = self.try_into_optd_plan(&node.input)?;

        let ordering_exprs = node
            .expr
            .iter()
            .map(|sort_expr| {
                let expr = self.try_into_optd_scalar_expr(&sort_expr.expr, node.input.schema())?;
                let direction = if sort_expr.asc {
                    TupleOrderingDirection::Asc
                } else {
                    TupleOrderingDirection::Desc
                };
                Ok((expr, direction))
            })
            .try_collect()?;

        Ok(OrderBy::new(input, ordering_exprs).into_operator())
    }

    pub fn try_into_optd_aggregate(
        &mut self,
        node: &logical_plan::Aggregate,
    ) -> Result<Arc<optd_core::ir::Operator>> {
        let input = self.try_into_optd_plan(&node.input)?;

        let exprs = node
            .aggr_expr
            .iter()
            .map(|e| self.try_into_optd_scalar_expr(e, node.input.schema()))
            .try_collect()
            .map(List::new)?;
        let keys = node
            .group_expr
            .iter()
            .map(|e| self.try_into_optd_scalar_expr(e, node.input.schema()))
            .try_collect()
            .map(List::new)?;

        let aggrgate_schema = node
            .aggr_expr
            .iter()
            .map(|e| {
                e.to_field(node.input.schema())
                    .map(|(_, field)| field)
                    .context(DataFusionSnafu)
            })
            .collect::<Result<Vec<_>>>()
            .map(Schema::new)?;

        let table_index = self
            .inner
            .add_binding(None, Arc::new(aggrgate_schema))
            .context(OptdSnafu)?;

        let aggregate =
            Aggregate::logical(table_index, input, exprs.into_scalar(), keys.into_scalar());

        Ok(aggregate.into_operator())
    }

    pub fn try_into_optd_project(
        &mut self,
        node: &logical_plan::Projection,
    ) -> Result<Arc<optd_core::ir::Operator>> {
        self.inner.binder_begin_scope();
        let input = self.try_into_optd_plan(&node.input)?;
        let projections = node
            .expr
            .iter()
            .map(|e| self.try_into_optd_scalar_expr(e, node.input.schema()))
            .try_collect()
            .map(List::new)?;
        self.inner.binder_end_scope();

        // Note: projection create unnamed binding with no table ref.
        let table_index = self
            .inner
            .add_binding(None, node.schema.inner().clone())
            .context(OptdSnafu)?;

        let project = Project::new(table_index, input, projections.into_scalar());
        Ok(project.into_operator())
    }

    pub fn try_into_optd_remap(
        &mut self,
        node: &logical_plan::SubqueryAlias,
    ) -> Result<Arc<optd_core::ir::Operator>> {
        self.inner.binder_begin_scope();
        let input = self.try_into_optd_plan(&node.input)?;
        let table_ref = Self::into_optd_table_ref(&node.alias);
        self.inner.binder_end_scope();
        let table_index = self
            .inner
            .add_binding(Some(table_ref), node.schema.inner().clone())
            .context(OptdSnafu)?;
        let remap = Remap::new(table_index, input);
        Ok(remap.into_operator())
    }

    pub fn try_into_optd_join(
        &mut self,
        node: &logical_plan::Join,
    ) -> Result<Arc<optd_core::ir::Operator>> {
        let left = self.try_into_optd_plan(&node.left)?;
        let right = self.try_into_optd_plan(&node.right)?;
        let join_type = Self::try_into_optd_join_type(node.join_type)?;

        let mut terms = Vec::with_capacity(node.on.len());
        for (left_key, right_key) in node.on.iter() {
            let lhs = self.try_into_optd_scalar_expr(left_key, &node.schema)?;
            let rhs = self.try_into_optd_scalar_expr(right_key, &node.schema)?;
            terms.push(lhs.eq(rhs));
        }

        if let Some(filter) = &node.filter {
            let non_equi_conds = self.try_into_optd_scalar_expr(filter, &node.schema)?;
            terms.push(non_equi_conds);
        }

        let join_cond = NaryOp::new(NaryOpKind::And, terms.into()).into_scalar();
        let join = Join::logical(join_type, left, right, join_cond);

        Ok(join.into_operator())
    }

    pub fn try_into_optd_select(
        &mut self,
        node: &logical_plan::Filter,
    ) -> Result<Arc<optd_core::ir::Operator>> {
        let input = self.try_into_optd_plan(node.input.as_ref())?;
        let predicate = self.try_into_optd_scalar_expr(&node.predicate, node.input.schema())?;

        let select = Select::new(input, predicate);
        Ok(select.into_operator())
    }

    pub fn try_into_optd_get(
        &mut self,
        node: &logical_plan::TableScan,
    ) -> Result<Arc<optd_core::ir::Operator>> {
        if !node.filters.is_empty() {
            whatever!(
                "do not support filters in TableScan, filters: {:?}",
                node.filters
            );
        }

        self.table_reference_to_source
            .insert(node.table_name.clone(), node.source.clone());

        let table_ref = Self::into_optd_table_ref(&node.table_name);
        let data_source_id = self
            .inner
            .catalog
            .table_by_ref(&table_ref)
            .context(CatalogSnafu)
            .context(OptdSnafu)?
            .id;

        let table_index = self
            .inner
            .add_binding(Some(table_ref), node.projected_schema.inner().clone())
            .context(OptdSnafu)?;

        let projections = node
            .projection
            .clone()
            .unwrap_or_else(|| (0..node.projected_schema.inner().fields().len()).collect_vec())
            .into();

        let logical_get = Get::new(data_source_id, table_index, projections, None);
        Ok(logical_get.into_operator())
    }
}

// All the scalar expression conversion implementations.
impl OptdQueryPlannerContext<'_> {
    pub fn try_into_optd_scalar_expr(
        &mut self,
        node: &DFExpr,
        input_schema: &DFSchema,
    ) -> Result<Arc<optd_core::ir::Scalar>> {
        match node {
            DFExpr::Column(column) => self.try_into_optd_column_ref(column),
            DFExpr::Literal(literal, _) => self.try_into_optd_literal(literal),
            DFExpr::BinaryExpr(binary_expr) => {
                self.try_into_optd_scalar_op(binary_expr, input_schema)
            }
            DFExpr::Alias(alias) => self.try_into_optd_scalar_expr(&alias.expr, input_schema),
            DFExpr::AggregateFunction(agg_func) => {
                self.try_into_optd_aggregate_func(agg_func, input_schema)
            }
            DFExpr::Cast(cast) => self.try_into_optd_cast(cast, input_schema),
            DFExpr::Like(like) => self.try_into_optd_like(like, input_schema),
            expr => {
                whatever!("Unsupported df logical expr: {}", expr);
            }
        }
    }

    pub fn try_into_optd_column_ref(
        &self,
        column: &datafusion::common::Column,
    ) -> Result<Arc<Scalar>> {
        let column = self.try_get_optd_column(column.relation.as_ref(), &column.name)?;
        Ok(ColumnRef::new(column).into_scalar())
    }

    pub fn try_into_optd_literal(&self, literal: &DFScalarValue) -> Result<Arc<Scalar>> {
        Self::try_into_optd_scalar_value(literal.clone()).map(optd_builder::literal)
    }

    pub fn try_into_optd_scalar_op(
        &mut self,
        binary_expr: &logical_expr::BinaryExpr,
        input_schema: &DFSchema,
    ) -> Result<Arc<Scalar>> {
        let op_kind = match &binary_expr.op {
            logical_expr::Operator::Eq => Either::Left(optd_core::ir::scalar::BinaryOpKind::Eq),
            logical_expr::Operator::Plus => Either::Left(optd_core::ir::scalar::BinaryOpKind::Plus),
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
            logical_expr::Operator::Lt => Either::Left(optd_core::ir::scalar::BinaryOpKind::Lt),
            logical_expr::Operator::LtEq => Either::Left(optd_core::ir::scalar::BinaryOpKind::Le),
            logical_expr::Operator::Gt => Either::Left(optd_core::ir::scalar::BinaryOpKind::Gt),
            logical_expr::Operator::GtEq => Either::Left(optd_core::ir::scalar::BinaryOpKind::Ge),
            logical_expr::Operator::IsNotDistinctFrom => {
                Either::Left(optd_core::ir::scalar::BinaryOpKind::IsNotDistinctFrom)
            }
            logical_expr::Operator::And => Either::Right(optd_core::ir::scalar::NaryOpKind::And),
            logical_expr::Operator::Or => Either::Right(optd_core::ir::scalar::NaryOpKind::Or),
            op => {
                whatever!("Unsupported binary expr op: {}", op);
            }
        };

        let left = self.try_into_optd_scalar_expr(&binary_expr.left, input_schema)?;
        let right = self.try_into_optd_scalar_expr(&binary_expr.right, input_schema)?;

        match op_kind {
            Either::Left(op_kind) => Ok(left.binary_op(right, op_kind)),
            Either::Right(op_kind) => Ok(left.nary_op(right, op_kind)),
        }
    }

    pub fn try_into_optd_aggregate_func(
        &mut self,
        agg_func: &AggregateFunction,
        input_schema: &DFSchema,
    ) -> Result<Arc<Scalar>> {
        if agg_func.params.distinct {
            whatever!("does not support distinct aggregate")
        }

        if agg_func.params.filter.is_some() {
            whatever!("does not support filter in aggregate")
        }

        if agg_func.params.filter.is_some() {
            whatever!("does not support filter in aggregate")
        }

        if !agg_func.params.order_by.is_empty() {
            whatever!("does not support order by in aggregate")
        }

        if agg_func.params.null_treatment.is_some() {
            whatever!("does not support special null treatment in aggregate")
        }

        let func_name = agg_func.func.name();

        let params: Vec<_> = agg_func
            .params
            .args
            .iter()
            .map(|x| self.try_into_optd_scalar_expr(x, input_schema))
            .try_collect()?;

        let input_types: Vec<_> = agg_func
            .params
            .args
            .iter()
            .map(|x| x.get_type(input_schema).context(DataFusionSnafu))
            .try_collect()?;

        let return_type = agg_func
            .func
            .return_type(&input_types)
            .context(DataFusionSnafu)?;
        Ok(
            Function::new_aggregate(func_name.to_string(), params.into(), return_type)
                .into_scalar(),
        )
    }

    pub fn try_into_optd_cast(
        &mut self,
        node: &DFCast,
        input_schema: &DFSchema,
    ) -> Result<Arc<Scalar>> {
        let input = self.try_into_optd_scalar_expr(&node.expr, input_schema)?;
        let cast = Cast::new(node.data_type.clone(), input);
        Ok(cast.into_scalar())
    }

    pub fn try_into_optd_like(
        &mut self,
        node: &DFLike,
        input_schema: &DFSchema,
    ) -> Result<Arc<Scalar>> {
        let expr = self.try_into_optd_scalar_expr(&node.expr, input_schema)?;
        let pattern = self.try_into_optd_scalar_expr(&node.pattern, input_schema)?;
        let like = Like::new(
            expr,
            pattern,
            node.negated,
            node.case_insensitive,
            node.escape_char,
        );
        Ok(like.into_scalar())
    }
}
