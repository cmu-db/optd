use std::{sync::Arc, vec};

use datafusion::{
    common::DFSchema,
    execution::FunctionRegistry,
    logical_expr::{
        self, Expr as DFExpr, LogicalPlan as DFLogicalPlan, logical_plan, projection_schema,
        utils::{enumerate_grouping_sets, exprlist_to_fields, grouping_set_to_exprlist},
    },
};
use itertools::Itertools;
use optd_core::ir::{
    Operator, OperatorKind, Scalar,
    operator::{
        Aggregate, AggregateBorrowed, EnforcerSort, EnforcerSortBorrowed, Get, GetBorrowed, Join,
        JoinBorrowed, Limit, LimitBorrowed, Project, ProjectBorrowed, Remap, RemapBorrowed, Select,
        SelectBorrowed, split_equi_and_non_equi_conditions,
    },
    properties::TupleOrderingDirection,
    scalar::{
        BinaryOp, BinaryOpBorrowed, BinaryOpKind, Cast, CastBorrowed, ColumnRef, ColumnRefBorrowed,
        Function, FunctionBorrowed, FunctionKind, Like, LikeBorrowed, List, Literal,
        LiteralBorrowed, NaryOp, NaryOpBorrowed, NaryOpKind,
    },
};
use snafu::{OptionExt, ResultExt, whatever};

use crate::planner::{DataFusionSnafu, OptdQueryPlannerContext, OptdSnafu, Result};

impl OptdQueryPlannerContext<'_> {
    pub fn try_from_optd_plan(&mut self, optd_plan: &Operator) -> Result<DFLogicalPlan> {
        match &optd_plan.kind {
            OperatorKind::Get(meta) => {
                let node = Get::borrow_raw_parts(meta, &optd_plan.common);
                self.try_from_optd_get(node)
            }
            OperatorKind::Select(meta) => {
                let node = Select::borrow_raw_parts(meta, &optd_plan.common);
                self.try_from_optd_select(node)
            }
            OperatorKind::Join(meta) => {
                let node = Join::borrow_raw_parts(meta, &optd_plan.common);
                self.try_from_optd_join(node)
            }
            OperatorKind::Project(meta) => {
                let node = Project::borrow_raw_parts(meta, &optd_plan.common);
                self.try_from_optd_project(node)
            }
            OperatorKind::Remap(meta) => {
                let node = Remap::borrow_raw_parts(meta, &optd_plan.common);
                self.try_from_optd_remap(node)
            }
            OperatorKind::Aggregate(meta) => {
                let node = Aggregate::borrow_raw_parts(meta, &optd_plan.common);
                self.try_from_optd_aggregate(node)
            }
            OperatorKind::Limit(meta) => {
                let node = Limit::borrow_raw_parts(meta, &optd_plan.common);
                self.try_from_optd_limit(node)
            }
            OperatorKind::EnforcerSort(meta) => {
                let node = EnforcerSort::borrow_raw_parts(meta, &optd_plan.common);
                self.try_from_optd_enforcer_sort(node)
            }
            kind => whatever!("unsupported operator type {kind:?}"),
        }
    }

    fn try_new_df_projection(
        &self,
        table_index: &i64,
        exprs: Vec<DFExpr>,
        input: Arc<DFLogicalPlan>,
    ) -> Result<logical_expr::Projection> {
        let binding = self.inner.get_binding(table_index).context(OptdSnafu)?;
        let table_ref = binding.table_ref();
        let schema = projection_schema(&input, &exprs).context(DataFusionSnafu)?;
        let alias = Self::from_optd_table_ref(table_ref);
        let schema = schema.as_ref().clone().replace_qualifier(alias);
        logical_expr::Projection::try_new_with_schema(exprs, input, Arc::new(schema))
            .context(DataFusionSnafu)
    }

    fn try_new_df_aggregate(
        &self,
        aggregate_table_index: &i64,
        input: Arc<DFLogicalPlan>,
        group_expr: Vec<DFExpr>,
        aggr_expr: Vec<DFExpr>,
    ) -> Result<logical_expr::Aggregate> {
        let group_expr = enumerate_grouping_sets(group_expr).context(DataFusionSnafu)?;
        let grouping_expr: Vec<&DFExpr> =
            grouping_set_to_exprlist(group_expr.as_slice()).context(DataFusionSnafu)?;

        let mut qualified_fields =
            exprlist_to_fields(grouping_expr, &input).context(DataFusionSnafu)?;

        let binding = self
            .inner
            .get_binding(aggregate_table_index)
            .context(OptdSnafu)?;
        let table_ref = binding.table_ref();

        let alias = Self::from_optd_table_ref(table_ref);
        qualified_fields.extend(
            exprlist_to_fields(aggr_expr.as_slice(), &input)
                .context(DataFusionSnafu)?
                .into_iter()
                .map(|(_, field)| (Some(alias.clone()), field)),
        );

        let schema =
            DFSchema::new_with_metadata(qualified_fields, input.schema().metadata().clone())
                .context(DataFusionSnafu)?;

        logical_expr::Aggregate::try_new_with_schema(input, group_expr, aggr_expr, Arc::new(schema))
            .context(DataFusionSnafu)
    }

    pub fn try_from_optd_aggregate(
        &mut self,
        node: AggregateBorrowed<'_>,
    ) -> Result<DFLogicalPlan> {
        let input = self.try_from_optd_plan(node.input())?;

        let keys = node.keys().borrow::<List>();
        let exprs = node.exprs().borrow::<List>();
        let group_expr = keys
            .members()
            .iter()
            .map(|e| self.try_from_optd_scalar_expr(e))
            .try_collect()?;
        let aggr_expr = exprs
            .members()
            .iter()
            .map(|e| self.try_from_optd_scalar_expr(e))
            .try_collect()?;

        let aggregate = self.try_new_df_aggregate(
            node.aggregate_table_index(),
            Arc::new(input),
            group_expr,
            aggr_expr,
        )?;
        Ok(DFLogicalPlan::Aggregate(aggregate))
    }
    pub fn try_from_optd_remap(&mut self, node: RemapBorrowed<'_>) -> Result<DFLogicalPlan> {
        let input = self.try_from_optd_plan(node.input())?;
        let binder = self.inner.binder.read().unwrap();
        let binding = binder
            .get_binding(node.table_index())
            .whatever_context("binding not found")?;
        let table_ref = binding.table_ref();

        let alias = Self::from_optd_table_ref(table_ref);

        let subquery_alias = logical_plan::SubqueryAlias::try_new(Arc::new(input), alias)
            .context(DataFusionSnafu)?;

        Ok(DFLogicalPlan::SubqueryAlias(subquery_alias))
    }

    pub fn try_from_optd_project(&mut self, node: ProjectBorrowed<'_>) -> Result<DFLogicalPlan> {
        let projection_list = node.projections().borrow::<List>();
        let exprs = projection_list
            .members()
            .iter()
            .map(|e| self.try_from_optd_scalar_expr(e))
            .try_collect()?;
        let input = self.try_from_optd_plan(node.input())?;

        let projection = self.try_new_df_projection(node.table_index(), exprs, Arc::new(input))?;

        Ok(DFLogicalPlan::Projection(projection))
    }

    pub fn try_from_optd_join(&mut self, node: JoinBorrowed<'_>) -> Result<DFLogicalPlan> {
        let outer = self.try_from_optd_plan(node.outer())?;
        let inner = self.try_from_optd_plan(node.inner())?;
        let (equi_conds, non_equi_conds) =
            split_equi_and_non_equi_conditions(&node, &self.inner).context(OptdSnafu)?;

        let on = equi_conds
            .iter()
            .map(|(l, r)| {
                Ok((
                    DFExpr::Column(self.try_from_optd_column(l)?),
                    DFExpr::Column(self.try_from_optd_column(r)?),
                ))
            })
            .try_collect()?;
        let filter: Vec<_> = non_equi_conds
            .iter()
            .map(|e| self.try_from_optd_scalar_expr(e))
            .try_collect()?;

        let filter = filter.into_iter().reduce(logical_expr::and);

        let join_type = Self::try_from_optd_join_type(node.join_type())?;
        let join = logical_plan::Join::try_new(
            Arc::new(outer),
            Arc::new(inner),
            on,
            filter,
            join_type,
            logical_expr::JoinConstraint::On,
            datafusion::common::NullEquality::NullEqualsNothing,
        )
        .context(DataFusionSnafu)?;
        Ok(DFLogicalPlan::Join(join))
    }

    pub fn try_from_optd_select(&mut self, node: SelectBorrowed<'_>) -> Result<DFLogicalPlan> {
        let input = self.try_from_optd_plan(node.input())?;
        let predicate = self.try_from_optd_scalar_expr(node.predicate())?;
        let filter =
            logical_plan::Filter::try_new(predicate, Arc::new(input)).context(DataFusionSnafu)?;
        Ok(DFLogicalPlan::Filter(filter))
    }

    pub fn try_from_optd_limit(&mut self, node: LimitBorrowed<'_>) -> Result<DFLogicalPlan> {
        let input = self.try_from_optd_plan(node.input())?;
        let skip_expr = self.try_from_optd_scalar_expr(node.skip())?;
        let fetch_expr = self.try_from_optd_scalar_expr(node.fetch())?;

        let skip = match skip_expr {
            DFExpr::Literal(datafusion::scalar::ScalarValue::Int64(Some(0)), _) => None,
            DFExpr::Literal(datafusion::scalar::ScalarValue::Int64(None), _) => None,
            expr => Some(Box::new(expr)),
        };
        let fetch = match fetch_expr {
            DFExpr::Literal(datafusion::scalar::ScalarValue::Int64(Some(0)), _) => None,
            DFExpr::Literal(datafusion::scalar::ScalarValue::Int64(None), _) => None,
            expr => Some(Box::new(expr)),
        };

        Ok(DFLogicalPlan::Limit(logical_plan::Limit {
            skip,
            fetch,
            input: Arc::new(input),
        }))
    }

    pub fn try_from_optd_enforcer_sort(
        &mut self,
        node: EnforcerSortBorrowed<'_>,
    ) -> Result<DFLogicalPlan> {
        let input = self.try_from_optd_plan(node.input())?;
        let expr = node
            .tuple_ordering()
            .iter()
            .map(|(column, direction)| {
                let expr = DFExpr::Column(self.try_from_optd_column(column)?);
                let asc = matches!(direction, TupleOrderingDirection::Asc);
                Ok(logical_expr::expr::Sort::new(expr, asc, !asc))
            })
            .try_collect()?;

        Ok(DFLogicalPlan::Sort(logical_plan::Sort {
            expr,
            input: Arc::new(input),
            fetch: None,
        }))
    }

    pub fn try_from_optd_get(&mut self, node: GetBorrowed<'_>) -> Result<DFLogicalPlan> {
        let binder = self.inner.binder.read().unwrap();
        let binding = binder
            .get_binding(node.table_index())
            .whatever_context("binding not found")?;
        let table_ref = binding.table_ref();
        let name = Self::from_optd_table_ref(table_ref);

        let table_source = self
            .table_reference_to_source
            .get(&name)
            .whatever_context("table source not found")?;

        let projections = (!node.projections().is_empty())
            .then(|| node.projections().iter().cloned().collect_vec());
        let table_scan =
            logical_plan::TableScan::try_new(name, table_source.clone(), projections, vec![], None)
                .whatever_context("failed to create TableScan")?;
        Ok(DFLogicalPlan::TableScan(table_scan))
    }
}

impl OptdQueryPlannerContext<'_> {
    pub fn try_from_optd_scalar_expr(&mut self, expr: &Scalar) -> Result<DFExpr> {
        match &expr.kind {
            optd_core::ir::ScalarKind::Literal(meta) => {
                let node = Literal::borrow_raw_parts(meta, &expr.common);
                self.try_from_optd_literal(node)
            }
            optd_core::ir::ScalarKind::ColumnRef(meta) => {
                let node = ColumnRef::borrow_raw_parts(meta, &expr.common);
                self.try_from_optd_column_ref(node)
            }
            optd_core::ir::ScalarKind::BinaryOp(meta) => {
                let node = BinaryOp::borrow_raw_parts(meta, &expr.common);
                self.try_from_optd_binary_op(node)
            }
            optd_core::ir::ScalarKind::NaryOp(meta) => {
                let node = NaryOp::borrow_raw_parts(meta, &expr.common);
                self.try_from_optd_nary_op(node)
            }
            optd_core::ir::ScalarKind::List(_) => {
                whatever!("expr list should not be extracted from this path")
            }
            optd_core::ir::ScalarKind::Function(meta) => {
                let node = Function::borrow_raw_parts(meta, &expr.common);
                self.try_from_optd_function(node)
            }
            optd_core::ir::ScalarKind::Cast(meta) => {
                let node = Cast::borrow_raw_parts(meta, &expr.common);
                self.try_from_optd_cast(node)
            }
            optd_core::ir::ScalarKind::Like(meta) => {
                let node = Like::borrow_raw_parts(meta, &expr.common);
                self.try_from_optd_like(node)
            }
        }
    }

    pub fn try_from_optd_literal(&self, node: LiteralBorrowed<'_>) -> Result<DFExpr> {
        let value = Self::from_optd_value(node.value().clone());
        Ok(DFExpr::Literal(value, None))
    }

    pub fn try_from_optd_column_ref(&self, node: ColumnRefBorrowed<'_>) -> Result<DFExpr> {
        let column = self.try_from_optd_column(node.column())?;
        Ok(DFExpr::Column(column))
    }

    pub fn try_from_optd_binary_op(&mut self, node: BinaryOpBorrowed<'_>) -> Result<DFExpr> {
        let left = self.try_from_optd_scalar_expr(node.lhs())?;
        let right = self.try_from_optd_scalar_expr(node.rhs())?;
        let op = match node.op_kind() {
            BinaryOpKind::Plus => logical_expr::Operator::Plus,
            BinaryOpKind::Minus => logical_expr::Operator::Minus,
            BinaryOpKind::Multiply => logical_expr::Operator::Multiply,
            BinaryOpKind::Divide => logical_expr::Operator::Divide,
            BinaryOpKind::Modulo => logical_expr::Operator::Modulo,
            BinaryOpKind::Eq => logical_expr::Operator::Eq,
            BinaryOpKind::IsNotDistinctFrom => logical_expr::Operator::IsNotDistinctFrom,
            BinaryOpKind::Lt => logical_expr::Operator::Lt,
            BinaryOpKind::Le => logical_expr::Operator::LtEq,
            BinaryOpKind::Gt => logical_expr::Operator::Gt,
            BinaryOpKind::Ge => logical_expr::Operator::GtEq,
        };

        Ok(logical_expr::binary_expr(left, op, right))
    }

    pub fn try_from_optd_nary_op(&mut self, node: NaryOpBorrowed<'_>) -> Result<DFExpr> {
        let op = match node.op_kind() {
            NaryOpKind::And => logical_expr::Operator::And,
            NaryOpKind::Or => logical_expr::Operator::Or,
        };

        let exprs: Vec<_> = node
            .terms()
            .iter()
            .map(|term| self.try_from_optd_scalar_expr(term))
            .try_collect()?;

        let nary_expr = exprs
            .into_iter()
            .reduce(|l, r| logical_expr::binary_expr(l, op, r))
            .whatever_context("nary expr should have a least one term")?;

        Ok(nary_expr)
    }

    pub fn try_from_optd_cast(&mut self, node: CastBorrowed<'_>) -> Result<DFExpr> {
        let input = self.try_from_optd_scalar_expr(node.expr())?;
        let cast = logical_expr::cast(input, node.data_type().clone());
        Ok(cast)
    }

    pub fn try_from_optd_like(&mut self, node: LikeBorrowed<'_>) -> Result<DFExpr> {
        let input = self.try_from_optd_scalar_expr(node.expr())?;
        let pattern = self.try_from_optd_scalar_expr(node.pattern())?;
        let like = logical_expr::Like::new(
            *node.negated(),
            Box::new(input),
            Box::new(pattern),
            *node.escape_char(),
            *node.case_insensative(),
        );
        Ok(DFExpr::Like(like))
    }

    pub fn try_from_optd_function(&mut self, node: FunctionBorrowed<'_>) -> Result<DFExpr> {
        let args = node
            .params()
            .iter()
            .map(|e| self.try_from_optd_scalar_expr(e))
            .try_collect()?;

        match node.kind() {
            FunctionKind::Scalar => {
                let udf = self.session_state.udf(node.id()).context(DataFusionSnafu)?;
                let func = logical_expr::expr::ScalarFunction::new_udf(udf, args);
                Ok(DFExpr::ScalarFunction(func))
            }
            FunctionKind::Aggregate => {
                let udaf = self
                    .session_state
                    .udaf(node.id())
                    .context(DataFusionSnafu)?;
                let func = logical_expr::expr::AggregateFunction::new_udf(
                    udaf,
                    args,
                    false,
                    None,
                    vec![],
                    None,
                );
                Ok(DFExpr::AggregateFunction(func))
            }
            FunctionKind::Window => {
                let udwf = self
                    .session_state
                    .udwf(node.id())
                    .context(DataFusionSnafu)?;
                let func = logical_expr::expr::WindowFunction::new(udwf, args);
                Ok(DFExpr::WindowFunction(Box::new(func)))
            }
        }
    }
}
