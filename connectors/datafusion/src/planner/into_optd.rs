use std::sync::Arc;

use datafusion::{
    common::{Column as DFColumn, DFSchema, JoinType as DFJoinType},
    logical_expr::{
        self, Cast as DFCast, Expr as DFExpr, ExprSchemable, Like as DFLike,
        LogicalPlan as DFLogicalPlan, expr::AggregateFunction, expr::ScalarFunction, logical_plan,
    },
    scalar::ScalarValue as DFScalarValue,
    sql::TableReference,
};
use itertools::{Either, Itertools};
use optd_core::{
    error::CatalogSnafu,
    ir::{
        Column, DataType, Scalar,
        builder::{self as optd_builder, literal},
        catalog::{Field, Schema},
        convert::{IntoOperator, IntoScalar},
        operator::{
            Aggregate, DependentJoin, Get, Join, Limit, OrderBy, Project, Remap, Select,
            join::JoinType,
        },
        properties::TupleOrderingDirection,
        scalar::{Case, Cast, ColumnRef, Function, InList, Like, List, NaryOp, NaryOpKind},
    },
};
use snafu::{OptionExt, ResultExt, whatever};

use crate::planner::{DataFusionSnafu, OptdQueryPlannerContext, OptdSnafu, Result};

impl OptdQueryPlannerContext<'_> {
    fn strip_alias(expr: &DFExpr) -> &DFExpr {
        match expr {
            DFExpr::Alias(alias) => Self::strip_alias(&alias.expr),
            _ => expr,
        }
    }

    fn aggregate_function_in_expr(expr: &DFExpr) -> Option<&AggregateFunction> {
        match Self::strip_alias(expr) {
            DFExpr::AggregateFunction(agg_func) => Some(agg_func),
            _ => None,
        }
    }

    fn validate_aggregate_function(agg_func: &AggregateFunction) -> Result<()> {
        if agg_func.params.filter.is_some() {
            whatever!("does not support filter in aggregate")
        }

        if !agg_func.params.order_by.is_empty() {
            whatever!("does not support order by in aggregate")
        }

        if agg_func.params.null_treatment.is_some() {
            whatever!("does not support special null treatment in aggregate")
        }

        Ok(())
    }

    fn df_conjuncts<'a>(expr: &'a DFExpr, conjuncts: &mut Vec<&'a DFExpr>) {
        if let DFExpr::BinaryExpr(binary_expr) = expr
            && binary_expr.op == logical_expr::Operator::And
        {
            Self::df_conjuncts(&binary_expr.left, conjuncts);
            Self::df_conjuncts(&binary_expr.right, conjuncts);
            return;
        }

        conjuncts.push(expr);
    }

    fn flush_residual_filter(
        &mut self,
        input: Arc<optd_core::ir::Operator>,
        predicates: &mut Vec<DFExpr>,
        input_schema: &DFSchema,
    ) -> Result<Arc<optd_core::ir::Operator>> {
        if predicates.is_empty() {
            return Ok(input);
        }

        let mut current = input;
        let mut rewritten = Vec::with_capacity(predicates.len());
        for expr in predicates.drain(..) {
            let (new_current, new_expr) =
                self.rewrite_predicate_subqueries_to_mark_expr(current, &expr, input_schema)?;
            current = new_current;
            rewritten.push(self.try_into_optd_scalar_expr(&new_expr, input_schema)?);
        }
        let predicate = Scalar::combine_conjuncts(rewritten);

        Ok(Select::new(current, predicate).into_operator())
    }

    fn next_internal_mark_df_column(&self) -> DFColumn {
        DFColumn::new(
            Some(TableReference::bare(format!(
                "__optd_mark_{}",
                self.df_mark_columns.len() + 1
            ))),
            "mark",
        )
    }

    fn allocate_mark_column(&mut self) -> Result<Column> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "mark",
            DataType::Boolean,
            false,
        )]));
        let table_index = self.inner.add_binding(None, schema).context(OptdSnafu)?;
        Ok(Column(table_index, 0))
    }

    fn mark_scalar_expr(mark_column: DFColumn, negated: bool) -> DFExpr {
        let expr = DFExpr::Column(mark_column);
        if negated {
            logical_expr::binary_expr(
                expr,
                logical_expr::Operator::Eq,
                DFExpr::Literal(DFScalarValue::Boolean(Some(false)), None),
            )
        } else {
            expr
        }
    }

    fn rewrite_predicate_subqueries_to_mark_expr(
        &mut self,
        input: Arc<optd_core::ir::Operator>,
        expr: &DFExpr,
        input_schema: &DFSchema,
    ) -> Result<(Arc<optd_core::ir::Operator>, DFExpr)> {
        match expr {
            DFExpr::Exists(exists) => {
                let inner = self.try_into_optd_plan(&exists.subquery.subquery)?;
                let mark_column = self.allocate_mark_column()?;
                let df_mark_column = self.next_internal_mark_df_column();
                self.register_df_mark_column(df_mark_column.clone(), mark_column);
                let join =
                    DependentJoin::new(JoinType::Mark(mark_column), input, inner, literal(true))
                        .into_operator();
                Ok((join, Self::mark_scalar_expr(df_mark_column, exists.negated)))
            }
            DFExpr::InSubquery(in_subquery) => {
                let inner = self.try_into_optd_plan(&in_subquery.subquery.subquery)?;
                let inner_cols = inner
                    .output_columns_in_order(&self.inner)
                    .context(OptdSnafu)?;
                if inner_cols.len() != 1 {
                    whatever!(
                        "InSubquery should return exactly one column, found {}",
                        inner_cols.len()
                    );
                }

                let expr = self.try_into_optd_scalar_expr(&in_subquery.expr, input_schema)?;
                let subquery_value = ColumnRef::new(inner_cols[0]).into_scalar();
                let join_cond = expr.eq(subquery_value);
                let mark_column = self.allocate_mark_column()?;
                let df_mark_column = self.next_internal_mark_df_column();
                self.register_df_mark_column(df_mark_column.clone(), mark_column);
                let join = DependentJoin::new(JoinType::Mark(mark_column), input, inner, join_cond)
                    .into_operator();
                Ok((
                    join,
                    Self::mark_scalar_expr(df_mark_column, in_subquery.negated),
                ))
            }
            DFExpr::Alias(alias) => {
                self.rewrite_predicate_subqueries_to_mark_expr(input, &alias.expr, input_schema)
            }
            DFExpr::Cast(cast) => {
                let (input, expr) = self.rewrite_predicate_subqueries_to_mark_expr(
                    input,
                    &cast.expr,
                    input_schema,
                )?;
                Ok((
                    input,
                    DFExpr::Cast(DFCast::new(Box::new(expr), cast.data_type.clone())),
                ))
            }
            DFExpr::BinaryExpr(binary_expr)
                if Self::scalar_subquery_in_expr(&binary_expr.left).is_some()
                    || Self::scalar_subquery_in_expr(&binary_expr.right).is_some() =>
            {
                whatever!("embedded scalar subquery comparisons are not supported")
            }
            DFExpr::BinaryExpr(binary_expr) => {
                let (input, left) = self.rewrite_predicate_subqueries_to_mark_expr(
                    input,
                    &binary_expr.left,
                    input_schema,
                )?;
                let (input, right) = self.rewrite_predicate_subqueries_to_mark_expr(
                    input,
                    &binary_expr.right,
                    input_schema,
                )?;
                Ok((
                    input,
                    logical_expr::binary_expr(left, binary_expr.op, right),
                ))
            }
            DFExpr::Case(case) => {
                let (input, base_expr) = match &case.expr {
                    Some(expr) => {
                        let (input, expr) = self.rewrite_predicate_subqueries_to_mark_expr(
                            input,
                            expr,
                            input_schema,
                        )?;
                        (input, Some(Box::new(expr)))
                    }
                    None => (input, None),
                };
                let mut current = input;
                let mut when_then_expr = Vec::with_capacity(case.when_then_expr.len());
                for (when, then) in &case.when_then_expr {
                    let (next, when) = self.rewrite_predicate_subqueries_to_mark_expr(
                        current,
                        when,
                        input_schema,
                    )?;
                    let (next, then) =
                        self.rewrite_predicate_subqueries_to_mark_expr(next, then, input_schema)?;
                    current = next;
                    when_then_expr.push((Box::new(when), Box::new(then)));
                }
                let else_expr = match &case.else_expr {
                    Some(expr) => {
                        let (current_input, expr) = self
                            .rewrite_predicate_subqueries_to_mark_expr(
                                current,
                                expr,
                                input_schema,
                            )?;
                        current = current_input;
                        Some(Box::new(expr))
                    }
                    None => None,
                };
                Ok((
                    current,
                    DFExpr::Case(logical_expr::expr::Case::new(
                        base_expr,
                        when_then_expr,
                        else_expr,
                    )),
                ))
            }
            DFExpr::Not(expr) => {
                let (input, expr) =
                    self.rewrite_predicate_subqueries_to_mark_expr(input, expr, input_schema)?;
                Ok((
                    input,
                    logical_expr::binary_expr(
                        expr,
                        logical_expr::Operator::Eq,
                        DFExpr::Literal(DFScalarValue::Boolean(Some(false)), None),
                    ),
                ))
            }
            DFExpr::IsNull(expr) => {
                let (input, expr) =
                    self.rewrite_predicate_subqueries_to_mark_expr(input, expr, input_schema)?;
                Ok((input, DFExpr::IsNull(Box::new(expr))))
            }
            DFExpr::IsNotNull(expr) => {
                let (input, expr) =
                    self.rewrite_predicate_subqueries_to_mark_expr(input, expr, input_schema)?;
                Ok((input, DFExpr::IsNotNull(Box::new(expr))))
            }
            DFExpr::Like(like) => {
                let (input, expr) = self.rewrite_predicate_subqueries_to_mark_expr(
                    input,
                    &like.expr,
                    input_schema,
                )?;
                let (input, pattern) = self.rewrite_predicate_subqueries_to_mark_expr(
                    input,
                    &like.pattern,
                    input_schema,
                )?;
                Ok((
                    input,
                    DFExpr::Like(DFLike::new(
                        like.negated,
                        Box::new(expr),
                        Box::new(pattern),
                        like.escape_char,
                        like.case_insensitive,
                    )),
                ))
            }
            DFExpr::InList(in_list) => {
                let (input, expr) = self.rewrite_predicate_subqueries_to_mark_expr(
                    input,
                    &in_list.expr,
                    input_schema,
                )?;
                let mut current = input;
                let mut list = Vec::with_capacity(in_list.list.len());
                for value in &in_list.list {
                    let (next, value) = self.rewrite_predicate_subqueries_to_mark_expr(
                        current,
                        value,
                        input_schema,
                    )?;
                    current = next;
                    list.push(value);
                }
                Ok((
                    current,
                    DFExpr::InList(logical_expr::expr::InList::new(
                        Box::new(expr),
                        list,
                        in_list.negated,
                    )),
                ))
            }
            DFExpr::Between(between) => {
                let (input, expr) = self.rewrite_predicate_subqueries_to_mark_expr(
                    input,
                    &between.expr,
                    input_schema,
                )?;
                let (input, low) = self.rewrite_predicate_subqueries_to_mark_expr(
                    input,
                    &between.low,
                    input_schema,
                )?;
                let (input, high) = self.rewrite_predicate_subqueries_to_mark_expr(
                    input,
                    &between.high,
                    input_schema,
                )?;
                Ok((
                    input,
                    DFExpr::Between(logical_expr::expr::Between::new(
                        Box::new(expr),
                        between.negated,
                        Box::new(low),
                        Box::new(high),
                    )),
                ))
            }
            _ => Ok((input, expr.clone())),
        }
    }

    fn scalar_subquery_in_expr(expr: &DFExpr) -> Option<&logical_expr::logical_plan::Subquery> {
        match expr {
            DFExpr::ScalarSubquery(subquery) => Some(subquery),
            DFExpr::Alias(alias) => Self::scalar_subquery_in_expr(&alias.expr),
            DFExpr::Cast(cast) => Self::scalar_subquery_in_expr(&cast.expr),
            _ => None,
        }
    }

    fn try_into_optd_scalar_expr_with_subquery_column(
        &mut self,
        node: &DFExpr,
        input_schema: &DFSchema,
        subquery_column: optd_core::ir::Column,
    ) -> Result<Arc<optd_core::ir::Scalar>> {
        match node {
            DFExpr::ScalarSubquery(_) => Ok(ColumnRef::new(subquery_column).into_scalar()),
            DFExpr::Alias(alias) => self.try_into_optd_scalar_expr_with_subquery_column(
                &alias.expr,
                input_schema,
                subquery_column,
            ),
            DFExpr::Cast(cast) => {
                let input = self.try_into_optd_scalar_expr_with_subquery_column(
                    &cast.expr,
                    input_schema,
                    subquery_column,
                )?;
                Ok(Cast::new(cast.data_type.clone(), input).into_scalar())
            }
            _ => self.try_into_optd_scalar_expr(node, input_schema),
        }
    }

    fn try_into_optd_binary_compare_op(
        op: &logical_expr::Operator,
    ) -> Result<optd_core::ir::scalar::BinaryOpKind> {
        match op {
            logical_expr::Operator::Eq => Ok(optd_core::ir::scalar::BinaryOpKind::Eq),
            logical_expr::Operator::NotEq => Ok(optd_core::ir::scalar::BinaryOpKind::Ne),
            logical_expr::Operator::Lt => Ok(optd_core::ir::scalar::BinaryOpKind::Lt),
            logical_expr::Operator::LtEq => Ok(optd_core::ir::scalar::BinaryOpKind::Le),
            logical_expr::Operator::Gt => Ok(optd_core::ir::scalar::BinaryOpKind::Gt),
            logical_expr::Operator::GtEq => Ok(optd_core::ir::scalar::BinaryOpKind::Ge),
            logical_expr::Operator::IsNotDistinctFrom => {
                Ok(optd_core::ir::scalar::BinaryOpKind::IsNotDistinctFrom)
            }
            op => whatever!("Unsupported scalar-subquery comparison op: {}", op),
        }
    }

    fn try_into_optd_in_subquery(
        &mut self,
        outer: Arc<optd_core::ir::Operator>,
        node: &logical_expr::expr::InSubquery,
        input_schema: &DFSchema,
    ) -> Result<Arc<optd_core::ir::Operator>> {
        let inner = self.try_into_optd_plan(&node.subquery.subquery)?;
        let inner_cols = inner
            .output_columns_in_order(&self.inner)
            .context(OptdSnafu)?;
        if inner_cols.len() != 1 {
            whatever!(
                "InSubquery should return exactly one column, found {}",
                inner_cols.len()
            );
        }

        let expr = self.try_into_optd_scalar_expr(&node.expr, input_schema)?;
        let subquery_value = ColumnRef::new(inner_cols[0]).into_scalar();
        let join_cond = expr.eq(subquery_value);
        let join_type = if node.negated {
            JoinType::LeftAnti
        } else {
            JoinType::LeftSemi
        };

        Ok(DependentJoin::new(join_type, outer, inner, join_cond).into_operator())
    }

    fn try_into_optd_exists(
        &mut self,
        outer: Arc<optd_core::ir::Operator>,
        node: &logical_expr::expr::Exists,
    ) -> Result<Arc<optd_core::ir::Operator>> {
        let inner = self.try_into_optd_plan(&node.subquery.subquery)?;
        let join_type = if node.negated {
            JoinType::LeftAnti
        } else {
            JoinType::LeftSemi
        };

        Ok(DependentJoin::new(join_type, outer, inner, literal(true)).into_operator())
    }

    fn try_into_optd_scalar_subquery_compare(
        &mut self,
        outer: Arc<optd_core::ir::Operator>,
        node: &logical_expr::BinaryExpr,
        input_schema: &DFSchema,
    ) -> Result<Arc<optd_core::ir::Operator>> {
        let lhs_subquery = Self::scalar_subquery_in_expr(&node.left);
        let rhs_subquery = Self::scalar_subquery_in_expr(&node.right);
        let subquery = match (lhs_subquery, rhs_subquery) {
            (Some(_), Some(_)) => {
                whatever!("does not support binary predicates with scalar subqueries on both sides")
            }
            (Some(subquery), None) | (None, Some(subquery)) => subquery,
            (None, None) => whatever!("binary expr does not contain a scalar subquery"),
        };

        let inner = self.try_into_optd_plan(&subquery.subquery)?;
        let inner_cols = inner
            .output_columns_in_order(&self.inner)
            .context(OptdSnafu)?;
        if inner_cols.len() != 1 {
            whatever!(
                "ScalarSubquery should return exactly one column, found {}",
                inner_cols.len()
            );
        }

        let lhs = self.try_into_optd_scalar_expr_with_subquery_column(
            &node.left,
            input_schema,
            inner_cols[0],
        )?;
        let rhs = self.try_into_optd_scalar_expr_with_subquery_column(
            &node.right,
            input_schema,
            inner_cols[0],
        )?;
        let op_kind = Self::try_into_optd_binary_compare_op(&node.op)?;
        let join_cond = lhs.binary_op(rhs, op_kind);

        Ok(DependentJoin::new(JoinType::Inner, outer, inner, join_cond).into_operator())
    }

    fn try_rewrite_distinct_aggregate(
        &mut self,
        node: &logical_plan::Aggregate,
    ) -> Result<Option<Arc<optd_core::ir::Operator>>> {
        let aggregate_functions = node
            .aggr_expr
            .iter()
            .map(|expr| match Self::aggregate_function_in_expr(expr) {
                Some(agg_func) => Ok(agg_func),
                None => whatever!("aggregate expression should be an aggregate function: {expr}"),
            })
            .collect::<Result<Vec<_>>>()?;

        for agg_func in &aggregate_functions {
            Self::validate_aggregate_function(agg_func)?;
        }

        let Some(first_distinct) = aggregate_functions
            .iter()
            .find(|agg_func| agg_func.params.distinct)
        else {
            return Ok(None);
        };

        if aggregate_functions
            .iter()
            .any(|agg_func| !agg_func.params.distinct)
        {
            whatever!("does not support mixing DISTINCT and non-DISTINCT aggregates");
        }

        if first_distinct.params.args.is_empty() {
            whatever!("DISTINCT aggregate must have at least one argument");
        }

        if aggregate_functions
            .iter()
            .any(|agg_func| agg_func.params.args != first_distinct.params.args)
        {
            whatever!("does not support DISTINCT aggregates with different arguments");
        }

        let inner_group_expr = node
            .group_expr
            .iter()
            .cloned()
            .chain(first_distinct.params.args.iter().cloned())
            .collect_vec();
        let inner_aggregate =
            logical_plan::Aggregate::try_new(Arc::clone(&node.input), inner_group_expr, vec![])
                .context(DataFusionSnafu)?;
        let inner_plan = DFLogicalPlan::Aggregate(inner_aggregate);

        let outer_group_expr = node
            .group_expr
            .iter()
            .enumerate()
            .map(|(idx, expr)| {
                let (_, name) = expr.qualified_name();
                DFExpr::Column(DFColumn::from(inner_plan.schema().qualified_field(idx))).alias(name)
            })
            .collect_vec();

        let distinct_arg_offset = node.group_expr.len();
        let rewritten_args = (0..first_distinct.params.args.len())
            .map(|idx| {
                DFExpr::Column(DFColumn::from(
                    inner_plan
                        .schema()
                        .qualified_field(distinct_arg_offset + idx),
                ))
            })
            .collect_vec();

        let outer_aggr_expr = aggregate_functions
            .iter()
            .zip_eq(node.aggr_expr.iter())
            .map(|(agg_func, original_expr)| {
                let (_, name) = original_expr.qualified_name();
                DFExpr::AggregateFunction(AggregateFunction::new_udf(
                    Arc::clone(&agg_func.func),
                    rewritten_args.clone(),
                    false,
                    agg_func.params.filter.clone(),
                    agg_func.params.order_by.clone(),
                    agg_func.params.null_treatment,
                ))
                .alias(name)
            })
            .collect_vec();

        let outer_aggregate = logical_plan::Aggregate::try_new_with_schema(
            Arc::new(inner_plan),
            outer_group_expr,
            outer_aggr_expr,
            Arc::clone(&node.schema),
        )
        .context(DataFusionSnafu)?;

        self.try_into_optd_plan(&DFLogicalPlan::Aggregate(outer_aggregate))
            .map(Some)
    }

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
            None => optd_builder::literal(0_i64),
        };

        Ok(Limit::new(input, skip, fetch).into_operator())
    }

    pub fn try_into_optd_order_by(
        &mut self,
        node: &logical_plan::Sort,
    ) -> Result<Arc<optd_core::ir::Operator>> {
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

        let operator = OrderBy::new(input, ordering_exprs).into_operator();

        let Some(fetch) = node.fetch else {
            return Ok(operator);
        };

        let fetch = i64::try_from(fetch).whatever_context("sort fetch exceeds i64 range")?;
        let skip = literal(0_i64);
        let fetch = literal(Some(fetch));

        Ok(Limit::new(operator, skip, fetch).into_operator())
    }

    pub fn try_into_optd_aggregate(
        &mut self,
        node: &logical_plan::Aggregate,
    ) -> Result<Arc<optd_core::ir::Operator>> {
        if let Some(rewritten) = self.try_rewrite_distinct_aggregate(node)? {
            return Ok(rewritten);
        }

        let input = self.try_into_optd_plan(&node.input)?;

        let keys = node
            .group_expr
            .iter()
            .map(|e| self.try_into_optd_scalar_expr(e, node.input.schema()))
            .try_collect()
            .map(List::new)?;
        let exprs = node
            .aggr_expr
            .iter()
            .map(|e| self.try_into_optd_scalar_expr(e, node.input.schema()))
            .try_collect()
            .map(List::new)
            .with_whatever_context(|e| format!("error converting aggregate expressions: {e}"))?;

        let key_schema = node
            .group_expr
            .iter()
            .map(|e| {
                let (_, name) = e.qualified_name();
                e.to_field(node.input.schema())
                    .map(|(_, field)| {
                        Arc::new(Field::new(
                            name,
                            field.data_type().clone(),
                            field.is_nullable(),
                        ))
                    })
                    .context(DataFusionSnafu)
            })
            .collect::<Result<Vec<_>>>()
            .map(Schema::new)?;

        let key_table_index = self
            .inner
            .add_binding(None, Arc::new(key_schema))
            .context(OptdSnafu)?;

        let aggrgate_schema = node
            .aggr_expr
            .iter()
            .map(|e| {
                let (_, name) = e.qualified_name();
                e.to_field(node.input.schema())
                    .map(|(_, field)| {
                        Arc::new(Field::new(
                            name,
                            field.data_type().clone(),
                            field.is_nullable(),
                        ))
                    })
                    .context(DataFusionSnafu)
            })
            .collect::<Result<Vec<_>>>()
            .map(Schema::new)?;

        let aggregate_table_index = self
            .inner
            .add_binding(None, Arc::new(aggrgate_schema))
            .context(OptdSnafu)?;

        let aggregate = Aggregate::new(
            key_table_index,
            aggregate_table_index,
            input,
            exprs.into_scalar(),
            keys.into_scalar(),
            None,
        );

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
            .map(List::new)
            .with_whatever_context(|e| format!("error converting projection: {e}"))?;
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
        let join_type = match node.join_type {
            DFJoinType::LeftMark => {
                let (qualifier, field) = node
                    .schema
                    .iter()
                    .last()
                    .with_whatever_context(|| "LeftMark join should expose a marker column")?;
                let df_mark_column = DFColumn::new(qualifier.cloned(), field.name());
                let mark_column = self.allocate_df_mark_column(df_mark_column)?;
                JoinType::Mark(mark_column)
            }
            DFJoinType::RightMark => whatever!("Unsupported join type: {}", node.join_type),
            _ => Self::try_into_optd_join_type(node.join_type)?,
        };

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
        let join = Join::new(join_type, left, right, join_cond, None);

        Ok(join.into_operator())
    }

    pub fn try_into_optd_select(
        &mut self,
        node: &logical_plan::Filter,
    ) -> Result<Arc<optd_core::ir::Operator>> {
        let input = self.try_into_optd_plan(node.input.as_ref())?;
        let mut current = input;
        let mut residual = Vec::new();
        let mut conjuncts = Vec::new();
        Self::df_conjuncts(&node.predicate, &mut conjuncts);

        for conjunct in conjuncts {
            match conjunct {
                DFExpr::InSubquery(in_subquery) => {
                    current =
                        self.flush_residual_filter(current, &mut residual, node.input.schema())?;
                    current =
                        self.try_into_optd_in_subquery(current, in_subquery, node.input.schema())?;
                }
                DFExpr::Exists(exists) => {
                    current =
                        self.flush_residual_filter(current, &mut residual, node.input.schema())?;
                    current = self.try_into_optd_exists(current, exists)?;
                }
                DFExpr::BinaryExpr(binary_expr)
                    if Self::scalar_subquery_in_expr(&binary_expr.left).is_some()
                        || Self::scalar_subquery_in_expr(&binary_expr.right).is_some() =>
                {
                    current =
                        self.flush_residual_filter(current, &mut residual, node.input.schema())?;
                    current = self.try_into_optd_scalar_subquery_compare(
                        current,
                        binary_expr,
                        node.input.schema(),
                    )?;
                }
                expr => residual.push(expr.clone()),
            }
        }

        self.flush_residual_filter(current, &mut residual, node.input.schema())
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
            DFExpr::OuterReferenceColumn(_, column) => self.try_into_optd_outer_column_ref(column),
            DFExpr::Literal(literal, _) => self.try_into_optd_literal(literal),
            DFExpr::BinaryExpr(binary_expr) => {
                self.try_into_optd_scalar_op(binary_expr, input_schema)
            }
            DFExpr::Alias(alias) => self.try_into_optd_scalar_expr(&alias.expr, input_schema),
            DFExpr::AggregateFunction(agg_func) => {
                self.try_into_optd_aggregate_func(agg_func, input_schema)
            }
            DFExpr::ScalarFunction(scalar_func) => {
                self.try_into_optd_scalar_func(scalar_func, input_schema)
            }
            DFExpr::Cast(cast) => self.try_into_optd_cast(cast, input_schema),
            DFExpr::Like(like) => self.try_into_optd_like(like, input_schema),
            DFExpr::Case(case) => self.try_into_optd_case(case, input_schema),
            DFExpr::Not(expr) => {
                let expr = self.try_into_optd_scalar_expr(expr, input_schema)?;
                Ok(expr.eq(literal(false)))
            }
            DFExpr::InList(in_list) => self.try_into_optd_in_list(in_list, input_schema),
            DFExpr::Between(between) => self.try_into_optd_between(between, input_schema),
            DFExpr::IsNull(expr) => self.try_into_optd_is_null(expr, input_schema),
            DFExpr::IsNotNull(expr) => self.try_into_optd_is_not_null(expr, input_schema),
            expr => {
                whatever!("Unsupported df logical expr: {}", expr);
            }
        }
    }

    pub fn try_into_optd_column_ref(
        &self,
        column: &datafusion::common::Column,
    ) -> Result<Arc<Scalar>> {
        let column = match self.try_get_optd_column(column.relation.as_ref(), &column.name) {
            Ok(column) => column,
            // attempt to match unmatched table_ref.
            Err(_) => self.try_get_optd_column(None, &column.name)?,
        };
        Ok(ColumnRef::new(column).into_scalar())
    }

    pub fn try_into_optd_outer_column_ref(
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
            logical_expr::Operator::NotEq => Either::Left(optd_core::ir::scalar::BinaryOpKind::Ne),
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
            logical_expr::Operator::IsDistinctFrom => {
                Either::Left(optd_core::ir::scalar::BinaryOpKind::IsDistinctFrom)
            }
            logical_expr::Operator::IsNotDistinctFrom => {
                Either::Left(optd_core::ir::scalar::BinaryOpKind::IsNotDistinctFrom)
            }
            logical_expr::Operator::RegexMatch => {
                Either::Left(optd_core::ir::scalar::BinaryOpKind::RegexMatch)
            }
            logical_expr::Operator::RegexIMatch => {
                Either::Left(optd_core::ir::scalar::BinaryOpKind::RegexIMatch)
            }
            logical_expr::Operator::RegexNotMatch => {
                Either::Left(optd_core::ir::scalar::BinaryOpKind::RegexNotMatch)
            }
            logical_expr::Operator::RegexNotIMatch => {
                Either::Left(optd_core::ir::scalar::BinaryOpKind::RegexNotIMatch)
            }
            logical_expr::Operator::LikeMatch => {
                Either::Left(optd_core::ir::scalar::BinaryOpKind::LikeMatch)
            }
            logical_expr::Operator::ILikeMatch => {
                Either::Left(optd_core::ir::scalar::BinaryOpKind::ILikeMatch)
            }
            logical_expr::Operator::NotLikeMatch => {
                Either::Left(optd_core::ir::scalar::BinaryOpKind::NotLikeMatch)
            }
            logical_expr::Operator::NotILikeMatch => {
                Either::Left(optd_core::ir::scalar::BinaryOpKind::NotILikeMatch)
            }
            logical_expr::Operator::And => Either::Right(optd_core::ir::scalar::NaryOpKind::And),
            logical_expr::Operator::Or => Either::Right(optd_core::ir::scalar::NaryOpKind::Or),
            logical_expr::Operator::BitwiseAnd => {
                Either::Left(optd_core::ir::scalar::BinaryOpKind::BitwiseAnd)
            }
            logical_expr::Operator::BitwiseOr => {
                Either::Left(optd_core::ir::scalar::BinaryOpKind::BitwiseOr)
            }
            logical_expr::Operator::BitwiseXor => {
                Either::Left(optd_core::ir::scalar::BinaryOpKind::BitwiseXor)
            }
            logical_expr::Operator::BitwiseShiftRight => {
                Either::Left(optd_core::ir::scalar::BinaryOpKind::BitwiseShiftRight)
            }
            logical_expr::Operator::BitwiseShiftLeft => {
                Either::Left(optd_core::ir::scalar::BinaryOpKind::BitwiseShiftLeft)
            }
            logical_expr::Operator::StringConcat => {
                Either::Left(optd_core::ir::scalar::BinaryOpKind::StringConcat)
            }
            logical_expr::Operator::AtArrow => {
                Either::Left(optd_core::ir::scalar::BinaryOpKind::AtArrow)
            }
            logical_expr::Operator::ArrowAt => {
                Either::Left(optd_core::ir::scalar::BinaryOpKind::ArrowAt)
            }
            logical_expr::Operator::Arrow => {
                Either::Left(optd_core::ir::scalar::BinaryOpKind::Arrow)
            }
            logical_expr::Operator::LongArrow => {
                Either::Left(optd_core::ir::scalar::BinaryOpKind::LongArrow)
            }
            logical_expr::Operator::HashArrow => {
                Either::Left(optd_core::ir::scalar::BinaryOpKind::HashArrow)
            }
            logical_expr::Operator::HashLongArrow => {
                Either::Left(optd_core::ir::scalar::BinaryOpKind::HashLongArrow)
            }
            logical_expr::Operator::AtAt => Either::Left(optd_core::ir::scalar::BinaryOpKind::AtAt),
            logical_expr::Operator::IntegerDivide => {
                Either::Left(optd_core::ir::scalar::BinaryOpKind::IntegerDivide)
            }
            logical_expr::Operator::HashMinus => {
                Either::Left(optd_core::ir::scalar::BinaryOpKind::HashMinus)
            }
            logical_expr::Operator::AtQuestion => {
                Either::Left(optd_core::ir::scalar::BinaryOpKind::AtQuestion)
            }
            logical_expr::Operator::Question => {
                Either::Left(optd_core::ir::scalar::BinaryOpKind::Question)
            }
            logical_expr::Operator::QuestionAnd => {
                Either::Left(optd_core::ir::scalar::BinaryOpKind::QuestionAnd)
            }
            logical_expr::Operator::QuestionPipe => {
                Either::Left(optd_core::ir::scalar::BinaryOpKind::QuestionPipe)
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

        Self::validate_aggregate_function(agg_func)?;

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

    pub fn try_into_optd_scalar_func(
        &mut self,
        scalar_func: &ScalarFunction,
        input_schema: &DFSchema,
    ) -> Result<Arc<Scalar>> {
        let func_name = scalar_func.name();

        let params: Vec<_> = scalar_func
            .args
            .iter()
            .map(|x| self.try_into_optd_scalar_expr(x, input_schema))
            .try_collect()?;

        let return_type = DFExpr::ScalarFunction(scalar_func.clone())
            .get_type(input_schema)
            .context(DataFusionSnafu)?;

        Ok(Function::new_scalar(func_name.to_string(), params.into(), return_type).into_scalar())
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

    pub fn try_into_optd_case(
        &mut self,
        node: &logical_expr::expr::Case,
        input_schema: &DFSchema,
    ) -> Result<Arc<Scalar>> {
        let expr = node
            .expr
            .as_ref()
            .map(|expr| self.try_into_optd_scalar_expr(expr, input_schema))
            .transpose()?;
        let when_then_expr: Vec<_> = node
            .when_then_expr
            .iter()
            .map(|(when, then)| {
                Ok((
                    self.try_into_optd_scalar_expr(when, input_schema)?,
                    self.try_into_optd_scalar_expr(then, input_schema)?,
                ))
            })
            .collect::<Result<_>>()?;
        let else_expr = node
            .else_expr
            .as_ref()
            .map(|expr| self.try_into_optd_scalar_expr(expr, input_schema))
            .transpose()?;

        Ok(Case::new(expr, when_then_expr.into(), else_expr).into_scalar())
    }

    pub fn try_into_optd_between(
        &mut self,
        node: &logical_expr::expr::Between,
        input_schema: &DFSchema,
    ) -> Result<Arc<Scalar>> {
        let expr = self.try_into_optd_scalar_expr(&node.expr, input_schema)?;
        let low = self.try_into_optd_scalar_expr(&node.low, input_schema)?;
        let high = self.try_into_optd_scalar_expr(&node.high, input_schema)?;

        if node.negated {
            Ok(expr.clone().lt(low).or(expr.gt(high)))
        } else {
            Ok(expr.clone().ge(low).and(expr.le(high)))
        }
    }

    pub fn try_into_optd_in_list(
        &mut self,
        node: &logical_expr::expr::InList,
        input_schema: &DFSchema,
    ) -> Result<Arc<Scalar>> {
        let expr = self.try_into_optd_scalar_expr(&node.expr, input_schema)?;
        let list = node
            .list
            .iter()
            .map(|e| self.try_into_optd_scalar_expr(e, input_schema))
            .try_collect()
            .map(List::new)?;

        Ok(InList::new(expr, list.into_scalar(), node.negated).into_scalar())
    }

    pub fn try_into_optd_is_null(
        &mut self,
        expr: &DFExpr,
        input_schema: &DFSchema,
    ) -> Result<Arc<Scalar>> {
        let expr = self.try_into_optd_scalar_expr(expr, input_schema)?;
        Ok(optd_builder::is_null(expr))
    }

    pub fn try_into_optd_is_not_null(
        &mut self,
        expr: &DFExpr,
        input_schema: &DFSchema,
    ) -> Result<Arc<Scalar>> {
        let expr = self.try_into_optd_scalar_expr(expr, input_schema)?;
        Ok(optd_builder::is_not_null(expr))
    }
}
