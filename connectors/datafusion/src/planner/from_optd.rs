use std::{sync::Arc, vec};

use datafusion::{
    common::{Column, DFSchema},
    execution::FunctionRegistry,
    logical_expr::{
        self, Expr as DFExpr, LogicalPlan as DFLogicalPlan, logical_plan,
        utils::enumerate_grouping_sets,
    },
};
use itertools::Itertools;
use optd_core::ir::{
    Operator, OperatorKind, Scalar,
    catalog::Field,
    convert::IntoOperator,
    operator::{
        Aggregate, AggregateBorrowed, EnforcerSort, EnforcerSortBorrowed, Get, GetBorrowed, Join,
        JoinBorrowed, Limit, LimitBorrowed, Project, ProjectBorrowed, Remap, RemapBorrowed, Select,
        SelectBorrowed, split_equi_and_non_equi_conditions,
    },
    properties::TupleOrderingDirection,
    scalar::{
        BinaryOp, BinaryOpBorrowed, BinaryOpKind, Case, CaseBorrowed, Cast, CastBorrowed,
        ColumnRef, ColumnRefBorrowed, Function, FunctionBorrowed, FunctionKind, InList,
        InListBorrowed, IsNotNull, IsNotNullBorrowed, IsNull, IsNullBorrowed, Like, LikeBorrowed,
        List, Literal, LiteralBorrowed, NaryOp, NaryOpBorrowed, NaryOpKind,
    },
    schema::OptdSchema,
    table_ref::TableRef,
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
        let optd_schema = binding.optd_schema();
        let exprs = Self::alias_exprs_for_schema(exprs, &optd_schema)?;
        let schema = Self::df_schema_from_optd_schema(&optd_schema)?;
        logical_expr::Projection::try_new_with_schema(exprs, input, Arc::new(schema))
            .context(DataFusionSnafu)
    }

    fn alias_if_changed(expr: DFExpr, table_ref: &TableRef, field: &Field) -> Result<DFExpr> {
        if table_ref.table().contains("__#") {
            expr.alias_if_changed(field.name().clone())
                .context(DataFusionSnafu)
        } else {
            let column_name = Column::new(
                Some(Self::from_optd_table_ref(table_ref)),
                field.name().clone(),
            )
            .to_string();
            expr.alias_if_changed(column_name).context(DataFusionSnafu)
        }
    }

    fn try_new_df_aggregate(
        &self,
        output_schema: &OptdSchema,
        input: Arc<DFLogicalPlan>,
        group_expr: Vec<DFExpr>,
        aggr_expr: Vec<DFExpr>,
    ) -> Result<logical_expr::Aggregate> {
        let qualified_fields = output_schema
            .iter()
            .map(|(table_ref, field)| (table_ref.clone(), field.clone()))
            .collect_vec();
        let (group_fields, aggr_fields) = qualified_fields.split_at(group_expr.len());
        let group_expr = Self::alias_exprs_for_qualified_fields(group_expr, group_fields)?;
        let group_expr = enumerate_grouping_sets(group_expr).context(DataFusionSnafu)?;
        let aggr_expr = Self::alias_exprs_for_qualified_fields(aggr_expr, aggr_fields)?;
        let schema = Self::df_schema_from_optd_schema(output_schema)?;

        logical_expr::Aggregate::try_new_with_schema(input, group_expr, aggr_expr, Arc::new(schema))
            .context(DataFusionSnafu)
    }

    fn alias_exprs_for_schema(exprs: Vec<DFExpr>, optd_schema: &OptdSchema) -> Result<Vec<DFExpr>> {
        let qualified_fields = optd_schema
            .iter()
            .map(|(table_ref, field)| (table_ref.clone(), field.clone()))
            .collect_vec();
        Self::alias_exprs_for_qualified_fields(exprs, qualified_fields.as_slice())
    }

    fn alias_exprs_for_qualified_fields(
        exprs: Vec<DFExpr>,
        qualified_fields: &[(TableRef, Arc<Field>)],
    ) -> Result<Vec<DFExpr>> {
        qualified_fields
            .iter()
            .zip_eq(exprs)
            .map(|((table_ref, field), expr)| Self::alias_if_changed(expr, table_ref, field))
            .collect()
    }

    fn df_schema_from_optd_schema(optd_schema: &OptdSchema) -> Result<DFSchema> {
        let qualified_fields = optd_schema
            .iter()
            .map(|(table_ref, field)| (Some(Self::from_optd_table_ref(table_ref)), field.clone()))
            .collect_vec();
        DFSchema::new_with_metadata(qualified_fields, optd_schema.inner().metadata().clone())
            .context(DataFusionSnafu)
    }

    pub fn try_from_optd_aggregate(
        &mut self,
        node: AggregateBorrowed<'_>,
    ) -> Result<DFLogicalPlan> {
        let output_schema = Aggregate::new(
            *node.key_table_index(),
            *node.aggregate_table_index(),
            node.input().clone(),
            node.exprs().clone(),
            node.keys().clone(),
            *node.implementation(),
        )
        .into_operator()
        .output_schema(&self.inner)
        .context(OptdSnafu)?;

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

        let aggregate =
            self.try_new_df_aggregate(&output_schema, Arc::new(input), group_expr, aggr_expr)?;
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
        let input = self.try_from_optd_plan(node.input())?;
        let projection_list = node.projections().borrow::<List>();
        let exprs = projection_list
            .members()
            .iter()
            .map(|e| self.try_from_optd_scalar_expr(e))
            .try_collect()?;

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
        if let optd_core::ir::operator::join::JoinType::Mark(mark_column) = node.join_type() {
            let (qualifier, field) = join
                .schema
                .iter()
                .last()
                .with_whatever_context(|| "LeftMark join should expose a marker column")?;
            self.register_optd_mark_column(
                *mark_column,
                Column::new(qualifier.cloned(), field.name()),
            );
        }
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::{
        arrow::datatypes::{DataType, Field, Schema},
        common::{Column, DFSchema, JoinType, NullEquality},
        execution::FunctionRegistry,
        execution::runtime_env::RuntimeEnv,
        functions_aggregate::expr_fn::sum,
        logical_expr::{self, Expr as DFExpr, LogicalPlan as DFLogicalPlan, expr_fn},
        prelude::SessionConfig,
        scalar::ScalarValue,
        sql::TableReference,
    };
    use optd_core::ir::{
        catalog::Catalog,
        explain::quick_explain,
        operator::OperatorKind,
        scalar::{Case, Function, FunctionKind},
        schema::OptdSchema,
        table_ref::TableRef,
    };
    use optd_core::rules::UnnestingRule;

    use crate::{
        create_optd_session_context, create_optd_session_context_with_catalog, memory_catalog,
    };

    use super::super::{OptdQueryPlanner, OptdQueryPlannerContext};

    fn new_test_ctx() -> OptdQueryPlannerContext<'static> {
        let session_ctx =
            create_optd_session_context(SessionConfig::new(), Arc::new(RuntimeEnv::default()));
        let session_state = Box::leak(Box::new(session_ctx.state()));
        let inner = OptdQueryPlanner::create_context(session_state).unwrap();
        OptdQueryPlannerContext::new(inner, session_state)
    }

    fn new_test_ctx_with_catalog() -> (OptdQueryPlannerContext<'static>, Arc<dyn Catalog>) {
        let catalog = memory_catalog();
        let session_ctx = create_optd_session_context_with_catalog(
            SessionConfig::new(),
            Arc::new(RuntimeEnv::default()),
            catalog.clone(),
        );
        let session_state = Box::leak(Box::new(session_ctx.state()));
        let inner = OptdQueryPlanner::create_context(session_state).unwrap();
        (OptdQueryPlannerContext::new(inner, session_state), catalog)
    }

    #[test]
    fn try_new_df_projection_restores_binding_aliases() {
        let session_ctx =
            create_optd_session_context(SessionConfig::new(), Arc::new(RuntimeEnv::default()));
        let session_state = session_ctx.state();
        let inner = OptdQueryPlanner::create_context(&session_state).unwrap();
        let ctx = OptdQueryPlannerContext::new(inner.clone(), &session_state);

        let input_schema = Arc::new(Schema::new(vec![
            Field::new("l_extendedprice", DataType::Int64, false),
            Field::new("l_discount", DataType::Int64, false),
            Field::new("l_quantity", DataType::Int64, false),
        ]));
        let input_schema =
            DFSchema::try_from_qualified_schema("lineitem", input_schema.as_ref()).unwrap();
        let input = DFLogicalPlan::EmptyRelation(logical_expr::EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(input_schema),
        });

        let project_schema = Arc::new(Schema::new(vec![
            Field::new("__common_expr_1", DataType::Int64, false),
            Field::new("l_quantity", DataType::Int64, false),
        ]));
        let table_index = inner.add_binding(None, project_schema).unwrap();

        let exprs = vec![
            logical_expr::binary_expr(
                DFExpr::Column(Column::from_qualified_name("lineitem.l_extendedprice")),
                logical_expr::Operator::Minus,
                DFExpr::Column(Column::from_qualified_name("lineitem.l_discount")),
            ),
            DFExpr::Column(Column::from_qualified_name("lineitem.l_quantity")),
        ];

        let projection = ctx
            .try_new_df_projection(&table_index, exprs, Arc::new(input))
            .unwrap();

        assert_eq!(projection.schema.field(0).name(), "__common_expr_1");
        assert_eq!(projection.schema.field(1).name(), "l_quantity");
        assert_eq!(
            projection.expr[0].schema_name().to_string(),
            "__common_expr_1"
        );
    }

    #[test]
    fn try_new_df_aggregate_restores_binding_aliases() {
        let session_ctx =
            create_optd_session_context(SessionConfig::new(), Arc::new(RuntimeEnv::default()));
        let session_state = session_ctx.state();
        let inner = OptdQueryPlanner::create_context(&session_state).unwrap();
        let ctx = OptdQueryPlannerContext::new(inner.clone(), &session_state);

        let input_schema = Arc::new(Schema::new(vec![
            Field::new("l_returnflag", DataType::Utf8, false),
            Field::new("l_quantity", DataType::Int64, false),
        ]));
        let input_schema =
            DFSchema::try_from_qualified_schema("__#2", input_schema.as_ref()).unwrap();
        let input = DFLogicalPlan::EmptyRelation(logical_expr::EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(input_schema),
        });

        let aggregate_schema = Arc::new(Schema::new(vec![Field::new(
            "sum(lineitem.l_quantity)",
            DataType::Int64,
            false,
        )]));
        let key_schema = Arc::new(Schema::new(vec![Field::new(
            "l_quantity",
            DataType::Int64,
            false,
        )]));
        let _aggregate_table_index = inner.add_binding(None, aggregate_schema).unwrap();
        let _key_table_index = inner.add_binding(None, key_schema).unwrap();
        let output_schema = OptdSchema::new_with_metadata(
            vec![
                (
                    TableRef::bare("__#3"),
                    Arc::new(Field::new("l_returnflag", DataType::Utf8, false)),
                ),
                (
                    TableRef::bare("__#4"),
                    Arc::new(Field::new(
                        "sum(lineitem.l_quantity)",
                        DataType::Int64,
                        false,
                    )),
                ),
            ],
            Default::default(),
        )
        .unwrap();

        let aggregate = ctx
            .try_new_df_aggregate(
                &output_schema,
                Arc::new(input),
                vec![DFExpr::Column(Column::from_qualified_name(
                    "__#2.l_returnflag",
                ))],
                vec![sum(DFExpr::Column(Column::from_qualified_name(
                    "__#2.l_quantity",
                )))],
            )
            .unwrap();

        assert_eq!(aggregate.schema.field(0).name(), "l_returnflag");
        assert_eq!(aggregate.schema.field(1).name(), "sum(lineitem.l_quantity)");
        assert_eq!(
            aggregate.aggr_expr[0].schema_name().to_string(),
            "sum(lineitem.l_quantity)"
        );
    }

    #[test]
    fn try_new_df_aggregate_keeps_group_keys_bindable_by_qualified_name() {
        let session_ctx =
            create_optd_session_context(SessionConfig::new(), Arc::new(RuntimeEnv::default()));
        let session_state = session_ctx.state();
        let inner = OptdQueryPlanner::create_context(&session_state).unwrap();
        let ctx = OptdQueryPlannerContext::new(inner.clone(), &session_state);

        let input_schema = Arc::new(Schema::new(vec![
            Field::new("p_brand", DataType::Utf8, false),
            Field::new("p_partkey", DataType::Int64, false),
        ]));
        let input_schema =
            DFSchema::try_from_qualified_schema("part", input_schema.as_ref()).unwrap();
        let input = DFLogicalPlan::EmptyRelation(logical_expr::EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(input_schema),
        });

        let key_schema = Arc::new(Schema::new(vec![Field::new(
            "p_brand",
            DataType::Utf8,
            false,
        )]));
        let aggregate_schema = Arc::new(Schema::new(vec![Field::new(
            "sum(part.p_partkey)",
            DataType::Int64,
            false,
        )]));
        let _key_table_index = inner.add_binding(None, key_schema).unwrap();
        let _aggregate_table_index = inner.add_binding(None, aggregate_schema).unwrap();
        let output_schema = OptdSchema::new_with_metadata(
            vec![
                (
                    TableRef::bare("part"),
                    Arc::new(Field::new("p_brand", DataType::Utf8, false)),
                ),
                (
                    TableRef::bare("__#4"),
                    Arc::new(Field::new("sum(part.p_partkey)", DataType::Int64, false)),
                ),
            ],
            Default::default(),
        )
        .unwrap();

        let aggregate = ctx
            .try_new_df_aggregate(
                &output_schema,
                Arc::new(input),
                vec![DFExpr::Column(Column::from_qualified_name("part.p_brand"))],
                vec![sum(DFExpr::Column(Column::from_qualified_name(
                    "part.p_partkey",
                )))],
            )
            .unwrap();

        assert!(
            aggregate
                .schema
                .has_column(&Column::from_qualified_name("part.p_brand"))
        );

        logical_expr::Projection::try_new(
            vec![DFExpr::Column(Column::from_qualified_name("part.p_brand"))],
            Arc::new(DFLogicalPlan::Aggregate(aggregate)),
        )
        .unwrap();
    }

    #[test]
    fn case_expr_round_trips_without_base_expr() {
        let mut ctx = new_test_ctx();
        let expr = DFExpr::Case(logical_expr::expr::Case::new(
            None,
            vec![(
                Box::new(logical_expr::binary_expr(
                    DFExpr::Literal(datafusion::scalar::ScalarValue::Int64(Some(1)), None),
                    logical_expr::Operator::Eq,
                    DFExpr::Literal(datafusion::scalar::ScalarValue::Int64(Some(1)), None),
                )),
                Box::new(DFExpr::Literal(
                    datafusion::scalar::ScalarValue::Utf8(Some("match".into())),
                    None,
                )),
            )],
            Some(Box::new(DFExpr::Literal(
                datafusion::scalar::ScalarValue::Utf8(Some("miss".into())),
                None,
            ))),
        ));

        let optd_expr = ctx
            .try_into_optd_scalar_expr(&expr, &DFSchema::empty())
            .unwrap();
        let optd_case = optd_expr.borrow::<Case>();
        assert!(optd_case.expr().is_none());
        assert_eq!(optd_case.when_then_expr().len(), 1);
        assert!(optd_case.else_expr().is_some());

        let restored = ctx.try_from_optd_scalar_expr(optd_expr.as_ref()).unwrap();
        assert_eq!(restored, expr);
    }

    #[test]
    fn case_expr_round_trips_with_base_expr() {
        let mut ctx = new_test_ctx();
        let expr = DFExpr::Case(logical_expr::expr::Case::new(
            Some(Box::new(DFExpr::Literal(
                datafusion::scalar::ScalarValue::Int64(Some(2)),
                None,
            ))),
            vec![
                (
                    Box::new(DFExpr::Literal(
                        datafusion::scalar::ScalarValue::Int64(Some(1)),
                        None,
                    )),
                    Box::new(DFExpr::Literal(
                        datafusion::scalar::ScalarValue::Utf8(Some("one".into())),
                        None,
                    )),
                ),
                (
                    Box::new(DFExpr::Literal(
                        datafusion::scalar::ScalarValue::Int64(Some(2)),
                        None,
                    )),
                    Box::new(DFExpr::Literal(
                        datafusion::scalar::ScalarValue::Utf8(Some("two".into())),
                        None,
                    )),
                ),
            ],
            Some(Box::new(DFExpr::Literal(
                datafusion::scalar::ScalarValue::Utf8(Some("other".into())),
                None,
            ))),
        ));

        let optd_expr = ctx
            .try_into_optd_scalar_expr(&expr, &DFSchema::empty())
            .unwrap();
        let optd_case = optd_expr.borrow::<Case>();
        assert!(optd_case.expr().is_some());
        assert_eq!(optd_case.when_then_expr().len(), 2);
        assert!(optd_case.else_expr().is_some());

        let restored = ctx.try_from_optd_scalar_expr(&optd_expr).unwrap();
        assert_eq!(restored, expr);
    }

    #[test]
    fn scalar_function_expr_round_trips() {
        let mut ctx = new_test_ctx();
        let udf = ctx.session_state.udf("lower").unwrap();
        let expr = DFExpr::ScalarFunction(logical_expr::expr::ScalarFunction::new_udf(
            udf,
            vec![DFExpr::Literal(ScalarValue::Utf8(Some("ABC".into())), None)],
        ));

        let optd_expr = ctx
            .try_into_optd_scalar_expr(&expr, &DFSchema::empty())
            .unwrap();
        let optd_function = optd_expr.borrow::<Function>();
        assert_eq!(optd_function.kind(), &FunctionKind::Scalar);
        assert_eq!(optd_function.id().as_ref(), "lower");
        assert_eq!(optd_function.params().len(), 1);

        let restored = ctx.try_from_optd_scalar_expr(optd_expr.as_ref()).unwrap();
        assert_eq!(restored, expr);
    }

    #[test]
    fn null_test_exprs_round_trip() {
        let mut ctx = new_test_ctx();
        let exprs = [
            DFExpr::IsNull(Box::new(DFExpr::Literal(ScalarValue::Utf8(None), None))),
            DFExpr::IsNotNull(Box::new(DFExpr::Literal(
                ScalarValue::Utf8(Some("x".into())),
                None,
            ))),
        ];

        for expr in exprs {
            let optd_expr = ctx
                .try_into_optd_scalar_expr(&expr, &DFSchema::empty())
                .unwrap();
            let restored = ctx.try_from_optd_scalar_expr(optd_expr.as_ref()).unwrap();
            assert_eq!(restored, expr);
        }
    }

    #[test]
    fn binary_operator_expr_round_trips_for_extended_operator_set() {
        let mut ctx = new_test_ctx();
        let lhs = DFExpr::Literal(ScalarValue::Utf8(Some("lhs".into())), None);
        let rhs = DFExpr::Literal(ScalarValue::Utf8(Some("rhs".into())), None);
        let operators = [
            logical_expr::Operator::IsDistinctFrom,
            logical_expr::Operator::RegexMatch,
            logical_expr::Operator::RegexNotIMatch,
            logical_expr::Operator::LikeMatch,
            logical_expr::Operator::NotILikeMatch,
            logical_expr::Operator::BitwiseAnd,
            logical_expr::Operator::BitwiseShiftLeft,
            logical_expr::Operator::StringConcat,
            logical_expr::Operator::AtArrow,
            logical_expr::Operator::Arrow,
            logical_expr::Operator::HashLongArrow,
            logical_expr::Operator::AtAt,
            logical_expr::Operator::IntegerDivide,
            logical_expr::Operator::HashMinus,
            logical_expr::Operator::AtQuestion,
            logical_expr::Operator::Question,
            logical_expr::Operator::QuestionAnd,
            logical_expr::Operator::QuestionPipe,
        ];

        for op in operators {
            let expr = logical_expr::binary_expr(lhs.clone(), op, rhs.clone());
            let optd_expr = ctx
                .try_into_optd_scalar_expr(&expr, &DFSchema::empty())
                .unwrap();
            let restored = ctx.try_from_optd_scalar_expr(optd_expr.as_ref()).unwrap();
            assert_eq!(restored, expr, "failed round trip for operator {op}");
        }
    }

    #[test]
    fn filter_plan_round_trips_with_extended_selection_operators() {
        let (mut ctx, catalog) = new_test_ctx_with_catalog();
        let schema = Schema::new(vec![Field::new("value", DataType::Utf8, true)]);
        catalog
            .create_table(TableRef::bare("t"), Arc::new(schema.clone()), None)
            .unwrap();

        let predicate = logical_expr::and(
            logical_expr::binary_expr(
                DFExpr::Literal(ScalarValue::Utf8(Some("abc".into())), None),
                logical_expr::Operator::RegexMatch,
                DFExpr::Literal(ScalarValue::Utf8(Some("^a".into())), None),
            ),
            logical_expr::binary_expr(
                DFExpr::Literal(ScalarValue::Int64(Some(1)), None),
                logical_expr::Operator::IsDistinctFrom,
                DFExpr::Literal(ScalarValue::Int64(Some(2)), None),
            ),
        );
        let plan = logical_expr::logical_plan::builder::table_scan(Some("t"), &schema, None)
            .unwrap()
            .filter(predicate.clone())
            .unwrap()
            .build()
            .unwrap();

        let optd_plan = ctx.try_into_optd_plan(&plan).unwrap();
        let restored = ctx.try_from_optd_plan(optd_plan.as_ref()).unwrap();

        let DFLogicalPlan::Filter(restored_filter) = restored else {
            panic!("expected filter plan after round trip");
        };
        assert_eq!(restored_filter.predicate, predicate);
    }

    #[test]
    fn left_mark_join_plan_round_trips() {
        let (mut ctx, catalog) = new_test_ctx_with_catalog();
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        catalog
            .create_table(TableRef::bare("t1"), schema.clone(), None)
            .unwrap();
        catalog
            .create_table(TableRef::bare("t2"), schema.clone(), None)
            .unwrap();

        let left = logical_expr::logical_plan::builder::table_scan(Some("t1"), &schema, None)
            .unwrap()
            .build()
            .unwrap();
        let right = logical_expr::logical_plan::builder::table_scan(Some("t2"), &schema, None)
            .unwrap()
            .build()
            .unwrap();
        let join = logical_expr::logical_plan::Join::try_new(
            Arc::new(left),
            Arc::new(right),
            vec![(
                DFExpr::Column(Column::from_qualified_name("t1.a")),
                DFExpr::Column(Column::from_qualified_name("t2.a")),
            )],
            None,
            JoinType::LeftMark,
            logical_expr::JoinConstraint::On,
            NullEquality::NullEqualsNothing,
        )
        .unwrap();
        let predicate = logical_expr::or(
            DFExpr::Column(Column::from_qualified_name("t2.mark")),
            logical_expr::binary_expr(
                DFExpr::Column(Column::from_qualified_name("t1.a")),
                logical_expr::Operator::Eq,
                DFExpr::Literal(ScalarValue::Int64(Some(1)), None),
            ),
        );
        let plan = logical_expr::LogicalPlanBuilder::from(DFLogicalPlan::Join(join))
            .filter(predicate.clone())
            .unwrap()
            .build()
            .unwrap();

        let optd_plan = ctx.try_into_optd_plan(&plan).unwrap();
        let restored = ctx.try_from_optd_plan(optd_plan.as_ref()).unwrap();

        let DFLogicalPlan::Filter(filter) = restored else {
            panic!("expected filter after round trip");
        };
        let DFLogicalPlan::Join(join) = filter.input.as_ref() else {
            panic!("expected join under filter after round trip");
        };
        assert_eq!(join.join_type, JoinType::LeftMark);
        assert_eq!(filter.predicate, predicate);
    }

    #[test]
    fn unqualified_mark_lookup_prefers_real_column_over_synthetic_fallback() {
        let mut ctx = new_test_ctx();
        let real_schema = Arc::new(Schema::new(vec![Field::new(
            "mark",
            DataType::Boolean,
            false,
        )]));
        let real_table_index = ctx
            .inner
            .add_binding(Some(TableRef::bare("t")), real_schema)
            .unwrap();
        let real_column = optd_core::ir::Column(real_table_index, 0);

        let synthetic_schema = Arc::new(Schema::new(vec![Field::new(
            "mark",
            DataType::Boolean,
            false,
        )]));
        let synthetic_table_index = ctx.inner.add_binding(None, synthetic_schema).unwrap();
        let synthetic_column = optd_core::ir::Column(synthetic_table_index, 0);
        let df_mark_column = Column::new(Some(TableReference::bare("__optd_mark_1")), "mark");
        ctx.register_df_mark_column(df_mark_column.clone(), synthetic_column);

        assert_eq!(ctx.try_get_optd_column(None, "mark").unwrap(), real_column);
        assert_eq!(
            ctx.try_get_optd_column(df_mark_column.relation.as_ref(), &df_mark_column.name)
                .unwrap(),
            synthetic_column
        );
    }

    #[test]
    fn project_above_left_mark_join_can_restore_mark_column() {
        let (mut ctx, catalog) = new_test_ctx_with_catalog();
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        catalog
            .create_table(TableRef::bare("t1"), schema.clone(), None)
            .unwrap();
        catalog
            .create_table(TableRef::bare("t2"), schema.clone(), None)
            .unwrap();

        let left = logical_expr::logical_plan::builder::table_scan(Some("t1"), &schema, None)
            .unwrap()
            .build()
            .unwrap();
        let right = logical_expr::logical_plan::builder::table_scan(Some("t2"), &schema, None)
            .unwrap()
            .build()
            .unwrap();
        let join = logical_expr::logical_plan::Join::try_new(
            Arc::new(left),
            Arc::new(right),
            vec![(
                DFExpr::Column(Column::from_qualified_name("t1.a")),
                DFExpr::Column(Column::from_qualified_name("t2.a")),
            )],
            None,
            JoinType::LeftMark,
            logical_expr::JoinConstraint::On,
            NullEquality::NullEqualsNothing,
        )
        .unwrap();
        let projection = DFExpr::Column(Column::from_qualified_name("t2.mark"));
        let plan = logical_expr::LogicalPlanBuilder::from(DFLogicalPlan::Join(join))
            .project(vec![projection.clone()])
            .unwrap()
            .build()
            .unwrap();

        let optd_plan = ctx.try_into_optd_plan(&plan).unwrap();
        let restored = ctx.try_from_optd_plan(optd_plan.as_ref());

        assert!(
            restored.is_ok(),
            "projecting a mark column above a mark join should round trip, got: {restored:?}"
        );

        let DFLogicalPlan::Projection(project) = restored.unwrap() else {
            panic!("expected projection after round trip");
        };
        println!("restored projection exprs: {:#?}", project.expr);
        println!("expected projection exprs: {:#?}", vec![projection.clone()]);
        let restored_expr = match &project.expr[0] {
            DFExpr::Alias(alias) => alias.expr.as_ref(),
            expr => expr,
        };
        assert_eq!(restored_expr, &projection);
    }

    #[test]
    fn exists_or_condition_lowers_through_mark_join_and_decorrelates() {
        let (mut ctx, catalog) = new_test_ctx_with_catalog();
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        catalog
            .create_table(TableRef::bare("t1"), schema.clone(), None)
            .unwrap();
        catalog
            .create_table(TableRef::bare("t2"), schema.clone(), None)
            .unwrap();

        let outer = logical_expr::logical_plan::builder::table_scan(Some("t1"), &schema, None)
            .unwrap()
            .build()
            .unwrap();
        let subquery = logical_expr::logical_plan::builder::table_scan(Some("t2"), &schema, None)
            .unwrap()
            .filter(logical_expr::binary_expr(
                DFExpr::Column(Column::from_qualified_name("t2.a")),
                logical_expr::Operator::Eq,
                DFExpr::OuterReferenceColumn(
                    Arc::new(Field::new("a", DataType::Int64, false)),
                    Column::from_qualified_name("t1.a"),
                ),
            ))
            .unwrap()
            .build()
            .unwrap();
        let predicate = logical_expr::or(
            expr_fn::exists(Arc::new(subquery)),
            logical_expr::binary_expr(
                DFExpr::Column(Column::from_qualified_name("t1.a")),
                logical_expr::Operator::Eq,
                DFExpr::Literal(ScalarValue::Int64(Some(1)), None),
            ),
        );
        let plan = logical_expr::LogicalPlanBuilder::from(outer)
            .filter(predicate)
            .unwrap()
            .build()
            .unwrap();

        let optd_plan = ctx.try_into_optd_plan(&plan).unwrap();
        let select = match &optd_plan.kind {
            OperatorKind::Select(meta) => {
                optd_core::ir::operator::Select::borrow_raw_parts(meta, &optd_plan.common)
            }
            _ => panic!(
                "expected filter lowering to produce a select, got:\n{}",
                quick_explain(&optd_plan, &ctx.inner)
            ),
        };
        match &select.input().kind {
            OperatorKind::DependentJoin(meta) => {
                let join = optd_core::ir::operator::DependentJoin::borrow_raw_parts(
                    meta,
                    &select.input().common,
                );
                assert!(matches!(
                    join.join_type(),
                    optd_core::ir::operator::join::JoinType::Mark(_)
                ));
            }
            _ => panic!(
                "expected embedded EXISTS to lower to a dependent mark join, got:\n{}",
                quick_explain(select.input(), &ctx.inner)
            ),
        }

        let decorrelated = UnnestingRule::new().apply(optd_plan, &ctx.inner).unwrap();
        let restored = ctx.try_from_optd_plan(decorrelated.as_ref()).unwrap();
        let rendered = restored.display_indent().to_string();
        assert!(
            rendered.contains("LeftMark Join"),
            "expected decorrelated plan to keep a LeftMark join, got:\n{rendered}"
        );

        let DFLogicalPlan::Filter(filter) = restored else {
            panic!("expected filter after restoring decorrelated plan");
        };
        let DFLogicalPlan::Join(join) = filter.input.as_ref() else {
            panic!("expected join under restored filter");
        };
        assert_eq!(join.join_type, JoinType::LeftMark);
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
            optd_core::ir::ScalarKind::IsNull(meta) => {
                let node = IsNull::borrow_raw_parts(meta, &expr.common);
                self.try_from_optd_is_null(node)
            }
            optd_core::ir::ScalarKind::IsNotNull(meta) => {
                let node = IsNotNull::borrow_raw_parts(meta, &expr.common);
                self.try_from_optd_is_not_null(node)
            }
            optd_core::ir::ScalarKind::Like(meta) => {
                let node = Like::borrow_raw_parts(meta, &expr.common);
                self.try_from_optd_like(node)
            }
            optd_core::ir::ScalarKind::Case(meta) => {
                let node = Case::borrow_raw_parts(meta, &expr.common);
                self.try_from_optd_case(node)
            }
            optd_core::ir::ScalarKind::InList(meta) => {
                let node = InList::borrow_raw_parts(meta, &expr.common);
                self.try_from_optd_in_list(node)
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
            BinaryOpKind::Ne => logical_expr::Operator::NotEq,
            BinaryOpKind::IsDistinctFrom => logical_expr::Operator::IsDistinctFrom,
            BinaryOpKind::IsNotDistinctFrom => logical_expr::Operator::IsNotDistinctFrom,
            BinaryOpKind::Lt => logical_expr::Operator::Lt,
            BinaryOpKind::Le => logical_expr::Operator::LtEq,
            BinaryOpKind::Gt => logical_expr::Operator::Gt,
            BinaryOpKind::Ge => logical_expr::Operator::GtEq,
            BinaryOpKind::RegexMatch => logical_expr::Operator::RegexMatch,
            BinaryOpKind::RegexIMatch => logical_expr::Operator::RegexIMatch,
            BinaryOpKind::RegexNotMatch => logical_expr::Operator::RegexNotMatch,
            BinaryOpKind::RegexNotIMatch => logical_expr::Operator::RegexNotIMatch,
            BinaryOpKind::LikeMatch => logical_expr::Operator::LikeMatch,
            BinaryOpKind::ILikeMatch => logical_expr::Operator::ILikeMatch,
            BinaryOpKind::NotLikeMatch => logical_expr::Operator::NotLikeMatch,
            BinaryOpKind::NotILikeMatch => logical_expr::Operator::NotILikeMatch,
            BinaryOpKind::BitwiseAnd => logical_expr::Operator::BitwiseAnd,
            BinaryOpKind::BitwiseOr => logical_expr::Operator::BitwiseOr,
            BinaryOpKind::BitwiseXor => logical_expr::Operator::BitwiseXor,
            BinaryOpKind::BitwiseShiftRight => logical_expr::Operator::BitwiseShiftRight,
            BinaryOpKind::BitwiseShiftLeft => logical_expr::Operator::BitwiseShiftLeft,
            BinaryOpKind::StringConcat => logical_expr::Operator::StringConcat,
            BinaryOpKind::AtArrow => logical_expr::Operator::AtArrow,
            BinaryOpKind::ArrowAt => logical_expr::Operator::ArrowAt,
            BinaryOpKind::Arrow => logical_expr::Operator::Arrow,
            BinaryOpKind::LongArrow => logical_expr::Operator::LongArrow,
            BinaryOpKind::HashArrow => logical_expr::Operator::HashArrow,
            BinaryOpKind::HashLongArrow => logical_expr::Operator::HashLongArrow,
            BinaryOpKind::AtAt => logical_expr::Operator::AtAt,
            BinaryOpKind::IntegerDivide => logical_expr::Operator::IntegerDivide,
            BinaryOpKind::HashMinus => logical_expr::Operator::HashMinus,
            BinaryOpKind::AtQuestion => logical_expr::Operator::AtQuestion,
            BinaryOpKind::Question => logical_expr::Operator::Question,
            BinaryOpKind::QuestionAnd => logical_expr::Operator::QuestionAnd,
            BinaryOpKind::QuestionPipe => logical_expr::Operator::QuestionPipe,
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

    pub fn try_from_optd_is_null(&mut self, node: IsNullBorrowed<'_>) -> Result<DFExpr> {
        let expr = self.try_from_optd_scalar_expr(node.expr())?;
        Ok(DFExpr::IsNull(Box::new(expr)))
    }

    pub fn try_from_optd_is_not_null(&mut self, node: IsNotNullBorrowed<'_>) -> Result<DFExpr> {
        let expr = self.try_from_optd_scalar_expr(node.expr())?;
        Ok(DFExpr::IsNotNull(Box::new(expr)))
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

    pub fn try_from_optd_case(&mut self, node: CaseBorrowed<'_>) -> Result<DFExpr> {
        let expr = node
            .expr()
            .map(|expr| self.try_from_optd_scalar_expr(expr))
            .transpose()?
            .map(Box::new);
        let when_then_expr = node
            .when_then_expr()
            .map(|(when, then)| {
                Ok((
                    Box::new(self.try_from_optd_scalar_expr(when)?),
                    Box::new(self.try_from_optd_scalar_expr(then)?),
                ))
            })
            .try_collect()?;
        let else_expr = node
            .else_expr()
            .map(|expr| self.try_from_optd_scalar_expr(expr))
            .transpose()?
            .map(Box::new);

        Ok(DFExpr::Case(logical_expr::expr::Case::new(
            expr,
            when_then_expr,
            else_expr,
        )))
    }

    pub fn try_from_optd_in_list(&mut self, node: InListBorrowed<'_>) -> Result<DFExpr> {
        let expr = self.try_from_optd_scalar_expr(node.expr())?;
        let list = node.list().borrow::<List>();
        let list = list
            .members()
            .iter()
            .map(|e| self.try_from_optd_scalar_expr(e))
            .try_collect()?;

        Ok(DFExpr::InList(logical_expr::expr::InList::new(
            Box::new(expr),
            list,
            *node.negated(),
        )))
    }
}
