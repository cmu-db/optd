use std::sync::Arc;

use datafusion::{
    execution::{SessionStateBuilder, runtime_env::RuntimeEnv},
    optimizer::{
        common_subexpr_eliminate::CommonSubexprEliminate,
        decorrelate_lateral_join::DecorrelateLateralJoin,
        decorrelate_predicate_subquery::DecorrelatePredicateSubquery,
        eliminate_cross_join::EliminateCrossJoin,
        eliminate_duplicated_expr::EliminateDuplicatedExpr, eliminate_filter::EliminateFilter,
        eliminate_group_by_constant::EliminateGroupByConstant, eliminate_join::EliminateJoin,
        eliminate_limit::EliminateLimit, eliminate_outer_join::EliminateOuterJoin,
        extract_equijoin_predicate::ExtractEquijoinPredicate,
        filter_null_join_keys::FilterNullJoinKeys, optimize_projections::OptimizeProjections,
        propagate_empty_relation::PropagateEmptyRelation, push_down_filter::PushDownFilter,
        push_down_limit::PushDownLimit, replace_distinct_aggregate::ReplaceDistinctWithAggregate,
        scalar_subquery_to_join::ScalarSubqueryToJoin, simplify_expressions::SimplifyExpressions,
        single_distinct_to_groupby::SingleDistinctToGroupBy,
    },
    prelude::{DataFrame, SessionConfig, SessionContext},
    sql::sqlparser::keywords::OPTIMIZER_COSTS,
};
use datafusion_cli::cli_context::CliSessionContext;
use optd_datafusion::{OptdExtensionConfig, SessionStateBuilderOptdExt};

pub struct OptdCliSessionContext {
    inner: SessionContext,
}

impl OptdCliSessionContext {
    pub fn new_with_config_rt(config: SessionConfig, runtime: Arc<RuntimeEnv>) -> Self {
        let config = config
            .with_option_extension(OptdExtensionConfig::default())
            .set_bool("optd.optd_enabled", true);
        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_runtime_env(runtime)
            .with_default_features()
            .with_optimizer_rules(vec![
                // Arc::new(OptimizeUnions::new()),
                Arc::new(SimplifyExpressions::new()),
                // Arc::new(ReplaceDistinctWithAggregate::new()),
                // Arc::new(EliminateJoin::new()),
                // Arc::new(DecorrelatePredicateSubquery::new()),
                // Arc::new(ScalarSubqueryToJoin::new()),
                // Arc::new(DecorrelateLateralJoin::new()),
                // // Arc::new(ExtractEquijoinPredicate::new()),
                // Arc::new(EliminateDuplicatedExpr::new()),
                // // Arc::new(EliminateFilter::new()),
                // // Arc::new(EliminateCrossJoin::new()),
                // Arc::new(EliminateLimit::new()),
                // Arc::new(PropagateEmptyRelation::new()),
                // Arc::new(FilterNullJoinKeys::default()),
                // Arc::new(EliminateOuterJoin::new()),
                // // Filters can't be pushed down past Limits, we should do PushDownFilter after PushDownLimit
                // Arc::new(PushDownLimit::new()),
                // Arc::new(PushDownFilter::new()),
                // Arc::new(SingleDistinctToGroupBy::new()),
                // // The previous optimizations added expressions and projections,
                // // that might benefit from the following rules
                // Arc::new(EliminateGroupByConstant::new()),
                // Arc::new(CommonSubexprEliminate::new()),
                // Arc::new(OptimizeProjections::new()),
            ])
            // .with_optimizer_rules(vec![])
            .with_optd_planner()
            .build();
        let inner = SessionContext::new_with_state(state);

        Self { inner }
    }
    pub async fn refresh_catalogs(&self) -> datafusion::common::Result<()> {
        self.inner.refresh_catalogs().await
    }

    pub fn enable_url_table(self) -> Self {
        let inner = self.inner.enable_url_table();
        Self { inner }
    }

    pub fn inner(&self) -> &SessionContext {
        &self.inner
    }

    pub fn return_empty_dataframe(&self) -> datafusion::common::Result<DataFrame> {
        let plan = datafusion::logical_expr::LogicalPlanBuilder::empty(false).build()?;
        Ok(DataFrame::new(self.inner.state(), plan))
    }
}

impl CliSessionContext for OptdCliSessionContext {
    fn task_ctx(&self) -> std::sync::Arc<datafusion::execution::TaskContext> {
        self.inner().task_ctx()
    }

    fn session_state(&self) -> datafusion::execution::SessionState {
        self.inner().state()
    }

    fn register_object_store(
        &self,
        url: &url::Url,
        object_store: std::sync::Arc<dyn object_store::ObjectStore>,
    ) -> Option<std::sync::Arc<dyn object_store::ObjectStore + 'static>> {
        self.inner().register_object_store(url, object_store)
    }

    fn register_table_options_extension_from_scheme(&self, scheme: &str) {
        self.inner()
            .register_table_options_extension_from_scheme(scheme);
    }

    fn execute_logical_plan<'life0, 'async_trait>(
        &'life0 self,
        plan: datafusion::logical_expr::LogicalPlan,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<
                    Output = Result<
                        datafusion::prelude::DataFrame,
                        datafusion::common::DataFusionError,
                    >,
                > + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        let fut = async {
            if let datafusion::logical_expr::LogicalPlan::Statement(stmt) = &plan {
                match stmt {
                    datafusion::logical_expr::Statement::TransactionStart(_) => {
                        println!("START TRANSACTION");
                        return self.return_empty_dataframe();
                    }
                    datafusion::logical_expr::Statement::TransactionEnd(transaction_end) => {
                        use datafusion::logical_expr::TransactionConclusion;
                        match transaction_end.conclusion {
                            TransactionConclusion::Commit => println!("COMMIT"),
                            TransactionConclusion::Rollback => println!("ROLLBACK"),
                        }
                        return self.return_empty_dataframe();
                    }
                    _ => (),
                }
            }

            self.inner.execute_logical_plan(plan).await
        };

        Box::pin(fut)
    }
}
