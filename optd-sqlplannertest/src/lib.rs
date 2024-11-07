use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};
use datafusion::catalog::CatalogList;
use datafusion::execution::context::{SessionConfig, SessionState};
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::prelude::SessionContext;
use datafusion::sql::parser::DFParser;
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion_optd_cli::helper::unescape_input;
use itertools::Itertools;
use lazy_static::lazy_static;
use mimalloc::MiMalloc;
use optd_datafusion_bridge::{DatafusionCatalog, OptdQueryPlanner};
use optd_datafusion_repr::DatafusionOptimizer;
use optd_datafusion_repr_adv_cost::adv_stats::stats::DataFusionBaseTableStats;
use optd_datafusion_repr_adv_cost::new_physical_adv_cost;
use regex::Regex;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use anyhow::{bail, Result};
use async_trait::async_trait;

#[derive(Default)]
pub struct DatafusionDBMS {
    ctx: SessionContext,
    /// Context enabling datafusion's logical optimizer.
    use_df_logical_ctx: SessionContext,
    /// Shared optd optimizer (for tweaking config)
    optd_optimizer: Option<Arc<OptdQueryPlanner>>,
}

impl DatafusionDBMS {
    pub async fn new() -> Result<Self> {
        let (ctx, optd_optimizer) = DatafusionDBMS::new_session_ctx(false, None, false).await?;
        let (use_df_logical_ctx, _) =
            Self::new_session_ctx(true, Some(ctx.state().catalog_list().clone()), false).await?;
        Ok(Self {
            ctx,
            use_df_logical_ctx,
            optd_optimizer: Some(optd_optimizer),
        })
    }

    pub async fn new_advanced_cost() -> Result<Self> {
        let (ctx, optd_optimizer) = DatafusionDBMS::new_session_ctx(false, None, true).await?;
        let (use_df_logical_ctx, _) =
            Self::new_session_ctx(true, Some(ctx.state().catalog_list().clone()), true).await?;
        Ok(Self {
            ctx,
            use_df_logical_ctx,
            optd_optimizer: Some(optd_optimizer),
        })
    }

    /// Creates a new session context. If the `use_df_logical` flag is set, datafusion's logical
    /// optimizer will be used.
    async fn new_session_ctx(
        use_df_logical: bool,
        catalog: Option<Arc<dyn CatalogList>>,
        with_advanced_cost: bool,
    ) -> Result<(SessionContext, Arc<OptdQueryPlanner>)> {
        let mut session_config = SessionConfig::from_env()?.with_information_schema(true);
        if !use_df_logical {
            session_config.options_mut().optimizer.max_passes = 0;
        }

        let rn_config = RuntimeConfig::new();
        let runtime_env = RuntimeEnv::new(rn_config.clone())?;
        let optd_optimizer;

        let ctx = {
            let mut state = if let Some(catalog) = catalog {
                SessionState::new_with_config_rt_and_catalog_list(
                    session_config.clone(),
                    Arc::new(runtime_env),
                    catalog,
                )
            } else {
                SessionState::new_with_config_rt(session_config.clone(), Arc::new(runtime_env))
            };
            let optimizer = if with_advanced_cost {
                new_physical_adv_cost(
                    Arc::new(DatafusionCatalog::new(state.catalog_list())),
                    DataFusionBaseTableStats::default(),
                    false,
                )
            } else {
                DatafusionOptimizer::new_physical(
                    Arc::new(DatafusionCatalog::new(state.catalog_list())),
                    false,
                )
            };
            if !use_df_logical {
                // clean up optimizer rules so that we can plug in our own optimizer
                state = state.with_optimizer_rules(vec![]);
            }
            state = state.with_physical_optimizer_rules(vec![]);
            // use optd-bridge query planner
            optd_optimizer = Arc::new(OptdQueryPlanner::new(optimizer));
            state = state.with_query_planner(optd_optimizer.clone());
            SessionContext::new_with_state(state)
        };
        ctx.refresh_catalogs().await?;
        Ok((ctx, optd_optimizer))
    }

    pub(crate) async fn execute(&self, sql: &str, flags: &TestFlags) -> Result<Vec<Vec<String>>> {
        {
            let mut guard = self
                .optd_optimizer
                .as_ref()
                .unwrap()
                .optimizer
                .lock()
                .unwrap();
            let optimizer = guard.as_mut().unwrap().optd_optimizer_mut();
            if flags.panic_on_budget {
                optimizer.panic_on_explore_limit(true);
            } else {
                optimizer.panic_on_explore_limit(false);
            }
            if flags.disable_pruning {
                optimizer.disable_pruning(true);
            } else {
                optimizer.disable_pruning(false);
            }
            let rules = optimizer.rules();
            if flags.enable_logical_rules.is_empty() {
                for r in 0..rules.len() {
                    optimizer.enable_rule(r);
                }
                guard.as_mut().unwrap().enable_heuristic(true);
            } else {
                for (rule_id, rule) in rules.as_ref().iter().enumerate() {
                    if rule.rule.is_impl_rule() {
                        optimizer.enable_rule(rule_id);
                    } else {
                        optimizer.disable_rule(rule_id);
                    }
                }
                let mut rules_to_enable = flags
                    .enable_logical_rules
                    .iter()
                    .map(|x| x.as_str())
                    .collect::<HashSet<_>>();
                for (rule_id, rule) in rules.as_ref().iter().enumerate() {
                    if rules_to_enable.remove(rule.rule.name()) {
                        optimizer.enable_rule(rule_id);
                    }
                }
                if !rules_to_enable.is_empty() {
                    bail!("Unknown logical rule: {:?}", rules_to_enable);
                }
                guard.as_mut().unwrap().enable_heuristic(false);
            }
        }
        let sql = unescape_input(sql)?;
        let dialect = Box::new(GenericDialect);
        let statements = DFParser::parse_sql_with_dialect(&sql, dialect.as_ref())?;
        let mut result = Vec::new();
        for statement in statements {
            let df = if flags.enable_df_logical {
                let plan = self
                    .use_df_logical_ctx
                    .state()
                    .statement_to_plan(statement)
                    .await?;
                self.use_df_logical_ctx.execute_logical_plan(plan).await?
            } else {
                let plan = self.ctx.state().statement_to_plan(statement).await?;

                self.ctx.execute_logical_plan(plan).await?
            };

            let batches = df.collect().await?;

            let options = FormatOptions::default();

            for batch in batches {
                let converters = batch
                    .columns()
                    .iter()
                    .map(|a| ArrayFormatter::try_new(a.as_ref(), &options))
                    .collect::<Result<Vec<_>, _>>()?;
                for row_idx in 0..batch.num_rows() {
                    let mut row = Vec::with_capacity(batch.num_columns());
                    for converter in converters.iter() {
                        let mut buffer = String::with_capacity(8);
                        converter.value(row_idx).write(&mut buffer)?;
                        row.push(buffer);
                    }
                    result.push(row);
                }
            }
        }
        if flags.dump_memo_table {
            let mut guard = self
                .optd_optimizer
                .as_ref()
                .unwrap()
                .optimizer
                .lock()
                .unwrap();
            let optimizer = guard.as_mut().unwrap().optd_optimizer_mut();
            optimizer.dump();
        }
        Ok(result)
    }

    /// Executes the `execute` task.
    async fn task_execute(&mut self, r: &mut String, sql: &str, flags: &TestFlags) -> Result<()> {
        use std::fmt::Write;
        if flags.verbose {
            bail!("Verbose flag is not supported for execute task");
        }
        let result = self.execute(sql, flags).await?;
        writeln!(r, "{}", result.into_iter().map(|x| x.join(" ")).join("\n"))?;
        writeln!(r)?;
        Ok(())
    }

    /// Executes the `explain` task.
    async fn task_explain(
        &mut self,
        r: &mut String,
        sql: &str,
        task: &str,
        flags: &TestFlags,
    ) -> Result<()> {
        use std::fmt::Write;

        let verbose = flags.verbose;
        let explain_sql = if verbose {
            format!("explain verbose {}", &sql)
        } else {
            format!("explain {}", &sql)
        };
        let result = self.execute(&explain_sql, flags).await?;
        let subtask_start_pos = task.rfind(':').unwrap() + 1;
        for subtask in task[subtask_start_pos..].split(',') {
            let subtask = subtask.trim();
            if subtask == "logical_datafusion" {
                writeln!(
                    r,
                    "{}",
                    result
                        .iter()
                        .find(|x| x[0] == "logical_plan after datafusion")
                        .map(|x| &x[1])
                        .unwrap()
                )?;
            } else if subtask == "logical_optd_heuristic" || subtask == "optimized_logical_optd" {
                writeln!(
                    r,
                    "{}",
                    result
                        .iter()
                        .find(|x| x[0] == "logical_plan after optd-heuristic")
                        .map(|x| &x[1])
                        .unwrap()
                )?;
            } else if subtask == "logical_optd" {
                writeln!(
                    r,
                    "{}",
                    result
                        .iter()
                        .find(|x| x[0] == "logical_plan after optd")
                        .map(|x| &x[1])
                        .unwrap()
                )?;
            } else if subtask == "physical_optd" {
                writeln!(
                    r,
                    "{}",
                    result
                        .iter()
                        .find(|x| x[0] == "physical_plan after optd")
                        .map(|x| &x[1])
                        .unwrap()
                )?;
            } else if subtask == "logical_join_orders" {
                writeln!(
                    r,
                    "{}",
                    result
                        .iter()
                        .find(|x| x[0] == "physical_plan after optd-all-logical-join-orders")
                        .map(|x| &x[1])
                        .unwrap()
                )?;
                writeln!(r)?;
            } else if subtask == "physical_datafusion" {
                writeln!(
                    r,
                    "{}",
                    result
                        .iter()
                        .find(|x| x[0] == "physical_plan")
                        .map(|x| &x[1])
                        .unwrap()
                )?;
            } else {
                bail!("Unknown subtask: {}", subtask);
            }
        }

        Ok(())
    }
}

#[async_trait]
impl sqlplannertest::PlannerTestRunner for DatafusionDBMS {
    async fn run(&mut self, test_case: &sqlplannertest::ParsedTestCase) -> Result<String> {
        if !test_case.before_sql.is_empty() {
            panic!("before is not supported in optd-sqlplannertest, always specify the task type to run");
        }

        let mut result = String::new();
        let r = &mut result;
        for task in &test_case.tasks {
            let flags = extract_flags(task)?;
            if task.starts_with("execute") {
                self.task_execute(r, &test_case.sql, &flags).await?;
            } else if task.starts_with("explain") {
                self.task_explain(r, &test_case.sql, task, &flags).await?;
            }
        }
        Ok(result)
    }
}

lazy_static! {
    static ref FLAGS_REGEX: Regex = Regex::new(r"\[(.*)\]").unwrap();
}

#[derive(Default, Debug)]
struct TestFlags {
    verbose: bool,
    enable_df_logical: bool,
    enable_logical_rules: Vec<String>,
    panic_on_budget: bool,
    dump_memo_table: bool,
    disable_pruning: bool,
}

/// Extract the flags from a task. The flags are specified in square brackets.
/// For example, the flags for the task `explain[use_df_logical, verbose]` are `["use_df_logical",
/// "verbose"]`.
fn extract_flags(task: &str) -> Result<TestFlags> {
    if let Some(captures) = FLAGS_REGEX.captures(task) {
        let flags = captures
            .get(1)
            .unwrap()
            .as_str()
            .split(',')
            .map(|x| x.trim().to_string())
            .collect_vec();
        let mut options = TestFlags::default();
        for flag in flags {
            if flag == "verbose" {
                options.verbose = true;
            } else if flag == "use_df_logical" {
                options.enable_df_logical = true;
            } else if flag.starts_with("logical_rules") {
                if let Some((_, flag)) = flag.split_once(':') {
                    options.enable_logical_rules = flag.split('+').map(|x| x.to_string()).collect();
                } else {
                    bail!("Failed to parse logical_rules flag: {}", flag);
                }
            } else if flag == "panic_on_budget" {
                options.panic_on_budget = true;
            } else if flag == "dump_memo_table" {
                options.dump_memo_table = true;
            } else if flag == "disable_pruning" {
                options.disable_pruning = true;
            } else {
                bail!("Unknown flag: {}", flag);
            }
        }
        Ok(options)
    } else {
        Ok(TestFlags::default())
    }
}
