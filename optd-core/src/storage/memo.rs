//! An implementation of the memo table using SQLite.

use std::{str::FromStr, sync::Arc, time::Duration};

use super::transaction::Transaction;
use anyhow::Result;
use sqlx::{
    sqlite::{SqliteConnectOptions, SqliteJournalMode},
    SqliteConnection, SqlitePool,
};

use crate::{
    cascades::{
        expressions::*,
        goal::OptimizationStatus,
        groups::{ExplorationStatus, RelationalGroupId, ScalarGroupId},
        memo::Memoize,
        rules::RuleId,
    },
    operators::relational::physical::PhysicalOperatorKind,
    plans::physical::PhysicalPlan,
};
use crate::{
    cascades::{
        goal::{Goal, GoalId},
        rules::ImplementationRuleId,
    },
    cost_model::Cost,
    operators::scalar::ScalarOperatorKind,
};
use crate::{
    cascades::{properties::PhysicalProperties, rules::TransformationRuleId},
    operators::relational::logical::LogicalOperatorKind,
};

/// A Storage manager that manages connections to the database.
#[derive(Debug)]
pub struct SqliteMemo {
    /// A async connection pool to the SQLite database.
    db: SqlitePool,
    /// SQL query string to get all logical expressions in a group.
    get_all_logical_exprs_in_group_query: String,
    /// SQL query string to get all scalar expressions in a group.
    get_all_scalar_exprs_in_group_query: String,
    /// SQL query string to get the winner physical epxression for a goal.
    get_winner_physical_expr_in_goal_query: String,
    /// SQL query string to get all physical expressions in a group.
    get_all_physical_exprs_in_goal_query: String,
}

impl SqliteMemo {
    /// Create a new storage manager that connects to the SQLite database at the given URL.
    pub async fn new(database_url: &str) -> anyhow::Result<Self> {
        let options = SqliteConnectOptions::from_str(database_url)?
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .busy_timeout(Duration::from_secs(30));
        Self::new_with_options(options).await
    }

    /// Create a new storage manager backed by an in-memory SQLite database.
    pub async fn new_in_memory() -> anyhow::Result<Self> {
        let options = SqliteConnectOptions::from_str(":memory:")?;
        Self::new_with_options(options).await
    }

    /// Creates a new storage manager with the given options.
    async fn new_with_options(options: SqliteConnectOptions) -> anyhow::Result<Self> {
        let memo = Self {
            db: SqlitePool::connect_with(options).await?,
            get_all_logical_exprs_in_group_query: get_all_logical_exprs_in_group_query().into(),
            get_all_scalar_exprs_in_group_query: get_all_scalar_exprs_in_group_query().into(),
            get_winner_physical_expr_in_goal_query: get_winner_physical_expr_in_goal_query().into(),
            get_all_physical_exprs_in_goal_query: get_all_physical_exprs_in_goal_query().into(),
        };
        memo.migrate().await?;
        Ok(memo)
    }

    /// Runs pending migrations.
    async fn migrate(&self) -> anyhow::Result<()> {
        // sqlx::migrate! takes the path relative to the root of the crate.
        sqlx::migrate!("src/storage/migrations")
            .run(&self.db)
            .await?;
        Ok(())
    }

    /// Begin a new transaction.
    pub(crate) async fn begin(&self) -> anyhow::Result<Transaction<'_>> {
        let txn = self.db.begin().await?;
        Transaction::new(txn).await
    }
}

impl Memoize for SqliteMemo {
    async fn create_or_get_goal(
        &self,
        group_id: RelationalGroupId,
        required_physical_props: PhysicalProperties,
    ) -> Result<Arc<Goal>> {
        let mut txn = self.begin().await?;
        let goal_id = txn.new_goal_id().await?;
        let goal: Goal = sqlx::query_as("INSERT INTO goals (id, representative_goal_id, group_id, required_physical_properties, optimization_status) VALUES ($1, $2, $3, $4, $5) ON CONFLICT (group_id, required_physical_properties) DO UPDATE SET group_id = group_id RETURNING *")
            .bind(goal_id)
            .bind(goal_id)
            .bind(group_id)
            .bind(serde_json::to_string(&required_physical_props)?)
            .bind(OptimizationStatus::Unoptimized)
            .fetch_one(&mut *txn)
            .await?;

        let goal = Arc::new(goal);

        if goal.representative_goal_id == goal_id {
            txn.commit().await?;
        }
        Ok(goal)
    }

    async fn get_goal(&self, goal_id: GoalId) -> Result<Arc<Goal>> {
        let mut txn = self.begin().await?;
        let goal = sqlx::query_as("SELECT representative_goal_id, group_id, required_physical_properties, optimization_status FROM goals WHERE id = $1")
            .bind(goal_id)
            .fetch_one(&mut *txn)
            .await?;
        txn.commit().await?;
        Ok(Arc::new(goal))
    }

    async fn update_goal_optimization_status(
        &self,
        goal_id: GoalId,
        status: OptimizationStatus,
    ) -> Result<()> {
        let mut txn = self.begin().await?;
        sqlx::query("UPDATE goals SET optimization_status = $1 WHERE id = $2")
            .bind(status)
            .bind(goal_id)
            .execute(&mut *txn)
            .await?;
        txn.commit().await?;
        Ok(())
    }

    async fn update_group_exploration_status(
        &self,
        group_id: RelationalGroupId,
        status: ExplorationStatus,
    ) -> Result<()> {
        let mut txn = self.begin().await?;
        sqlx::query("UPDATE relation_groups SET exploration_status = $1 WHERE id = $2")
            .bind(status)
            .bind(group_id)
            .execute(&mut *txn)
            .await?;
        txn.commit().await?;
        Ok(())
    }

    async fn get_group_exploration_status(
        &self,
        group_id: RelationalGroupId,
    ) -> Result<ExplorationStatus> {
        let mut txn = self.begin().await?;
        let status: ExplorationStatus =
            sqlx::query_scalar("SELECT exploration_status FROM relation_groups WHERE id = $1")
                .bind(group_id)
                .fetch_one(&mut *txn)
                .await?;
        txn.commit().await?;
        Ok(status)
    }

    async fn get_all_logical_exprs_in_group(
        &self,
        group_id: RelationalGroupId,
    ) -> Result<Vec<(LogicalExpressionId, Arc<LogicalExpression>)>> {
        #[derive(sqlx::FromRow)]
        struct LogicalExprRecord {
            logical_expression_id: LogicalExpressionId,
            data: sqlx::types::Json<Arc<LogicalExpression>>,
        }

        let mut txn = self.begin().await?;
        let representative_group_id = self.get_representative_group_id(&mut txn, group_id).await?;
        let logical_exprs: Vec<LogicalExprRecord> =
            sqlx::query_as(&self.get_all_logical_exprs_in_group_query)
                .bind(representative_group_id)
                .fetch_all(&mut *txn)
                .await?;

        txn.commit().await?;
        Ok(logical_exprs
            .into_iter()
            .map(|record| (record.logical_expression_id, record.data.0))
            .collect())
    }

    async fn add_logical_expr_to_group(
        &self,
        logical_expr: &LogicalExpression,
        group_id: RelationalGroupId,
    ) -> Result<RelationalGroupId> {
        let group_id = self
            .add_logical_expr_to_group_inner(logical_expr, Some(group_id))
            .await?;
        Ok(group_id)
    }

    async fn add_logical_expr(
        &self,
        logical_expr: &LogicalExpression,
    ) -> Result<RelationalGroupId> {
        let group_id = self
            .add_logical_expr_to_group_inner(logical_expr, None)
            .await?;
        Ok(group_id)
    }

    async fn get_all_scalar_exprs_in_group(
        &self,
        group_id: ScalarGroupId,
    ) -> Result<Vec<(ScalarExpressionId, Arc<ScalarExpression>)>> {
        #[derive(sqlx::FromRow)]
        struct ScalarExprRecord {
            scalar_expression_id: ScalarExpressionId,
            data: sqlx::types::Json<Arc<ScalarExpression>>,
        }

        let mut txn = self.begin().await?;
        let representative_group_id = self
            .get_representative_scalar_group_id(&mut txn, group_id)
            .await?;
        let scalar_exprs: Vec<ScalarExprRecord> =
            sqlx::query_as(&self.get_all_scalar_exprs_in_group_query)
                .bind(representative_group_id)
                .fetch_all(&mut *txn)
                .await?;

        txn.commit().await?;
        Ok(scalar_exprs
            .into_iter()
            .map(|record| (record.scalar_expression_id, record.data.0))
            .collect())
    }

    async fn add_scalar_expr_to_group(
        &self,
        scalar_expr: &ScalarExpression,
        group_id: ScalarGroupId,
    ) -> Result<ScalarGroupId> {
        let group_id = self
            .add_scalar_expr_to_group_inner(scalar_expr, Some(group_id))
            .await?;
        Ok(group_id)
    }

    async fn add_scalar_expr(&self, scalar_expr: &ScalarExpression) -> Result<ScalarGroupId> {
        let group_id = self
            .add_scalar_expr_to_group_inner(scalar_expr, None)
            .await?;
        Ok(group_id)
    }

    async fn merge_relation_group(
        &self,
        from: RelationalGroupId,
        to: RelationalGroupId,
    ) -> Result<RelationalGroupId> {
        let mut txn = self.begin().await?;
        self.set_representative_group_id(&mut txn, from, to).await?;
        txn.commit().await?;
        Ok(to)
    }

    async fn merge_scalar_group(
        &self,
        from: ScalarGroupId,
        to: ScalarGroupId,
    ) -> Result<ScalarGroupId> {
        let mut txn = self.begin().await?;
        self.set_representative_scalar_group_id(&mut txn, from, to)
            .await?;
        txn.commit().await?;
        Ok(to)
    }

    async fn merge_goal(&self, from: GoalId, to: GoalId) -> Result<GoalId> {
        let mut txn = self.begin().await?;
        self.set_representative_goal_id(&mut txn, from, to).await?;
        txn.commit().await?;
        Ok(to)
    }

    async fn get_all_physical_exprs_in_goal(
        &self,
        goal_id: GoalId,
    ) -> Result<Vec<(PhysicalExpressionId, Arc<PhysicalExpression>)>> {
        #[derive(sqlx::FromRow)]
        struct PhysicalExprRecord {
            physical_expression_id: PhysicalExpressionId,
            data: sqlx::types::Json<Arc<PhysicalExpression>>,
        }

        let mut txn = self.begin().await?;
        let representative_goal_id = self.get_representative_goal_id(&mut txn, goal_id).await?;
        let logical_exprs: Vec<PhysicalExprRecord> =
            sqlx::query_as(&self.get_all_physical_exprs_in_goal_query)
                .bind(representative_goal_id)
                .fetch_all(&mut *txn)
                .await?;

        txn.commit().await?;
        Ok(logical_exprs
            .into_iter()
            .map(|record| (record.physical_expression_id, record.data.0))
            .collect())
    }

    async fn add_physical_expr_to_goal(
        &self,
        physical_expr: &PhysicalExpression,
        cost: Cost,
        goal_id: GoalId,
    ) -> Result<GoalId> {
        self.add_physical_expr_to_goal_inner(physical_expr, cost, goal_id)
            .await
    }

    async fn get_winner_physical_expr_in_goal(
        &self,
        goal_id: GoalId,
    ) -> Result<Option<(PhysicalExpressionId, Arc<PhysicalExpression>, Cost)>> {
        #[derive(sqlx::FromRow)]
        struct PhysicalExprRecord {
            physical_expression_id: PhysicalExpressionId,
            data: sqlx::types::Json<Arc<PhysicalExpression>>,
            cost: Cost,
        }

        let mut txn = self.begin().await?;
        let representative_goal_id = self.get_representative_goal_id(&mut txn, goal_id).await?;

        let winner: Option<PhysicalExprRecord> =
            sqlx::query_as(&self.get_winner_physical_expr_in_goal_query)
                .bind(representative_goal_id)
                .fetch_optional(&mut *txn)
                .await?;

        let winner =
            winner.map(|record| (record.physical_expression_id, record.data.0, record.cost));

        txn.commit().await?;
        Ok(winner)
    }

    async fn get_matching_transformation_rules(
        &self,
        logical_expr: &LogicalExpression,
    ) -> Result<Vec<TransformationRuleId>> {
        todo!()
    }

    async fn get_matching_rules(&self, physical_expr: &LogicalExpression) -> Result<Vec<RuleId>> {
        todo!()
    }

    #[doc = " Adds a physical expression to a goal in the memo table."]
    #[doc = " TODO (Sarvesh): This is not correct and we should not be doing this at all. My next commit will probably change this"]
    async fn add_physical_plan_top_node(
        &self,
        physical_plan: &PhysicalPlan,
        cost: Cost,
        goal_id: GoalId,
    ) -> Result<GoalId> {
        todo!()
    }

    async fn get_group_id_from_logical_expr(
        &self,
        logical_expr: &LogicalExpression,
    ) -> Result<RelationalGroupId> {
        todo!()
    }

    async fn get_goal_id_from_physical_expr(
        &self,
        physical_expr: &PhysicalExpression,
    ) -> Result<GoalId> {
        todo!()
    }
}

// Helper functions for implementing the `Memoize` trait.
impl SqliteMemo {
    /// Gets the representative group id of a relational group.
    async fn get_representative_group_id(
        &self,
        db: &mut SqliteConnection,
        group_id: RelationalGroupId,
    ) -> anyhow::Result<RelationalGroupId> {
        let representative_group_id: RelationalGroupId =
            sqlx::query_scalar("SELECT representative_group_id FROM relation_groups WHERE id = $1")
                .bind(group_id)
                .fetch_one(db)
                .await?;
        Ok(representative_group_id)
    }

    /// Sets the representative group id of a relational group.
    async fn set_representative_group_id(
        &self,
        db: &mut SqliteConnection,
        group_id: RelationalGroupId,
        representative_group_id: RelationalGroupId,
    ) -> anyhow::Result<()> {
        sqlx::query("UPDATE relation_groups SET representative_group_id = $1 WHERE representative_group_id = $2")
            .bind(representative_group_id)
            .bind(group_id)
            .execute(db)
            .await?;
        Ok(())
    }

    /// Gets the representative group id of a relational group.
    async fn get_representative_goal_id(
        &self,
        db: &mut SqliteConnection,
        goal_id: GoalId,
    ) -> anyhow::Result<GoalId> {
        println!("get_representative_goal_id: {:?}", goal_id);
        let representative_goal_id: GoalId =
            sqlx::query_scalar("SELECT representative_goal_id FROM goals WHERE id = $1")
                .bind(goal_id)
                .fetch_one(db)
                .await?;
        Ok(representative_goal_id)
    }

    /// Sets the representative group id of a relational group.
    async fn set_representative_goal_id(
        &self,
        db: &mut SqliteConnection,
        goal_id: GoalId,
        representative_goal_id: GoalId,
    ) -> anyhow::Result<()> {
        sqlx::query(
            "UPDATE goals SET representative_goal_id = $1 WHERE representative_goal_id = $2",
        )
        .bind(representative_goal_id)
        .bind(goal_id)
        .execute(db)
        .await?;
        Ok(())
    }

    /// Gets the representative group id of a scalar group.
    async fn get_representative_scalar_group_id(
        &self,
        db: &mut SqliteConnection,
        group_id: ScalarGroupId,
    ) -> anyhow::Result<ScalarGroupId> {
        let representative_group_id: ScalarGroupId =
            sqlx::query_scalar("SELECT representative_group_id FROM scalar_groups WHERE id = $1")
                .bind(group_id)
                .fetch_one(db)
                .await?;
        Ok(representative_group_id)
    }

    /// Sets the representative group id of a scalar group.
    async fn set_representative_scalar_group_id(
        &self,
        db: &mut SqliteConnection,
        group_id: ScalarGroupId,
        representative_group_id: ScalarGroupId,
    ) -> anyhow::Result<()> {
        sqlx::query("UPDATE scalar_groups SET representative_group_id = $1 WHERE representative_group_id = $2")
            .bind(representative_group_id)
            .bind(group_id)
            .execute(db)
            .await?;
        Ok(())
    }

    /// Inserts a scalar expression into the database. If the `add_to_group_id` is `Some`,
    /// we will attempt to add the scalar expression to the specified group.
    /// If the scalar expression already exists in the database, the existing group id will be returned.
    /// Otherwise, a new group id will be created.
    async fn add_scalar_expr_to_group_inner(
        &self,
        scalar_expr: &ScalarExpression,
        add_to_group_id: Option<ScalarGroupId>,
    ) -> anyhow::Result<ScalarGroupId> {
        let mut txn = self.begin().await?;
        let group_id = if let Some(group_id) = add_to_group_id {
            self.get_representative_scalar_group_id(&mut txn, group_id)
                .await?
        } else {
            let group_id = txn.new_scalar_group_id().await?;
            sqlx::query(
                "INSERT INTO scalar_groups (id, representative_group_id, exploration_status) VALUES ($1, $2, $3)",
            )
            .bind(group_id)
            .bind(group_id)
            .bind(ExplorationStatus::Unexplored)
            .execute(&mut *txn)
            .await?;
            group_id
        };

        let scalar_expr_id = txn.new_scalar_expression_id().await?;
        let inserted_group_id: ScalarGroupId = match scalar_expr {
            ScalarExpression::Constant(constant) => {
                Self::insert_into_scalar_expressions(
                    &mut txn,
                    scalar_expr_id,
                    group_id,
                    ScalarOperatorKind::Constant,
                )
                .await?;

                sqlx::query_scalar("INSERT INTO scalar_constants (scalar_expression_id, group_id, value) VALUES ($1, $2, $3) ON CONFLICT DO UPDATE SET group_id = group_id RETURNING group_id")
                    .bind(scalar_expr_id)
                    .bind(group_id)
                    .bind(serde_json::to_string(&constant)?)
                    .fetch_one(&mut *txn)
                    .await?
            }
            ScalarExpression::ColumnRef(column_ref) => {
                Self::insert_into_scalar_expressions(
                    &mut txn,
                    scalar_expr_id,
                    group_id,
                    ScalarOperatorKind::ColumnRef,
                )
                .await?;

                sqlx::query_scalar("INSERT INTO scalar_column_refs (scalar_expression_id, group_id, column_index) VALUES ($1, $2, $3) ON CONFLICT DO UPDATE SET group_id = group_id RETURNING group_id")
                    .bind(scalar_expr_id)
                    .bind(group_id)
                    .bind(serde_json::to_string(&column_ref.column_index)?)
                    .fetch_one(&mut *txn)
                    .await?
            }
            ScalarExpression::BinaryOp(binary_op) => {
                Self::insert_into_scalar_expressions(
                    &mut txn,
                    scalar_expr_id,
                    group_id,
                    ScalarOperatorKind::Binary,
                )
                .await?;

                sqlx::query_scalar("INSERT INTO scalar_binary_ops (scalar_expression_id, group_id, kind, left_group_id, right_group_id) VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO UPDATE SET group_id = group_id RETURNING group_id")
                    .bind(scalar_expr_id)
                    .bind(group_id)
                    .bind(serde_json::to_string(&binary_op.kind)?)
                    .bind(binary_op.left)
                    .bind(binary_op.right)
                    .fetch_one(&mut *txn)
                    .await?
            }
            ScalarExpression::UnaryOp(unary_op) => {
                Self::insert_into_scalar_expressions(
                    &mut txn,
                    scalar_expr_id,
                    group_id,
                    ScalarOperatorKind::Binary,
                )
                .await?;

                sqlx::query_scalar("INSERT INTO scalar_unary_ops (scalar_expression_id, group_id, kind, child_group_id) VALUES ($1, $2, $3, $4) ON CONFLICT DO UPDATE SET group_id = group_id RETURNING group_id")
                    .bind(scalar_expr_id)
                    .bind(group_id)
                    .bind(serde_json::to_string(&unary_op.kind)?)
                    .bind(unary_op.child)
                    .fetch_one(&mut *txn)
                    .await?
            }
            ScalarExpression::LogicOp(logic) => {
                Self::insert_into_scalar_expressions(
                    &mut txn,
                    scalar_expr_id,
                    group_id,
                    ScalarOperatorKind::Logic,
                )
                .await?;

                sqlx::query_scalar("INSERT INTO scalar_logic_ops (scalar_expression_id, group_id, kind, children_group_ids) VALUES ($1, $2, $3, $4) ON CONFLICT DO UPDATE SET group_id = group_id RETURNING group_id")
                    .bind(scalar_expr_id)
                    .bind(group_id)
                    .bind(serde_json::to_string(&logic.kind)?)
                    .bind(serde_json::to_value(&logic.children)?)
                    .fetch_one(&mut *txn)
                    .await?
            }
        };

        if inserted_group_id == group_id {
            // There is no duplicate, we should commit the transaction.
            txn.commit().await?;
        } else if add_to_group_id.is_some() {
            // merge the two groups.
            self.set_representative_scalar_group_id(&mut txn, group_id, inserted_group_id)
                .await?;

            // We should remove the dangling logical expression. We waste one id here but it is ok.
            self.remove_dangling_scalar_expr(&mut txn, scalar_expr_id)
                .await?;
            txn.commit().await?;
        }
        Ok(inserted_group_id)
    }

    /// Inserts an entry into the `scalar_expressions` table.
    async fn insert_into_scalar_expressions(
        db: &mut SqliteConnection,
        scalar_expr_id: ScalarExpressionId,
        group_id: ScalarGroupId,
        operator_kind: ScalarOperatorKind,
    ) -> anyhow::Result<()> {
        sqlx::query("INSERT INTO scalar_expressions (id, group_id, operator_kind, exploration_status) VALUES ($1, $2, $3, $4)")
                .bind(scalar_expr_id)
                .bind(group_id)
                .bind(operator_kind)
                .bind(ExplorationStatus::Unexplored)
                .execute(&mut *db)
                .await?;
        Ok(())
    }

    /// Removes a dangling scalar expression from the `scalar_expressions` table.
    async fn remove_dangling_scalar_expr(
        &self,
        db: &mut SqliteConnection,
        scalar_expr_id: ScalarExpressionId,
    ) -> anyhow::Result<()> {
        sqlx::query("DELETE FROM scalar_expressions WHERE id = $1")
            .bind(scalar_expr_id)
            .execute(db)
            .await?;
        Ok(())
    }

    /// Inserts a logical expression into the memo table. If the `add_to_group_id` is `Some`,
    /// we will attempt to add the logical expression to the specified group.
    /// If the logical expression already exists in the database, the existing group id will be returned.
    /// Otherwise, a new group id will be created.
    async fn add_logical_expr_to_group_inner(
        &self,
        logical_expr: &LogicalExpression,
        add_to_group_id: Option<RelationalGroupId>,
    ) -> anyhow::Result<RelationalGroupId> {
        let mut txn = self.begin().await?;
        let group_id = if let Some(group_id) = add_to_group_id {
            self.get_representative_group_id(&mut txn, group_id).await?
        } else {
            let group_id = txn.new_relational_group_id().await?;
            sqlx::query(
                "INSERT INTO relation_groups (id, representative_group_id, exploration_status) VALUES ($1, $2, $3)",
            )
            .bind(group_id)
            .bind(group_id)
            .bind(ExplorationStatus::Unexplored)
            .execute(&mut *txn)
            .await?;
            group_id
        };

        let logical_expr_id = txn.new_logical_expression_id().await?;

        // The inserted group id could be different from the original group id
        // if the logical expression already exists in the group.
        let inserted_group_id: RelationalGroupId = match logical_expr {
            LogicalExpression::Scan(scan) => {
                Self::insert_into_logical_expressions(
                    &mut txn,
                    logical_expr_id,
                    group_id,
                    LogicalOperatorKind::Scan,
                )
                .await?;

                sqlx::query_scalar("INSERT INTO scans (logical_expression_id, group_id, table_name, predicate_group_id) VALUES ($1, $2, $3, $4) ON CONFLICT DO UPDATE SET group_id = group_id RETURNING group_id")
                    .bind(logical_expr_id)
                    .bind(group_id)
                    .bind(serde_json::to_string(&scan.table_name)?)
                    .bind(scan.predicate)
                    .fetch_one(&mut *txn)
                    .await?
            }
            LogicalExpression::Filter(filter) => {
                Self::insert_into_logical_expressions(
                    &mut txn,
                    logical_expr_id,
                    group_id,
                    LogicalOperatorKind::Filter,
                )
                .await?;

                sqlx::query_scalar("INSERT INTO filters (logical_expression_id, group_id, child_group_id, predicate_group_id) VALUES ($1, $2, $3, $4) ON CONFLICT DO UPDATE SET group_id = group_id RETURNING group_id")
                    .bind(logical_expr_id)
                    .bind(group_id)
                    .bind(filter.child)
                    .bind(filter.predicate)
                    .fetch_one(&mut *txn)
                    .await?
            }
            LogicalExpression::Join(join) => {
                Self::insert_into_logical_expressions(
                    &mut txn,
                    logical_expr_id,
                    group_id,
                    LogicalOperatorKind::Join,
                )
                .await?;

                sqlx::query_scalar("INSERT INTO joins (logical_expression_id, group_id, join_type, left_group_id, right_group_id, condition_group_id) VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT DO UPDATE SET group_id = group_id RETURNING group_id")
                    .bind(logical_expr_id)
                    .bind(group_id)
                    .bind(serde_json::to_string(&join.join_type)?)
                    .bind(join.left)
                    .bind(join.right)
                    .bind(join.condition)
                    .fetch_one(&mut *txn)
                    .await?
            }
            LogicalExpression::Project(project) => {
                Self::insert_into_logical_expressions(
                    &mut txn,
                    logical_expr_id,
                    group_id,
                    LogicalOperatorKind::Project,
                )
                .await?;

                sqlx::query_scalar("INSERT INTO projects (logical_expression_id, group_id, child_group_id, fields_group_ids) VALUES ($1, $2, $3, $4) ON CONFLICT DO UPDATE SET group_id = group_id RETURNING group_id")
                    .bind(logical_expr_id)
                    .bind(group_id)
                    .bind(project.child)
                    .bind(serde_json::to_value(&project.fields)?)
                    .fetch_one(&mut *txn)
                    .await?
            }
        };

        if inserted_group_id == group_id {
            // There is no duplicate, we should commit the transaction.
            txn.commit().await?;
        } else if add_to_group_id.is_some() {
            // Merge the two groups.
            self.set_representative_group_id(&mut txn, group_id, inserted_group_id)
                .await?;
            // TODO: merge group here, move all expressions to the representative group.

            // We should remove the dangling logical expression. We waste one id here but it is ok.
            self.remove_dangling_logical_expr(&mut txn, logical_expr_id)
                .await?;
            txn.commit().await?;
        }
        Ok(inserted_group_id)
    }

    /// Inserts an entry into the `logical_expressions` table.
    async fn insert_into_logical_expressions(
        txn: &mut SqliteConnection,
        logical_expr_id: LogicalExpressionId,
        group_id: RelationalGroupId,
        operator_kind: LogicalOperatorKind,
    ) -> anyhow::Result<()> {
        sqlx::query("INSERT INTO logical_expressions (id, group_id, operator_kind, exploration_status) VALUES ($1, $2, $3, $4)")
                .bind(logical_expr_id)
                .bind(group_id)
                .bind(operator_kind)
                .bind(ExplorationStatus::Unexplored)
                .execute(&mut *txn)
                .await?;
        Ok(())
    }

    /// Removes a dangling logical expression from the `logical_expressions` table.
    async fn remove_dangling_logical_expr(
        &self,
        db: &mut SqliteConnection,
        logical_expr_id: LogicalExpressionId,
    ) -> anyhow::Result<()> {
        sqlx::query("DELETE FROM logical_expressions WHERE id = $1")
            .bind(logical_expr_id)
            .execute(db)
            .await?;
        Ok(())
    }

    async fn add_physical_expr_to_goal_inner(
        &self,
        physical_expr: &PhysicalExpression,
        cost: Cost,
        representative_goal_id: GoalId,
    ) -> anyhow::Result<GoalId> {
        let mut txn = self.begin().await?;
        let goal_id = self
            .get_representative_goal_id(&mut txn, representative_goal_id)
            .await?;
        let physical_expr_id = txn.new_physical_expression_id().await?;

        let inserted_goal_id: GoalId = match physical_expr {
            PhysicalExpression::TableScan(scan) => {
                Self::insert_into_physical_expressions(
                    &mut txn,
                    physical_expr_id,
                    goal_id,
                    PhysicalOperatorKind::TableScan,
                    cost,
                )
                .await?;

                sqlx::query_scalar("INSERT INTO table_scans (physical_expression_id, goal_id, table_name, predicate_group_id) VALUES ($1, $2, $3, $4) ON CONFLICT DO UPDATE SET goal_id = goal_id RETURNING goal_id")
                    .bind(physical_expr_id)
                    .bind(goal_id)
                    .bind(serde_json::to_string(&scan.table_name)?)
                    .bind(scan.predicate)
                    .fetch_one(&mut *txn)
                    .await?
            }
            PhysicalExpression::Filter(filter) => {
                Self::insert_into_physical_expressions(
                    &mut txn,
                    physical_expr_id,
                    goal_id,
                    PhysicalOperatorKind::Filter,
                    cost,
                )
                .await?;

                sqlx::query_scalar("INSERT INTO physical_filters (physical_expression_id, goal_id, child_goal_id, predicate_group_id) VALUES ($1, $2, $3, $4) ON CONFLICT DO UPDATE SET goal_id = goal_id RETURNING goal_id")
                    .bind(physical_expr_id)
                    .bind(goal_id)
                    .bind(filter.child)
                    .bind(filter.predicate)
                    .fetch_one(&mut *txn)
                    .await?
            }
            PhysicalExpression::NestedLoopJoin(join) => {
                Self::insert_into_physical_expressions(
                    &mut txn,
                    physical_expr_id,
                    goal_id,
                    PhysicalOperatorKind::NestedLoopJoin,
                    cost,
                )
                .await?;

                sqlx::query_scalar("INSERT INTO nested_loop_joins (physical_expression_id, goal_id, join_type, outer_goal_id, inner_goal_id, condition_group_id) VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT DO UPDATE SET goal_id = goal_id RETURNING goal_id")
                    .bind(physical_expr_id)
                    .bind(goal_id)
                    .bind(serde_json::to_string(&join.join_type)?)
                    .bind(join.outer)
                    .bind(join.inner)
                    .bind(join.condition)
                    .fetch_one(&mut *txn)
                    .await?
            }
            PhysicalExpression::Project(project) => {
                Self::insert_into_physical_expressions(
                    &mut txn,
                    physical_expr_id,
                    goal_id,
                    PhysicalOperatorKind::Project,
                    cost,
                )
                .await?;

                sqlx::query_scalar("INSERT INTO physical_projects (physical_expression_id, goal_id, child_goal_id, fields_group_ids) VALUES ($1, $2, $3, $4) ON CONFLICT DO UPDATE SET goal_id = goal_id RETURNING goal_id")
                    .bind(physical_expr_id)
                    .bind(goal_id)
                    .bind(project.child)
                    .bind(serde_json::to_value(&project.fields)?)
                    .fetch_one(&mut *txn)
                    .await?
            }
            _ => unimplemented!(),
        };
        txn.commit().await?;

        Ok(inserted_goal_id)
    }

    /// Inserts an entry into the `physical_expressions` table.
    async fn insert_into_physical_expressions(
        txn: &mut SqliteConnection,
        physical_expr_id: PhysicalExpressionId,
        goal_id: GoalId,
        operator_kind: PhysicalOperatorKind,
        cost: Cost,
    ) -> anyhow::Result<()> {
        sqlx::query(
            "INSERT INTO physical_expressions (id, goal_id, operator_kind, cost) VALUES ($1, $2, $3, $4)",
        )
        .bind(physical_expr_id)
        .bind(goal_id)
        .bind(operator_kind)
        .bind(cost)
        .execute(&mut *txn)
        .await?;
        Ok(())
    }
}

/// The SQL query to get all logical expressions in a group.
/// For each of the operators, the logical_expression_id is selected,
/// as well as the data fields in json form.
const fn get_all_logical_exprs_in_group_query() -> &'static str {
    concat!(
        "SELECT logical_expression_id, json_object('Scan', json_object('table_name', json(table_name), 'predicate', predicate_group_id)) as data FROM scans WHERE group_id = $1",
        " UNION ALL ",
        "SELECT logical_expression_id, json_object('Filter', json_object('child', child_group_id, 'predicate', predicate_group_id)) as data FROM filters WHERE group_id = $1",
        " UNION ALL ",
        "SELECT logical_expression_id, json_object('Join', json_object('join_type', json(join_type), 'left', left_group_id, 'right', right_group_id, 'condition', condition_group_id)) as data FROM joins WHERE group_id = $1",
        " UNION ALL ",
        "SELECT logical_expression_id, json_object('Project', json_object('child', child_group_id, 'fields', json(fields_group_ids))) as data FROM projects WHERE group_id = $1"
    )
}

/// The SQL query to get all scalar expressions in a group.
/// For each of the operators, the scalar_expression_id is selected,
/// as well as the data fields in json form.
const fn get_all_scalar_exprs_in_group_query() -> &'static str {
    concat!(
        "SELECT scalar_expression_id, json_object('Constant', json(value)) as data FROM scalar_constants WHERE group_id = $1",
        " UNION ALL ",
        "SELECT scalar_expression_id, json_object('ColumnRef', json_object('column_index', json(column_index))) as data FROM scalar_column_refs WHERE group_id = $1",
        " UNION ALL ",
        "SELECT scalar_expression_id, json_object('BinaryOp', json_object('kind', json(kind), 'left', left_group_id, 'right', right_group_id)) as data FROM scalar_binary_ops WHERE group_id = $1",
        " UNION ALL ",
        "SELECT scalar_expression_id, json_object('LogicOp', json_object('kind', json(kind), 'children', json(children_group_ids))) as data FROM scalar_logic_ops WHERE group_id = $1",
        " UNION ALL ",
        "SELECT scalar_expression_id, json_object('UnaryOp', json_object('kind', json(kind), 'child', child_group_id)) as data FROM scalar_unary_ops WHERE group_id = $1",
    )
}

/// The SQL query to get the winner physical expression in a goal.
/// For each of the operators, the physical_expression_id and the
/// associated cost is selected as well as the data fields in json form.
const fn get_winner_physical_expr_in_goal_query() -> &'static str {
    concat!(
        " WITH winner(id, operator_kind, cost) AS (",
        " SELECT id, operator_kind, min(cost) FROM physical_expressions WHERE goal_id = $1",
        " ) SELECT winner.id as physical_expression_id, CASE",
        "     WHEN winner.operator_kind = 'TableScan'",
        "        THEN (SELECT json_object('TableScan', json_object('table_name', json(table_name), 'predicate', predicate_group_id)) FROM table_scans WHERE physical_expression_id = winner.id)",
        "     WHEN winner.operator_kind = 'Filter'",
        "        THEN (SELECT json_object('Filter', json_object('child', child_goal_id, 'predicate', predicate_group_id)) FROM physical_filters WHERE physical_expression_id = winner.id)",
        "     WHEN winner.operator_kind = 'NestedLoopJoin'",
        "        THEN (SELECT json_object('NestedLoopJoin', json_object('join_type', json(join_type), 'outer', outer_goal_id, 'inner', inner_goal_id, 'condition', condition_group_id)) FROM nested_loop_joins WHERE physical_expression_id = winner.id)",
        "     WHEN winner.operator_kind = 'Project'",
        "        THEN (SELECT json_object('Project', json_object('child', child_goal_id, 'fields', json(fields_group_ids))) FROM physical_projects WHERE physical_expression_id = winner.id)",
        " END as data, winner.cost as cost FROM winner"
    )
}

/// The SQL query to get all physical expressions in a goal.
/// For each of the operators, the physical_expression_id is selected,
/// as well as the data fields in json form.
const fn get_all_physical_exprs_in_goal_query() -> &'static str {
    concat!(
        "SELECT physical_expression_id, json_object('TableScan', json_object('table_name', json(table_name), 'predicate', predicate_group_id)) as data FROM table_scans WHERE goal_id = $1",
        " UNION ALL ",
        "SELECT physical_expression_id, json_object('Filter', json_object('child', child_goal_id, 'predicate', predicate_group_id)) as data FROM physical_filters WHERE goal_id = $1",
        " UNION ALL ",
        "SELECT physical_expression_id, json_object('NestedLoopJoin', json_object('join_type', json(join_type), 'outer', outer_goal_id, 'inner', inner_goal_id, 'condition', condition_group_id)) as data FROM nested_loop_joins WHERE goal_id = $1",
        " UNION ALL ",
        "SELECT physical_expression_id, json_object('Project', json_object('child', child_goal_id, 'fields', json(fields_group_ids))) as data FROM physical_projects WHERE goal_id = $1"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operators::relational::logical::*;
    use crate::operators::scalar::*;
    use crate::values::OptdValue;

    #[tokio::test]
    async fn test_insert_expr_with_memo() -> anyhow::Result<()> {
        let memo = SqliteMemo::new_in_memory().await?;

        let true_predicate =
            ScalarExpression::Constant(constants::Constant::new(OptdValue::Bool(true)));
        let true_predicate_group = memo.add_scalar_expr(&true_predicate).await?;
        let scan1 = Arc::new(scan::scan("t1", true_predicate_group));
        let scan1_group = memo.add_logical_expr(&scan1).await?;
        let dup_scan1_group = memo.add_logical_expr(&scan1).await?;
        assert_eq!(scan1_group, dup_scan1_group);

        let scan2 = Arc::new(scan::scan("t2", true_predicate_group));
        let scan2_group = memo.add_logical_expr(&scan2).await?;
        let dup_scan2_group = memo.add_logical_expr(&scan2).await?;
        assert_eq!(scan2_group, dup_scan2_group);

        let t1v1 = column_ref::column_ref(1);
        let t1v1_group_id = memo.add_scalar_expr(&t1v1).await?;
        let t2v2 = column_ref::column_ref(2);
        let t2v2_group_id = memo.add_scalar_expr(&t2v2).await?;

        let join_cond = binary_op::equal(t1v1_group_id, t2v2_group_id);
        let join_cond_group_id = memo.add_scalar_expr(&join_cond).await?;
        let join = Arc::new(join::join(
            "inner",
            scan1_group,
            scan2_group,
            join_cond_group_id,
        ));

        let join_group = memo.add_logical_expr(&join).await?;
        let dup_join_group = memo.add_logical_expr(&join).await?;
        assert_eq!(join_group, dup_join_group);

        let join_alt = Arc::new(join::join(
            "inner",
            scan2_group,
            scan1_group,
            join_cond_group_id,
        ));
        let join_alt_group = memo
            .add_logical_expr_to_group(&join_alt, join_group)
            .await?;
        assert_eq!(join_group, join_alt_group);

        let logical_exprs: Vec<Arc<LogicalExpression>> = memo
            .get_all_logical_exprs_in_group(join_group)
            .await?
            .into_iter()
            .map(|(_, expr)| expr)
            .collect();
        assert!(logical_exprs.contains(&join));
        assert!(logical_exprs.contains(&join_alt));

        let children_groups = join.children_relations();
        assert_eq!(children_groups.len(), 2);
        assert_eq!(children_groups[0], scan1_group);
        assert_eq!(children_groups[1], scan2_group);

        let children_groups = join_alt.children_relations();
        assert_eq!(children_groups.len(), 2);
        assert_eq!(children_groups[0], scan2_group);
        assert_eq!(children_groups[1], scan1_group);

        let logical_exprs: Vec<Arc<LogicalExpression>> = memo
            .get_all_logical_exprs_in_group(scan1_group)
            .await?
            .into_iter()
            .map(|(_, expr)| expr)
            .collect();
        assert!(logical_exprs.contains(&scan1));
        assert_eq!(scan1.children_relations().len(), 0);

        let logical_exprs: Vec<Arc<LogicalExpression>> = memo
            .get_all_logical_exprs_in_group(scan2_group)
            .await?
            .into_iter()
            .map(|(_, expr)| expr)
            .collect();
        assert!(logical_exprs.contains(&scan2));
        assert_eq!(scan2.children_relations().len(), 0);

        Ok(())
    }
}
