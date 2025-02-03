//! A persistent memo table implementation.

use sqlx::{Executor, SqliteConnection};

use crate::{
    operator::{relational::logical::LogicalOperatorKind, scalar::ScalarOperatorKind},
    storage::StorageManager,
};

use super::*;

/// A persistent memo table backed by a database.
pub struct PersistentMemo {
    storage: StorageManager,
    /// SQL query string to get all logical expressions in a group.
    get_all_logical_exprs_in_group_query: String,
    /// SQL query string to get all scalar expressions in a group.
    get_all_scalar_exprs_in_group_query: String,
}

impl PersistentMemo {
    /// Create a new persistent memo table backed by a SQLite database at the given URL.
    pub async fn new(database_url: &str) -> anyhow::Result<Self> {
        let storage = StorageManager::new(database_url).await?;
        storage.migrate().await?;
        let get_all_logical_exprs_in_group_query = get_all_logical_exprs_in_group_query();
        let get_all_scalar_exprs_in_group_query = get_all_scalar_exprs_in_group_query();
        storage
            .db()
            .await?
            .prepare(&get_all_logical_exprs_in_group_query)
            .await?;
        Ok(Self {
            storage,
            get_all_logical_exprs_in_group_query,
            get_all_scalar_exprs_in_group_query,
        })
    }

    /// Create a new persistent memo table backed by an in-memory SQLite database.
    pub async fn new_in_memory() -> anyhow::Result<Self> {
        Self::new("sqlite::memory:").await
    }

    // /// Creates a new scalar group for testing purposes.
    // #[cfg(test)]
    // pub async fn new_scalar_group_for_test(&self) -> anyhow::Result<ScalarGroupId> {
    //     let mut txn = self.storage.begin().await?;
    //     let scalar_group_id = txn.new_scalar_group_id().await?;
    //     sqlx::query(
    //         "INSERT INTO scalar_groups (id, representative_group_id, exploration_status) VALUES ($1, $2, $3)",
    //     )
    //     .bind(scalar_group_id)
    //     .bind(scalar_group_id)
    //     .bind(ExplorationStatus::Unexplored)
    //     .execute( &mut *txn)
    //     .await?;
    //     txn.commit().await?;
    //     Ok(scalar_group_id)
    // }

    async fn get_representative_group_id(
        &self,
        db: &mut SqliteConnection,
        group_id: GroupId,
    ) -> anyhow::Result<GroupId> {
        let representative_group_id: GroupId =
            sqlx::query_scalar("SELECT representative_group_id FROM relation_groups WHERE id = $1")
                .bind(group_id)
                .fetch_one(db)
                .await?;
        Ok(representative_group_id)
    }

    async fn set_representative_group_id(
        &self,
        db: &mut SqliteConnection,
        group_id: GroupId,
        representative_group_id: GroupId,
    ) -> anyhow::Result<()> {
        sqlx::query("UPDATE relation_groups SET representative_group_id = $1 WHERE representative_group_id = $2")
            .bind(representative_group_id)
            .bind(group_id)
            .execute(db)
            .await?;
        Ok(())
    }

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

    async fn add_scalar_expr_to_group_inner(
        &self,
        scalar_expr: &ScalarExpression,
        add_to_group_id: Option<ScalarGroupId>,
    ) -> anyhow::Result<ScalarGroupId> {
        let mut txn = self.storage.begin().await?;
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
                let group_id = sqlx::query_scalar("INSERT INTO scalar_constants (scalar_expression_id, group_id, payload) VALUES ($1, $2, $3) ON CONFLICT DO UPDATE SET group_id = group_id RETURNING group_id")
                    .bind(scalar_expr_id)
                    .bind(group_id)
                    .bind(serde_json::to_value(constant)?)
                    .fetch_one(&mut *txn)
                    .await?;
                group_id
            }
            ScalarExpression::ColumnRef(column_ref) => {
                Self::insert_into_scalar_expressions(
                    &mut txn,
                    scalar_expr_id,
                    group_id,
                    ScalarOperatorKind::ColumnRef,
                )
                .await?;
                let group_id = sqlx::query_scalar("INSERT INTO scalar_column_refs (scalar_expression_id, group_id, payload) VALUES ($1, $2, $3) ON CONFLICT DO UPDATE SET group_id = group_id RETURNING group_id")
                    .bind(scalar_expr_id)
                    .bind(group_id)
                    .bind(serde_json::to_value(column_ref)?)
                    .fetch_one(&mut *txn)
                    .await?;
                group_id
            }
            ScalarExpression::Add(add) => {
                Self::insert_into_scalar_expressions(
                    &mut txn,
                    scalar_expr_id,
                    group_id,
                    ScalarOperatorKind::Add,
                )
                .await?;
                let group_id = sqlx::query_scalar("INSERT INTO scalar_adds (scalar_expression_id, group_id, left_group_id, right_group_id) VALUES ($1, $2, $3, $4) ON CONFLICT DO UPDATE SET group_id = group_id RETURNING group_id")
                    .bind(scalar_expr_id)
                    .bind(group_id)
                    .bind(add.left)
                    .bind(add.right)
                    .fetch_one(&mut *txn)
                    .await?;
                group_id
            }
            ScalarExpression::Equal(equal) => {
                Self::insert_into_scalar_expressions(
                    &mut txn,
                    scalar_expr_id,
                    group_id,
                    ScalarOperatorKind::Equal,
                )
                .await?;
                let group_id = sqlx::query_scalar("INSERT INTO scalar_equals (scalar_expression_id, group_id, left_group_id, right_group_id) VALUES ($1, $2, $3, $4) ON CONFLICT DO UPDATE SET group_id = group_id RETURNING group_id")
                    .bind(scalar_expr_id)
                    .bind(group_id)
                    .bind(equal.left)
                    .bind(equal.right)
                    .fetch_one(&mut *txn)
                    .await?;
                group_id
            }
        };

        if inserted_group_id == group_id {
            // There is no duplicate, we should commit the transaction.
            println!("committing");
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

    /// Inserts a scalar expression into the database.
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

    async fn add_logical_expr_to_group_inner(
        &self,
        logical_expr: &LogicalExpression,
        add_to_group_id: Option<GroupId>,
    ) -> anyhow::Result<GroupId> {
        let mut txn = self.storage.begin().await?;
        let group_id = if let Some(group_id) = add_to_group_id {
            self.get_representative_group_id(&mut txn, group_id).await?
        } else {
            let group_id = txn.new_group_id().await?;
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
        let inserted_group_id: GroupId = match logical_expr {
            LogicalExpression::Scan(scan) => {
                Self::insert_into_logical_expressions(
                    &mut txn,
                    logical_expr_id,
                    group_id,
                    LogicalOperatorKind::Scan,
                )
                .await?;
                let group_id= sqlx::query_scalar("INSERT INTO scans (logical_expression_id, group_id, table_name, predicate_group_id) VALUES ($1, $2, $3, $4) ON CONFLICT DO UPDATE SET group_id = group_id RETURNING group_id")
                    .bind(logical_expr_id)
                    .bind(group_id)
                    .bind(&scan.table_name)
                    .bind(scan.predicate)
                    .fetch_one(&mut *txn)
                    .await?;
                group_id
            }
            LogicalExpression::Filter(filter) => {
                Self::insert_into_logical_expressions(
                    &mut txn,
                    logical_expr_id,
                    group_id,
                    LogicalOperatorKind::Filter,
                )
                .await?;
                let group_id = sqlx::query_scalar("INSERT INTO filters (logical_expression_id, group_id, child_group_id, predicate_group_id) VALUES ($1, $2, $3, $4) ON CONFLICT DO UPDATE SET group_id = group_id RETURNING group_id")
                    .bind(logical_expr_id)
                    .bind(group_id)
                    .bind(filter.child)
                    .bind(filter.predicate)
                    .fetch_one(&mut *txn)
                    .await?;
                group_id
            }
            LogicalExpression::Project(project) => {
                Self::insert_into_logical_expressions(
                    &mut txn,
                    logical_expr_id,
                    group_id,
                    LogicalOperatorKind::Project,
                )
                .await?;
                let group_id = sqlx::query_scalar("INSERT INTO projects (logical_expression_id, group_id, child_group_id, fields_group_ids) VALUES ($1, $2, $3, $4) ON CONFLICT DO UPDATE SET group_id = group_id RETURNING group_id")
                    .bind(logical_expr_id)
                    .bind(group_id)
                    .bind(project.child)
                    .bind(serde_json::to_value(&project.fields)?)
                    .fetch_one(&mut *txn)
                    .await?;
                group_id
            }
            LogicalExpression::Join(join) => {
                Self::insert_into_logical_expressions(
                    &mut txn,
                    logical_expr_id,
                    group_id,
                    LogicalOperatorKind::Join,
                )
                .await?;
                let group_id = sqlx::query_scalar("INSERT INTO joins (logical_expression_id, group_id, join_type, left_group_id, right_group_id, condition_group_id) VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT DO UPDATE SET group_id = group_id RETURNING group_id")
                    .bind(logical_expr_id)
                    .bind(group_id)
                    .bind(join.join_type)
                    .bind(join.left)
                    .bind(join.right)
                    .bind(join.condition)
                    .fetch_one(&mut *txn)
                    .await?;
                group_id
            }
        };

        if inserted_group_id == group_id {
            // There is no duplicate, we should commit the transaction.
            println!("committing");
            txn.commit().await?;
        } else if add_to_group_id.is_some() {
            // merge the two groups.
            self.set_representative_group_id(&mut txn, group_id, inserted_group_id)
                .await?;

            // We should remove the dangling logical expression. We waste one id here but it is ok.
            self.remove_dangling_logical_expr(&mut txn, logical_expr_id)
                .await?;
            txn.commit().await?;
        }
        Ok(inserted_group_id)
    }

    async fn insert_into_logical_expressions(
        txn: &mut SqliteConnection,
        logical_expr_id: LogicalExpressionId,
        group_id: GroupId,
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
}

impl Memoize for PersistentMemo {
    /// Gets the exploration status of a group.
    async fn get_group_exploration_status(
        &self,
        group_id: GroupId,
    ) -> anyhow::Result<ExplorationStatus> {
        let mut txn = self.storage.begin().await?;
        let status =
            sqlx::query_scalar("SELECT exploration_status FROM relation_groups WHERE id = $1")
                .bind(group_id)
                .fetch_one(&mut *txn)
                .await?;
        txn.commit().await?;
        Ok(status)
    }

    /// Sets the exploration status of a group.
    async fn set_group_exploration_status(
        &self,
        group_id: GroupId,
        status: ExplorationStatus,
    ) -> anyhow::Result<()> {
        let mut txn = self.storage.begin().await?;
        sqlx::query("UPDATE relation_groups SET exploration_status = $1 WHERE id = $2")
            .bind(status)
            .bind(group_id)
            .execute(&mut *txn)
            .await?;
        txn.commit().await?;
        Ok(())
    }

    async fn get_all_logical_exprs_in_group(
        &self,
        group_id: GroupId,
    ) -> anyhow::Result<Vec<LogicalExpression>> {
        #[derive(sqlx::FromRow)]
        struct LogicalExprRecord {
            data: sqlx::types::Json<LogicalExpression>,
        }

        let mut txn = self.storage.begin().await?;
        let logical_exprs: Vec<LogicalExprRecord> =
            sqlx::query_as(&self.get_all_logical_exprs_in_group_query)
                .bind(group_id)
                .fetch_all(&mut *txn)
                .await?;

        txn.commit().await?;
        Ok(logical_exprs.into_iter().map(|r| r.data.0).collect())
    }

    async fn add_logical_expr_to_group(
        &self,
        logical_expr: &LogicalExpression,
        group_id: GroupId,
    ) -> anyhow::Result<GroupId> {
        let group_id = self
            .add_logical_expr_to_group_inner(logical_expr, Some(group_id))
            .await?;
        Ok(group_id)
    }

    async fn add_logical_expr(&self, logical_expr: &LogicalExpression) -> anyhow::Result<GroupId> {
        let group_id = self
            .add_logical_expr_to_group_inner(logical_expr, None)
            .await?;
        Ok(group_id)
    }

    async fn get_scalar_group_exploration_status(
        &self,
        group_id: ScalarGroupId,
    ) -> anyhow::Result<ExplorationStatus> {
        let mut txn = self.storage.begin().await?;
        let status =
            sqlx::query_scalar("SELECT exploration_status FROM scalar_groups WHERE id = $1")
                .bind(group_id)
                .fetch_one(&mut *txn)
                .await?;
        txn.commit().await?;
        Ok(status)
    }

    async fn set_scalar_group_exploration_status(
        &self,
        group_id: ScalarGroupId,
        status: ExplorationStatus,
    ) -> anyhow::Result<()> {
        let mut txn = self.storage.begin().await?;
        sqlx::query("UPDATE scalar_groups SET exploration_status = $1 WHERE id = $2")
            .bind(status)
            .bind(group_id)
            .execute(&mut *txn)
            .await?;
        txn.commit().await?;
        Ok(())
    }

    /// Gets all scalar expressions in a group.
    async fn get_all_scalar_exprs_in_group(
        &self,
        group_id: ScalarGroupId,
    ) -> anyhow::Result<Vec<ScalarExpression>> {
        #[derive(sqlx::FromRow)]
        struct ScalarExprRecord {
            data: sqlx::types::Json<ScalarExpression>,
        }

        let mut txn = self.storage.begin().await?;
        let logical_exprs: Vec<ScalarExprRecord> =
            sqlx::query_as(&self.get_all_scalar_exprs_in_group_query)
                .bind(group_id)
                .fetch_all(&mut *txn)
                .await?;

        txn.commit().await?;
        Ok(logical_exprs.into_iter().map(|r| r.data.0).collect())
    }

    /// Adds a scalar expression to a group.
    async fn add_scalar_expr_to_group(
        &self,
        scalar_expr: &ScalarExpression,
        group_id: ScalarGroupId,
    ) -> anyhow::Result<ScalarGroupId> {
        let group_id = self
            .add_scalar_expr_to_group_inner(scalar_expr, Some(group_id))
            .await?;
        Ok(group_id)
    }

    /// Adds a scalar expression.
    async fn add_scalar_expr(
        &self,
        scalar_expr: &ScalarExpression,
    ) -> anyhow::Result<ScalarGroupId> {
        let group_id = self
            .add_scalar_expr_to_group_inner(scalar_expr, None)
            .await?;
        Ok(group_id)
    }
}

/// The SQL query to get all logical expressions in a group.
///
/// The query gets all data fields in each operator and converts them to JSON objects, which are later deserialized into `LogicalExpression` objects.
fn get_all_logical_exprs_in_group_query() -> String {
    [
        "SELECT json_object('Scan', json_object('table_name', table_name, 'predicate', predicate_group_id)) as data FROM scans WHERE group_id = $1",
        "SELECT json_object('Filter', json_object('child', child_group_id, 'predicate', predicate_group_id)) as data FROM filters WHERE group_id = $1",
        "SELECT json_object('Join', json_object('join_type', join_type, 'left', left_group_id, 'right', right_group_id, 'condition', condition_group_id)) as data FROM joins WHERE group_id = $1",
        "SELECT json_object('Project', json_object('child', child_group_id, 'fields', fields_group_ids)) as data FROM projects WHERE group_id = $1",
    ].join(" UNION ALL ")
}

/// The SQL query to get all scalar expressions in a group.
///
/// The query gets all data fields in each operator and converts them to JSON objects, which are later deserialized into `ScalarExpression` objects.
fn get_all_scalar_exprs_in_group_query() -> String {
    [
        "SELECT payload as data FROM scalar_constants WHERE group_id = $1",
        "SELECT payload as data FROM scalar_column_refs WHERE group_id = $1",
        "SELECT json_object('Add', json_object('left', left_group_id, 'right', right_group_id)) as data FROM scalar_adds WHERE group_id = $1",
        "SELECT json_object('Equal', json_object('left', left_group_id, 'right', right_group_id)) as data FROM scalar_equals WHERE group_id = $1"
    ].join(" UNION ALL ")
}

#[cfg(test)]
mod tests {
    use crate::operator::{
        relational::{
            logical::{self, join::JoinType},
            RelationChildren,
        },
        scalar,
    };

    use super::*;

    #[tokio::test]
    async fn test_insert_expr_with_memo() -> anyhow::Result<()> {
        let memo = PersistentMemo::new_in_memory().await?;

        let true_predicate = scalar::constants::boolean(true);
        let true_predicate_group = memo.add_scalar_expr(&true_predicate).await?;
        let scan1 = logical::scan("t1", true_predicate_group);
        let scan1_group = memo.add_logical_expr(&scan1).await?;
        let dup_scan1_group = memo.add_logical_expr(&scan1).await?;
        assert_eq!(scan1_group, dup_scan1_group);
        let status = memo.get_group_exploration_status(scan1_group).await?;
        assert_eq!(status, ExplorationStatus::Unexplored);

        let scan2 = logical::scan("t2", true_predicate_group);
        let scan2_group = memo.add_logical_expr(&scan2).await?;
        let dup_scan2_group = memo.add_logical_expr(&scan2).await?;
        assert_eq!(scan2_group, dup_scan2_group);
        let status = memo.get_group_exploration_status(scan1_group).await?;
        assert_eq!(status, ExplorationStatus::Unexplored);

        let t1v1 = scalar::column_ref::qualified_column_ref("t1", "v1");
        let t1v1_group_id = memo.add_scalar_expr(&t1v1).await?;
        let t2v2 = scalar::column_ref::qualified_column_ref("t1", "v1");
        let t2v2_group_id = memo.add_scalar_expr(&t2v2).await?;

        let join_cond = scalar::equal::equal(t1v1_group_id, t2v2_group_id);
        let join_cond_group_id = memo.add_scalar_expr(&join_cond).await?;
        let join = logical::join(
            JoinType::Inner,
            scan1_group,
            scan2_group,
            join_cond_group_id,
        );
        let join_group = memo.add_logical_expr(&join).await?;
        let dup_join_group = memo.add_logical_expr(&join).await?;
        assert_eq!(join_group, dup_join_group);
        let status = memo.get_group_exploration_status(scan1_group).await?;
        assert_eq!(status, ExplorationStatus::Unexplored);

        memo.set_group_exploration_status(join_group, ExplorationStatus::Exploring)
            .await?;

        let join_alt = logical::join(
            JoinType::Inner,
            scan2_group,
            scan1_group,
            join_cond_group_id,
        );

        let join_alt_group = memo
            .add_logical_expr_to_group(&join_alt, join_group)
            .await?;
        assert_eq!(join_group, join_alt_group);

        let status = memo.get_group_exploration_status(join_alt_group).await?;
        assert_eq!(status, ExplorationStatus::Exploring);

        let logical_exprs = memo.get_all_logical_exprs_in_group(join_group).await?;
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

        let logical_exprs = memo.get_all_logical_exprs_in_group(scan1_group).await?;
        assert!(logical_exprs.contains(&scan1));
        assert_eq!(scan1.children_relations().len(), 0);

        let logical_exprs = memo.get_all_logical_exprs_in_group(scan2_group).await?;
        assert!(logical_exprs.contains(&scan2));
        assert_eq!(scan2.children_relations().len(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_merge_group() -> anyhow::Result<()> {
        let memo = PersistentMemo::new_in_memory().await?;

        let true_predicate = scalar::constants::boolean(true);
        let true_predicate_group = memo.add_scalar_expr(&true_predicate).await?;
        let scan = logical::scan("t1", true_predicate_group);
        let scan_group = memo.add_logical_expr(&scan).await?;

        let filter = logical::filter(scan_group, true_predicate_group);
        let filter_group = memo.add_logical_expr(&filter).await?;
        let one_constant = scalar::constants::integer(1);
        let one_constant_group = memo.add_scalar_expr(&one_constant).await?;
        let one_equal_one = scalar::equal::equal(one_constant_group, one_constant_group);
        let one_equal_one_predicate_group = memo.add_scalar_expr(&one_equal_one).await?;
        let top_filter = logical::filter(filter_group, one_equal_one_predicate_group);
        let top_filter_group = memo.add_logical_expr(&top_filter).await?;
        let top_filter_2 = logical::filter(scan_group, one_equal_one_predicate_group);
        let top_filter_2_group = memo
            .add_logical_expr_to_group(&top_filter_2, top_filter_group)
            .await?;
        assert_eq!(top_filter_group, top_filter_2_group);
        let _ = memo
            .add_scalar_expr_to_group(&true_predicate, one_constant_group)
            .await?;
        let _ = memo
            .add_logical_expr_to_group(&scan, top_filter_group)
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_variable_length_operator() -> anyhow::Result<()> {
        let memo = PersistentMemo::new_in_memory().await?;

        let true_predicate = scalar::constants::boolean(true);
        let true_predicate_group = memo.add_scalar_expr(&true_predicate).await?;
        let scan = logical::scan("t1", true_predicate_group);
        let scan_group = memo.add_logical_expr(&scan).await?;

        let project = logical::project(scan_group, vec![true_predicate_group]);
        let project_group_1 = memo.add_logical_expr(&project).await?;
        let project_group_2 = memo.add_logical_expr(&project).await?;
        assert_eq!(project_group_1, project_group_2);

        Ok(())
    }
}
