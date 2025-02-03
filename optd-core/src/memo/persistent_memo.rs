//! A persistent memo table implementation.

use sqlx::{Executor, SqliteConnection};

use crate::{operator::relational::logical::LogicalOperatorKind, storage::StorageManager};

use super::*;

/// A persistent memo table backed by a database.
pub struct PersistentMemo {
    storage: StorageManager,
    get_all_logical_exprs_in_group_query: String,
}

impl PersistentMemo {
    /// Creates a new scalar group for testing purposes.
    #[cfg(test)]
    pub async fn new_scalar_group_for_test(&self) -> anyhow::Result<ScalarGroupId> {
        let mut txn = self.storage.begin().await?;
        let scalar_group_id = txn.new_scalar_group_id().await?;
        sqlx::query(
            "INSERT INTO scalar_groups (id, representative_group_id, exploration_status) VALUES ($1, $2, $3)",
        )
        .bind(scalar_group_id)
        .bind(scalar_group_id)
        .bind(ExplorationStatus::Unexplored)
        .execute( &mut *txn)
        .await?;
        txn.commit().await?;
        Ok(scalar_group_id)
    }

    /// Create a new persistent memo table.
    pub async fn new(storage: StorageManager) -> anyhow::Result<Self> {
        let get_all_logical_exprs_in_group_query = get_all_logical_exprs_in_group_query();
        storage
            .db()
            .await?
            .prepare(&get_all_logical_exprs_in_group_query)
            .await?;
        Ok(Self {
            storage,
            get_all_logical_exprs_in_group_query,
        })
    }

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
        let inserted_group_id = match logical_expr {
            LogicalExpression::Scan(scan) => {
                Self::insert_into_logical_expressions(
                    &mut txn,
                    logical_expr_id,
                    group_id,
                    LogicalOperatorKind::Scan,
                )
                .await?;
                let group_id: GroupId = sqlx::query_scalar("INSERT INTO scans (logical_expression_id, group_id, table_name, predicate_group_id) VALUES ($1, $2, $3, $4) ON CONFLICT DO UPDATE SET group_id = group_id RETURNING group_id")
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
                let group_id: GroupId = sqlx::query_scalar("INSERT INTO filters (logical_expression_id, group_id, child_group_id, predicate_group_id) VALUES ($1, $2, $3, $4) ON CONFLICT DO UPDATE SET group_id = group_id RETURNING group_id")
                    .bind(logical_expr_id)
                    .bind(group_id)
                    .bind(filter.child)
                    .bind(filter.predicate)
                    .fetch_one(&mut *txn)
                    .await?;
                group_id
            }
            LogicalExpression::Project(_project) => unimplemented!(),
            LogicalExpression::Join(join) => {
                Self::insert_into_logical_expressions(
                    &mut txn,
                    logical_expr_id,
                    group_id,
                    LogicalOperatorKind::Join,
                )
                .await?;
                let group_id: GroupId = sqlx::query_scalar("INSERT INTO joins (logical_expression_id, group_id, join_type, left_group_id, right_group_id, condition_group_id) VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT DO UPDATE SET group_id = group_id RETURNING group_id")
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

        // Consider three cases:
        // 1. inserted group id is the same as the original representative group id.
        //   - This means the logical expression is new to the group.

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
}

/// The SQL query to get all logical expressions in a group.
///
/// The query gets all data fields in each operator and converts them to JSON objects, which are later deserialized into `LogicalExpression` objects.
fn get_all_logical_exprs_in_group_query() -> String {
    [
        "SELECT json_object('Scan', json_object('table_name', table_name, 'predicate', predicate_group_id)) as data FROM scans WHERE group_id = $1",
        "SELECT json_object('Filter', json_object('child', child_group_id, 'predicate', predicate_group_id)) as data FROM filters WHERE group_id = $1",
        "SELECT json_object('Join', json_object('join_type', join_type, 'left', left_group_id, 'right', right_group_id, 'condition', condition_group_id)) as data FROM joins WHERE group_id = $1"
    ].join(" UNION ALL ")
}

#[cfg(test)]
mod tests {
    use crate::operator::relational::{logical::join::JoinType, RelationChildren};

    use super::*;

    #[tokio::test]
    async fn test_insert_logical_expr_with_memo() -> anyhow::Result<()> {
        let storage = StorageManager::new_in_memory().await?;
        storage.migrate().await?;
        let memo = PersistentMemo::new(storage).await?;

        let predicate_group_id = memo.new_scalar_group_for_test().await?;
        let scan1 = LogicalExpression::scan("t1", predicate_group_id);
        let scan1_group = memo.add_logical_expr(&scan1).await?;
        let dup_scan1_group = memo.add_logical_expr(&scan1).await?;
        assert_eq!(scan1_group, dup_scan1_group);
        let status = memo.get_group_exploration_status(scan1_group).await?;
        assert_eq!(status, ExplorationStatus::Unexplored);

        let predicate_group_id = memo.new_scalar_group_for_test().await?;
        let scan2 = LogicalExpression::scan("t2", predicate_group_id);
        let scan2_group = memo.add_logical_expr(&scan2).await?;
        let dup_scan2_group = memo.add_logical_expr(&scan2).await?;
        assert_eq!(scan2_group, dup_scan2_group);
        let status = memo.get_group_exploration_status(scan1_group).await?;
        assert_eq!(status, ExplorationStatus::Unexplored);

        let join_cond_group_id = memo.new_scalar_group_for_test().await?;
        let join = LogicalExpression::join(
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

        let join_alt = LogicalExpression::join(
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
        let storage = StorageManager::new("sqlite://memo.db?mode=rwc").await?;
        storage.migrate().await?;
        let memo = PersistentMemo::new(storage).await?;

        let true_predicate_group = memo.new_scalar_group_for_test().await?;
        let scan = LogicalExpression::scan("t1", true_predicate_group);
        let scan_group = memo.add_logical_expr(&scan).await?;

        let filter = LogicalExpression::filter(scan_group, true_predicate_group);
        let filter_group = memo.add_logical_expr(&filter).await?;
        let one_equal_one_predicate_group = memo.new_scalar_group_for_test().await?;
        let top_filter = LogicalExpression::filter(filter_group, one_equal_one_predicate_group);
        let top_filter_group = memo.add_logical_expr(&top_filter).await?;
        let top_filter_2 = LogicalExpression::filter(scan_group, one_equal_one_predicate_group);
        let top_filter_2_group = memo
            .add_logical_expr_to_group(&top_filter_2, top_filter_group)
            .await?;
        assert_eq!(top_filter_group, top_filter_2_group);
        let new_group = memo
            .add_logical_expr_to_group(&scan, top_filter_group)
            .await?;

        println!("new group: {new_group:?}");

        // Filter: true
        // - Scan: t1
        Ok(())
    }
}
