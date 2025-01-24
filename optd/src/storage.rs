use diesel::prelude::*;
use diesel_migrations::{embed_migrations, EmbeddedMigrations};
use models::{
    logical_expr::{
        LogicalExpr, LogicalExprId, LogicalExprRecord, LogicalExprWithId, LogicalOperatorStorage,
    },
    logical_operators::{LogicalFilter, LogicalJoin, LogicalOpKindId, LogicalScan},
    rel_group::RelGroupId,
};

pub mod models;
pub mod schema;

const MIGRATIONS: EmbeddedMigrations = embed_migrations!("./migrations");

/// The storage manager implements CRUD operation for persisting the optimizer state.
pub struct StorageManager {
    /// Connection to the SQLite database.
    pub conn: SqliteConnection,
}

impl StorageManager {
    /// Execute the diesel migrations that are built into this binary
    pub fn migration_run(&mut self) -> anyhow::Result<()> {
        use diesel_migrations::{HarnessWithOutput, MigrationHarness};

        HarnessWithOutput::write_to_stdout(&mut self.conn)
            .run_pending_migrations(MIGRATIONS)
            .map_err(|e| anyhow::anyhow!(e))?;
        Ok(())
    }
    /// Create a new `StorageManager` instance.
    pub fn new(database_url: &str) -> anyhow::Result<Self> {
        let connection = SqliteConnection::establish(database_url)?;
        Ok(Self { conn: connection })
    }

    pub fn new_in_memory() -> anyhow::Result<Self> {
        let connection = SqliteConnection::establish(":memory:")?;
        Ok(Self { conn: connection })
    }

    pub fn add_logical_expr(&mut self, logical_expr: LogicalExpr) -> (LogicalExprId, RelGroupId) {
        match logical_expr {
            LogicalExpr::Scan(logical_scan) => logical_scan.add(self),
            LogicalExpr::Filter(logical_filter) => logical_filter.add(self),
            LogicalExpr::Join(logical_join) => logical_join.add(self),
        }
    }

    pub fn add_logical_expr_to_group(
        &mut self,
        logical_expr: LogicalExpr,
        rel_group_id: RelGroupId,
    ) -> LogicalExprId {
        match logical_expr {
            LogicalExpr::Scan(logical_scan) => logical_scan.add_to_group(rel_group_id, self),
            LogicalExpr::Filter(logical_filter) => logical_filter.add_to_group(rel_group_id, self),
            LogicalExpr::Join(logical_join) => logical_join.add_to_group(rel_group_id, self),
        }
    }

    pub fn get_logical_expr_identifiers(
        &mut self,
        logical_expr: &LogicalExpr,
    ) -> Option<(LogicalExprId, RelGroupId)> {
        match logical_expr {
            LogicalExpr::Scan(logical_scan) => logical_scan.get_identifiers(self),
            LogicalExpr::Filter(logical_filter) => logical_filter.get_identifiers(self),
            LogicalExpr::Join(logical_join) => logical_join.get_identifiers(self),
        }
    }

    /// Get all equivalent logical expressions in a relational group.
    pub fn get_all_logical_exprs_in_group(
        &mut self,
        rel_group_id: RelGroupId,
    ) -> Vec<LogicalExprWithId> {
        use schema::logical_exprs::dsl::*;

        // Get all the logical expression records in the group.
        let records = logical_exprs
            .inner_join(schema::logical_op_kinds::dsl::logical_op_kinds)
            .filter(group_id.eq(rel_group_id))
            .order_by(id)
            .select((
                LogicalExprRecord::as_select(),
                schema::logical_op_kinds::dsl::name,
            ))
            .load::<(LogicalExprRecord, String)>(&mut self.conn)
            .unwrap();

        let mut exprs = Vec::with_capacity(records.len());

        // For each record, fit it into a type-safe ogical expression
        // based on its operator kind.
        for (record, name) in records {
            // TODO(yuchen): there is a better way to do this.
            let expr = match name {
                name if name == LogicalScan::op_name() => {
                    LogicalExpr::Scan(LogicalScan::get(record.id, self))
                }
                name if name == LogicalFilter::op_name() => {
                    LogicalExpr::Filter(LogicalFilter::get(record.id, self))
                }
                name if name == LogicalJoin::op_name() => {
                    LogicalExpr::Join(LogicalJoin::get(record.id, self))
                }
                _ => unreachable!(),
            };
            exprs.push(LogicalExprWithId {
                id: record.id,
                inner: expr,
            });
        }

        exprs
    }

    /// Gets the group id of a logical expression.
    fn rel_group_of_logical_expr(&mut self, logical_expr: LogicalExprId) -> RelGroupId {
        use schema::logical_exprs::dsl::*;

        logical_exprs
            .filter(id.eq(logical_expr))
            .select(group_id)
            .first(&mut self.conn)
            .expect("Invalid database state: logical expression must belongs to a group")
    }

    /// Creates a new relational group and returns the group id.
    fn create_rel_group(&mut self) -> RelGroupId {
        use schema::rel_groups::dsl::*;
        diesel::insert_into(rel_groups)
            .default_values()
            .returning(id)
            .get_result(&mut self.conn)
            .expect("Failed to create a new relational group")
    }

    /// Adds a logical expression record to the logical expression base table.
    /// Returns the newly generated logical expression id.
    fn add_logical_expr_record(
        &mut self,
        logical_op_kind_id: LogicalOpKindId,
        group_id: RelGroupId,
    ) -> LogicalExprId {
        use schema::logical_exprs;
        diesel::insert_into(logical_exprs::table)
            .values((
                logical_exprs::logical_op_kind_id.eq(logical_op_kind_id),
                logical_exprs::group_id.eq(group_id),
            ))
            .returning(logical_exprs::id)
            .get_result(&mut self.conn)
            .expect("Failed to insert a new logical expression record")
    }
}
