//! Shared helpers for connector unit tests.

use sea_orm::{Database, DatabaseConnection};
use tempfile::TempDir;

/// Opens an isolated file-backed SQLite database in a temporary directory.
///
/// Keep the returned directory alive until the connection and its clones are no longer used.
/// Schema migrations are left to the caller.
pub(crate) async fn test_sqlite_connection() -> (TempDir, DatabaseConnection) {
    let directory = tempfile::tempdir().expect("create SQLite test directory");
    let path = directory.path().join("catalog.sqlite");
    let db = Database::connect(format!("sqlite://{}?mode=rwc", path.display()))
        .await
        .expect("connect to test SQLite database");
    (directory, db)
}

#[cfg(test)]
mod tests {
    use sea_orm::ConnectionTrait;

    use super::test_sqlite_connection;

    #[tokio::test]
    async fn sqlite_connections_have_isolated_temporary_databases() {
        let (first_directory, first) = test_sqlite_connection().await;
        let (second_directory, second) = test_sqlite_connection().await;
        assert_ne!(first_directory.path(), second_directory.path());
        for (directory, db) in [(&first_directory, &first), (&second_directory, &second)] {
            assert!(directory.path().join("catalog.sqlite").exists());
            db.execute_unprepared("CREATE TABLE marker (id INTEGER)")
                .await
                .unwrap();
        }
        first.close().await.unwrap();
        second.close().await.unwrap();
    }
}
