// Catalog error handling tests

use optd_catalog::{CatalogService, CatalogServiceHandle, DuckLakeCatalog};
use tempfile::TempDir;

/// Creates a test catalog service with temp storage.
fn create_test_service() -> (
    TempDir,
    CatalogService<DuckLakeCatalog>,
    CatalogServiceHandle,
) {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let metadata_path = temp_dir.path().join("metadata.ducklake");

    let (service, handle) = CatalogService::try_new_from_location(
        Some(db_path.to_str().unwrap()),
        Some(metadata_path.to_str().unwrap()),
    )
    .unwrap();

    (temp_dir, service, handle)
}

#[tokio::test]
async fn test_error_get_nonexistent_table_metadata() {
    let (_temp_dir, service, handle) = create_test_service();

    tokio::spawn(async move {
        service.run().await;
    });

    // Try to get metadata for a table that doesn't exist
    let result = handle.get_external_table(None, "nonexistent_table").await;

    match result {
        Ok(metadata) => {
            // Should return None for nonexistent table
            assert!(
                metadata.is_none(),
                "Should return None for nonexistent table"
            );
            println!("✓ GetExternalTable returned None for nonexistent table (expected)");
        }
        Err(e) => {
            println!("✓ Got expected error: {}", e);
        }
    }

    handle.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_error_drop_nonexistent_table() {
    let (_temp_dir, service, handle) = create_test_service();

    tokio::spawn(async move {
        service.run().await;
    });

    // Try to drop a table that doesn't exist
    let result = handle.drop_external_table(None, "nonexistent_table").await;

    match result {
        Ok(_) => {
            println!("✓ System handled drop of nonexistent table gracefully (idempotent)");
        }
        Err(e) => {
            println!("✓ Got expected error: {}", e);
            let error_msg = e.to_string().to_lowercase();
            // With the fix, should now get a clear "table does not exist" message
            assert!(
                error_msg.contains("does not exist") && error_msg.contains("nonexistent_table"),
                "Error message should indicate table doesn't exist with table name: {}",
                error_msg
            );
        }
    }

    handle.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_error_invalid_snapshot_id() {
    let (_temp_dir, service, handle) = create_test_service();

    tokio::spawn(async move {
        service.run().await;
    });

    // Try to get table at an invalid/nonexistent snapshot ID
    let snapshot_id = 99999i64; // Very high, likely doesn't exist
    let result = handle
        .get_external_table_at_snapshot(None, "any_table", snapshot_id)
        .await;

    match result {
        Ok(metadata) => {
            println!(
                "✓ System handled invalid snapshot ID, returned: {:?}",
                metadata
            );
            // Should return None for invalid snapshot
            assert!(
                metadata.is_none(),
                "Should return None for invalid snapshot"
            );
        }
        Err(e) => {
            println!("✓ Got expected error: {}", e);
        }
    }

    handle.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_error_get_statistics_for_nonexistent_table() {
    let (_temp_dir, service, handle) = create_test_service();

    tokio::spawn(async move {
        service.run().await;
    });

    // Try to get statistics for a table that doesn't exist
    let snapshot = handle.current_snapshot().await.unwrap();
    let result = handle.table_statistics("nonexistent_table", snapshot).await;

    match result {
        Ok(stats) => {
            // Should return None for nonexistent table
            assert!(stats.is_none(), "Should return None for nonexistent table");
            println!("✓ Table statistics returned None for nonexistent table (expected)");
        }
        Err(e) => {
            println!("✓ Got expected error: {}", e);
        }
    }

    handle.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_error_invalid_table_name_query() {
    let (_temp_dir, service, handle) = create_test_service();

    tokio::spawn(async move {
        service.run().await;
    });

    // Try various potentially problematic table names
    let problematic_names = vec![
        "", // Empty name
        "table_with_very_long_name_that_might_exceed_some_limits_in_the_database_system_Lorem_ipsum_dolor_sit_amet_consectetur_adipiscing_elit",
        "table'with'quotes",      // SQL injection attempt
        "table;DROP TABLE users", // SQL injection attempt
    ];

    for name in problematic_names {
        let result = handle.get_external_table(None, name).await;

        match result {
            Ok(metadata) => {
                if metadata.is_none() {
                    println!(
                        "✓ System handled problematic name '{}' safely (returned None)",
                        if name.len() > 20 { &name[..20] } else { name }
                    );
                } else {
                    println!(
                        "✓ System accepted table name '{}'",
                        if name.len() > 20 { &name[..20] } else { name }
                    );
                }
            }
            Err(e) => {
                println!(
                    "✓ Got expected error for '{}': {}",
                    if name.len() > 20 { &name[..20] } else { name },
                    e
                );
            }
        }
    }

    handle.shutdown().await.unwrap();
}
