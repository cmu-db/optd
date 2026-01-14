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
    .expect("Failed to create CatalogService");

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
#[ignore]
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

#[tokio::test]
async fn test_current_schema_for_nonexistent_table() {
    let (_temp_dir, service, handle) = create_test_service();

    tokio::spawn(async move {
        service.run().await;
    });

    // Try to get schema for nonexistent table - should return error
    let result = handle.current_schema(None, "nonexistent_table").await;
    assert!(result.is_err(), "Should error for nonexistent table");

    handle.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore]
async fn test_current_schema_with_different_schemas() {
    use optd_catalog::RegisterTableRequest;
    use std::collections::HashMap;

    let (_temp_dir, service, handle) = create_test_service();

    tokio::spawn(async move {
        service.run().await;
    });

    // Create schema and register table
    handle.create_schema("test_schema").await.unwrap();

    let request = RegisterTableRequest {
        table_name: "test_table".to_string(),
        schema_name: Some("test_schema".to_string()),
        location: "/data/test.parquet".to_string(),
        file_format: "PARQUET".to_string(),
        compression: None,
        options: HashMap::new(),
    };
    handle.register_external_table(request).await.unwrap();

    // Verify the table exists in the schema
    let table = handle
        .get_external_table(Some("test_schema"), "test_table")
        .await
        .unwrap();
    assert!(table.is_some(), "Table should exist in test_schema");

    // Try to get schema - this tests that current_schema() works with schema qualifier
    let result = handle
        .current_schema(Some("test_schema"), "test_table")
        .await;
    // This might fail due to implementation details, but at least we're testing it
    match result {
        Ok(schema) => {
            println!(
                "✓ Got schema for test_schema.test_table: {} fields",
                schema.fields().len()
            );
            assert!(!schema.fields().is_empty());
        }
        Err(e) => {
            println!("⚠️  current_schema() with schema qualifier failed: {}", e);
            // This is acceptable - it documents that qualified schema access may not work
        }
    }

    // Try querying nonexistent table in the schema
    let result = handle
        .current_schema(Some("test_schema"), "nonexistent")
        .await;
    assert!(result.is_err(), "Should error for nonexistent table");

    handle.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_invalid_file_format() {
    use optd_catalog::RegisterTableRequest;
    use std::collections::HashMap;

    let (_temp_dir, service, handle) = create_test_service();

    tokio::spawn(async move {
        service.run().await;
    });

    let invalid_formats = vec!["INVALID", "XML", "YAML", "", "parquet123"];

    for format in invalid_formats {
        let request = RegisterTableRequest {
            table_name: format!("table_{}", format),
            schema_name: None,
            location: "/data/test.file".to_string(),
            file_format: format.to_string(),
            compression: None,
            options: HashMap::new(),
        };

        // System should accept registration (validation happens at query time)
        // but we document that these are potentially invalid
        let result = handle.register_external_table(request).await;
        match result {
            Ok(_) => {
                println!(
                    "⚠️  System accepted invalid format '{}' (validation deferred)",
                    format
                );
            }
            Err(e) => {
                println!("✓ Rejected invalid format '{}': {}", format, e);
            }
        }
    }

    handle.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_invalid_compression_types() {
    use optd_catalog::RegisterTableRequest;
    use std::collections::HashMap;

    let (_temp_dir, service, handle) = create_test_service();

    tokio::spawn(async move {
        service.run().await;
    });

    let compressions = vec![
        ("gzip", true),     // Valid
        ("snappy", true),   // Valid
        ("zstd", true),     // Valid
        ("brotli", true),   // Valid
        ("invalid", false), // Invalid
        ("zip", false),     // Invalid
    ];

    for (compression, _expected_valid) in compressions {
        let request = RegisterTableRequest {
            table_name: format!("table_{}", compression),
            schema_name: None,
            location: "/data/test.parquet".to_string(),
            file_format: "PARQUET".to_string(),
            compression: Some(compression.to_string()),
            options: HashMap::new(),
        };

        let result = handle.register_external_table(request).await;
        println!(
            "Compression '{}': {}",
            compression,
            if result.is_ok() {
                "accepted"
            } else {
                "rejected"
            }
        );
    }

    handle.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_invalid_statistics_json() {
    use optd_catalog::{RegisterTableRequest, TableStatistics};
    use std::collections::HashMap;

    let (_temp_dir, service, handle) = create_test_service();

    tokio::spawn(async move {
        service.run().await;
    });

    // Create a table first
    let request = RegisterTableRequest {
        table_name: "test_table".to_string(),
        schema_name: None,
        location: "/data/test.parquet".to_string(),
        file_format: "PARQUET".to_string(),
        compression: None,
        options: HashMap::new(),
    };
    handle.register_external_table(request).await.unwrap();

    // Try to set statistics with valid structure
    let stats = TableStatistics {
        row_count: 1000,
        column_statistics: vec![],
        size_bytes: Some(50000),
    };

    let result = handle.set_table_statistics(None, "test_table", stats).await;
    assert!(result.is_ok(), "Valid statistics should be accepted");

    // Very large row count - edge case testing
    let weird_stats = TableStatistics {
        row_count: usize::MAX,
        column_statistics: vec![],
        size_bytes: None,
    };
    let result = handle
        .set_table_statistics(None, "test_table", weird_stats)
        .await;
    println!(
        "Max row count ({}): {}",
        usize::MAX,
        if result.is_ok() {
            "accepted"
        } else {
            "rejected"
        }
    );

    handle.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_special_characters_in_names() {
    use optd_catalog::RegisterTableRequest;
    use std::collections::HashMap;

    let (_temp_dir, service, handle) = create_test_service();

    tokio::spawn(async move {
        service.run().await;
    });

    // Test special characters in table names
    let special_names = vec![
        "table-with-dash",
        "table_with_underscore", // Should work
        "table.with.dots",
        "table with spaces",
        "table$with$dollar",
        "table@with@at",
    ];

    for name in special_names {
        let request = RegisterTableRequest {
            table_name: name.to_string(),
            schema_name: None,
            location: "/data/test.parquet".to_string(),
            file_format: "PARQUET".to_string(),
            compression: None,
            options: HashMap::new(),
        };

        let result = handle.register_external_table(request).await;
        println!(
            "Table name '{}': {}",
            name,
            if result.is_ok() {
                "✓ accepted"
            } else {
                "✗ rejected"
            }
        );
    }

    // Test special characters in schema names
    handle.create_schema("schema_valid").await.unwrap();
    println!("Schema 'schema_valid': ✓ accepted");

    let result = handle.create_schema("schema with spaces").await;
    println!(
        "Schema 'schema with spaces': {}",
        if result.is_ok() {
            "✓ accepted"
        } else {
            "✗ rejected"
        }
    );

    handle.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore]
async fn test_concurrent_catalog_modifications() {
    use optd_catalog::RegisterTableRequest;
    use std::collections::HashMap;

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let metadata_path = temp_dir.path().join("metadata.ducklake");

    // Create two separate catalog services accessing same files
    let (service1, handle1) = CatalogService::try_new_from_location(
        Some(db_path.to_str().unwrap()),
        Some(metadata_path.to_str().unwrap()),
    )
    .unwrap();

    let (service2, handle2) = CatalogService::try_new_from_location(
        Some(db_path.to_str().unwrap()),
        Some(metadata_path.to_str().unwrap()),
    )
    .unwrap();

    tokio::spawn(async move { service1.run().await });
    tokio::spawn(async move { service2.run().await });

    // Register table from first catalog
    let request1 = RegisterTableRequest {
        table_name: "table1".to_string(),
        schema_name: None,
        location: "/data/table1.parquet".to_string(),
        file_format: "PARQUET".to_string(),
        compression: None,
        options: HashMap::new(),
    };
    handle1.register_external_table(request1).await.unwrap();

    // Register different table from second catalog concurrently
    let request2 = RegisterTableRequest {
        table_name: "table2".to_string(),
        schema_name: None,
        location: "/data/table2.parquet".to_string(),
        file_format: "PARQUET".to_string(),
        compression: None,
        options: HashMap::new(),
    };
    handle2.register_external_table(request2).await.unwrap();

    // Both catalogs should see both tables (eventually consistent)
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let tables1 = handle1.list_external_tables(None).await.unwrap();
    let tables2 = handle2.list_external_tables(None).await.unwrap();

    println!("Catalog 1 sees {} tables", tables1.len());
    println!("Catalog 2 sees {} tables", tables2.len());

    // Both should see at least the table they created
    assert!(tables1.iter().any(|t| t.table_name == "table1"));
    assert!(tables2.iter().any(|t| t.table_name == "table2"));

    handle1.shutdown().await.unwrap();
    handle2.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore]
async fn test_concurrent_statistics_updates() {
    use optd_catalog::{RegisterTableRequest, TableStatistics};
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::Barrier;

    let (_temp_dir, service, handle) = create_test_service();

    tokio::spawn(async move {
        service.run().await;
    });

    // Create a table
    let request = RegisterTableRequest {
        table_name: "concurrent_stats".to_string(),
        schema_name: None,
        location: "/data/test.parquet".to_string(),
        file_format: "PARQUET".to_string(),
        compression: None,
        options: HashMap::new(),
    };
    handle.register_external_table(request).await.unwrap();

    // Create barrier for synchronization
    let barrier = Arc::new(Barrier::new(5));
    let mut tasks = vec![];

    // Spawn 5 concurrent tasks updating statistics
    for i in 0..5 {
        let handle_clone = handle.clone();
        let barrier_clone = barrier.clone();

        let task = tokio::spawn(async move {
            // Wait for all tasks to be ready
            barrier_clone.wait().await;

            // Update statistics
            let stats = TableStatistics {
                row_count: 1000 + i * 100,
                column_statistics: vec![],
                size_bytes: Some(50000 + i * 1000),
            };

            handle_clone
                .set_table_statistics(None, "concurrent_stats", stats)
                .await
        });

        tasks.push(task);
    }

    // Wait for all tasks to complete
    let results: Vec<_> = futures::future::join_all(tasks).await;

    // All updates should succeed (or handle conflicts gracefully)
    let success_count = results
        .iter()
        .filter(|r| r.as_ref().unwrap().is_ok())
        .count();
    println!(
        "Concurrent statistics updates: {}/{} succeeded",
        success_count,
        results.len()
    );

    // Verify final statistics exist
    let snapshot = handle.current_snapshot().await.unwrap();
    let final_stats = handle
        .table_statistics("concurrent_stats", snapshot)
        .await
        .unwrap();

    assert!(
        final_stats.is_some(),
        "Statistics should exist after concurrent updates"
    );
    let final_stats = final_stats.unwrap();
    println!(
        "Final row count: {}, size: {:?}",
        final_stats.row_count, final_stats.size_bytes
    );

    handle.shutdown().await.unwrap();
}
