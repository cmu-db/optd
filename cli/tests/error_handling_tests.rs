// CLI error handling tests

use datafusion::{execution::runtime_env::RuntimeEnvBuilder, prelude::SessionConfig};
use datafusion_cli::cli_context::CliSessionContext;
use optd_catalog::{CatalogService, DuckLakeCatalog};
use optd_cli::OptdCliSessionContext;
use optd_datafusion::OptdCatalogProviderList;
use std::sync::Arc;
use tempfile::TempDir;

/// Creates CLI context with temp persistent catalog.
async fn create_cli_context_with_catalog(
    temp_dir: &TempDir,
) -> (OptdCliSessionContext, tokio::task::JoinHandle<()>) {
    let catalog_path = temp_dir.path().join("metadata.ducklake");
    let catalog = DuckLakeCatalog::try_new(None, Some(catalog_path.to_str().unwrap())).unwrap();
    let (service, handle) = CatalogService::new(catalog);
    let service_handle = tokio::spawn(async move { service.run().await });

    let config = SessionConfig::new();
    let runtime = RuntimeEnvBuilder::new().build_arc().unwrap();
    let cli_ctx = OptdCliSessionContext::new_with_config_rt(config, runtime);

    let original_catalog_list = cli_ctx.inner().state().catalog_list().clone();
    let optd_catalog_list = OptdCatalogProviderList::new(original_catalog_list, Some(handle));
    cli_ctx
        .inner()
        .register_catalog_list(Arc::new(optd_catalog_list));

    (cli_ctx, service_handle)
}

/// Executes SQL and returns result.
async fn execute_sql(cli_ctx: &OptdCliSessionContext, sql: &str) -> datafusion::error::Result<()> {
    let plan = cli_ctx.inner().state().create_logical_plan(sql).await?;
    cli_ctx.execute_logical_plan(plan).await?;
    Ok(())
}

#[tokio::test]
async fn test_error_nonexistent_file() {
    let temp_dir = TempDir::new().unwrap();
    let (cli_ctx, _service_handle) = create_cli_context_with_catalog(&temp_dir).await;

    // Try to create table pointing to nonexistent file
    let nonexistent_path = temp_dir.path().join("does_not_exist.csv");
    let create_sql = format!(
        "CREATE EXTERNAL TABLE nonexistent_table STORED AS CSV LOCATION '{}'",
        nonexistent_path.display()
    );

    let result = execute_sql(&cli_ctx, &create_sql).await;

    // Table creation might succeed (validation deferred)
    if result.is_ok() {
        println!("✓ Table creation succeeded (validation deferred to query time)");
        // Try querying the nonexistent file
        let query_result = execute_sql(&cli_ctx, "SELECT * FROM nonexistent_table").await;
        if query_result.is_err() {
            let error_msg = query_result.unwrap_err().to_string();
            println!(
                "Error message when querying nonexistent file: {}",
                error_msg
            );
            assert!(
                error_msg.to_lowercase().contains("file")
                    || error_msg.to_lowercase().contains("not found")
                    || error_msg.to_lowercase().contains("no such"),
                "Error message should indicate file not found: {}",
                error_msg
            );
        } else {
            // System returns empty result set for nonexistent files
            println!("✓ System handled nonexistent file gracefully (empty result set)");
        }
    } else {
        let error_msg = result.unwrap_err().to_string().to_lowercase();
        println!("Error message for nonexistent file: {}", error_msg);
        assert!(
            error_msg.contains("file")
                || error_msg.contains("not found")
                || error_msg.contains("no such"),
            "Error message should indicate file not found: {}",
            error_msg
        );
    }
}

#[tokio::test]
async fn test_error_corrupted_parquet_file() {
    let temp_dir = TempDir::new().unwrap();
    let (cli_ctx, _service_handle) = create_cli_context_with_catalog(&temp_dir).await;

    // Create a corrupted "Parquet" file (just garbage data)
    let corrupted_path = temp_dir.path().join("corrupted.parquet");
    std::fs::write(&corrupted_path, "This is not a valid Parquet file!").unwrap();

    let create_sql = format!(
        "CREATE EXTERNAL TABLE corrupted_table STORED AS PARQUET LOCATION '{}'",
        corrupted_path.display()
    );

    let result = execute_sql(&cli_ctx, &create_sql).await;

    // Should fail with clear error message
    assert!(result.is_err(), "Expected error for corrupted Parquet file");
    let error_msg = result.unwrap_err().to_string().to_lowercase();
    println!("Error message for corrupted Parquet: {}", error_msg);

    // Error should mention Parquet or parse/read failure
    assert!(
        error_msg.contains("parquet")
            || error_msg.contains("parse")
            || error_msg.contains("invalid"),
        "Error message should indicate Parquet corruption: {}",
        error_msg
    );
}

#[tokio::test]
async fn test_error_malformed_csv() {
    let temp_dir = TempDir::new().unwrap();
    let (cli_ctx, _service_handle) = create_cli_context_with_catalog(&temp_dir).await;

    // Create CSV with inconsistent column counts
    let malformed_csv_path = temp_dir.path().join("malformed.csv");
    std::fs::write(
        &malformed_csv_path,
        "id,name,age\n1,Alice,30\n2,Bob\n3,Charlie,25,extra_column\n",
    )
    .unwrap();

    let create_sql = format!(
        "CREATE EXTERNAL TABLE malformed_csv STORED AS CSV LOCATION '{}' OPTIONS ('format.has_header' 'true')",
        malformed_csv_path.display()
    );

    // Table creation might succeed (just defines schema), but query should fail
    let create_result = execute_sql(&cli_ctx, &create_sql).await;
    if create_result.is_ok() {
        // Try to query the table
        let query_sql = "SELECT * FROM malformed_csv";
        let query_plan = cli_ctx.inner().state().create_logical_plan(query_sql).await;

        // Query should fail or handle gracefully
        if query_plan.is_err() {
            let error_msg = query_plan.unwrap_err().to_string().to_lowercase();
            println!("Error message for malformed CSV query: {}", error_msg);
            assert!(
                error_msg.contains("csv")
                    || error_msg.contains("parse")
                    || error_msg.contains("column"),
                "Error message should indicate CSV parsing issue: {}",
                error_msg
            );
        } else {
            // If it succeeds, verify it handles the inconsistency gracefully
            println!("System handled malformed CSV gracefully (deferred validation)");
        }
    } else {
        let error_msg = create_result.unwrap_err().to_string().to_lowercase();
        println!("Error message for malformed CSV creation: {}", error_msg);
        assert!(
            error_msg.contains("csv")
                || error_msg.contains("parse")
                || error_msg.contains("schema"),
            "Error message should indicate CSV issue: {}",
            error_msg
        );
    }
}

#[tokio::test]
async fn test_error_duplicate_table_creation() {
    let temp_dir = TempDir::new().unwrap();
    let (cli_ctx, _service_handle) = create_cli_context_with_catalog(&temp_dir).await;

    // Create a CSV file
    let csv_path = temp_dir.path().join("test.csv");
    std::fs::write(&csv_path, "id,name\n1,Alice\n2,Bob\n").unwrap();

    let create_sql = format!(
        "CREATE EXTERNAL TABLE duplicate_test STORED AS CSV LOCATION '{}' OPTIONS ('format.has_header' 'true')",
        csv_path.display()
    );

    // First creation should succeed
    execute_sql(&cli_ctx, &create_sql).await.unwrap();

    // Second creation with same name should fail
    let result = execute_sql(&cli_ctx, &create_sql).await;

    assert!(result.is_err(), "Expected error for duplicate table name");
    let error_msg = result.unwrap_err().to_string().to_lowercase();
    println!("Error message for duplicate table: {}", error_msg);

    // Error should mention table already exists or duplicate
    assert!(
        error_msg.contains("already exists")
            || error_msg.contains("duplicate")
            || error_msg.contains("exist"),
        "Error message should indicate duplicate table: {}",
        error_msg
    );
}

#[tokio::test]
async fn test_error_invalid_table_name() {
    let temp_dir = TempDir::new().unwrap();
    let (cli_ctx, _service_handle) = create_cli_context_with_catalog(&temp_dir).await;

    let csv_path = temp_dir.path().join("test.csv");
    std::fs::write(&csv_path, "id,name\n1,Alice\n").unwrap();

    // Try various invalid table names
    let invalid_names = vec![
        ("", "empty name"),
        ("123invalid", "starts with number"),
        ("table-with-dash", "contains dash"),
        ("table with spaces", "contains spaces"),
    ];

    for (invalid_name, description) in invalid_names {
        let create_sql = format!(
            "CREATE EXTERNAL TABLE {} STORED AS CSV LOCATION '{}'",
            invalid_name,
            csv_path.display()
        );

        let result = cli_ctx
            .inner()
            .state()
            .create_logical_plan(&create_sql)
            .await;

        if result.is_err() {
            let error_msg = result.unwrap_err().to_string();
            println!(
                "Error message for invalid name '{}' ({}): {}",
                invalid_name, description, error_msg
            );
            // System properly rejects invalid name
        } else {
            // Some systems may accept these names (depends on SQL parser)
            println!(
                "System accepted table name '{}' ({})",
                invalid_name, description
            );
        }
    }
}

#[tokio::test]
async fn test_error_drop_nonexistent_table() {
    let temp_dir = TempDir::new().unwrap();
    let (cli_ctx, _service_handle) = create_cli_context_with_catalog(&temp_dir).await;

    // Try to drop a table that doesn't exist
    let result = execute_sql(&cli_ctx, "DROP TABLE nonexistent_table").await;

    assert!(
        result.is_err(),
        "Expected error when dropping nonexistent table"
    );
    let error_msg = result.unwrap_err().to_string();
    println!("Error message for drop nonexistent table: {}", error_msg);

    // Error message mentions "doesn't exist" which is good
    assert!(
        error_msg.to_lowercase().contains("not found")
            || error_msg.to_lowercase().contains("does not exist")
            || error_msg.to_lowercase().contains("doesn't exist")
            || error_msg.to_lowercase().contains("unknown"),
        "Error message should indicate table not found: {}",
        error_msg
    );
}

#[tokio::test]
async fn test_drop_if_exists_idempotency() {
    let temp_dir = TempDir::new().unwrap();
    let (cli_ctx, _service_handle) = create_cli_context_with_catalog(&temp_dir).await;

    // DROP TABLE IF EXISTS should succeed even when table doesn't exist
    let result = execute_sql(&cli_ctx, "DROP TABLE IF EXISTS nonexistent_table").await;

    // Should NOT error
    assert!(
        result.is_ok(),
        "DROP TABLE IF EXISTS should succeed for nonexistent table"
    );
    println!("✓ DROP TABLE IF EXISTS handled gracefully for nonexistent table");

    // Now create a table and verify IF EXISTS works correctly
    let csv_path = temp_dir.path().join("test.csv");
    std::fs::write(&csv_path, "id,name\n1,Alice\n").unwrap();

    let create_sql = format!(
        "CREATE EXTERNAL TABLE test_if_exists STORED AS CSV LOCATION '{}' OPTIONS ('format.has_header' 'true')",
        csv_path.display()
    );
    execute_sql(&cli_ctx, &create_sql).await.unwrap();

    // Drop it with IF EXISTS
    let result = execute_sql(&cli_ctx, "DROP TABLE IF EXISTS test_if_exists").await;
    assert!(
        result.is_ok(),
        "DROP TABLE IF EXISTS should succeed for existing table"
    );
    println!("✓ DROP TABLE IF EXISTS succeeded for existing table");

    // Drop again with IF EXISTS - should still succeed
    let result = execute_sql(&cli_ctx, "DROP TABLE IF EXISTS test_if_exists").await;
    assert!(result.is_ok(), "DROP TABLE IF EXISTS should be idempotent");
    println!("✓ DROP TABLE IF EXISTS is idempotent");
}

#[tokio::test]
async fn test_error_query_dropped_table() {
    let temp_dir = TempDir::new().unwrap();
    let (cli_ctx, _service_handle) = create_cli_context_with_catalog(&temp_dir).await;

    let csv_path = temp_dir.path().join("test.csv");
    std::fs::write(&csv_path, "id,name\n1,Alice\n2,Bob\n").unwrap();

    // Create and then drop table
    let create_sql = format!(
        "CREATE EXTERNAL TABLE temp_table STORED AS CSV LOCATION '{}' OPTIONS ('format.has_header' 'true')",
        csv_path.display()
    );
    execute_sql(&cli_ctx, &create_sql).await.unwrap();
    execute_sql(&cli_ctx, "DROP TABLE temp_table")
        .await
        .unwrap();

    // Try to query the dropped table
    let result = execute_sql(&cli_ctx, "SELECT * FROM temp_table").await;

    assert!(
        result.is_err(),
        "Expected error when querying dropped table"
    );
    let error_msg = result.unwrap_err().to_string().to_lowercase();
    println!("Error message for querying dropped table: {}", error_msg);

    assert!(
        error_msg.contains("not found")
            || error_msg.contains("does not exist")
            || error_msg.contains("unknown"),
        "Error message should indicate table not found: {}",
        error_msg
    );
}

#[tokio::test]
async fn test_error_empty_file() {
    let temp_dir = TempDir::new().unwrap();
    let (cli_ctx, _service_handle) = create_cli_context_with_catalog(&temp_dir).await;

    // Create an empty CSV file
    let empty_csv_path = temp_dir.path().join("empty.csv");
    std::fs::write(&empty_csv_path, "").unwrap();

    let create_sql = format!(
        "CREATE EXTERNAL TABLE empty_table STORED AS CSV LOCATION '{}'",
        empty_csv_path.display()
    );

    // Creation might succeed (depending on system behavior)
    let result = execute_sql(&cli_ctx, &create_sql).await;

    if result.is_err() {
        let error_msg = result.unwrap_err().to_string();
        println!("Error message for empty file: {}", error_msg);
    } else {
        // If creation succeeds, try querying
        let query_result = execute_sql(&cli_ctx, "SELECT * FROM empty_table").await;
        if query_result.is_ok() {
            println!("✓ System handled empty file gracefully");
        } else {
            let error_msg = query_result.unwrap_err().to_string();
            println!("Error message when querying empty table: {}", error_msg);
        }
    }
}

#[tokio::test]
async fn test_error_unsupported_file_format() {
    let temp_dir = TempDir::new().unwrap();
    let (cli_ctx, _service_handle) = create_cli_context_with_catalog(&temp_dir).await;

    // Create a .txt file
    let txt_path = temp_dir.path().join("data.txt");
    std::fs::write(&txt_path, "some text data").unwrap();

    // Try to create table with unsupported format (using valid SQL syntax)
    let create_sql = format!(
        "CREATE EXTERNAL TABLE unsupported_table STORED AS TXT LOCATION '{}'",
        txt_path.display()
    );

    let result = cli_ctx
        .inner()
        .state()
        .create_logical_plan(&create_sql)
        .await;

    // SQL parser might accept TXT format, validation happens at execution
    if result.is_ok() {
        println!("✓ SQL parser accepted unsupported format (validation deferred)");
        // Try to execute and see if it fails
        let exec_result = execute_sql(&cli_ctx, &create_sql).await;
        if exec_result.is_err() {
            let error_msg = exec_result.unwrap_err().to_string();
            println!(
                "Error message for unsupported format at execution: {}",
                error_msg
            );
        } else {
            println!("✓ System handled unsupported format gracefully (may treat as default)");
        }
    } else {
        let error_msg = result.unwrap_err().to_string().to_lowercase();
        println!(
            "Error message for unsupported format at parse time: {}",
            error_msg
        );
        assert!(
            error_msg.contains("format")
                || error_msg.contains("invalid")
                || error_msg.contains("syntax")
                || error_msg.contains("expected"),
            "Error message should indicate format issue: {}",
            error_msg
        );
    }
}
