use datafusion::arrow::array::{Array, Int64Array};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::*;
use datafusion_cli::cli_context::CliSessionContext;
use optd_catalog::{CatalogService, DuckLakeCatalog};
use optd_cli::OptdCliSessionContext;
use optd_datafusion::OptdCatalogProviderList;
use std::sync::Arc;
use tempfile::TempDir;

#[tokio::test(flavor = "multi_thread")]
async fn test_cli_cross_session_persistence() {
    let temp_dir = TempDir::new().unwrap();
    let metadata_path = temp_dir.path().join("metadata.ducklake");
    let csv_path = temp_dir.path().join("persistent.csv");
    std::fs::write(&csv_path, "id,value\n1,alpha\n2,beta\n").unwrap();

    // Session 1: Create table
    {
        let catalog =
            DuckLakeCatalog::try_new(None, Some(metadata_path.to_str().unwrap())).unwrap();
        let (service, handle) = CatalogService::new(catalog);
        let service_handle = tokio::spawn(async move { service.run().await });

        let config = SessionConfig::new();
        let runtime = RuntimeEnvBuilder::new().build_arc().unwrap();
        let cli_ctx = OptdCliSessionContext::new_with_config_rt(config, runtime);

        let original_catalog_list = cli_ctx.inner().state().catalog_list().clone();
        let optd_catalog_list =
            OptdCatalogProviderList::new(original_catalog_list, Some(handle.clone()));
        cli_ctx
            .inner()
            .register_catalog_list(Arc::new(optd_catalog_list));

        // Create external table
        let create_sql = format!(
            "CREATE EXTERNAL TABLE persistent STORED AS CSV LOCATION '{}'",
            csv_path.display()
        );
        let logical_plan = cli_ctx
            .inner()
            .state()
            .create_logical_plan(&create_sql)
            .await
            .unwrap();
        cli_ctx.execute_logical_plan(logical_plan).await.unwrap();

        // Verify it works in session 1
        let df = cli_ctx
            .inner()
            .sql("SELECT * FROM persistent")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        assert_eq!(batches[0].num_rows(), 2);

        // Shutdown
        handle.shutdown().await.unwrap();
        service_handle.await.unwrap();
    }

    // Session 2: Query the same table (new CLI context, same catalog file)
    {
        let catalog =
            DuckLakeCatalog::try_new(None, Some(metadata_path.to_str().unwrap())).unwrap();
        let (service, handle) = CatalogService::new(catalog);
        let _service_handle = tokio::spawn(async move { service.run().await });

        let config = SessionConfig::new();
        let runtime = RuntimeEnvBuilder::new().build_arc().unwrap();
        let cli_ctx = OptdCliSessionContext::new_with_config_rt(config, runtime);

        let original_catalog_list = cli_ctx.inner().state().catalog_list().clone();
        let optd_catalog_list =
            OptdCatalogProviderList::new(original_catalog_list, Some(handle.clone()));
        cli_ctx
            .inner()
            .register_catalog_list(Arc::new(optd_catalog_list));

        // Query without creating - should lazy-load from catalog
        let df = cli_ctx
            .inner()
            .sql("SELECT * FROM persistent")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2, "Should load table from catalog");

        // Verify data integrity
        let id_array = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(id_array.value(0), 1);
        assert_eq!(id_array.value(1), 2);

        handle.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_cli_multiple_sessions_concurrent_read() {
    let temp_dir = TempDir::new().unwrap();
    let metadata_path = temp_dir.path().join("metadata.ducklake");
    let csv_path = temp_dir.path().join("shared.csv");
    std::fs::write(&csv_path, "id,data\n1,test1\n2,test2\n3,test3\n").unwrap();

    // Create catalog and register table
    let catalog = DuckLakeCatalog::try_new(None, Some(metadata_path.to_str().unwrap())).unwrap();
    let (service, handle) = CatalogService::new(catalog);
    let _service_handle = tokio::spawn(async move { service.run().await });

    let config = SessionConfig::new();
    let runtime = RuntimeEnvBuilder::new().build_arc().unwrap();
    let cli_ctx = OptdCliSessionContext::new_with_config_rt(config, runtime);

    let original_catalog_list = cli_ctx.inner().state().catalog_list().clone();
    let optd_catalog_list =
        OptdCatalogProviderList::new(original_catalog_list, Some(handle.clone()));
    cli_ctx
        .inner()
        .register_catalog_list(Arc::new(optd_catalog_list));

    // Create table
    let create_sql = format!(
        "CREATE EXTERNAL TABLE shared STORED AS CSV LOCATION '{}'",
        csv_path.display()
    );
    let logical_plan = cli_ctx
        .inner()
        .state()
        .create_logical_plan(&create_sql)
        .await
        .unwrap();
    cli_ctx.execute_logical_plan(logical_plan).await.unwrap();

    // Simulate multiple concurrent sessions reading
    let mut tasks = vec![];
    for i in 0..5 {
        let handle_clone = handle.clone();

        let task = tokio::spawn(async move {
            let config = SessionConfig::new();
            let runtime = RuntimeEnvBuilder::new().build_arc().unwrap();
            let session_ctx = OptdCliSessionContext::new_with_config_rt(config, runtime);

            let original_catalog_list = session_ctx.inner().state().catalog_list().clone();
            let optd_catalog_list =
                OptdCatalogProviderList::new(original_catalog_list, Some(handle_clone));
            session_ctx
                .inner()
                .register_catalog_list(Arc::new(optd_catalog_list));

            // Each session queries the shared table
            let df = session_ctx
                .inner()
                .sql("SELECT COUNT(*) as cnt FROM shared")
                .await
                .unwrap();
            let batches = df.collect().await.unwrap();
            let count = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0);

            (i, count)
        });
        tasks.push(task);
    }

    // Wait for all sessions to complete
    for task in tasks {
        let (session_id, count) = task.await.unwrap();
        assert_eq!(count, 3, "Session {} should see 3 rows", session_id);
    }

    handle.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_cli_session_isolation_after_drop() {
    let temp_dir = TempDir::new().unwrap();
    let metadata_path = temp_dir.path().join("metadata.ducklake");
    let csv_path = temp_dir.path().join("droppable.csv");
    std::fs::write(&csv_path, "id\n1\n2\n").unwrap();

    // Session 1: Create and drop table
    {
        let catalog =
            DuckLakeCatalog::try_new(None, Some(metadata_path.to_str().unwrap())).unwrap();
        let (service, handle) = CatalogService::new(catalog);
        let service_handle = tokio::spawn(async move { service.run().await });

        let config = SessionConfig::new();
        let runtime = RuntimeEnvBuilder::new().build_arc().unwrap();
        let cli_ctx = OptdCliSessionContext::new_with_config_rt(config, runtime);

        let original_catalog_list = cli_ctx.inner().state().catalog_list().clone();
        let optd_catalog_list =
            OptdCatalogProviderList::new(original_catalog_list, Some(handle.clone()));
        cli_ctx
            .inner()
            .register_catalog_list(Arc::new(optd_catalog_list));

        // Create table
        let create_sql = format!(
            "CREATE EXTERNAL TABLE droppable STORED AS CSV LOCATION '{}'",
            csv_path.display()
        );
        let logical_plan = cli_ctx
            .inner()
            .state()
            .create_logical_plan(&create_sql)
            .await
            .unwrap();
        cli_ctx.execute_logical_plan(logical_plan).await.unwrap();

        // Drop table
        let drop_sql = "DROP TABLE droppable";
        let logical_plan = cli_ctx
            .inner()
            .state()
            .create_logical_plan(drop_sql)
            .await
            .unwrap();
        cli_ctx.execute_logical_plan(logical_plan).await.unwrap();

        handle.shutdown().await.unwrap();
        service_handle.await.unwrap();
    }

    // Session 2: Verify table is dropped
    {
        let catalog =
            DuckLakeCatalog::try_new(None, Some(metadata_path.to_str().unwrap())).unwrap();
        let (service, handle) = CatalogService::new(catalog);
        let _service_handle = tokio::spawn(async move { service.run().await });

        let config = SessionConfig::new();
        let runtime = RuntimeEnvBuilder::new().build_arc().unwrap();
        let cli_ctx = OptdCliSessionContext::new_with_config_rt(config, runtime);

        let original_catalog_list = cli_ctx.inner().state().catalog_list().clone();
        let optd_catalog_list =
            OptdCatalogProviderList::new(original_catalog_list, Some(handle.clone()));
        cli_ctx
            .inner()
            .register_catalog_list(Arc::new(optd_catalog_list));

        // Try to query dropped table - should fail
        let result = cli_ctx.inner().sql("SELECT * FROM droppable").await;
        assert!(result.is_err(), "Should not find dropped table");

        handle.shutdown().await.unwrap();
    }
}
