use optd_catalog::{CatalogService, RegisterTableRequest};
use std::collections::HashMap;
use tempfile::TempDir;

#[tokio::test]
async fn test_service_register_external_table() {
    let temp_dir = TempDir::new().unwrap();
    let metadata_path = temp_dir.path().join("metadata.ducklake");

    let (service, handle) =
        CatalogService::try_new_from_location(None, Some(metadata_path.to_str().unwrap())).unwrap();

    tokio::spawn(async move {
        service.run().await;
    });

    let mut options = HashMap::new();
    options.insert("has_header".to_string(), "true".to_string());

    let request = RegisterTableRequest {
        table_name: "users".to_string(),
        schema_name: None,
        location: "/data/users.csv".to_string(),
        file_format: "CSV".to_string(),
        compression: Some("gzip".to_string()),
        options: options.clone(),
    };

    let metadata = handle.register_external_table(request).await.unwrap();

    assert_eq!(metadata.table_name, "users");
    assert_eq!(metadata.location, "/data/users.csv");
    assert_eq!(metadata.file_format, "CSV");
    assert_eq!(metadata.compression, Some("gzip".to_string()));
    assert_eq!(metadata.options.get("has_header").unwrap(), "true");

    handle.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_service_get_external_table() {
    let temp_dir = TempDir::new().unwrap();
    let metadata_path = temp_dir.path().join("metadata.ducklake");

    let (service, handle) =
        CatalogService::try_new_from_location(None, Some(metadata_path.to_str().unwrap())).unwrap();

    tokio::spawn(async move {
        service.run().await;
    });

    // Register first
    let request = RegisterTableRequest {
        table_name: "products".to_string(),
        schema_name: None,
        location: "/data/products.parquet".to_string(),
        file_format: "PARQUET".to_string(),
        compression: None,
        options: HashMap::new(),
    };

    let registered = handle.register_external_table(request).await.unwrap();

    // Get it back
    let retrieved = handle
        .get_external_table(None, "products")
        .await
        .unwrap()
        .expect("Table should exist");

    assert_eq!(retrieved.table_id, registered.table_id);
    assert_eq!(retrieved.table_name, "products");
    assert_eq!(retrieved.location, "/data/products.parquet");

    handle.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_service_get_nonexistent_table() {
    let temp_dir = TempDir::new().unwrap();
    let metadata_path = temp_dir.path().join("metadata.ducklake");

    let (service, handle) =
        CatalogService::try_new_from_location(None, Some(metadata_path.to_str().unwrap())).unwrap();

    tokio::spawn(async move {
        service.run().await;
    });

    let result = handle
        .get_external_table(None, "nonexistent")
        .await
        .unwrap();
    assert!(result.is_none());

    handle.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_service_list_external_tables() {
    let temp_dir = TempDir::new().unwrap();
    let metadata_path = temp_dir.path().join("metadata.ducklake");

    let (service, handle) =
        CatalogService::try_new_from_location(None, Some(metadata_path.to_str().unwrap())).unwrap();

    tokio::spawn(async move {
        service.run().await;
    });

    // Register 3 tables
    for i in 1..=3 {
        let request = RegisterTableRequest {
            table_name: format!("table{}", i),
            schema_name: None,
            location: format!("/data/table{}.parquet", i),
            file_format: "PARQUET".to_string(),
            compression: None,
            options: HashMap::new(),
        };
        handle.register_external_table(request).await.unwrap();
    }

    let tables = handle.list_external_tables(None).await.unwrap();
    assert_eq!(tables.len(), 3);

    handle.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_service_drop_external_table() {
    let temp_dir = TempDir::new().unwrap();
    let metadata_path = temp_dir.path().join("metadata.ducklake");

    let (service, handle) =
        CatalogService::try_new_from_location(None, Some(metadata_path.to_str().unwrap())).unwrap();

    tokio::spawn(async move {
        service.run().await;
    });

    // Register
    let request = RegisterTableRequest {
        table_name: "temp_table".to_string(),
        schema_name: None,
        location: "/data/temp.parquet".to_string(),
        file_format: "PARQUET".to_string(),
        compression: None,
        options: HashMap::new(),
    };

    handle.register_external_table(request).await.unwrap();

    // Verify exists
    let before = handle.get_external_table(None, "temp_table").await.unwrap();
    assert!(before.is_some());

    // Drop
    handle
        .drop_external_table(None, "temp_table")
        .await
        .unwrap();

    // Verify gone
    let after = handle.get_external_table(None, "temp_table").await.unwrap();
    assert!(after.is_none());

    handle.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_service_concurrent_operations() {
    let temp_dir = TempDir::new().unwrap();
    let metadata_path = temp_dir.path().join("metadata.ducklake");

    let (service, handle) =
        CatalogService::try_new_from_location(None, Some(metadata_path.to_str().unwrap())).unwrap();

    tokio::spawn(async move {
        service.run().await;
    });

    // Spawn multiple concurrent registration tasks
    let mut tasks = vec![];
    for i in 0..10 {
        let handle_clone = handle.clone();
        let task = tokio::spawn(async move {
            let request = RegisterTableRequest {
                table_name: format!("concurrent_table_{}", i),
                schema_name: None,
                location: format!("/data/table_{}.parquet", i),
                file_format: "PARQUET".to_string(),
                compression: None,
                options: HashMap::new(),
            };
            handle_clone.register_external_table(request).await
        });
        tasks.push(task);
    }

    // Wait for all to complete
    for task in tasks {
        let result = task.await.unwrap();
        assert!(result.is_ok());
    }

    // Verify all 10 tables registered
    let tables = handle.list_external_tables(None).await.unwrap();
    assert_eq!(tables.len(), 10);

    handle.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_service_mixed_operations() {
    let temp_dir = TempDir::new().unwrap();
    let metadata_path = temp_dir.path().join("metadata.ducklake");

    let (service, handle) =
        CatalogService::try_new_from_location(None, Some(metadata_path.to_str().unwrap())).unwrap();

    tokio::spawn(async move {
        service.run().await;
    });

    // Register
    let request = RegisterTableRequest {
        table_name: "mixed_test".to_string(),
        schema_name: None,
        location: "/data/mixed.parquet".to_string(),
        file_format: "PARQUET".to_string(),
        compression: None,
        options: HashMap::new(),
    };
    handle.register_external_table(request).await.unwrap();

    // Get
    let retrieved = handle.get_external_table(None, "mixed_test").await.unwrap();
    assert!(retrieved.is_some());

    // List
    let tables = handle.list_external_tables(None).await.unwrap();
    assert_eq!(tables.len(), 1);

    // Drop
    handle
        .drop_external_table(None, "mixed_test")
        .await
        .unwrap();

    // List again
    let tables = handle.list_external_tables(None).await.unwrap();
    assert_eq!(tables.len(), 0);

    handle.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_service_persistence_across_restarts() {
    let temp_dir = TempDir::new().unwrap();
    let metadata_path = temp_dir.path().join("metadata.ducklake");
    let metadata_str = metadata_path.to_str().unwrap().to_string();

    // First service instance
    {
        let (service, handle) =
            CatalogService::try_new_from_location(None, Some(&metadata_str)).unwrap();

        tokio::spawn(async move {
            service.run().await;
        });

        let request = RegisterTableRequest {
            table_name: "persistent".to_string(),
            schema_name: None,
            location: "/data/persistent.parquet".to_string(),
            file_format: "PARQUET".to_string(),
            compression: None,
            options: HashMap::new(),
        };

        handle.register_external_table(request).await.unwrap();
        handle.shutdown().await.unwrap();
    }

    // Second service instance
    {
        let (service, handle) =
            CatalogService::try_new_from_location(None, Some(&metadata_str)).unwrap();

        tokio::spawn(async move {
            service.run().await;
        });

        let retrieved = handle
            .get_external_table(None, "persistent")
            .await
            .unwrap()
            .expect("Table should persist across service restarts");

        assert_eq!(retrieved.table_name, "persistent");
        assert_eq!(retrieved.location, "/data/persistent.parquet");

        handle.shutdown().await.unwrap();
    }
}
