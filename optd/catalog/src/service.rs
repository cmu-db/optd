use crate::{
    Catalog, CurrentSchema, DuckLakeCatalog, Error, ExternalTableMetadata, RegisterTableRequest,
    SchemaRef, SnapshotId, SnapshotInfo, TableStatistics,
};
use tokio::sync::{mpsc, oneshot};

/// Max pending requests
const CHANNEL_BUFFER_SIZE: usize = 1000;

/// Trait defining the catalog backend that can be used with the service.
pub trait CatalogBackend: Send + 'static {
    fn current_snapshot(&mut self) -> Result<SnapshotId, Error>;
    fn current_snapshot_info(&mut self) -> Result<SnapshotInfo, Error>;
    fn current_schema(&mut self, schema: Option<&str>, table: &str) -> Result<SchemaRef, Error>;
    fn current_schema_info(&mut self) -> Result<CurrentSchema, Error>;
    fn table_statistics(
        &mut self,
        table_name: &str,
        snapshot: SnapshotId,
    ) -> Result<Option<TableStatistics>, Error>;
    fn set_table_statistics(
        &mut self,
        schema_name: Option<&str>,
        table_name: &str,
        stats: TableStatistics,
    ) -> Result<(), Error>;
    fn update_table_column_stats(
        &mut self,
        column_id: i64,
        table_id: i64,
        stats_type: &str,
        payload: &str,
    ) -> Result<(), Error>;
    fn register_external_table(
        &mut self,
        request: RegisterTableRequest,
    ) -> Result<ExternalTableMetadata, Error>;
    fn get_external_table(
        &mut self,
        schema_name: Option<&str>,
        table_name: &str,
    ) -> Result<Option<ExternalTableMetadata>, Error>;
    fn list_external_tables(
        &mut self,
        schema_name: Option<&str>,
    ) -> Result<Vec<ExternalTableMetadata>, Error>;
    fn drop_external_table(
        &mut self,
        schema_name: Option<&str>,
        table_name: &str,
    ) -> Result<(), Error>;
    fn list_snapshots(&mut self) -> Result<Vec<SnapshotInfo>, Error>;
    fn get_external_table_at_snapshot(
        &mut self,
        schema_name: Option<&str>,
        table_name: &str,
        snapshot_id: i64,
    ) -> Result<Option<ExternalTableMetadata>, Error>;
    fn list_external_tables_at_snapshot(
        &mut self,
        schema_name: Option<&str>,
        snapshot_id: i64,
    ) -> Result<Vec<ExternalTableMetadata>, Error>;
    fn create_schema(&mut self, schema_name: &str) -> Result<(), Error>;
    fn list_schemas(&mut self) -> Result<Vec<String>, Error>;
    fn drop_schema(&mut self, schema_name: &str) -> Result<(), Error>;
}

/// Implement CatalogBackend for any type that implements Catalog
impl<T: Catalog + Send + 'static> CatalogBackend for T {
    fn current_snapshot(&mut self) -> Result<SnapshotId, Error> {
        Catalog::current_snapshot(self)
    }

    fn current_snapshot_info(&mut self) -> Result<SnapshotInfo, Error> {
        Catalog::current_snapshot_info(self)
    }

    fn current_schema(&mut self, schema: Option<&str>, table: &str) -> Result<SchemaRef, Error> {
        Catalog::current_schema(self, schema, table)
    }

    fn current_schema_info(&mut self) -> Result<CurrentSchema, Error> {
        Catalog::current_schema_info(self)
    }

    fn table_statistics(
        &mut self,
        table_name: &str,
        snapshot: SnapshotId,
    ) -> Result<Option<TableStatistics>, Error> {
        Catalog::table_statistics(self, table_name, snapshot)
    }

    fn set_table_statistics(
        &mut self,
        schema_name: Option<&str>,
        table_name: &str,
        stats: TableStatistics,
    ) -> Result<(), Error> {
        Catalog::set_table_statistics(self, schema_name, table_name, stats)
    }

    fn update_table_column_stats(
        &mut self,
        column_id: i64,
        table_id: i64,
        stats_type: &str,
        payload: &str,
    ) -> Result<(), Error> {
        Catalog::update_table_column_stats(self, column_id, table_id, stats_type, payload)
    }

    fn register_external_table(
        &mut self,
        request: RegisterTableRequest,
    ) -> Result<ExternalTableMetadata, Error> {
        Catalog::register_external_table(self, request)
    }

    fn get_external_table(
        &mut self,
        schema_name: Option<&str>,
        table_name: &str,
    ) -> Result<Option<ExternalTableMetadata>, Error> {
        Catalog::get_external_table(self, schema_name, table_name)
    }

    fn list_external_tables(
        &mut self,
        schema_name: Option<&str>,
    ) -> Result<Vec<ExternalTableMetadata>, Error> {
        Catalog::list_external_tables(self, schema_name)
    }

    fn drop_external_table(
        &mut self,
        schema_name: Option<&str>,
        table_name: &str,
    ) -> Result<(), Error> {
        Catalog::drop_external_table(self, schema_name, table_name)
    }

    fn list_snapshots(&mut self) -> Result<Vec<SnapshotInfo>, Error> {
        Catalog::list_snapshots(self)
    }

    fn get_external_table_at_snapshot(
        &mut self,
        schema_name: Option<&str>,
        table_name: &str,
        snapshot_id: i64,
    ) -> Result<Option<ExternalTableMetadata>, Error> {
        Catalog::get_external_table_at_snapshot(self, schema_name, table_name, snapshot_id)
    }

    fn list_external_tables_at_snapshot(
        &mut self,
        schema_name: Option<&str>,
        snapshot_id: i64,
    ) -> Result<Vec<ExternalTableMetadata>, Error> {
        Catalog::list_external_tables_at_snapshot(self, schema_name, snapshot_id)
    }

    fn create_schema(&mut self, schema_name: &str) -> Result<(), Error> {
        Catalog::create_schema(self, schema_name)
    }

    fn list_schemas(&mut self) -> Result<Vec<String>, Error> {
        Catalog::list_schemas(self)
    }

    fn drop_schema(&mut self, schema_name: &str) -> Result<(), Error> {
        Catalog::drop_schema(self, schema_name)
    }
}

#[derive(Debug)]
pub enum CatalogRequest {
    CurrentSnapshot {
        respond_to: oneshot::Sender<Result<SnapshotId, Error>>,
    },

    CurrentSnapshotInfo {
        respond_to: oneshot::Sender<Result<SnapshotInfo, Error>>,
    },

    CurrentSchema {
        schema: Option<String>,
        table: String,
        respond_to: oneshot::Sender<Result<SchemaRef, Error>>,
    },

    CurrentSchemaInfo {
        respond_to: oneshot::Sender<Result<CurrentSchema, Error>>,
    },

    TableStatistics {
        table_name: String,
        snapshot: SnapshotId,
        respond_to: oneshot::Sender<Result<Option<TableStatistics>, Error>>,
    },

    UpdateTableColumnStats {
        column_id: i64,
        table_id: i64,
        stats_type: String,
        payload: String,
        respond_to: oneshot::Sender<Result<(), Error>>,
    },

    RegisterExternalTable {
        request: RegisterTableRequest,
        respond_to: oneshot::Sender<Result<ExternalTableMetadata, Error>>,
    },

    GetExternalTable {
        schema_name: Option<String>,
        table_name: String,
        respond_to: oneshot::Sender<Result<Option<ExternalTableMetadata>, Error>>,
    },

    ListExternalTables {
        schema_name: Option<String>,
        respond_to: oneshot::Sender<Result<Vec<ExternalTableMetadata>, Error>>,
    },

    DropExternalTable {
        schema_name: Option<String>,
        table_name: String,
        respond_to: oneshot::Sender<Result<(), Error>>,
    },

    ListSnapshots {
        respond_to: oneshot::Sender<Result<Vec<SnapshotInfo>, Error>>,
    },

    GetExternalTableAtSnapshot {
        schema_name: Option<String>,
        table_name: String,
        snapshot_id: i64,
        respond_to: oneshot::Sender<Result<Option<ExternalTableMetadata>, Error>>,
    },

    ListExternalTablesAtSnapshot {
        schema_name: Option<String>,
        snapshot_id: i64,
        respond_to: oneshot::Sender<Result<Vec<ExternalTableMetadata>, Error>>,
    },

    SetTableStatistics {
        schema_name: Option<String>,
        table_name: String,
        stats: TableStatistics,
        respond_to: oneshot::Sender<Result<(), Error>>,
    },

    CreateSchema {
        schema_name: String,
        respond_to: oneshot::Sender<Result<(), Error>>,
    },

    ListSchemas {
        respond_to: oneshot::Sender<Result<Vec<String>, Error>>,
    },

    DropSchema {
        schema_name: String,
        respond_to: oneshot::Sender<Result<(), Error>>,
    },

    Shutdown,
}

/// Handle for catalog service interaction
#[derive(Clone, Debug)]
pub struct CatalogServiceHandle {
    sender: mpsc::Sender<CatalogRequest>,
}

impl CatalogServiceHandle {
    pub async fn current_snapshot(&self) -> Result<SnapshotId, Error> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(CatalogRequest::CurrentSnapshot { respond_to: tx })
            .await
            .map_err(|_| Error::QueryExecution {
                source: duckdb::Error::ExecuteReturnedResults,
            })?;

        rx.await.map_err(|_| Error::QueryExecution {
            source: duckdb::Error::ExecuteReturnedResults,
        })?
    }

    pub async fn current_snapshot_info(&self) -> Result<SnapshotInfo, Error> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(CatalogRequest::CurrentSnapshotInfo { respond_to: tx })
            .await
            .map_err(|_| Error::QueryExecution {
                source: duckdb::Error::ExecuteReturnedResults,
            })?;

        rx.await.map_err(|_| Error::QueryExecution {
            source: duckdb::Error::ExecuteReturnedResults,
        })?
    }

    pub async fn current_schema(
        &self,
        schema: Option<&str>,
        table: &str,
    ) -> Result<SchemaRef, Error> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(CatalogRequest::CurrentSchema {
                schema: schema.map(|s| s.to_string()),
                table: table.to_string(),
                respond_to: tx,
            })
            .await
            .map_err(|_| Error::QueryExecution {
                source: duckdb::Error::ExecuteReturnedResults,
            })?;

        rx.await.map_err(|_| Error::QueryExecution {
            source: duckdb::Error::ExecuteReturnedResults,
        })?
    }

    pub async fn current_schema_info(&self) -> Result<CurrentSchema, Error> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(CatalogRequest::CurrentSchemaInfo { respond_to: tx })
            .await
            .map_err(|_| Error::QueryExecution {
                source: duckdb::Error::ExecuteReturnedResults,
            })?;

        rx.await.map_err(|_| Error::QueryExecution {
            source: duckdb::Error::ExecuteReturnedResults,
        })?
    }

    pub async fn table_statistics(
        &self,
        table_name: &str,
        snapshot: SnapshotId,
    ) -> Result<Option<TableStatistics>, Error> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(CatalogRequest::TableStatistics {
                table_name: table_name.to_string(),
                snapshot,
                respond_to: tx,
            })
            .await
            .map_err(|_| Error::QueryExecution {
                source: duckdb::Error::ExecuteReturnedResults,
            })?;

        rx.await.map_err(|_| Error::QueryExecution {
            source: duckdb::Error::ExecuteReturnedResults,
        })?
    }

    pub async fn update_table_column_stats(
        &self,
        column_id: i64,
        table_id: i64,
        stats_type: &str,
        payload: &str,
    ) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(CatalogRequest::UpdateTableColumnStats {
                column_id,
                table_id,
                stats_type: stats_type.to_string(),
                payload: payload.to_string(),
                respond_to: tx,
            })
            .await
            .map_err(|_| Error::QueryExecution {
                source: duckdb::Error::ExecuteReturnedResults,
            })?;

        rx.await.map_err(|_| Error::QueryExecution {
            source: duckdb::Error::ExecuteReturnedResults,
        })?
    }

    pub async fn set_table_statistics(
        &self,
        schema_name: Option<&str>,
        table_name: &str,
        stats: TableStatistics,
    ) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(CatalogRequest::SetTableStatistics {
                schema_name: schema_name.map(|s| s.to_string()),
                table_name: table_name.to_string(),
                stats,
                respond_to: tx,
            })
            .await
            .map_err(|_| Error::QueryExecution {
                source: duckdb::Error::ExecuteReturnedResults,
            })?;

        rx.await.map_err(|_| Error::QueryExecution {
            source: duckdb::Error::ExecuteReturnedResults,
        })?
    }

    pub async fn register_external_table(
        &self,
        request: RegisterTableRequest,
    ) -> Result<ExternalTableMetadata, Error> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(CatalogRequest::RegisterExternalTable {
                request,
                respond_to: tx,
            })
            .await
            .map_err(|_| Error::QueryExecution {
                source: duckdb::Error::ExecuteReturnedResults,
            })?;

        rx.await.map_err(|_| Error::QueryExecution {
            source: duckdb::Error::ExecuteReturnedResults,
        })?
    }

    pub async fn get_external_table(
        &self,
        schema_name: Option<&str>,
        table_name: &str,
    ) -> Result<Option<ExternalTableMetadata>, Error> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(CatalogRequest::GetExternalTable {
                schema_name: schema_name.map(|s| s.to_string()),
                table_name: table_name.to_string(),
                respond_to: tx,
            })
            .await
            .map_err(|_| Error::QueryExecution {
                source: duckdb::Error::ExecuteReturnedResults,
            })?;

        rx.await.map_err(|_| Error::QueryExecution {
            source: duckdb::Error::ExecuteReturnedResults,
        })?
    }

    pub async fn list_external_tables(
        &self,
        schema_name: Option<&str>,
    ) -> Result<Vec<ExternalTableMetadata>, Error> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(CatalogRequest::ListExternalTables {
                schema_name: schema_name.map(|s| s.to_string()),
                respond_to: tx,
            })
            .await
            .map_err(|_| Error::QueryExecution {
                source: duckdb::Error::ExecuteReturnedResults,
            })?;

        rx.await.map_err(|_| Error::QueryExecution {
            source: duckdb::Error::ExecuteReturnedResults,
        })?
    }

    pub async fn list_snapshots(&self) -> Result<Vec<SnapshotInfo>, Error> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(CatalogRequest::ListSnapshots { respond_to: tx })
            .await
            .map_err(|_| Error::QueryExecution {
                source: duckdb::Error::ExecuteReturnedResults,
            })?;

        rx.await.map_err(|_| Error::QueryExecution {
            source: duckdb::Error::ExecuteReturnedResults,
        })?
    }

    pub async fn get_external_table_at_snapshot(
        &self,
        schema_name: Option<&str>,
        table_name: &str,
        snapshot_id: i64,
    ) -> Result<Option<ExternalTableMetadata>, Error> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(CatalogRequest::GetExternalTableAtSnapshot {
                schema_name: schema_name.map(|s| s.to_string()),
                table_name: table_name.to_string(),
                snapshot_id,
                respond_to: tx,
            })
            .await
            .map_err(|_| Error::QueryExecution {
                source: duckdb::Error::ExecuteReturnedResults,
            })?;

        rx.await.map_err(|_| Error::QueryExecution {
            source: duckdb::Error::ExecuteReturnedResults,
        })?
    }

    pub async fn list_external_tables_at_snapshot(
        &self,
        schema_name: Option<&str>,
        snapshot_id: i64,
    ) -> Result<Vec<ExternalTableMetadata>, Error> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(CatalogRequest::ListExternalTablesAtSnapshot {
                schema_name: schema_name.map(|s| s.to_string()),
                snapshot_id,
                respond_to: tx,
            })
            .await
            .map_err(|_| Error::QueryExecution {
                source: duckdb::Error::ExecuteReturnedResults,
            })?;

        rx.await.map_err(|_| Error::QueryExecution {
            source: duckdb::Error::ExecuteReturnedResults,
        })?
    }

    pub async fn drop_external_table(
        &self,
        schema_name: Option<&str>,
        table_name: &str,
    ) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(CatalogRequest::DropExternalTable {
                schema_name: schema_name.map(|s| s.to_string()),
                table_name: table_name.to_string(),
                respond_to: tx,
            })
            .await
            .map_err(|_| Error::QueryExecution {
                source: duckdb::Error::ExecuteReturnedResults,
            })?;

        rx.await.map_err(|_| Error::QueryExecution {
            source: duckdb::Error::ExecuteReturnedResults,
        })?
    }

    pub async fn create_schema(&self, schema_name: &str) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(CatalogRequest::CreateSchema {
                schema_name: schema_name.to_string(),
                respond_to: tx,
            })
            .await
            .map_err(|_| Error::QueryExecution {
                source: duckdb::Error::ExecuteReturnedResults,
            })?;

        rx.await.map_err(|_| Error::QueryExecution {
            source: duckdb::Error::ExecuteReturnedResults,
        })?
    }

    pub async fn list_schemas(&self) -> Result<Vec<String>, Error> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(CatalogRequest::ListSchemas { respond_to: tx })
            .await
            .map_err(|_| Error::QueryExecution {
                source: duckdb::Error::ExecuteReturnedResults,
            })?;

        rx.await.map_err(|_| Error::QueryExecution {
            source: duckdb::Error::ExecuteReturnedResults,
        })?
    }

    pub fn blocking_list_schemas(&self) -> Result<Vec<String>, Error> {
        let sender = self.sender.clone();
        tokio::task::block_in_place(move || {
            tokio::runtime::Handle::current().block_on(async move {
                let (tx, rx) = oneshot::channel();
                sender
                    .send(CatalogRequest::ListSchemas { respond_to: tx })
                    .await
                    .map_err(|_| Error::QueryExecution {
                        source: duckdb::Error::ExecuteReturnedResults,
                    })?;

                rx.await.map_err(|_| Error::QueryExecution {
                    source: duckdb::Error::ExecuteReturnedResults,
                })?
            })
        })
    }

    pub async fn drop_schema(&self, schema_name: &str) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(CatalogRequest::DropSchema {
                schema_name: schema_name.to_string(),
                respond_to: tx,
            })
            .await
            .map_err(|_| Error::QueryExecution {
                source: duckdb::Error::ExecuteReturnedResults,
            })?;

        rx.await.map_err(|_| Error::QueryExecution {
            source: duckdb::Error::ExecuteReturnedResults,
        })?
    }

    pub async fn shutdown(&self) -> Result<(), Error> {
        self.sender
            .send(CatalogRequest::Shutdown)
            .await
            .map_err(|_| Error::QueryExecution {
                source: duckdb::Error::ExecuteReturnedResults,
            })
    }
}

/// The catalog service that processes requests in the background
pub struct CatalogService<B: CatalogBackend> {
    backend: B,
    receiver: mpsc::Receiver<CatalogRequest>,
}

impl<B: CatalogBackend> CatalogService<B> {
    /// Create service with provided backend catalog
    pub fn new(backend: B) -> (Self, CatalogServiceHandle) {
        let (sender, receiver) = mpsc::channel(CHANNEL_BUFFER_SIZE);

        let service = CatalogService { backend, receiver };
        let handle = CatalogServiceHandle { sender };

        (service, handle)
    }

    /// Runs the service, processing requests until shutdown.
    ///
    /// Spawn with `tokio::spawn(async move { service.run().await; })`.
    pub async fn run(mut self) {
        while let Some(request) = self.receiver.recv().await {
            match request {
                CatalogRequest::CurrentSnapshot { respond_to } => {
                    let result = self.backend.current_snapshot();
                    let _ = respond_to.send(result);
                }

                CatalogRequest::CurrentSnapshotInfo { respond_to } => {
                    let result = self.backend.current_snapshot_info();
                    let _ = respond_to.send(result);
                }

                CatalogRequest::CurrentSchema {
                    schema,
                    table,
                    respond_to,
                } => {
                    let result = self.backend.current_schema(schema.as_deref(), &table);
                    let _ = respond_to.send(result);
                }

                CatalogRequest::CurrentSchemaInfo { respond_to } => {
                    let result = self.backend.current_schema_info();
                    let _ = respond_to.send(result);
                }

                CatalogRequest::TableStatistics {
                    table_name,
                    snapshot,
                    respond_to,
                } => {
                    let result = self.backend.table_statistics(&table_name, snapshot);
                    let _ = respond_to.send(result);
                }

                CatalogRequest::UpdateTableColumnStats {
                    column_id,
                    table_id,
                    stats_type,
                    payload,
                    respond_to,
                } => {
                    let result = self.backend.update_table_column_stats(
                        column_id,
                        table_id,
                        &stats_type,
                        &payload,
                    );
                    let _ = respond_to.send(result);
                }

                CatalogRequest::RegisterExternalTable {
                    request,
                    respond_to,
                } => {
                    let result = self.backend.register_external_table(request);
                    let _ = respond_to.send(result);
                }

                CatalogRequest::GetExternalTable {
                    schema_name,
                    table_name,
                    respond_to,
                } => {
                    let result = self
                        .backend
                        .get_external_table(schema_name.as_deref(), &table_name);
                    let _ = respond_to.send(result);
                }

                CatalogRequest::ListExternalTables {
                    schema_name,
                    respond_to,
                } => {
                    let result = self.backend.list_external_tables(schema_name.as_deref());
                    let _ = respond_to.send(result);
                }

                CatalogRequest::DropExternalTable {
                    schema_name,
                    table_name,
                    respond_to,
                } => {
                    let result = self
                        .backend
                        .drop_external_table(schema_name.as_deref(), &table_name);
                    let _ = respond_to.send(result);
                }

                CatalogRequest::ListSnapshots { respond_to } => {
                    let result = self.backend.list_snapshots();
                    let _ = respond_to.send(result);
                }

                CatalogRequest::GetExternalTableAtSnapshot {
                    schema_name,
                    table_name,
                    snapshot_id,
                    respond_to,
                } => {
                    let result = self.backend.get_external_table_at_snapshot(
                        schema_name.as_deref(),
                        &table_name,
                        snapshot_id,
                    );
                    let _ = respond_to.send(result);
                }

                CatalogRequest::ListExternalTablesAtSnapshot {
                    schema_name,
                    snapshot_id,
                    respond_to,
                } => {
                    let result = self
                        .backend
                        .list_external_tables_at_snapshot(schema_name.as_deref(), snapshot_id);
                    let _ = respond_to.send(result);
                }

                CatalogRequest::SetTableStatistics {
                    schema_name,
                    table_name,
                    stats,
                    respond_to,
                } => {
                    let result = self.backend.set_table_statistics(
                        schema_name.as_deref(),
                        &table_name,
                        stats,
                    );
                    let _ = respond_to.send(result);
                }

                CatalogRequest::CreateSchema {
                    schema_name,
                    respond_to,
                } => {
                    let result = self.backend.create_schema(&schema_name);
                    let _ = respond_to.send(result);
                }

                CatalogRequest::ListSchemas { respond_to } => {
                    let result = self.backend.list_schemas();
                    let _ = respond_to.send(result);
                }

                CatalogRequest::DropSchema {
                    schema_name,
                    respond_to,
                } => {
                    let result = self.backend.drop_schema(&schema_name);
                    let _ = respond_to.send(result);
                }

                CatalogRequest::Shutdown => {
                    // drop the receiver to stop accepting new requests
                    break;
                }
            }
        }
    }
}

// Convenience methods for creating service with DuckLakeCatalog
impl CatalogService<DuckLakeCatalog> {
    /// Create service from location paths using DuckLakeCatalog backend
    pub fn try_new_from_location(
        location: Option<&str>,
        metadata_path: Option<&str>,
    ) -> Result<(Self, CatalogServiceHandle), Error> {
        let catalog = DuckLakeCatalog::try_new(location, metadata_path)?;
        Ok(Self::new(catalog))
    }

    /// Get a reference to the underlying DuckLakeCatalog for test setup only.
    /// Only available in test/debug builds and should
    /// only be used for setting up test fixtures.
    #[cfg(any(test, debug_assertions))]
    pub fn catalog_for_setup(&self) -> &DuckLakeCatalog {
        &self.backend
    }
}
