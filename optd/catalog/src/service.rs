use crate::{
    Catalog, CurrentSchema, DuckLakeCatalog, Error, SchemaRef, SnapshotId, SnapshotInfo,
    TableStatistics,
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
    fn update_table_column_stats(
        &mut self,
        column_id: i64,
        table_id: i64,
        stats_type: &str,
        payload: &str,
    ) -> Result<(), Error>;
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

    fn update_table_column_stats(
        &mut self,
        column_id: i64,
        table_id: i64,
        stats_type: &str,
        payload: &str,
    ) -> Result<(), Error> {
        Catalog::update_table_column_stats(self, column_id, table_id, stats_type, payload)
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

    Shutdown,
}

/// Handle for catalog service interaction
#[derive(Clone)]
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

    /// Run the service, processing requests until shutdown
    ///
    /// Spawn with tokio:
    /// ```ignore
    /// tokio::spawn(async move {
    ///     service.run().await;
    /// });
    /// ```
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
