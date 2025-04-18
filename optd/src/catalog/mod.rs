use async_trait::async_trait;
use std::collections::HashMap;
use std::fmt::Debug;

pub mod iceberg;

/// A Catalog Error.
/// TODO(connor): IMPORTANT!!! Figure out error handling (ideally unify error types).
#[derive(Debug)]
pub enum CatalogError {
    Unknown(String),
}

/// An interface for a database catalog that provides CRUD operations over table metadata.
///
/// This trait is similar and derived from Apache Iceberg's
/// [`Catalog`](https://docs.rs/iceberg/latest/iceberg/trait.Catalog.html) Rust trait.
///
/// TODO(connor): THIS TRAIT IS VERY SUBJECT TO CHANGE.
#[async_trait]
pub trait Catalog: Debug + Sync + Send {
    async fn get_table_properties(
        &self,
        table_name: &str,
    ) -> Result<HashMap<String, String>, CatalogError>;

    async fn get_table_columns(&self, table_name: &str) -> Result<Vec<String>, CatalogError>;
}
