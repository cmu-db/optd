use async_trait::async_trait;
use std::fmt::Debug;

/// An interface for a database catalog that provides CRUD operations over table metadata.
///
/// This trait is similar and derived from Apache Iceberg's
/// [`Catalog`](https://docs.rs/iceberg/latest/iceberg/trait.Catalog.html) Rust trait.
///
/// TODO(connor): IMPORTANT!!! Figure out error handling (ideally unify error types).
/// TODO(connor): THIS TRAIT IS VERY SUBJECT TO CHANGE.
#[async_trait]
pub trait Catalog: Debug + Sync + Send {
    async fn get_table_columns(&self, table_name: &str) -> Result<Vec<String>, String>;
}
