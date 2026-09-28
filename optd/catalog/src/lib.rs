//! Snapshot-aware catalog persistence for Optd.
//!
//! The crate contains a narrow catalog kernel adapted from `origin/spring26`:
//! SeaORM entities and migrations, transactional catalog
//! mutations, and snapshot-pinned readers.

pub mod catalog;
pub mod entity;
pub mod migration;
pub mod relation;
pub mod schema;
pub mod snapshot;

pub use catalog::{Catalog, CatalogReader};
pub use snapshot::SnapshotId;
