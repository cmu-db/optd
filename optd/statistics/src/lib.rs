mod ducklake_connection;
mod statistics;

pub use ducklake_connection::{query, ConnectionMode, DuckLakeConnectionBuilder};
pub use statistics::{DuckLakeStatisticsProvider, Error as InterfaceError, StatisticsProvider};
