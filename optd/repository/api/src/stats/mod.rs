mod get_all_table_stats;
mod get_table_stats;
mod update_table_stats;

use optd_core::ir::statistics::TableStatistics;

pub use get_all_table_stats::*;
pub use get_table_stats::*;
pub use update_table_stats::*;

#[derive(Debug, Clone, PartialEq)]
pub struct GetTableStatsInfo {
    pub table_id: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UpdateTableStatsInfo {
    pub table_id: i64,
    pub stats: TableStatistics,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TableStatsInfo {
    pub table_id: i64,
    pub stats: TableStatistics,
}
