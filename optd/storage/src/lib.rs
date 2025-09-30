#![allow(dead_code)]

use std::collections::BTreeSet;

enum CompactionType {
    MergeAdjacentTables,
    RewriteDeletes,
}

enum CleanupType {
    OldFiles,
    OrphanedFiles,
}

struct DuckLakeTag {
    key: String,
    value: String,
}

struct DuckLakeSchemaSetting {
    schema_id: usize,
    tag: DuckLakeTag,
}

struct DuckLakeTableSetting {
    schema_id: usize,
    tag: DuckLakeTag,
}

struct DuckLakeMetadata {
    tags: Vec<DuckLakeTag>,
    schema_settings: Vec<DuckLakeSchemaSetting>,
    table_settings: Vec<DuckLakeTableSetting>,
}

struct DuckLakeSchemaInfo {
    id: usize,
    uuid: String,
    name: String,
    path: String,
    tags: Vec<DuckLakeTag>,
}

struct DuckLakeColumnInfo {
    /// Field index.
    id: usize,
    name: String,
    typ: String,
    // TODO: switch to value type?
    initial_default: String,
    default_value: String,
    nulls_allowed: bool,
    children: Vec<DuckLakeColumnInfo>,
    tags: Vec<DuckLakeTag>,
}

struct DuckLakeInlinedTableInfo {
    table_name: String,
    schema_version: usize,
}

struct DuckLakeTableInfo {
    /// Table index.
    id: usize,
    /// Schema index.
    schema_id: usize,
    uuid: String,
    name: String,
    columns: Vec<DuckLakeColumnInfo>,
    tags: Vec<DuckLakeTag>,
    inlined_data_tables: Vec<DuckLakeInlinedTableInfo>,
}

struct DuckLakeColumnStatsInfo {
    column_id: usize,
    value_count: String,
    null_count: String,
    column_size_bytes: String,
    min_val: String,
    max_val: String,
    contains_nan: String,
    extra_stats: String,
}

struct DuckLakeFilePartitionInfo {
    partition_column_index: usize,
    partition_value: String,
}

struct DuckLakePartialFileInfo {
    snapshot_id: usize,
    max_row_count: usize,
}

struct DuckLakeFileInfo {
    // DataFileIndex,
    id: usize,
    // TableIndex
    table_id: usize,
    file_name: String,
    row_count: usize,
    file_size_bytes: usize,
    footer_size: Option<usize>,
    row_id_start: Option<usize>,
    partition_id: Option<usize>,
    begin_snapshot: Option<usize>,
    max_partial_file_snapshot: Option<usize>,
    encryption_key: Option<usize>,
    mapping_id: usize,
    column_stats: Vec<DuckLakeColumnStatsInfo>,
    partition_values: Vec<DuckLakeFilePartitionInfo>,
    partial_file_info: Vec<DuckLakeFilePartitionInfo>,
}

// struct DuckLakeInlinedData {
// 	data: Box<ColumnDataCollection>,
// 	column_stats: BTreeMap<usize, DuckLakeColumnStats>,
// }

// struct DuckLakeInlinedDataDeletes {
//     rows: BTreeSet<usize>,
// }

// struct DuckLakeInlinedDataInfo {
//     table_id: usize,
//     row_id_start: usize,
//     data: Option<Box<DuckLakeInlinedData>>,
// }

struct DuckLakeDeletedInlinedDataInfo {
    table_id: usize,
    table_name: String,
    deleted_row_ids: Vec<usize>,
}

struct DuckLakeDeleteFileInfo {
    id: usize,
    table_id: usize,
    data_file_id: usize,
    path: String,
    delete_count: usize,
    file_size_bytes: usize,
    footer_size: usize,
    encryption_key: String,
}

struct DuckLakePartitionFieldInfo {
    // default = 0
    partition_key_index: usize,
    field_id: usize,
    transform: String,
}

struct DuckLakePartitionInfo {
    id: Option<usize>,
    table_id: usize,
    fields: Vec<DuckLakePartitionFieldInfo>,
}

struct DuckLakeGlobalColumnStatsInfo {
    column_id: usize,
    contains_null: bool,
    has_contains_null: bool,
    contains_nan: bool,
    has_contains_nan: bool,
    min_val: String,
    has_min: bool,
    // TODO(yuchen): should this be Option<String>?
    max_val: String,
    has_max: bool,
    extra_stats: String,
    has_extra_stats: bool,
}

struct DuckLakeGlobalStatsInfo {
    table_id: usize,
    initialized: bool,
    record_count: usize,
    next_row_id: usize,
    table_size_bytes: usize,
    column_stats: Vec<DuckLakeGlobalColumnStatsInfo>,
}

struct SnapshotChangeInfo {
    changes_made: String,
}

struct SnapshotDeletedFromFiles {
    /// DataFileIndex
    deleted_from_files: BTreeSet<usize>,
}

struct DuckLakeSnapshotInfo {
    id: usize,
    // TODO: timestamp_tz_t
    time: String,
    schema_version: usize,
    change_info: SnapshotChangeInfo,
    author: String,
    commit_message: String,
    commit_extra_info: String,
}

struct DuckLakeViewInfo {
    id: usize,
    schema_id: usize,
    uuid: String,
    name: String,
    dialect: String,
    column_aliases: Vec<String>,
    sql: String,
    tags: Vec<DuckLakeTag>,
}

struct DuckLakeTagInfo {
    id: usize,
    key: String,
    value: String,
}

struct DuckLakeColumnTagInfo {
    table_id: usize,
    field_index: usize,
    key: String,
    value: String,
}

struct DuckLakeDroppedColumn {
    table_id: usize,
    field_id: usize,
}

struct DuckLakeNewColumn {
    table_id: usize,
    column_info: DuckLakeColumnInfo,
    parent_index: Option<usize>,
}

struct DuckLakeCatalogInfo {
    schemas: Vec<DuckLakeSchemaInfo>,
    tables: Vec<DuckLakeTableInfo>,
    views: Vec<DuckLakeViewInfo>,
    partitions: Vec<DuckLakePartitionInfo>,
}

struct DuckLakeFileData {
    path: String,
    encryption_key: String,
    file_size_bytes: usize,
    footer_size: Option<usize>,
}

enum DuckLakeDataType {
    DataFile,
    InlinedData,
    TransactionLocalInlinedData,
}

struct DuckLakeFileListEntry {
    file: DuckLakeFileData,
    delete_file: DuckLakeFileData,
    row_id_start: Option<usize>,
    snapshot_id: Option<usize>,
    max_row_count: Option<usize>,
    snapshot_filter: Option<usize>,
    mapping_id: usize,
    ///  default: DuckLakeDataType::DataFile;
    data_type: DuckLakeDataType,
}
