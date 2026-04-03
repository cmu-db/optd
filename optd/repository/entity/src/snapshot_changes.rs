use std::str::FromStr;

use sea_orm::entity::prelude::*;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "optd_snapshot_changes")]
pub struct Model {
    /// Refers to a `snapshot_id` from the `optd_snapshot` table.
    ///
    /// This is the snapshot for which the changes were recorded.
    /// Each snapshot has a corresponding entry describing its changes.
    #[sea_orm(primary_key, auto_increment = false)]
    pub snapshot_id: i64,

    /// A comma-separated list of high-level changes made by the snapshot.
    ///
    /// Each entry follows the format `<change_type>:<identifier>`, e.g.:
    /// - `created_schema:schema_name`
    /// - `created_table:table_name`
    /// - `created_view:view_name`
    /// - `inserted_into_table:table_id`
    /// - `deleted_from_table:table_id`
    /// - `compacted_table:table_id`
    /// - `dropped_schema:schema_id`
    /// - `dropped_table:table_id`
    /// - `dropped_view:view_id`
    /// - `altered_table:table_id`
    /// - `altered_view:view_id`
    ///
    /// Names are written using SQL-style quoted escaping when necessary.
    pub changes_made: String,

    /// Author of the snapshot.
    ///
    /// Can be `NULL` if no author information is provided.
    pub author: Option<String>,

    /// Commit message associated with the snapshot.
    ///
    /// Can be `NULL`.
    pub commit_message: Option<String>,

    /// Additional metadata about the commit.
    ///
    /// Can be `NULL`.
    pub commit_extra_info: Option<String>,

    #[sea_orm(belongs_to, from = "snapshot_id", to = "snapshot_id")]
    pub snapshot: HasOne<super::snapshot::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}

#[derive(Debug, Clone, PartialEq, derive_more::Display)]
pub enum ChangesMade {
    #[display("created_schema:{_0}")]
    CreateSchema(String),
    #[display("created_table:{_0}")]
    CreateTable(String),
    #[display("created_view:{_0}")]
    CreateView(String),
    #[display("inserted_into_table:{_0}")]
    InsertIntoTable(i64),
    #[display("deleted_from_table:{_0}")]
    DeleteFromTable(i64),
    #[display("compacted_table:{_0}")]
    CompactTable(i64),
    #[display("dropped_schema:{_0}")]
    DropSchema(i64),
    #[display("dropped_table:{_0}")]
    DropTable(i64),
    #[display("dropped_view:{_0}")]
    DropView(i64),
    #[display("altered_table:{_0}")]
    AlterTable(i64),
    #[display("altered_view:{_0}")]
    AlterView(i64),
}

impl FromStr for ChangesMade {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (first, second) = s.split_once(':').ok_or_else(|| s.to_string())?;
        let parse_id = || second.parse::<i64>().map_err(|_| s.to_string());

        match first {
            "created_schema" => Ok(Self::CreateSchema(second.to_owned())),
            "created_table" => Ok(Self::CreateTable(second.to_owned())),
            "created_view" => Ok(Self::CreateView(second.to_owned())),
            "inserted_into_table" => Ok(Self::InsertIntoTable(parse_id()?)),
            "deleted_from_table" => Ok(Self::DeleteFromTable(parse_id()?)),
            "compacted_table" => Ok(Self::CompactTable(parse_id()?)),
            "dropped_schema" => Ok(Self::DropSchema(parse_id()?)),
            "dropped_table" => Ok(Self::DropTable(parse_id()?)),
            "dropped_view" => Ok(Self::DropView(parse_id()?)),
            "altered_table" => Ok(Self::AlterTable(parse_id()?)),
            "altered_view" => Ok(Self::AlterView(parse_id()?)),
            _ => Err(s.to_string()),
        }
    }
}
