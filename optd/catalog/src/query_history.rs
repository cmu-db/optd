//! Query templates and optimization requests, independent of metadata snapshots.
//!
//! These short-lived writes never publish metadata or allocate catalog object IDs.
//! Payloads are connector-defined JSON; callers must apply their capture/redaction
//! policy before passing SQL, parameter values, configuration, or error details here.
use crate::{
    SnapshotId,
    entity::{query, query_instance},
};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, Condition, DatabaseConnection, DbErr, EntityTrait, QueryFilter,
    QueryOrder, QuerySelect, Set, TransactionTrait,
    entity::prelude::DateTimeUtc,
    sea_query::{Expr, prelude::Utc},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use snafu::Snafu;
use uuid::Uuid;

const ENCODING_VERSION: i32 = 1;
const MAX_PAYLOAD_BYTES: usize = 1024 * 1024;
/// Maximum number of instances returned by a single history read.
pub const MAX_PAGE_SIZE: u32 = 200;

/// Errors in history persistence; these must not replace a query's own outcome.
#[derive(Debug, Snafu)]
pub enum HistoryError {
    #[snafu(display("query history database error: {source}"))]
    Database { source: DbErr },
    #[snafu(display("query history JSON error: {source}"))]
    Json { source: serde_json::Error },
    #[snafu(display("invalid query history input: {message}"))]
    InvalidInput { message: &'static str },
    #[snafu(display("invalid persisted query history: {message}"))]
    InvalidRecord { message: &'static str },
    #[snafu(display("query template fingerprint collision"))]
    FingerprintCollision,
    #[snafu(display("query instance {instance_id} not found"))]
    InstanceNotFound { instance_id: QueryInstanceId },
    #[snafu(display("query instance {instance_id} has conflicting lifecycle state"))]
    LifecycleConflict { instance_id: QueryInstanceId },
}
impl From<DbErr> for HistoryError {
    fn from(source: DbErr) -> Self {
        Self::Database { source }
    }
}
impl From<serde_json::Error> for HistoryError {
    fn from(source: serde_json::Error) -> Self {
        Self::Json { source }
    }
}

macro_rules! history_id {
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(Uuid);
        impl $name {
            /// Allocates a UUIDv7 without touching metadata counters.
            pub fn new() -> Self {
                Self(Uuid::now_v7())
            }
        }
        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }
        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                self.0.fmt(f)
            }
        }
        impl std::str::FromStr for $name {
            type Err = HistoryError;
            fn from_str(value: &str) -> Result<Self, Self::Err> {
                let id = Uuid::parse_str(value).map_err(|_| HistoryError::InvalidInput {
                    message: "expected a UUIDv7",
                })?;
                if id.get_version_num() != 7 {
                    return Err(HistoryError::InvalidInput {
                        message: "expected a UUIDv7",
                    });
                }
                Ok(Self(id))
            }
        }
    };
}
history_id!(QueryId, "Identity of an immutable, scoped query template.");
history_id!(
    QueryInstanceId,
    "Identity of one optimization request, not one execution."
);

/// Connector-produced, syntactic SQL template. Not a plan-cache key.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueryTemplate {
    pub normalization_version: i32,
    pub dialect: String,
    pub resolution_scope: Value,
    pub template: String,
    /// Literal kinds or known types; syntax-only normalizers must not invent resolved types.
    pub parameter_shape: Value,
}
impl QueryTemplate {
    /// Stable digest of all identity fields; JSON object key order is insignificant.
    pub fn fingerprint(&self) -> Result<String, HistoryError> {
        self.validate()?;
        let identity = serde_json::json!({
            "encoding_version": ENCODING_VERSION,
            "input_kind": "sql",
            "identity": self,
        });
        Ok(format!(
            "{:x}",
            Sha256::digest(encode(identity)?.as_bytes())
        ))
    }
    fn validate(&self) -> Result<(), HistoryError> {
        if self.normalization_version <= 0 || self.dialect.is_empty() || self.template.is_empty() {
            return Err(HistoryError::InvalidInput {
                message: "template version, dialect and SQL are required",
            });
        }
        Ok(())
    }
}

/// An immutable template and its database identity.
#[derive(Debug, Clone, PartialEq)]
pub struct StoredQuery {
    pub query_id: QueryId,
    pub created_at: DateTimeUtc,
    pub fingerprint: String,
    pub definition: QueryTemplate,
}

/// Initial request data. A missing template is valid for malformed/unhandled input.
#[derive(Debug, Clone)]
pub struct BeginQueryInstance {
    pub instance_id: QueryInstanceId,
    pub template: Option<QueryTemplate>,
    pub request: Value,
    pub resolution_context: Value,
    pub optimizer_configuration: Value,
    pub implementation_descriptor: Value,
}

/// Optimization lifecycle only. `Optimized` does not imply successful execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryStatus {
    Running,
    Optimized,
    Bypassed,
    Failed,
    Cancelled,
}
impl QueryStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Optimized => "optimized",
            Self::Bypassed => "bypassed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }
    fn decode(value: &str) -> Result<Self, HistoryError> {
        match value {
            "running" => Ok(Self::Running),
            "optimized" => Ok(Self::Optimized),
            "bypassed" => Ok(Self::Bypassed),
            "failed" => Ok(Self::Failed),
            "cancelled" => Ok(Self::Cancelled),
            _ => Err(HistoryError::InvalidRecord {
                message: "unknown optimization status",
            }),
        }
    }
}

/// Terminal optimization information; repeated identical completion is idempotent.
#[derive(Debug, Clone, PartialEq)]
pub struct FinishQueryInstance {
    pub status: QueryStatus,
    /// Observed request wall time before execution, not CPU or summed pass time.
    /// Instrumentation overhead may be included by the submitting connector.
    pub optimization_duration_ns: i64,
    pub outcome_details: Value,
}

/// Stored request with optional, actually consumed metadata snapshot provenance.
#[derive(Debug, Clone, PartialEq)]
pub struct QueryInstance {
    pub instance_id: QueryInstanceId,
    pub query_id: Option<QueryId>,
    pub started_at: DateTimeUtc,
    pub finished_at: Option<DateTimeUtc>,
    pub status: QueryStatus,
    pub catalog_snapshot_id: Option<SnapshotId>,
    pub request: Value,
    pub resolution_context: Value,
    pub optimizer_configuration: Value,
    pub implementation_descriptor: Value,
    pub optimization_duration_ns: Option<i64>,
    pub outcome_details: Option<Value>,
}
impl QueryInstance {
    /// Exclusive cursor for continuing an ascending, stable history listing.
    pub fn cursor(&self) -> InstanceCursor {
        InstanceCursor {
            started_at: self.started_at,
            instance_id: self.instance_id,
        }
    }
}

/// Keyset cursor; timestamps alone do not distinguish concurrent submissions.
#[derive(Debug, Clone, PartialEq)]
pub struct InstanceCursor {
    pub started_at: DateTimeUtc,
    pub instance_id: QueryInstanceId,
}

/// Bounded instance listing, optionally scoped to a template or planning snapshot.
#[derive(Debug, Clone)]
pub struct InstanceFilter {
    pub query_id: Option<QueryId>,
    pub catalog_snapshot_id: Option<SnapshotId>,
    pub after: Option<InstanceCursor>,
    pub limit: u32,
}
impl Default for InstanceFilter {
    fn default() -> Self {
        Self {
            query_id: None,
            catalog_snapshot_id: None,
            after: None,
            limit: 50,
        }
    }
}

/// Non-temporal history persistence. Never holds a transaction across optimization.
#[derive(Clone)]
pub struct QueryHistoryStore {
    db: DatabaseConnection,
}
impl QueryHistoryStore {
    pub fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    /// Atomically registers/deduplicates a template and inserts a running instance.
    /// Duplicate instance IDs fail rather than altering an existing request.
    pub async fn begin(&self, info: BeginQueryInstance) -> Result<QueryInstance, HistoryError> {
        if info.instance_id.0.get_version_num() != 7 {
            return Err(HistoryError::InvalidInput {
                message: "instance ID must be UUIDv7",
            });
        }
        // Serialize before acquiring a database writer, and reject excessive payloads.
        let request = encode(info.request)?;
        let resolution_context = encode(info.resolution_context)?;
        let optimizer_configuration = encode(info.optimizer_configuration)?;
        let implementation_descriptor = encode(info.implementation_descriptor)?;
        let template = info
            .template
            .map(|definition| {
                let fingerprint = definition.fingerprint()?;
                Ok::<_, HistoryError>((definition, fingerprint))
            })
            .transpose()?;
        let txn = self.db.begin().await?;
        let query_id = if let Some((definition, fingerprint)) = template {
            let model = query::ActiveModel {
                query_id: Set(QueryId::new().to_string()),
                created_at: Set(Utc::now()),
                encoding_version: Set(ENCODING_VERSION),
                normalization_version: Set(definition.normalization_version),
                dialect: Set(definition.dialect.clone()),
                resolution_scope: Set(encode(definition.resolution_scope.clone())?),
                fingerprint: Set(fingerprint.clone()),
                template: Set(definition.template.clone()),
                parameter_shape: Set(encode(definition.parameter_shape.clone())?),
            };
            query::Entity::insert(model)
                .on_conflict_do_nothing_on([query::Column::Fingerprint])
                .exec(&txn)
                .await?;
            let stored = query::Entity::find()
                .filter(query::Column::Fingerprint.eq(fingerprint))
                .one(&txn)
                .await?
                .ok_or(HistoryError::InvalidRecord {
                    message: "template disappeared after insertion",
                })?;
            let stored = decode_query(stored)?;
            // Compare canonical content, not a hash alone (also detects persisted corruption).
            if stored.definition != definition {
                txn.rollback().await?;
                return Err(HistoryError::FingerprintCollision);
            }
            Some(stored.query_id.to_string())
        } else {
            None
        };
        query_instance::ActiveModel {
            query_instance_id: Set(info.instance_id.to_string()),
            query_id: Set(query_id),
            started_at: Set(Utc::now()),
            finished_at: Set(None),
            status: Set(QueryStatus::Running.as_str().into()),
            encoding_version: Set(ENCODING_VERSION),
            catalog_snapshot_id: Set(None),
            request: Set(request),
            resolution_context: Set(resolution_context),
            optimizer_configuration: Set(optimizer_configuration),
            implementation_descriptor: Set(implementation_descriptor),
            optimization_duration_ns: Set(None),
            outcome_details: Set(None),
        }
        .insert(&txn)
        .await?;
        txn.commit().await?;
        self.require_instance(info.instance_id).await
    }

    /// Attaches the snapshot actually used by planning, once. A different snapshot
    /// or a first attachment after completion is a lifecycle conflict.
    pub async fn attach_snapshot(
        &self,
        id: QueryInstanceId,
        snapshot: SnapshotId,
    ) -> Result<(), HistoryError> {
        let result = query_instance::Entity::update_many()
            .col_expr(
                query_instance::Column::CatalogSnapshotId,
                Expr::value(snapshot.as_i64()),
            )
            .filter(query_instance::Column::QueryInstanceId.eq(id.to_string()))
            .filter(query_instance::Column::Status.eq(QueryStatus::Running.as_str()))
            .filter(query_instance::Column::EncodingVersion.eq(ENCODING_VERSION))
            .filter(query_instance::Column::CatalogSnapshotId.is_null())
            .exec(&self.db)
            .await?;
        if result.rows_affected == 0
            && self.require_instance(id).await?.catalog_snapshot_id != Some(snapshot)
        {
            return Err(HistoryError::LifecycleConflict { instance_id: id });
        }
        Ok(())
    }

    /// Completes optimization only; never changes an already recorded outcome.
    pub async fn finish(
        &self,
        id: QueryInstanceId,
        info: FinishQueryInstance,
    ) -> Result<QueryInstance, HistoryError> {
        if info.status == QueryStatus::Running || info.optimization_duration_ns < 0 {
            return Err(HistoryError::InvalidInput {
                message: "completion requires a terminal status and nonnegative duration",
            });
        }
        let details = encode(info.outcome_details.clone())?;
        let result = query_instance::Entity::update_many()
            .col_expr(
                query_instance::Column::Status,
                Expr::value(info.status.as_str()),
            )
            .col_expr(query_instance::Column::FinishedAt, Expr::value(Utc::now()))
            .col_expr(
                query_instance::Column::OptimizationDurationNs,
                Expr::value(info.optimization_duration_ns),
            )
            .col_expr(query_instance::Column::OutcomeDetails, Expr::value(details))
            .filter(query_instance::Column::QueryInstanceId.eq(id.to_string()))
            .filter(query_instance::Column::Status.eq(QueryStatus::Running.as_str()))
            .filter(query_instance::Column::EncodingVersion.eq(ENCODING_VERSION))
            .exec(&self.db)
            .await?;
        let stored = self.require_instance(id).await?;
        if result.rows_affected == 0
            && (stored.status != info.status
                || stored.optimization_duration_ns != Some(info.optimization_duration_ns)
                || stored.outcome_details != Some(info.outcome_details))
        {
            return Err(HistoryError::LifecycleConflict { instance_id: id });
        }
        Ok(stored)
    }

    pub async fn get_query(&self, id: QueryId) -> Result<Option<StoredQuery>, HistoryError> {
        query::Entity::find_by_id(id.to_string())
            .one(&self.db)
            .await?
            .map(decode_query)
            .transpose()
    }
    pub async fn get_instance(
        &self,
        id: QueryInstanceId,
    ) -> Result<Option<QueryInstance>, HistoryError> {
        query_instance::Entity::find_by_id(id.to_string())
            .one(&self.db)
            .await?
            .map(decode_instance)
            .transpose()
    }
    async fn require_instance(&self, id: QueryInstanceId) -> Result<QueryInstance, HistoryError> {
        self.get_instance(id)
            .await?
            .ok_or(HistoryError::InstanceNotFound { instance_id: id })
    }

    pub async fn list_instances(
        &self,
        filter: InstanceFilter,
    ) -> Result<Vec<QueryInstance>, HistoryError> {
        if filter.limit == 0 || filter.limit > MAX_PAGE_SIZE {
            return Err(HistoryError::InvalidInput {
                message: "page size must be between 1 and 200",
            });
        }
        let mut select = query_instance::Entity::find();
        if let Some(id) = filter.query_id {
            select = select.filter(query_instance::Column::QueryId.eq(id.to_string()));
        }
        if let Some(id) = filter.catalog_snapshot_id {
            select = select.filter(query_instance::Column::CatalogSnapshotId.eq(id.as_i64()));
        }
        if let Some(cursor) = filter.after {
            select = select.filter(
                Condition::any()
                    .add(query_instance::Column::StartedAt.gt(cursor.started_at))
                    .add(
                        Condition::all()
                            .add(query_instance::Column::StartedAt.eq(cursor.started_at))
                            .add(
                                query_instance::Column::QueryInstanceId
                                    .gt(cursor.instance_id.to_string()),
                            ),
                    ),
            );
        }
        select
            .order_by_asc(query_instance::Column::StartedAt)
            .order_by_asc(query_instance::Column::QueryInstanceId)
            .limit(u64::from(filter.limit))
            .all(&self.db)
            .await?
            .into_iter()
            .map(decode_instance)
            .collect()
    }
}

fn canonicalize(value: Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut entries: Vec<_> = map.into_iter().collect();
            entries.sort_by(|a, b| a.0.cmp(&b.0));
            Value::Object(
                entries
                    .into_iter()
                    .map(|(key, value)| (key, canonicalize(value)))
                    .collect(),
            )
        }
        Value::Array(values) => Value::Array(values.into_iter().map(canonicalize).collect()),
        other => other,
    }
}
fn encode(value: Value) -> Result<String, HistoryError> {
    let encoded = serde_json::to_string(&canonicalize(value))?;
    if encoded.len() > MAX_PAYLOAD_BYTES {
        return Err(HistoryError::InvalidInput {
            message: "history JSON exceeds 1 MiB",
        });
    }
    Ok(encoded)
}
fn check_version(version: i32) -> Result<(), HistoryError> {
    if version != ENCODING_VERSION {
        return Err(HistoryError::InvalidRecord {
            message: "unsupported history encoding version",
        });
    }
    Ok(())
}
fn decode_query(model: query::Model) -> Result<StoredQuery, HistoryError> {
    check_version(model.encoding_version)?;
    let definition = QueryTemplate {
        normalization_version: model.normalization_version,
        dialect: model.dialect,
        resolution_scope: serde_json::from_str(&model.resolution_scope)?,
        template: model.template,
        parameter_shape: serde_json::from_str(&model.parameter_shape)?,
    };
    definition.validate()?;
    Ok(StoredQuery {
        query_id: model.query_id.parse()?,
        created_at: model.created_at,
        fingerprint: model.fingerprint,
        definition,
    })
}
fn decode_instance(model: query_instance::Model) -> Result<QueryInstance, HistoryError> {
    check_version(model.encoding_version)?;
    let status = QueryStatus::decode(&model.status)?;
    if (status == QueryStatus::Running) != model.finished_at.is_none()
        || model
            .optimization_duration_ns
            .is_some_and(|value| value < 0)
        || (status == QueryStatus::Running
            && (model.optimization_duration_ns.is_some() || model.outcome_details.is_some()))
        || (status != QueryStatus::Running
            && (model.optimization_duration_ns.is_none() || model.outcome_details.is_none()))
        || model.catalog_snapshot_id.is_some_and(|value| value < 0)
    {
        return Err(HistoryError::InvalidRecord {
            message: "inconsistent optimization lifecycle",
        });
    }
    Ok(QueryInstance {
        instance_id: model.query_instance_id.parse()?,
        query_id: model.query_id.map(|id| id.parse()).transpose()?,
        started_at: model.started_at,
        finished_at: model.finished_at,
        status,
        catalog_snapshot_id: model.catalog_snapshot_id.map(SnapshotId::from_raw),
        request: serde_json::from_str(&model.request)?,
        resolution_context: serde_json::from_str(&model.resolution_context)?,
        optimizer_configuration: serde_json::from_str(&model.optimizer_configuration)?,
        implementation_descriptor: serde_json::from_str(&model.implementation_descriptor)?,
        optimization_duration_ns: model.optimization_duration_ns,
        outcome_details: model
            .outcome_details
            .map(|value| serde_json::from_str(&value))
            .transpose()?,
    })
}
