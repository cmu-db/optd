//! Optional advanced objects, separate from the established basic table/column statistics.
//!
//! Updates preserve identity and immutable metadata. Drop or relation-wide invalidation
//! retires an object; subsequent collection creates a new ID. Contents are caller-collected
//! estimates, not enforced facts or automatically refreshed provider metadata.
use std::cmp::Ordering;

use arrow_schema::DataType;
use sea_orm::{
    ActiveValue::{NotSet, Set},
    ColumnTrait, Condition, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder,
    prelude::ChronoUtc,
    sea_query::Expr,
};
use serde::{Deserialize, Serialize};

use crate::{
    CatalogError, CatalogResult,
    entity::advanced_statistics::{self as entity, Column},
    relation::{GetRelationInfo, RelationInfo, get_relation},
    snapshot::{SnapshotId, SnapshotInfo},
    statistics::{StatisticsValue, decode_column_ids, encode_column_ids},
};

const ENCODING_VERSION: i32 = 1;

/// One histogram interval: (-infinity, first upper bound], then (previous, upper].
/// Frequencies count non-null observations in the collected population, not NULLs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistogramBucket {
    pub upper_bound: StatisticsValue,
    pub frequency: u64,
}

/// Version-1 contents. Additional algorithms need explicit, versioned variants.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
// External tagging avoids Serde's buffered internally-tagged decoder, which
// cannot deserialize the i128 used by Decimal128 bounds.
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum AdvancedStatisticsContents {
    /// Number of distinct tuples excluding rows with any NULL target value.
    /// Targets are an unordered set of at least two durable columns.
    JointNdv { distinct_count: u64 },
    /// One scalar column, strictly ascending homogeneous upper bounds. All observed
    /// non-null values must be covered; the final bucket is not an overflow bucket.
    Histogram { buckets: Vec<HistogramBucket> },
}

impl AdvancedStatisticsContents {
    fn kind(&self) -> &'static str {
        match self {
            Self::JointNdv { .. } => "joint_ndv",
            Self::Histogram { .. } => "histogram",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateAdvancedStatisticsInfo {
    pub relation_id: i64,
    /// Unique among active advanced objects of the relation, independent of kind.
    pub statistics_name: String,
    /// Canonicalized on publication; duplicates and invisible columns are rejected.
    pub column_ids: Vec<i64>,
    pub contents: AdvancedStatisticsContents,
}

/// Only contents can change. Owner, name, targets, and kind remain immutable.
#[derive(Debug, Clone, PartialEq)]
pub struct UpdateAdvancedStatisticsInfo {
    pub statistics_id: i64,
    pub contents: AdvancedStatisticsContents,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DropAdvancedStatisticsInfo {
    pub statistics_id: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AdvancedStatisticsInfo {
    pub statistics_id: i64,
    pub relation_id: i64,
    pub statistics_name: String,
    pub column_ids: Vec<i64>,
    pub begin_snapshot: SnapshotId,
    pub end_snapshot: Option<SnapshotId>,
    pub contents: AdvancedStatisticsContents,
}

fn invalid(reason: impl Into<String>) -> CatalogError {
    CatalogError::InvalidAdvancedStatistics {
        reason: reason.into(),
    }
}

fn visible(snapshot: &SnapshotInfo) -> Condition {
    let s = snapshot.snapshot_id.as_i64();
    Condition::all().add(Column::BeginSnapshot.lte(s)).add(
        Condition::any()
            .add(Column::EndSnapshot.is_null())
            .add(Column::EndSnapshot.gt(s)),
    )
}

pub(crate) async fn get<C: ConnectionTrait>(
    db: &C,
    snapshot: &SnapshotInfo,
    id: i64,
) -> CatalogResult<Option<entity::Model>> {
    Ok(entity::Entity::find()
        .filter(Column::StatisticsId.eq(id))
        .filter(visible(snapshot))
        .one(db)
        .await?)
}

pub(crate) async fn list<C: ConnectionTrait>(
    db: &C,
    snapshot: &SnapshotInfo,
    relation_id: i64,
) -> CatalogResult<Vec<entity::Model>> {
    Ok(entity::Entity::find()
        .filter(Column::RelationId.eq(relation_id))
        .filter(visible(snapshot))
        .order_by_asc(Column::StatisticsId)
        .all(db)
        .await?)
}

pub(crate) fn decode(
    row: entity::Model,
    relation: &RelationInfo,
) -> CatalogResult<AdvancedStatisticsInfo> {
    if row.value_encoding_version != ENCODING_VERSION {
        return Err(invalid(format!(
            "unsupported advanced encoding {}",
            row.value_encoding_version
        )));
    }
    let columns = decode_column_ids(row.column_ids)?;
    let contents: AdvancedStatisticsContents =
        serde_json::from_str(&row.contents).map_err(|e| invalid(e.to_string()))?;
    if contents.kind() != row.statistics_kind {
        return Err(invalid("stored kind does not match contents"));
    }
    validate(&columns, &contents, relation)?;
    Ok(AdvancedStatisticsInfo {
        statistics_id: row.statistics_id,
        relation_id: row.relation_id,
        statistics_name: row.statistics_name,
        column_ids: columns,
        begin_snapshot: SnapshotId::from_raw(row.begin_snapshot),
        end_snapshot: row.end_snapshot.map(SnapshotId::from_raw),
        contents,
    })
}

fn bound_matches(value: &StatisticsValue, ty: &DataType) -> bool {
    match (value, ty) {
        (StatisticsValue::Boolean(_), DataType::Boolean)
        | (StatisticsValue::Int32(_), DataType::Int32)
        | (StatisticsValue::Int64(_), DataType::Int64)
        | (StatisticsValue::Utf8(_), DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View)
        | (StatisticsValue::Date32(_), DataType::Date32) => true,
        (StatisticsValue::Float64(v), DataType::Float64) => v.is_finite(),
        (
            StatisticsValue::Decimal128 {
                value,
                precision,
                scale,
            },
            DataType::Decimal128(p, s),
        ) => {
            precision == p
                && scale == s
                && (1..=38).contains(p)
                && value.unsigned_abs() < 10_u128.pow(u32::from(*p))
        }
        _ => false,
    }
}

fn compare_bounds(a: &StatisticsValue, b: &StatisticsValue) -> Option<Ordering> {
    match (a, b) {
        (StatisticsValue::Boolean(a), StatisticsValue::Boolean(b)) => a.partial_cmp(b),
        (StatisticsValue::Int32(a), StatisticsValue::Int32(b)) => a.partial_cmp(b),
        (StatisticsValue::Int64(a), StatisticsValue::Int64(b)) => a.partial_cmp(b),
        (StatisticsValue::Float64(a), StatisticsValue::Float64(b)) => a.partial_cmp(b),
        (StatisticsValue::Utf8(a), StatisticsValue::Utf8(b)) => a.partial_cmp(b),
        (StatisticsValue::Date32(a), StatisticsValue::Date32(b)) => a.partial_cmp(b),
        (
            StatisticsValue::Decimal128 { value: a, .. },
            StatisticsValue::Decimal128 { value: b, .. },
        ) => a.partial_cmp(b),
        _ => None,
    }
}

fn validate(
    columns: &[i64],
    contents: &AdvancedStatisticsContents,
    relation: &RelationInfo,
) -> CatalogResult<()> {
    for id in columns {
        if !relation.columns.iter().any(|c| c.column_id == *id) {
            return Err(CatalogError::StatisticsColumnNotFound {
                relation_id: relation.relation_id,
                column_id: *id,
            });
        }
    }
    match contents {
        AdvancedStatisticsContents::JointNdv { .. } if columns.len() >= 2 => Ok(()),
        AdvancedStatisticsContents::JointNdv { .. } => {
            Err(invalid("joint NDV needs at least two columns"))
        }
        AdvancedStatisticsContents::Histogram { buckets } => {
            if columns.len() != 1 || buckets.is_empty() {
                return Err(invalid(
                    "histogram needs one column and at least one bucket",
                ));
            }
            let ty = &relation
                .columns
                .iter()
                .find(|c| c.column_id == columns[0])
                .expect("validated column")
                .column_type
                .0;
            if buckets.iter().any(|b| !bound_matches(&b.upper_bound, ty)) {
                return Err(invalid(
                    "histogram bounds must be finite and match the target column type",
                ));
            }
            if buckets.windows(2).any(|b| {
                compare_bounds(&b[0].upper_bound, &b[1].upper_bound) != Some(Ordering::Less)
            }) {
                return Err(invalid("histogram bounds must be strictly ascending"));
            }
            buckets
                .iter()
                .try_fold(0_u64, |sum, b| sum.checked_add(b.frequency))
                .ok_or_else(|| invalid("histogram frequency total overflows u64"))?;
            Ok(())
        }
    }
}

pub(crate) async fn create<C: ConnectionTrait>(
    db: &C,
    snapshot: &SnapshotInfo,
    id: i64,
    new_snapshot: i64,
    info: CreateAdvancedStatisticsInfo,
) -> CatalogResult<()> {
    if info.statistics_name.trim().is_empty() {
        return Err(invalid("name must not be empty"));
    }
    let columns = encode_column_ids(info.column_ids)?;
    let canonical = decode_column_ids(columns.clone())?;
    let relation = get_relation(
        GetRelationInfo {
            relation_id: info.relation_id,
        },
        db,
        snapshot,
    )
    .await?
    .ok_or(CatalogError::RelationNotFound {
        relation_id: info.relation_id,
    })?;
    validate(&canonical, &info.contents, &relation)?;
    if entity::Entity::find()
        .filter(Column::RelationId.eq(info.relation_id))
        .filter(Column::StatisticsName.eq(&info.statistics_name))
        .filter(visible(snapshot))
        .one(db)
        .await?
        .is_some()
    {
        return Err(CatalogError::AdvancedStatisticsAlreadyExists {
            relation_id: info.relation_id,
            statistics_name: info.statistics_name,
        });
    }
    entity::Entity::insert(entity::ActiveModel {
        row_id: NotSet,
        statistics_id: Set(id),
        relation_id: Set(info.relation_id),
        statistics_name: Set(info.statistics_name),
        statistics_kind: Set(info.contents.kind().to_owned()),
        column_ids: Set(columns),
        begin_snapshot: Set(new_snapshot),
        end_snapshot: Set(None),
        value_encoding_version: Set(ENCODING_VERSION),
        contents: Set(serde_json::to_string(&info.contents).map_err(|e| invalid(e.to_string()))?),
        collected_at: Set(ChronoUtc::now()),
    })
    .exec(db)
    .await?;
    Ok(())
}

pub(crate) async fn update<C: ConnectionTrait>(
    db: &C,
    snapshot: &SnapshotInfo,
    new_snapshot: i64,
    info: UpdateAdvancedStatisticsInfo,
) -> CatalogResult<()> {
    let old = get(db, snapshot, info.statistics_id).await?.ok_or(
        CatalogError::AdvancedStatisticsNotFound {
            statistics_id: info.statistics_id,
        },
    )?;
    if old.statistics_kind != info.contents.kind() {
        return Err(invalid("update cannot change statistics kind"));
    }
    let relation = get_relation(
        GetRelationInfo {
            relation_id: old.relation_id,
        },
        db,
        snapshot,
    )
    .await?
    .ok_or(CatalogError::RelationNotFound {
        relation_id: old.relation_id,
    })?;
    validate(
        &decode_column_ids(old.column_ids.clone())?,
        &info.contents,
        &relation,
    )?;
    close(db, info.statistics_id, new_snapshot).await?;
    entity::Entity::insert(entity::ActiveModel {
        row_id: NotSet,
        statistics_id: Set(old.statistics_id),
        relation_id: Set(old.relation_id),
        statistics_name: Set(old.statistics_name),
        statistics_kind: Set(old.statistics_kind),
        column_ids: Set(old.column_ids),
        begin_snapshot: Set(new_snapshot),
        end_snapshot: Set(None),
        value_encoding_version: Set(ENCODING_VERSION),
        contents: Set(serde_json::to_string(&info.contents).map_err(|e| invalid(e.to_string()))?),
        collected_at: Set(ChronoUtc::now()),
    })
    .exec(db)
    .await?;
    Ok(())
}

pub(crate) async fn close<C: ConnectionTrait>(db: &C, id: i64, end: i64) -> CatalogResult<()> {
    entity::Entity::update_many()
        .col_expr(Column::EndSnapshot, Expr::value(end))
        .filter(Column::StatisticsId.eq(id))
        .filter(Column::EndSnapshot.is_null())
        .exec(db)
        .await?;
    Ok(())
}

pub(crate) async fn close_relation<C: ConnectionTrait>(
    db: &C,
    relation_id: i64,
    end: i64,
) -> CatalogResult<()> {
    entity::Entity::update_many()
        .col_expr(Column::EndSnapshot, Expr::value(end))
        .filter(Column::RelationId.eq(relation_id))
        .filter(Column::EndSnapshot.is_null())
        .exec(db)
        .await?;
    Ok(())
}
