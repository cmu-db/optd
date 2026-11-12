//! Evaluate simple relational operators over Arrow sample batches.
//!
//! These helpers are intentionally IR-agnostic: callers translate planner expressions into
//! [`SamplePredicate`] / join key names before invoking them.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Date32Type, Float64Type, Int32Type, Int64Type};
use arrow_array::{Array, BooleanArray, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use arrow_select::concat::concat_batches;
use arrow_select::filter::filter_record_batch;
use arrow_select::take::take;

use crate::{SampleError, TableSample};

/// Scalar literal used by sample predicates.
#[derive(Debug, Clone, PartialEq)]
pub enum SampleScalar {
    Null,
    Boolean(bool),
    Int32(i32),
    Int64(i64),
    Float64(f64),
    /// Days since the UNIX epoch.
    Date32(i32),
    Utf8(String),
}

/// Comparison operator for sample predicates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SampleCompareOp {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
}

/// Predicate forms supported by sample filtering.
#[derive(Debug, Clone, PartialEq)]
pub enum SamplePredicate {
    /// `column <op> literal` (NULL never matches ordered/eq compares).
    CompareColumnLiteral {
        column: String,
        op: SampleCompareOp,
        value: SampleScalar,
    },
    /// `column <op> column` on the same relation.
    CompareColumns {
        left: String,
        op: SampleCompareOp,
        right: String,
    },
    /// SQL LIKE / ILIKE against a literal pattern (`%` / `_` wildcards).
    Like {
        column: String,
        pattern: String,
        negated: bool,
        case_insensitive: bool,
    },
    /// Conjunction of supported predicates.
    And(Vec<SamplePredicate>),
    /// Disjunction of supported predicates.
    Or(Vec<SamplePredicate>),
}

/// Intermediate sample relation used while walking an operator tree.
#[derive(Debug, Clone)]
pub struct SampleRelation {
    pub schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
    /// Product of leaf scale factors (`source_rows / sample_rows`) collected so far.
    pub scale: f64,
    /// Conservative lower bound on the scaled cardinality (LpBound / exact-leaf cases).
    pub lower: f64,
    /// Pessimistic upper bound on the scaled cardinality (LpBound max-degree bounds).
    pub upper: f64,
}

impl SampleRelation {
    /// Builds a relation from a catalog table sample, projecting `columns` by name when nonempty.
    pub fn from_table_sample(
        sample: &TableSample,
        columns: &[String],
    ) -> Result<Self, SampleError> {
        let sample_rows = sample.num_rows();
        let scale = if sample_rows == 0 {
            0.0
        } else {
            sample.source_rows as f64 / sample_rows as f64
        };
        let batches = if columns.is_empty() {
            sample.batches.clone()
        } else {
            project_batches(&sample.batches, columns)?
        };
        let schema = if columns.is_empty() {
            sample.schema.clone()
        } else if let Some(batch) = batches.first() {
            batch.schema()
        } else {
            let fields = columns
                .iter()
                .map(|name| {
                    sample
                        .schema
                        .field_with_name(name)
                        .cloned()
                        .map_err(|_| SampleError::MissingColumn(name.clone()))
                })
                .collect::<Result<Vec<_>, _>>()?;
            Arc::new(Schema::new(fields))
        };
        // A scan's population is known from collection metadata.
        let rows = sample.source_rows as f64;
        Ok(Self {
            schema,
            batches,
            scale,
            lower: rows,
            upper: rows,
        })
    }

    /// Total rows across batches before scaling.
    pub fn num_rows(&self) -> usize {
        self.batches.iter().map(|batch| batch.num_rows()).sum()
    }

    /// Umbra-style scaled cardinality estimate.
    pub fn estimated_rows(&self) -> f64 {
        self.num_rows() as f64 * self.scale
    }
}

/// Projects each batch onto `columns` (by field name, preserving order).
pub fn project_batches(
    batches: &[RecordBatch],
    columns: &[String],
) -> Result<Vec<RecordBatch>, SampleError> {
    if columns.is_empty() {
        return Ok(batches.to_vec());
    }
    let mut projected = Vec::with_capacity(batches.len());
    for batch in batches {
        let mut fields = Vec::with_capacity(columns.len());
        let mut arrays = Vec::with_capacity(columns.len());
        for name in columns {
            let index = batch
                .schema()
                .index_of(name)
                .map_err(|_| SampleError::MissingColumn(name.clone()))?;
            fields.push(batch.schema().field(index).clone());
            arrays.push(batch.column(index).clone());
        }
        let schema = Arc::new(Schema::new(fields));
        projected.push(
            RecordBatch::try_new(schema, arrays)
                .map_err(|error| SampleError::Arrow(error.to_string()))?,
        );
    }
    Ok(projected)
}

/// Filters sample batches with a supported predicate, preserving scale.
pub fn filter_relation(
    relation: &SampleRelation,
    predicate: &SamplePredicate,
) -> Result<SampleRelation, SampleError> {
    let mut filtered = Vec::with_capacity(relation.batches.len());
    for batch in &relation.batches {
        let mask = evaluate_predicate(batch, predicate)?;
        let kept = filter_record_batch(batch, &mask)
            .map_err(|error| SampleError::Arrow(error.to_string()))?;
        if kept.num_rows() > 0 {
            filtered.push(kept);
        }
    }
    Ok(SampleRelation {
        schema: relation.schema.clone(),
        batches: filtered,
        scale: relation.scale,
        // Filtering cannot increase cardinality; without residual selectivity proofs the lower
        // bound stays at zero while the upper inherits the input's pessimistic bound.
        lower: 0.0,
        upper: relation.upper,
    })
}

/// Fraction of `left` sample rows whose equi-key appears in the `right` sample key set.
///
/// Used for Rusu-style semi/anti **existence** estimates: unlike inclusion NDV formulas, this
/// observes actual key overlap in the samples (false negatives for match are possible when the
/// right sample misses a key that exists in the full right table).
pub fn key_presence_match_fraction(
    left: &SampleRelation,
    right: &SampleRelation,
    left_keys: &[String],
    right_keys: &[String],
) -> Result<f64, SampleError> {
    if left_keys.len() != right_keys.len() || left_keys.is_empty() {
        return Err(SampleError::InvalidJoinKeys);
    }
    let right_batch = concat_or_empty(right)?;
    let mut right_keys_set = HashSet::new();
    for row in 0..right_batch.num_rows() {
        if let Some(key) = row_key(&right_batch, right_keys, row)? {
            right_keys_set.insert(key);
        }
    }
    let left_batch = concat_or_empty(left)?;
    let total = left_batch.num_rows();
    if total == 0 {
        return Ok(0.0);
    }
    let mut matches = 0usize;
    for row in 0..total {
        let Some(key) = row_key(&left_batch, left_keys, row)? else {
            continue;
        };
        if right_keys_set.contains(&key) {
            matches += 1;
        }
    }
    Ok(matches as f64 / total as f64)
}

/// Fraction of sample rows matching `predicate` (`matches / n`). Returns `0.0` when `n == 0` or
/// when no rows match (callers that want a floor must apply it themselves).
pub fn match_fraction(
    relation: &SampleRelation,
    predicate: &SamplePredicate,
) -> Result<f64, SampleError> {
    let total = relation.num_rows();
    if total == 0 {
        return Ok(0.0);
    }
    let mut matches = 0usize;
    for batch in &relation.batches {
        let mask = evaluate_predicate(batch, predicate)?;
        for index in 0..mask.len() {
            if mask.is_valid(index) && mask.value(index) {
                matches += 1;
            }
        }
    }
    Ok(matches as f64 / total as f64)
}

/// Inner equijoin over one or more column pairs with the same names on each side.
///
/// Output columns are left columns followed by right columns. Duplicate names on the right are
/// suffixed with `_right` when they collide with a left name.
///
/// The pessimistic upper bound follows the LpBound max-degree formula:
/// `min(|L|·maxdeg(R), |R|·maxdeg(L), |L|·|R|)` with degrees scaled from the samples.
pub fn equijoin_relations(
    left: &SampleRelation,
    right: &SampleRelation,
    left_keys: &[String],
    right_keys: &[String],
) -> Result<SampleRelation, SampleError> {
    if left_keys.len() != right_keys.len() || left_keys.is_empty() {
        return Err(SampleError::InvalidJoinKeys);
    }

    let left_batch = concat_or_empty(left)?;
    let right_batch = concat_or_empty(right)?;
    let joined = hash_equijoin(&left_batch, &right_batch, left_keys, right_keys)?;
    let schema = joined.schema();
    let batches = if joined.num_rows() == 0 {
        Vec::new()
    } else {
        vec![joined]
    };
    let upper = lpbound_equijoin_upper(left, right, left_keys, right_keys)?;
    Ok(SampleRelation {
        schema,
        batches,
        scale: left.scale * right.scale,
        lower: 0.0,
        upper,
    })
}

/// Pessimistic upper bound for an inner equijoin from scaled sample max degrees.
pub fn lpbound_equijoin_upper(
    left: &SampleRelation,
    right: &SampleRelation,
    left_keys: &[String],
    right_keys: &[String],
) -> Result<f64, SampleError> {
    let left_rows = left.upper;
    let right_rows = right.upper;
    let left_max_degree = max_key_frequency(left, left_keys)? as f64 * left.scale;
    let right_max_degree = max_key_frequency(right, right_keys)? as f64 * right.scale;
    Ok((left_rows * right_max_degree)
        .min(right_rows * left_max_degree)
        .min(left_rows * right_rows))
}

fn max_key_frequency(relation: &SampleRelation, keys: &[String]) -> Result<usize, SampleError> {
    if relation.num_rows() == 0 {
        return Ok(0);
    }
    let batch = concat_or_empty(relation)?;
    let mut counts: HashMap<Vec<u8>, usize> = HashMap::new();
    let mut max = 0usize;
    for row in 0..batch.num_rows() {
        let Some(key) = row_key(&batch, keys, row)? else {
            continue;
        };
        let entry = counts.entry(key).or_insert(0);
        *entry += 1;
        max = max.max(*entry);
    }
    Ok(max)
}

fn concat_or_empty(relation: &SampleRelation) -> Result<RecordBatch, SampleError> {
    if relation.num_rows() == 0 {
        return Ok(RecordBatch::new_empty(relation.schema.clone()));
    }
    concat_batches(&relation.schema, &relation.batches)
        .map_err(|error| SampleError::Arrow(error.to_string()))
}

fn evaluate_predicate(
    batch: &RecordBatch,
    predicate: &SamplePredicate,
) -> Result<BooleanArray, SampleError> {
    match predicate {
        SamplePredicate::And(parts) => {
            if parts.is_empty() {
                return Ok(BooleanArray::from(vec![true; batch.num_rows()]));
            }
            let mut mask = evaluate_predicate(batch, &parts[0])?;
            for part in &parts[1..] {
                let next = evaluate_predicate(batch, part)?;
                mask = combine_bool(&mask, &next, true)?;
            }
            Ok(mask)
        }
        SamplePredicate::Or(parts) => {
            if parts.is_empty() {
                return Ok(BooleanArray::from(vec![false; batch.num_rows()]));
            }
            let mut mask = evaluate_predicate(batch, &parts[0])?;
            for part in &parts[1..] {
                let next = evaluate_predicate(batch, part)?;
                mask = combine_bool(&mask, &next, false)?;
            }
            Ok(mask)
        }
        SamplePredicate::CompareColumnLiteral { column, op, value } => {
            compare_column_literal(batch, column, *op, value)
        }
        SamplePredicate::CompareColumns { left, op, right } => {
            compare_columns(batch, left, *op, right)
        }
        SamplePredicate::Like {
            column,
            pattern,
            negated,
            case_insensitive,
        } => like_column(batch, column, pattern, *negated, *case_insensitive),
    }
}

fn combine_bool(
    left: &BooleanArray,
    right: &BooleanArray,
    and: bool,
) -> Result<BooleanArray, SampleError> {
    if left.len() != right.len() {
        return Err(SampleError::Arrow("boolean mask length mismatch".into()));
    }
    let values: Vec<Option<bool>> = (0..left.len())
        .map(|index| {
            let l = left.is_valid(index).then(|| left.value(index));
            let r = right.is_valid(index).then(|| right.value(index));
            match (l, r) {
                (Some(l), Some(r)) => Some(if and { l && r } else { l || r }),
                _ => Some(false),
            }
        })
        .collect();
    Ok(BooleanArray::from(values))
}

fn compare_column_literal(
    batch: &RecordBatch,
    column: &str,
    op: SampleCompareOp,
    value: &SampleScalar,
) -> Result<BooleanArray, SampleError> {
    let array = batch
        .column_by_name(column)
        .ok_or_else(|| SampleError::MissingColumn(column.to_string()))?;
    if matches!(value, SampleScalar::Null) {
        return Ok(BooleanArray::from(vec![false; batch.num_rows()]));
    }
    match (array.data_type(), value) {
        (DataType::Int64, SampleScalar::Int64(expected)) => {
            let array = array.as_primitive::<Int64Type>();
            Ok(BooleanArray::from(
                array
                    .iter()
                    .map(|cell| Some(cell.is_some_and(|v| cmp_ord(op, &v, expected))))
                    .collect::<Vec<_>>(),
            ))
        }
        (DataType::Int32, SampleScalar::Int32(expected)) => {
            let array = array.as_primitive::<Int32Type>();
            Ok(BooleanArray::from(
                array
                    .iter()
                    .map(|cell| Some(cell.is_some_and(|v| cmp_ord(op, &v, expected))))
                    .collect::<Vec<_>>(),
            ))
        }
        (DataType::Date32, SampleScalar::Date32(expected)) => {
            let array = array.as_primitive::<Date32Type>();
            Ok(BooleanArray::from(
                array
                    .iter()
                    .map(|cell| Some(cell.is_some_and(|v| cmp_ord(op, &v, expected))))
                    .collect::<Vec<_>>(),
            ))
        }
        (DataType::Float64, SampleScalar::Float64(expected)) => {
            let array = array.as_primitive::<Float64Type>();
            Ok(BooleanArray::from(
                array
                    .iter()
                    .map(|cell| {
                        Some(cell.is_some_and(|v| {
                            expected.is_finite() && v.is_finite() && cmp_ord(op, &v, expected)
                        }))
                    })
                    .collect::<Vec<_>>(),
            ))
        }
        (DataType::Utf8, SampleScalar::Utf8(expected)) => {
            let array = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| SampleError::Arrow(format!("column '{column}' is not utf8")))?;
            Ok(BooleanArray::from(
                array
                    .iter()
                    .map(|cell| Some(cell.is_some_and(|v| cmp_ord(op, v, expected.as_str()))))
                    .collect::<Vec<_>>(),
            ))
        }
        (DataType::Boolean, SampleScalar::Boolean(expected)) => {
            let array = array.as_boolean();
            Ok(BooleanArray::from(
                array
                    .iter()
                    .map(|cell| Some(cell.is_some_and(|v| cmp_ord(op, &v, expected))))
                    .collect::<Vec<_>>(),
            ))
        }
        (data_type, _) => Err(SampleError::UnsupportedPredicate(format!(
            "compare {op:?} between {data_type} and {value:?}"
        ))),
    }
}

fn compare_columns(
    batch: &RecordBatch,
    left: &str,
    op: SampleCompareOp,
    right: &str,
) -> Result<BooleanArray, SampleError> {
    let left_array = batch
        .column_by_name(left)
        .ok_or_else(|| SampleError::MissingColumn(left.to_string()))?;
    let right_array = batch
        .column_by_name(right)
        .ok_or_else(|| SampleError::MissingColumn(right.to_string()))?;
    if left_array.data_type() != right_array.data_type() {
        return Err(SampleError::UnsupportedPredicate(format!(
            "column compare type mismatch {} vs {}",
            left_array.data_type(),
            right_array.data_type()
        )));
    }
    match left_array.data_type() {
        DataType::Int64 => {
            let left = left_array.as_primitive::<Int64Type>();
            let right = right_array.as_primitive::<Int64Type>();
            Ok(BooleanArray::from(
                (0..batch.num_rows())
                    .map(|row| {
                        Some(
                            !left.is_null(row)
                                && !right.is_null(row)
                                && cmp_ord(op, &left.value(row), &right.value(row)),
                        )
                    })
                    .collect::<Vec<_>>(),
            ))
        }
        DataType::Int32 => {
            let left = left_array.as_primitive::<Int32Type>();
            let right = right_array.as_primitive::<Int32Type>();
            Ok(BooleanArray::from(
                (0..batch.num_rows())
                    .map(|row| {
                        Some(
                            !left.is_null(row)
                                && !right.is_null(row)
                                && cmp_ord(op, &left.value(row), &right.value(row)),
                        )
                    })
                    .collect::<Vec<_>>(),
            ))
        }
        DataType::Date32 => {
            let left = left_array.as_primitive::<Date32Type>();
            let right = right_array.as_primitive::<Date32Type>();
            Ok(BooleanArray::from(
                (0..batch.num_rows())
                    .map(|row| {
                        Some(
                            !left.is_null(row)
                                && !right.is_null(row)
                                && cmp_ord(op, &left.value(row), &right.value(row)),
                        )
                    })
                    .collect::<Vec<_>>(),
            ))
        }
        DataType::Utf8 => {
            let left = left_array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| SampleError::Arrow("expected utf8".into()))?;
            let right = right_array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| SampleError::Arrow("expected utf8".into()))?;
            Ok(BooleanArray::from(
                (0..batch.num_rows())
                    .map(|row| {
                        Some(
                            !left.is_null(row)
                                && !right.is_null(row)
                                && cmp_ord(op, left.value(row), right.value(row)),
                        )
                    })
                    .collect::<Vec<_>>(),
            ))
        }
        other => Err(SampleError::UnsupportedPredicate(format!(
            "unsupported column compare type {other}"
        ))),
    }
}

fn like_column(
    batch: &RecordBatch,
    column: &str,
    pattern: &str,
    negated: bool,
    case_insensitive: bool,
) -> Result<BooleanArray, SampleError> {
    let array = batch
        .column_by_name(column)
        .ok_or_else(|| SampleError::MissingColumn(column.to_string()))?;
    let array = array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| SampleError::UnsupportedPredicate(format!("LIKE on non-utf8 '{column}'")))?;
    Ok(BooleanArray::from(
        array
            .iter()
            .map(|cell| {
                let matched = cell.is_some_and(|value| {
                    sql_like_match(value, pattern, case_insensitive)
                });
                Some(if negated { !matched } else { matched })
            })
            .collect::<Vec<_>>(),
    ))
}

fn cmp_ord<T: PartialOrd + ?Sized>(op: SampleCompareOp, left: &T, right: &T) -> bool {
    match op {
        SampleCompareOp::Eq => left == right,
        SampleCompareOp::NotEq => left != right,
        SampleCompareOp::Lt => left < right,
        SampleCompareOp::LtEq => left <= right,
        SampleCompareOp::Gt => left > right,
        SampleCompareOp::GtEq => left >= right,
    }
}

/// SQL LIKE with `%` (any run) and `_` (one char). No escape character.
fn sql_like_match(text: &str, pattern: &str, case_insensitive: bool) -> bool {
    if case_insensitive {
        sql_like_match_chars(
            &text.to_lowercase().chars().collect::<Vec<_>>(),
            &pattern.to_lowercase().chars().collect::<Vec<_>>(),
        )
    } else {
        sql_like_match_chars(
            &text.chars().collect::<Vec<_>>(),
            &pattern.chars().collect::<Vec<_>>(),
        )
    }
}

fn sql_like_match_chars(text: &[char], pattern: &[char]) -> bool {
    let mut t = 0usize;
    let mut p = 0usize;
    let mut star_p = None;
    let mut star_t = 0usize;
    while t < text.len() {
        if p < pattern.len() && (pattern[p] == '_' || pattern[p] == text[t]) {
            t += 1;
            p += 1;
        } else if p < pattern.len() && pattern[p] == '%' {
            star_p = Some(p);
            star_t = t;
            p += 1;
        } else if let Some(sp) = star_p {
            p = sp + 1;
            star_t += 1;
            t = star_t;
        } else {
            return false;
        }
    }
    while p < pattern.len() && pattern[p] == '%' {
        p += 1;
    }
    p == pattern.len()
}

fn hash_equijoin(
    left: &RecordBatch,
    right: &RecordBatch,
    left_keys: &[String],
    right_keys: &[String],
) -> Result<RecordBatch, SampleError> {
    let mut index: HashMap<Vec<u8>, Vec<usize>> = HashMap::new();
    for row in 0..right.num_rows() {
        if let Some(key) = row_key(right, right_keys, row)? {
            index.entry(key).or_default().push(row);
        }
    }

    let mut left_indices = Vec::new();
    let mut right_indices = Vec::new();
    for row in 0..left.num_rows() {
        let Some(key) = row_key(left, left_keys, row)? else {
            continue;
        };
        if let Some(matches) = index.get(&key) {
            for &right_row in matches {
                left_indices.push(row as u32);
                right_indices.push(right_row as u32);
            }
        }
    }

    let left_taken = take_indices(left, &left_indices)?;
    let right_taken = take_indices(right, &right_indices)?;
    concat_horizontal(&left_taken, &right_taken)
}

fn row_key(
    batch: &RecordBatch,
    keys: &[String],
    row: usize,
) -> Result<Option<Vec<u8>>, SampleError> {
    let mut key = Vec::new();
    for name in keys {
        let array = batch
            .column_by_name(name)
            .ok_or_else(|| SampleError::MissingColumn(name.clone()))?;
        if array.is_null(row) {
            return Ok(None);
        }
        encode_key_value(array, row, &mut key)?;
        key.push(0xff);
    }
    Ok(Some(key))
}

fn encode_key_value(array: &dyn Array, row: usize, out: &mut Vec<u8>) -> Result<(), SampleError> {
    match array.data_type() {
        DataType::Int64 => {
            let value = array.as_primitive::<Int64Type>().value(row);
            out.extend_from_slice(&value.to_le_bytes());
        }
        DataType::Int32 => {
            let value = array.as_primitive::<Int32Type>().value(row);
            out.extend_from_slice(&value.to_le_bytes());
        }
        DataType::Utf8 => {
            let array = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| SampleError::Arrow("expected utf8 join key".into()))?;
            out.extend_from_slice(array.value(row).as_bytes());
        }
        DataType::Boolean => {
            out.push(u8::from(array.as_boolean().value(row)));
        }
        other => {
            return Err(SampleError::UnsupportedPredicate(format!(
                "unsupported join key type {other}"
            )));
        }
    }
    Ok(())
}

fn take_indices(batch: &RecordBatch, indices: &[u32]) -> Result<RecordBatch, SampleError> {
    let indices = arrow_array::UInt32Array::from(indices.to_vec());
    let arrays = batch
        .columns()
        .iter()
        .map(|column| {
            take(column.as_ref(), &indices, None)
                .map_err(|error| SampleError::Arrow(error.to_string()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    RecordBatch::try_new(batch.schema(), arrays)
        .map_err(|error| SampleError::Arrow(error.to_string()))
}

fn concat_horizontal(left: &RecordBatch, right: &RecordBatch) -> Result<RecordBatch, SampleError> {
    let left_names: std::collections::HashSet<_> = left
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    let mut fields: Vec<Field> = left
        .schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    let mut columns = left.columns().to_vec();
    for (field, column) in right.schema().fields().iter().zip(right.columns()) {
        let name = if left_names.contains(field.name()) {
            format!("{}_right", field.name())
        } else {
            field.name().clone()
        };
        fields.push(Field::new(
            name,
            field.data_type().clone(),
            field.is_nullable(),
        ));
        columns.push(column.clone());
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map_err(|error| SampleError::Arrow(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field, Schema};

    use crate::{SampleProvenance, TableSample};

    fn batch(ids: &[i64], names: &[&str]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids.to_vec())),
                Arc::new(StringArray::from(
                    names.iter().map(|name| Some(*name)).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap()
    }

    #[test]
    fn filters_equality_predicate_and_scales() {
        let sample = TableSample::try_new(
            batch(&[1, 2, 2, 3], &["a", "b", "b", "c"]).schema(),
            vec![batch(&[1, 2, 2, 3], &["a", "b", "b", "c"])],
            4,
            400,
            SampleProvenance::Past,
        )
        .unwrap();
        let relation = SampleRelation::from_table_sample(&sample, &[]).unwrap();
        assert_eq!(relation.estimated_rows(), 400.0);

        let filtered = filter_relation(
            &relation,
            &SamplePredicate::CompareColumnLiteral {
                column: "id".into(),
                op: SampleCompareOp::Eq,
                value: SampleScalar::Int64(2),
            },
        )
        .unwrap();
        assert_eq!(filtered.num_rows(), 2);
        assert_eq!(filtered.estimated_rows(), 200.0);
        assert_eq!(
            match_fraction(
                &relation,
                &SamplePredicate::CompareColumnLiteral {
                    column: "id".into(),
                    op: SampleCompareOp::Eq,
                    value: SampleScalar::Int64(2),
                },
            )
            .unwrap(),
            0.5
        );
    }

    #[test]
    fn filters_date32_range_and_reports_zero_match_fraction() {
        use arrow_array::Date32Array;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "d",
            DataType::Date32,
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Date32Array::from(vec![
                Some(10),
                Some(20),
                Some(30),
                Some(40),
            ]))],
        )
        .unwrap();
        let sample =
            TableSample::try_new(schema, vec![batch], 4, 400, SampleProvenance::Past).unwrap();
        let relation = SampleRelation::from_table_sample(&sample, &[]).unwrap();
        let predicate = SamplePredicate::CompareColumnLiteral {
            column: "d".into(),
            op: SampleCompareOp::Lt,
            value: SampleScalar::Date32(25),
        };
        assert_eq!(match_fraction(&relation, &predicate).unwrap(), 0.5);
        let none = SamplePredicate::CompareColumnLiteral {
            column: "d".into(),
            op: SampleCompareOp::Gt,
            value: SampleScalar::Date32(100),
        };
        assert_eq!(match_fraction(&relation, &none).unwrap(), 0.0);
    }

    #[test]
    fn like_matches_percent_wildcards() {
        let relation = SampleRelation::from_table_sample(
            &TableSample::try_new(
                batch(&[1, 2, 3], &["alpha", "beta", "alp"]).schema(),
                vec![batch(&[1, 2, 3], &["alpha", "beta", "alp"])],
                3,
                30,
                SampleProvenance::Past,
            )
            .unwrap(),
            &[],
        )
        .unwrap();
        let predicate = SamplePredicate::Like {
            column: "name".into(),
            pattern: "alp%".into(),
            negated: false,
            case_insensitive: false,
        };
        assert_eq!(match_fraction(&relation, &predicate).unwrap(), 2.0 / 3.0);
    }

    #[test]
    fn key_presence_match_fraction_counts_left_rows_with_right_keys() {
        let left = SampleRelation {
            schema: batch(&[1, 2, 3, 4], &["a", "b", "c", "d"]).schema(),
            batches: vec![batch(&[1, 2, 3, 4], &["a", "b", "c", "d"])],
            scale: 1.0,
            lower: 4.0,
            upper: 4.0,
        };
        let right = SampleRelation {
            schema: batch(&[1, 2], &["x", "y"]).schema(),
            batches: vec![batch(&[1, 2], &["x", "y"])],
            scale: 1.0,
            lower: 2.0,
            upper: 2.0,
        };
        assert_eq!(
            key_presence_match_fraction(&left, &right, &["id".into()], &["id".into()]).unwrap(),
            0.5
        );
    }

    #[test]
    fn equijoins_and_multiplies_scale_factors() {
        let left_batch = batch(&[1, 2, 2], &["a", "b", "c"]);
        let right_batch = batch(&[2, 3], &["x", "y"]);
        let left = SampleRelation {
            schema: left_batch.schema(),
            batches: vec![left_batch],
            scale: 10.0,
            lower: 30.0,
            upper: 30.0,
        };
        let right = SampleRelation {
            schema: right_batch.schema(),
            batches: vec![right_batch],
            scale: 5.0,
            lower: 10.0,
            upper: 10.0,
        };
        let joined = equijoin_relations(&left, &right, &["id".into()], &["id".into()]).unwrap();
        // left rows with id=2 appear twice against one right row → 2 output rows
        assert_eq!(joined.num_rows(), 2);
        assert_eq!(joined.scale, 50.0);
        assert_eq!(joined.estimated_rows(), 100.0);
        // maxdeg_left_sample=2 → scaled 20; maxdeg_right_sample=1 → scaled 5
        // min(30*5, 10*20, 30*10) = min(150, 200, 300) = 150
        assert_eq!(joined.upper, 150.0);
        assert_eq!(joined.lower, 0.0);
    }
}
