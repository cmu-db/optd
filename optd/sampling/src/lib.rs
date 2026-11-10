//! Table samples for cardinality estimation.
//!
//! Samples are Arrow record batches wrapped with collection metadata so a catalog can store past
//! (or future current-run) reservoirs without embedding raw batches inside summary statistics.

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use arrow_select::concat::concat_batches;
use rand::Rng;

/// How a [`TableSample`] was obtained relative to the estimating query.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SampleProvenance {
    /// Collected before the current query (offline fixture, prior run, or catalog refresh).
    Past,
}

/// Arrow-backed sample rows for one table, plus the metadata estimators need to scale counts.
#[derive(Debug, Clone, PartialEq)]
pub struct TableSample {
    /// Schema of the sampled batches; must match the catalog table schema when installed.
    pub schema: SchemaRef,
    /// Sampled row batches. Combined row count is at most [`Self::block_size`].
    pub batches: Vec<RecordBatch>,
    /// Target reservoir capacity supplied at collection time.
    pub block_size: usize,
    /// Table population (row count) at collection time, used to scale sample cardinalities.
    pub source_rows: usize,
    /// Whether these rows came from a past collection or the current run.
    pub provenance: SampleProvenance,
}

impl TableSample {
    /// Builds a sample after validating that every batch matches `schema`.
    pub fn try_new(
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
        block_size: usize,
        source_rows: usize,
        provenance: SampleProvenance,
    ) -> Result<Self, SampleError> {
        if block_size == 0 {
            return Err(SampleError::ZeroBlockSize);
        }
        for batch in &batches {
            if batch.schema().as_ref() != schema.as_ref() {
                return Err(SampleError::SchemaMismatch);
            }
        }
        let rows = batches.iter().map(|batch| batch.num_rows()).sum::<usize>();
        if rows > block_size {
            return Err(SampleError::ExceedsBlockSize { rows, block_size });
        }
        Ok(Self {
            schema,
            batches,
            block_size,
            source_rows,
            provenance,
        })
    }

    /// Total number of sampled rows across all batches.
    pub fn num_rows(&self) -> usize {
        self.batches.iter().map(|batch| batch.num_rows()).sum()
    }
}

/// Errors produced while constructing or sampling table data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SampleError {
    ZeroBlockSize,
    SchemaMismatch,
    ExceedsBlockSize { rows: usize, block_size: usize },
    EmptyInput,
    Arrow(String),
}

impl std::fmt::Display for SampleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ZeroBlockSize => f.write_str("sample block_size must be nonzero"),
            Self::SchemaMismatch => f.write_str("sample batch schema does not match sample schema"),
            Self::ExceedsBlockSize { rows, block_size } => write!(
                f,
                "sample contains {rows} rows but block_size is {block_size}"
            ),
            Self::EmptyInput => f.write_str("cannot sample from zero input batches"),
            Self::Arrow(message) => write!(f, "arrow error: {message}"),
        }
    }
}

impl std::error::Error for SampleError {}

/// Online Algorithm-R reservoir over Arrow record batches.
///
/// Call [`ReservoirSampler::offer`] for each input batch, then [`ReservoirSampler::finish`].
#[derive(Debug)]
pub struct ReservoirSampler {
    capacity: usize,
    schema: Option<SchemaRef>,
    /// One-row batches currently in the reservoir (length ≤ capacity).
    selected: Vec<RecordBatch>,
    seen: usize,
    provenance: SampleProvenance,
}

impl ReservoirSampler {
    /// Creates a sampler that retains at most `capacity` rows.
    pub fn new(capacity: usize, provenance: SampleProvenance) -> Result<Self, SampleError> {
        if capacity == 0 {
            return Err(SampleError::ZeroBlockSize);
        }
        Ok(Self {
            capacity,
            schema: None,
            selected: Vec::with_capacity(capacity),
            seen: 0,
            provenance,
        })
    }

    /// Offers one batch of source rows to the reservoir.
    pub fn offer<R: Rng + ?Sized>(
        &mut self,
        batch: &RecordBatch,
        rng: &mut R,
    ) -> Result<(), SampleError> {
        if batch.num_rows() == 0 {
            if self.schema.is_none() {
                self.schema = Some(batch.schema());
            } else if self
                .schema
                .as_ref()
                .is_some_and(|schema| schema.as_ref() != batch.schema().as_ref())
            {
                return Err(SampleError::SchemaMismatch);
            }
            return Ok(());
        }

        match &self.schema {
            Some(schema) if schema.as_ref() != batch.schema().as_ref() => {
                return Err(SampleError::SchemaMismatch);
            }
            None => self.schema = Some(batch.schema()),
            Some(_) => {}
        }

        for row in 0..batch.num_rows() {
            self.seen += 1;
            let slice = batch.slice(row, 1);
            if self.selected.len() < self.capacity {
                self.selected.push(slice);
            } else {
                let j = rng.random_range(0..self.seen);
                if j < self.capacity {
                    self.selected[j] = slice;
                }
            }
        }
        Ok(())
    }

    /// Consumes the sampler and returns the retained sample.
    ///
    /// `source_rows` should be the full table population when known; when omitted, the number of
    /// rows offered to the sampler is used.
    pub fn finish(self, source_rows: Option<usize>) -> Result<TableSample, SampleError> {
        let schema = self.schema.ok_or(SampleError::EmptyInput)?;
        let source_rows = source_rows.unwrap_or(self.seen);
        let batches = if self.selected.is_empty() {
            Vec::new()
        } else {
            let combined = concat_batches(&schema, &self.selected)
                .map_err(|error| SampleError::Arrow(error.to_string()))?;
            vec![combined]
        };
        TableSample::try_new(schema, batches, self.capacity, source_rows, self.provenance)
    }
}

/// Reservoir-samples up to `capacity` rows from `batches` using Algorithm R.
pub fn reservoir_sample<I, R>(
    batches: I,
    capacity: usize,
    provenance: SampleProvenance,
    source_rows: Option<usize>,
    rng: &mut R,
) -> Result<TableSample, SampleError>
where
    I: IntoIterator<Item = RecordBatch>,
    R: Rng + ?Sized,
{
    let mut sampler = ReservoirSampler::new(capacity, provenance)?;
    let mut offered = false;
    for batch in batches {
        offered = true;
        sampler.offer(&batch, rng)?;
    }
    if !offered {
        return Err(SampleError::EmptyInput);
    }
    sampler.finish(source_rows)
}

/// Convenience constructor that wraps already-sampled batches without re-sampling.
pub fn table_sample_from_batches(
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    block_size: usize,
    source_rows: usize,
    provenance: SampleProvenance,
) -> Result<TableSample, SampleError> {
    TableSample::try_new(schema, batches, block_size, source_rows, provenance)
}

pub use arrow_array;
pub use arrow_schema;

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use rand::SeedableRng;
    use rand::rngs::StdRng;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    fn batch_with_ids(ids: &[i64]) -> RecordBatch {
        let schema = test_schema();
        let names: Vec<Option<&str>> = ids
            .iter()
            .map(|id| Some(if id % 2 == 0 { "even" } else { "odd" }))
            .collect();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids.to_vec())),
                Arc::new(StringArray::from(names)),
            ],
        )
        .unwrap()
    }

    #[test]
    fn reservoir_retains_all_rows_when_under_capacity() {
        let mut rng = StdRng::seed_from_u64(1);
        let sample = reservoir_sample(
            [batch_with_ids(&[1, 2, 3])],
            8,
            SampleProvenance::Past,
            Some(3),
            &mut rng,
        )
        .unwrap();

        assert_eq!(sample.block_size, 8);
        assert_eq!(sample.source_rows, 3);
        assert_eq!(sample.provenance, SampleProvenance::Past);
        assert_eq!(sample.num_rows(), 3);
        let ids = sample.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ids.values(), &[1, 2, 3]);
    }

    #[test]
    fn reservoir_retains_exactly_capacity_rows() {
        let mut rng = StdRng::seed_from_u64(7);
        let sample = reservoir_sample(
            [
                batch_with_ids(&(0..50).collect::<Vec<_>>()),
                batch_with_ids(&(50..100).collect::<Vec<_>>()),
            ],
            10,
            SampleProvenance::Past,
            Some(1000),
            &mut rng,
        )
        .unwrap();

        assert_eq!(sample.num_rows(), 10);
        assert_eq!(sample.block_size, 10);
        assert_eq!(sample.source_rows, 1000);
    }

    #[test]
    fn reservoir_rejects_zero_capacity() {
        assert!(matches!(
            ReservoirSampler::new(0, SampleProvenance::Past),
            Err(SampleError::ZeroBlockSize)
        ));
    }

    #[test]
    fn table_sample_rejects_schema_mismatch() {
        let schema = test_schema();
        let other = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)]));
        let batch =
            RecordBatch::try_new(other, vec![Arc::new(Int64Array::from(vec![1_i64]))]).unwrap();
        assert_eq!(
            TableSample::try_new(schema, vec![batch], 1, 1, SampleProvenance::Past),
            Err(SampleError::SchemaMismatch)
        );
    }
}
