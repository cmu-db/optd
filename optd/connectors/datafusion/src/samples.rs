//! Install catalog table samples by reservoir-sampling registered DataFusion tables.
//!
//! This is the fixture path for tests and experiments: scan a registered table, retain up to
//! `block_size` rows with Algorithm R, and store them via [`Catalog::set_table_samples`]. Runtime
//! collection during query execution is intentionally out of scope for the first stack branch.

use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::prelude::SessionContext;
use optd_core::{Catalog, SampleProvenance, TableRef, TableSample, reservoir_sample};
use rand::SeedableRng;
use rand::rngs::StdRng;

/// Reservoir-samples up to `block_size` rows from `table_name` and stores them in `catalog`.
///
/// `source_rows`, when provided, is recorded as the table population for scaling. When omitted, the
/// number of rows observed while sampling is used. `seed` makes the reservoir deterministic.
pub async fn collect_and_set_table_samples(
    session: &SessionContext,
    catalog: &dyn Catalog,
    table_ref: TableRef,
    table_name: &str,
    block_size: usize,
    source_rows: Option<usize>,
    seed: u64,
) -> DFResult<TableSample> {
    let frame = session.table(table_name).await?;
    let batches = frame.collect().await?;
    if batches.is_empty() {
        return Err(DataFusionError::External(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("table '{table_name}' produced no record batches to sample"),
        ))));
    }

    let mut rng = StdRng::seed_from_u64(seed);
    let samples = reservoir_sample(
        batches,
        block_size,
        SampleProvenance::Past,
        source_rows,
        &mut rng,
    )
    .map_err(|error| DataFusionError::External(Box::new(error)))?;

    catalog
        .set_table_samples(table_ref, samples.clone())
        .map_err(|error| DataFusionError::External(Box::new(error)))?;
    Ok(samples)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use optd_core::MemoryCatalog;
    use std::sync::Arc;

    async fn session_with_table(rows: Vec<i64>) -> SessionContext {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(rows))]).unwrap();
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        let session = SessionContext::new();
        session.register_table("t", Arc::new(table)).unwrap();
        session
    }

    #[tokio::test]
    async fn installs_reservoir_sample_into_catalog() {
        let session = session_with_table((0..100).collect()).await;
        let catalog = MemoryCatalog::new("memory", "public");
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        catalog
            .create_table(TableRef::bare("t"), schema, None)
            .unwrap();

        let samples = collect_and_set_table_samples(
            &session,
            &catalog,
            TableRef::bare("t"),
            "t",
            8,
            Some(1000),
            9,
        )
        .await
        .unwrap();

        assert_eq!(samples.num_rows(), 8);
        assert_eq!(samples.block_size, 8);
        assert_eq!(samples.source_rows, 1000);
        assert_eq!(samples.provenance, SampleProvenance::Past);
        assert_eq!(
            catalog.table_samples(&TableRef::bare("t")).unwrap(),
            Some(samples)
        );
    }
}
