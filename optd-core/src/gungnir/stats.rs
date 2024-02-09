// The Gungnir™ team licenses this file to you under the MIT License (MIT);
// you may not use this file except in compliance with the License.
//
// Author: Alexis Schlomer <aschlome@andrew.cmu.edu>
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.

use arrow_schema::DataType;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::collections::HashMap;
use std::fs::File;

use super::tdigest::TDigest;

pub struct Stats {
    pub table_stats: HashMap<String, TableStats>,
}

pub struct TableStats {
    row_cnt: usize,
    column_stats: HashMap<usize, ColumnStats>,
}

/// Represents the statistics of a one-dimensional column for a table in memory.
/// NOTE: Subject to change if we use T-Digest.
pub struct ColumnStatsData<T: PartialOrd> {
    name: String,                // Table name || Attribute name.
    n_distinct: u32,             // Number of unique values in the column.
    most_common_vals: Vec<T>,    // i.e. MCV.
    most_common_freqs: Vec<f64>, // Associated frequency of MCV.
    histogram_bounds: Vec<T>,    // Assuming equi-height histrograms.
}

/// Enumeration of currently supported data types in Gungnir™.
pub enum ColumnStats {
    Int(ColumnStatsData<i32>),
    Float(ColumnStatsData<f64>),
    String(ColumnStatsData<String>),
}

/// Returns the statistics over all columns of the Parquet file.
pub fn compute_stats(
    path: &str,
) -> Result<HashMap<usize, ColumnStats>, Box<dyn std::error::Error>> {
    // Obtain a batch iterator over the Parquet file.
    let mut reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?.build()?;

    // Initialize an empty statistics array.
    let stats = HashMap::new();

    // Iterate over all record batches.
    while let Some(batch_result) = reader.next() {
        let batch = batch_result?;

        for column in batch.columns() {
            match column.data_type() {
                DataType::Float64 => {
                    // Cast in proper type and iterate over values.
                }
                _ => {
                    // Skip unsupported data types.
                }
            }
        }
    }

    Ok(stats)
}

pub fn t_digest() {
    let mut t_digest = TDigest::new(4.0);

    // Add some sample data
    /*for i in 0..1000 {
        t_digest.add(i as f64);
        // Perform compression only after every 10 data points
        if i % 100 == 0 {
            t_digest.compress();
        }
    }

    // Calculate and print some quantiles
    println!("Quantile at 0.01: {}", t_digest.quantile(0.001));
    println!("Quantile at 0.01: {}", t_digest.quantile(0.01));
    println!("Quantile at 0.25: {}", t_digest.quantile(0.25));
    println!("Quantile at 0.5: {}", t_digest.quantile(0.5));
    println!("Quantile at 0.75: {}", t_digest.quantile(0.75));
    println!("Quantile at 0.99: {}", t_digest.quantile(0.99));
    println!("Quantile at 0.999: {}", t_digest.quantile(0.999));*/
}
