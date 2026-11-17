//! Sketch-corrected bound-corrected grouping NDV estimation (SCBC).
//!
//! Based on *Every Row Counts* (CIDR 2019), §3.2–3.3.3.
//!
//! Hash-map frequency computation is intentionally a simple prototype of §4. Samples must
//! represent the population passed by the caller; this helper does not join samples or fetch
//! base-table statistics. Reservoir samples use the paper's without-replacement lower bound.

use std::collections::HashMap;

use arrow_array::{Array, BooleanArray, Float64Array, Int32Array, Int64Array, StringArray};
use arrow_schema::DataType;

use crate::SampleRelation;

/// Estimates grouping-tuple NDV with sketch-corrected bound correction (SCBC).
///
/// `column_ndvs` are population NDVs **including NULL as one value** when present.
/// Callers must add known population NULL presence to non-null HLL/profile estimates, even
/// when rare NULLs miss the sample. NULL is one grouping value, not an omitted row. Returns `None` for empty/undersized
/// samples, invalid inputs, missing columns, or unsupported types. Full-population samples
/// return their exact tuple count. Results obey observed/marginal/product/population bounds.
pub fn estimate_grouping_scbc(
    sample: &SampleRelation,
    keys: &[String],
    column_ndvs: &[f64],
    population_rows: f64,
) -> Option<f64> {
    let n = sample.num_rows() as f64;
    if keys.len() < 2
        || keys.len() != column_ndvs.len()
        || n < 2.0
        || !population_rows.is_finite()
        || population_rows < n
        || column_ndvs.iter().any(|d| !d.is_finite() || *d < 0.0)
    {
        return None;
    }
    let mut tuples = HashMap::<Vec<Vec<u8>>, usize>::new();
    let mut columns = vec![HashMap::<Vec<u8>, usize>::new(); keys.len()];
    for batch in &sample.batches {
        let arrays = keys
            .iter()
            .map(|key| batch.column_by_name(key))
            .collect::<Option<Vec<_>>>()?;
        for row in 0..batch.num_rows() {
            let mut tuple = Vec::with_capacity(keys.len());
            for (j, array) in arrays.iter().enumerate() {
                let value = grouping_value(array.as_ref(), row)?;
                *columns[j].entry(value.clone()).or_default() += 1;
                tuple.push(value);
            }
            *tuples.entry(tuple).or_default() += 1;
        }
    }
    let d = tuples.len() as f64;
    if n == population_rows {
        return Some(d);
    }
    let f1 = tuples.values().filter(|&&count| count == 1).count() as f64;
    let repeated = d - f1; // sum_{i>=2} f_i, NOT the number of repeated rows
    let ndvs: Vec<_> = column_ndvs
        .iter()
        .enumerate()
        .map(|(j, &ndv)| ndv.max(columns[j].len() as f64).min(population_rows))
        .collect();
    let max_ndv = ndvs.iter().copied().fold(0.0, f64::max);
    let product = ndvs
        .iter()
        .fold(1.0, |acc, &d| (acc * d).min(population_rows));
    let max_singletons = columns
        .iter()
        .zip(&ndvs)
        .map(|(freq, &ndv)| {
            let repeated = freq.values().filter(|&&count| count >= 2).count() as f64;
            (ndv - repeated).max(0.0)
        })
        .fold(0.0, f64::max);

    // Eq. 5, without-replacement variant at the end of §3.2. ln_1p avoids cancellation
    // for tiny reservoir fractions. The census case above avoids log(0) at r=1.
    let r = n / population_rows;
    let log_remaining = (-r).ln_1p();
    let threshold_fraction = (log_remaining * (1.0 / r - 1.0)).exp();
    let lower_d = if f1 == 0.0 {
        0.0
    } else if f1 >= n * threshold_fraction {
        population_rows / ((f1 / n).ln() / log_remaining + 1.0)
    } else {
        f1 / threshold_fraction
    }
    .clamp(d, population_rows);
    // Eq. 6 is also a conservative upper bound for sampling without replacement.
    let observed_fraction = -(n * (-1.0 / population_rows).ln_1p()).exp_m1();
    let upper_d = (d / observed_fraction).clamp(d, population_rows);
    let lower_bc = f1.max(lower_d - repeated);
    let upper_bc = (population_rows * (f1 / n)).min(upper_d - repeated);
    // Eqs. 16–18, with sanity bounds (11). Approximate sketch bounds can cross;
    // preserve the lower evidence bound and apply final marginal/product caps.
    let lower = lower_bc.max(max_singletons);
    let upper = upper_bc.min(product).max(lower);
    let result = lower.sqrt() * upper.sqrt() + repeated;
    let minimum = d.max(max_ndv);
    Some(result.clamp(minimum, product.max(minimum).min(population_rows)))
}

// Column position supplies the type; length-delimited Vec components avoid string collisions.
fn grouping_value(array: &dyn Array, row: usize) -> Option<Vec<u8>> {
    if array.is_null(row) {
        return Some(vec![0]);
    }
    let mut out = vec![1];
    match array.data_type() {
        DataType::Int64 => out.extend_from_slice(
            &array
                .as_any()
                .downcast_ref::<Int64Array>()?
                .value(row)
                .to_le_bytes(),
        ),
        DataType::Int32 => out.extend_from_slice(
            &array
                .as_any()
                .downcast_ref::<Int32Array>()?
                .value(row)
                .to_le_bytes(),
        ),
        DataType::Date32 => out.extend_from_slice(
            &array
                .as_any()
                .downcast_ref::<arrow_array::Date32Array>()?
                .value(row)
                .to_le_bytes(),
        ),
        DataType::Float64 => {
            let value = array.as_any().downcast_ref::<Float64Array>()?.value(row);
            let bits = if value == 0.0 {
                0
            } else if value.is_nan() {
                f64::NAN.to_bits()
            } else {
                value.to_bits()
            };
            out.extend_from_slice(&bits.to_le_bytes());
        }
        DataType::Boolean => out.push(u8::from(
            array.as_any().downcast_ref::<BooleanArray>()?.value(row),
        )),
        DataType::Utf8 => out.extend_from_slice(
            array
                .as_any()
                .downcast_ref::<StringArray>()?
                .value(row)
                .as_bytes(),
        ),
        _ => return None,
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{SampleProvenance, TableSample};
    use arrow_array::RecordBatch;
    use arrow_schema::{Field, Schema};
    use std::sync::Arc;

    fn sample(a: Vec<Option<i64>>, b: Vec<Option<i64>>, population: usize) -> SampleRelation {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
        ]));
        let n = a.len();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(a)), Arc::new(Int64Array::from(b))],
        )
        .unwrap();
        let table = TableSample::try_new(
            schema,
            vec![batch],
            n.max(1),
            population,
            SampleProvenance::Past,
        )
        .unwrap();
        SampleRelation::from_table_sample(&table, &[]).unwrap()
    }
    fn keys() -> Vec<String> {
        vec!["a".into(), "b".into()]
    }

    #[test]
    fn scbc_correlated_columns_beat_product() {
        let values: Vec<_> = (0..200).map(|i| Some(i % 100)).collect();
        let s = sample(values.clone(), values, 100_000);
        let estimate = estimate_grouping_scbc(&s, &keys(), &[100.0, 100.0], 100_000.0).unwrap();
        assert_eq!(estimate, 100.0);
        assert!((estimate - 100.0).abs() < (10_000.0_f64 - 100.0).abs());
    }

    #[test]
    fn scbc_nulls_census_and_invalid_inputs() {
        let s = sample(
            vec![None, None, Some(1), Some(1)],
            vec![None, None, Some(1), Some(2)],
            4,
        );
        assert_eq!(
            estimate_grouping_scbc(&s, &keys(), &[1.0, 2.0], 4.0),
            Some(3.0)
        );
        let nulls = sample(vec![None; 10], vec![None; 10], 100);
        assert_eq!(
            estimate_grouping_scbc(&nulls, &keys(), &[1.0, 1.0], 100.0),
            Some(1.0)
        );
        assert_eq!(
            estimate_grouping_scbc(&s, &keys(), &[f64::NAN, 1.0], 10.0),
            None
        );
        assert_eq!(estimate_grouping_scbc(&s, &keys(), &[1.0], 10.0), None);
        assert_eq!(estimate_grouping_scbc(&s, &keys(), &[1.0, 2.0], 2.0), None);
        let singleton = sample(vec![Some(1)], vec![Some(1)], 100);
        assert_eq!(
            estimate_grouping_scbc(&singleton, &keys(), &[1.0, 1.0], 100.0),
            None
        );
    }

    #[test]
    fn scbc_mixed_frequencies_follow_published_equation_17() {
        let s = sample(
            vec![Some(0), Some(0), Some(1)],
            vec![Some(0), Some(0), Some(1)],
            100,
        );
        let estimate = estimate_grouping_scbc(&s, &keys(), &[2.0, 2.0], 100.0).unwrap();
        let lower_d = 1.0 / 0.97_f64.powf(1.0 / 0.03 - 1.0);
        // Eq. 17 bounds F1 by product(D_j), not product(D_j) minus repeated classes.
        let expected = ((lower_d - 1.0) * 4.0).sqrt() + 1.0;
        assert!(
            (estimate - expected).abs() < 1e-12,
            "{estimate} vs {expected}"
        );
    }

    #[test]
    fn scbc_all_singletons_and_small_sampling_fraction_are_finite() {
        let v: Vec<_> = (0..100).map(Some).collect();
        let s = sample(v.clone(), v, 1_000_000);
        let e = estimate_grouping_scbc(&s, &keys(), &[1_000.0, 1_000.0], 1_000_000.0).unwrap();
        assert!(e.is_finite() && (1_000.0..=1_000_000.0).contains(&e));
    }
}
