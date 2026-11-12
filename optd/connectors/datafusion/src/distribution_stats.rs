//! Build catalog [`ColumnDistributionStatistics`] (MCV + residual equi-depth histogram)
//! from a full column scan.

use std::cmp::Ordering;

use optd_core::{
    ColumnDistributionStatistics, Histogram, HistogramBucket, MAX_HISTOGRAM_BUCKETS,
    MAX_MOST_COMMON_VALUES, MostCommonValue, MostCommonValues, ScalarValue, StatisticsCompleteness,
    StatisticsProvenance, scalar_order,
};

/// Builds MCV + residual equi-depth histogram from non-null column values.
///
/// Most-common values are the top frequencies (up to [`MAX_MOST_COMMON_VALUES`]). Remaining rows
/// form an equi-depth residual histogram with at most [`MAX_HISTOGRAM_BUCKETS`] buckets.
pub(crate) fn build_column_distribution(
    values: &[&ScalarValue],
) -> Option<ColumnDistributionStatistics> {
    if values.is_empty() {
        return None;
    }

    for value in values {
        // Skip columns that cannot participate in ordered histograms / MCV uniqueness.
        scalar_order(value, value)?;
    }

    let mut sorted = values.iter().map(|value| (*value).clone()).collect::<Vec<_>>();
    sorted.sort_by(|left, right| scalar_order(left, right).unwrap_or(Ordering::Equal));

    let mut by_freq = Vec::new();
    let mut index = 0usize;
    while index < sorted.len() {
        let value = sorted[index].clone();
        let mut frequency = 0usize;
        while index < sorted.len()
            && scalar_order(&sorted[index], &value).is_some_and(|order| order.is_eq())
        {
            frequency += 1;
            index += 1;
        }
        by_freq.push((value, frequency));
    }
    by_freq.sort_by(|(left_value, left_freq), (right_value, right_freq)| {
        right_freq
            .cmp(left_freq)
            .then_with(|| scalar_order(left_value, right_value).unwrap_or(Ordering::Equal))
    });

    let mcv_count = by_freq.len().min(MAX_MOST_COMMON_VALUES);
    let mcv_complete = by_freq.len() <= MAX_MOST_COMMON_VALUES;
    let mcv_entries = by_freq[..mcv_count]
        .iter()
        .map(|(value, frequency)| MostCommonValue {
            value: value.clone(),
            frequency: *frequency,
        })
        .collect::<Vec<_>>();
    let most_common_values = MostCommonValues::try_new(
        mcv_entries,
        if mcv_complete {
            StatisticsCompleteness::Complete
        } else {
            StatisticsCompleteness::Partial
        },
        StatisticsProvenance::FullScan,
    )
    .ok()?;

    let residual = if mcv_complete {
        Vec::new()
    } else {
        let mut residual = by_freq[mcv_count..].to_vec();
        residual.sort_by(|(left, _), (right, _)| {
            scalar_order(left, right).unwrap_or(Ordering::Equal)
        });
        residual
    };

    let histogram = if residual.is_empty() {
        None
    } else {
        Some(build_equi_depth_histogram(&residual)?)
    };

    Some(ColumnDistributionStatistics {
        null_count: None,
        most_common_values: Some(most_common_values),
        histogram,
    })
}

fn build_equi_depth_histogram(
    residual: &[(ScalarValue, usize)],
) -> Option<Histogram> {
    let total: usize = residual.iter().map(|(_, frequency)| *frequency).sum();
    if total == 0 {
        return None;
    }
    let target_buckets = MAX_HISTOGRAM_BUCKETS.min(residual.len()).max(1);
    let target_per_bucket = total.div_ceil(target_buckets);

    let mut buckets = Vec::new();
    let mut index = 0usize;
    while index < residual.len() && buckets.len() < MAX_HISTOGRAM_BUCKETS {
        let lower = residual[index].0.clone();
        let mut upper = lower.clone();
        let mut frequency = 0usize;
        while index < residual.len() {
            let (value, count) = &residual[index];
            if frequency > 0 && frequency + count > target_per_bucket && buckets.len() + 1 < target_buckets
            {
                break;
            }
            upper = value.clone();
            frequency += count;
            index += 1;
            if frequency >= target_per_bucket && buckets.len() + 1 < target_buckets {
                break;
            }
        }
        buckets.push(HistogramBucket {
            lower_bound: lower,
            upper_bound: upper,
            frequency,
        });
    }

    Histogram::try_new(
        buckets,
        StatisticsCompleteness::Complete,
        StatisticsProvenance::FullScan,
    )
    .ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_complete_mcv_when_distinct_fits() {
        let values = [
            ScalarValue::Int64(1),
            ScalarValue::Int64(1),
            ScalarValue::Int64(2),
            ScalarValue::Int64(3),
        ];
        let refs = values.iter().collect::<Vec<_>>();
        let distribution = build_column_distribution(&refs).unwrap();
        let mcv = distribution.most_common_values.unwrap();
        assert_eq!(mcv.completeness(), StatisticsCompleteness::Complete);
        assert!(distribution.histogram.is_none());
        assert_eq!(mcv.entries()[0].value, ScalarValue::Int64(1));
        assert_eq!(mcv.entries()[0].frequency, 2);
    }

    #[test]
    fn builds_residual_histogram_when_mcv_is_partial() {
        let mut values = Vec::new();
        // One heavy hitter and many singleton residuals.
        for _ in 0..50 {
            values.push(ScalarValue::Int64(0));
        }
        for index in 1..=140 {
            values.push(ScalarValue::Int64(index));
        }
        let refs = values.iter().collect::<Vec<_>>();
        let distribution = build_column_distribution(&refs).unwrap();
        let mcv = distribution.most_common_values.as_ref().unwrap();
        assert_eq!(mcv.completeness(), StatisticsCompleteness::Partial);
        assert_eq!(mcv.entries()[0].value, ScalarValue::Int64(0));
        let hist = distribution.histogram.as_ref().unwrap();
        assert!(!hist.buckets().is_empty());
        assert!(hist.buckets().len() <= MAX_HISTOGRAM_BUCKETS);
        // Top MAX_MOST_COMMON_VALUES by frequency: heavy hitter 0 plus 127 of the 140
        // singletons → 13 residual singleton rows in the histogram.
        let residual_rows: usize = hist.buckets().iter().map(|bucket| bucket.frequency).sum();
        assert_eq!(residual_rows, 140 - (MAX_MOST_COMMON_VALUES - 1));
    }
}
