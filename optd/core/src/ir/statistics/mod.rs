mod histogram;

use std::ops::{Bound, RangeBounds};

pub use histogram::{Bucket, Histogram};
use snafu::whatever;

use crate::error::Result;
use crate::ir::ScalarValue;

#[derive(Debug, Clone, PartialEq)]
pub enum ValueHistogram {
    Int32(Histogram<i32>),
    Int64(Histogram<i64>),
}

impl ValueHistogram {
    pub fn total_count(&self) -> f64 {
        match self {
            ValueHistogram::Int32(hist) => hist.total_count(),
            ValueHistogram::Int64(hist) => hist.total_count(),
        }
    }

    pub fn total_distinct(&self) -> f64 {
        match self {
            ValueHistogram::Int32(hist) => hist.total_distinct(),
            ValueHistogram::Int64(hist) => hist.total_distinct(),
        }
    }

    pub fn estimate_equal(&self, value: &ScalarValue) -> Result<[f64; 2]> {
        match (self, value) {
            (ValueHistogram::Int32(hist), ScalarValue::Int32(Some(v))) => {
                Ok(hist.estimate_equal(v))
            }
            (ValueHistogram::Int64(hist), ScalarValue::Int64(Some(v))) => {
                Ok(hist.estimate_equal(v))
            }
            _ => whatever!("Value type does not match histogram type"),
        }
    }

    fn into_i32_bound(bound: Bound<&ScalarValue>) -> Result<Bound<i32>> {
        match bound {
            Bound::Included(ScalarValue::Int32(Some(v))) => Ok(Bound::Included(*v)),
            Bound::Excluded(ScalarValue::Int32(Some(v))) => Ok(Bound::Excluded(*v)),
            Bound::Unbounded => Ok(Bound::Unbounded),
            Bound::Included(_) | Bound::Excluded(_) => {
                whatever!("Value type does not match histogram type")
            }
        }
    }

    fn into_i64_bound(bound: Bound<&ScalarValue>) -> Result<Bound<i64>> {
        match bound {
            Bound::Included(ScalarValue::Int64(Some(v))) => Ok(Bound::Included(*v)),
            Bound::Excluded(ScalarValue::Int64(Some(v))) => Ok(Bound::Excluded(*v)),
            Bound::Unbounded => Ok(Bound::Unbounded),
            Bound::Included(_) | Bound::Excluded(_) => {
                whatever!("Value type does not match histogram type")
            }
        }
    }

    pub fn estimate_range(&self, bounds: &impl RangeBounds<ScalarValue>) -> Result<[f64; 2]> {
        match self {
            ValueHistogram::Int32(hist) => {
                // Convert bounds to Option<i32>
                let start = Self::into_i32_bound(bounds.start_bound())?;
                let end = Self::into_i32_bound(bounds.end_bound())?;
                Ok(hist.estimate_range(&(start, end)))
            }
            ValueHistogram::Int64(hist) => {
                // Convert bounds to Option<i64>
                let start = Self::into_i64_bound(bounds.start_bound())?;
                let end = Self::into_i64_bound(bounds.end_bound())?;
                Ok(hist.estimate_range(&(start, end)))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use histogram::Bucket;

    fn create_test_histogram_i32() -> Histogram<i32> {
        Histogram::new(vec![
            Bucket::new(10, 50., 5.),
            Bucket::new(20, 100., 10.),
            Bucket::new(30, 150., 15.),
        ])
    }

    fn create_test_histogram_i64() -> Histogram<i64> {
        Histogram::new(vec![
            Bucket::new(100, 200., 20.),
            Bucket::new(200, 300., 30.),
            Bucket::new(300, 400., 40.),
        ])
    }

    #[test]
    fn test_total_count_i32() {
        let hist = ValueHistogram::Int32(create_test_histogram_i32());
        assert_eq!(hist.total_count(), 300.0);
    }

    #[test]
    fn test_total_count_i64() {
        let hist = ValueHistogram::Int64(create_test_histogram_i64());
        assert_eq!(hist.total_count(), 900.0);
    }

    #[test]
    fn test_total_distinct_i32() {
        let hist = ValueHistogram::Int32(create_test_histogram_i32());
        assert_eq!(hist.total_distinct(), 30.0);
    }

    #[test]
    fn test_total_distinct_i64() {
        let hist = ValueHistogram::Int64(create_test_histogram_i64());
        assert_eq!(hist.total_distinct(), 90.0);
    }

    #[test]
    fn test_estimate_equal_i32_matching_type() {
        let hist = ValueHistogram::Int32(create_test_histogram_i32());
        let value = ScalarValue::Int32(Some(15));
        let result = hist.estimate_equal(&value).unwrap();
        // Value 15 is in bucket (10, 20] with count=100, distinct=10
        assert_eq!(result, [10.0, 1.0]);
    }

    #[test]
    fn test_estimate_equal_i64_matching_type() {
        let hist = ValueHistogram::Int64(create_test_histogram_i64());
        let value = ScalarValue::Int64(Some(150));
        let result = hist.estimate_equal(&value).unwrap();
        // Value 150 is in bucket (100, 200] with count=300, distinct=30
        assert_eq!(result, [10.0, 1.0]);
    }

    #[test]
    fn test_estimate_equal_type_mismatch_i32_i64() {
        let hist = ValueHistogram::Int32(create_test_histogram_i32());
        let value = ScalarValue::Int64(Some(15));
        let result = hist.estimate_equal(&value);
        assert!(result.is_err());
    }

    #[test]
    fn test_estimate_equal_type_mismatch_i64_i32() {
        let hist = ValueHistogram::Int64(create_test_histogram_i64());
        let value = ScalarValue::Int32(Some(150));
        let result = hist.estimate_equal(&value);
        assert!(result.is_err());
    }

    #[test]
    fn test_estimate_equal_null_value() {
        let hist = ValueHistogram::Int32(create_test_histogram_i32());
        let value = ScalarValue::Int32(None);
        let result = hist.estimate_equal(&value);
        assert!(result.is_err());
    }

    #[test]
    fn test_estimate_range_i32_inclusive() {
        let hist = ValueHistogram::Int32(create_test_histogram_i32());
        let start = ScalarValue::Int32(Some(10));
        let end = ScalarValue::Int32(Some(20));
        let result = hist.estimate_range(&(start..=end)).unwrap();
        // Should include full bucket (10, 20] with count=100, distinct=10
        assert_eq!(result, [100.0, 10.0]);
    }

    #[test]
    fn test_estimate_range_i32_exclusive() {
        let hist = ValueHistogram::Int32(create_test_histogram_i32());
        let start = ScalarValue::Int32(Some(10));
        let end = ScalarValue::Int32(Some(20));
        let result = hist.estimate_range(&(start..end)).unwrap();
        // Should include full first bucket plus partial second bucket
        let [count, distinct] = result;
        assert!(count >= 50.0 && count < 150.0);
        assert!(distinct >= 5.0 && distinct < 15.0);
    }

    #[test]
    fn test_estimate_range_i64_inclusive() {
        let hist = ValueHistogram::Int64(create_test_histogram_i64());
        let start = ScalarValue::Int64(Some(100));
        let end = ScalarValue::Int64(Some(200));
        let result = hist.estimate_range(&(start..=end)).unwrap();
        // Should include full bucket (100, 200] with count=300, distinct=30
        assert_eq!(result, [300.0, 30.0]);
    }

    #[test]
    fn test_estimate_range_i32_unbounded() {
        let hist = ValueHistogram::Int32(create_test_histogram_i32());
        let result = hist.estimate_range(&(..)).unwrap();
        // Should include all buckets
        assert_eq!(result, [300.0, 30.0]);
    }

    #[test]
    fn test_estimate_range_i32_unbounded_start() {
        let hist = ValueHistogram::Int32(create_test_histogram_i32());
        let end = ScalarValue::Int32(Some(20));
        let result = hist.estimate_range(&(..=end)).unwrap();
        // Should include first two buckets
        assert_eq!(result, [150.0, 15.0]);
    }

    #[test]
    fn test_estimate_range_i32_unbounded_end() {
        let hist = ValueHistogram::Int32(create_test_histogram_i32());
        let start = ScalarValue::Int32(Some(20));
        let result = hist.estimate_range(&(start..)).unwrap();
        // Should include partial second bucket plus full third bucket
        let [count, distinct] = result;
        assert!(count >= 150.0 && count <= 250.0);
        assert!(distinct >= 15.0 && distinct <= 25.0);
    }

    #[test]
    fn test_estimate_range_type_mismatch() {
        let hist = ValueHistogram::Int32(create_test_histogram_i32());
        let start = ScalarValue::Int64(Some(10));
        let end = ScalarValue::Int64(Some(20));
        let result = hist.estimate_range(&(start..=end));
        assert!(result.is_err());
    }

    #[test]
    fn test_into_i32_bound_included() {
        let value = ScalarValue::Int32(Some(42));
        let bound = ValueHistogram::into_i32_bound(Bound::Included(&value)).unwrap();
        assert_eq!(bound, Bound::Included(42));
    }

    #[test]
    fn test_into_i32_bound_excluded() {
        let value = ScalarValue::Int32(Some(42));
        let bound = ValueHistogram::into_i32_bound(Bound::Excluded(&value)).unwrap();
        assert_eq!(bound, Bound::Excluded(42));
    }

    #[test]
    fn test_into_i32_bound_unbounded() {
        let bound = ValueHistogram::into_i32_bound(Bound::Unbounded).unwrap();
        assert_eq!(bound, Bound::Unbounded);
    }

    #[test]
    fn test_into_i32_bound_wrong_type() {
        let value = ScalarValue::Int64(Some(42));
        let result = ValueHistogram::into_i32_bound(Bound::Included(&value));
        assert!(result.is_err());
    }

    #[test]
    fn test_into_i64_bound_included() {
        let value = ScalarValue::Int64(Some(42));
        let bound = ValueHistogram::into_i64_bound(Bound::Included(&value)).unwrap();
        assert_eq!(bound, Bound::Included(42));
    }

    #[test]
    fn test_into_i64_bound_excluded() {
        let value = ScalarValue::Int64(Some(42));
        let bound = ValueHistogram::into_i64_bound(Bound::Excluded(&value)).unwrap();
        assert_eq!(bound, Bound::Excluded(42));
    }

    #[test]
    fn test_into_i64_bound_unbounded() {
        let bound = ValueHistogram::into_i64_bound(Bound::Unbounded).unwrap();
        assert_eq!(bound, Bound::Unbounded);
    }

    #[test]
    fn test_into_i64_bound_wrong_type() {
        let value = ScalarValue::Int32(Some(42));
        let result = ValueHistogram::into_i64_bound(Bound::Included(&value));
        assert!(result.is_err());
    }

    #[test]
    fn test_empty_histogram_i32() {
        let hist = ValueHistogram::Int32(Histogram::new(vec![]));
        assert_eq!(hist.total_count(), 0.0);
        assert_eq!(hist.total_distinct(), 0.0);

        let value = ScalarValue::Int32(Some(42));
        let result = hist.estimate_equal(&value).unwrap();
        assert_eq!(result, [0.0, 0.0]);
    }

    #[test]
    fn test_single_bucket_i32() {
        let hist = ValueHistogram::Int32(Histogram::new(vec![Bucket::new(100, 50., 10.)]));
        assert_eq!(hist.total_count(), 50.0);
        assert_eq!(hist.total_distinct(), 10.0);

        let value = ScalarValue::Int32(Some(50));
        let result = hist.estimate_equal(&value).unwrap();
        assert_eq!(result, [5.0, 1.0]);
    }
}
