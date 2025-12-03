use std::collections::BTreeMap;

use crate::ir::ScalarValue;

mod histogram;
mod histogram_simple;
mod mcvs;

pub use histogram_simple::{Bin as HistogramBin, Histogram};

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnStatistics {
    pub histogram: ScalarValueHistogram,
    pub count: usize,
    pub n_distinct: usize,
    pub mcvs: ScalarValueMCVs,
}

impl ColumnStatistics {
    pub fn dummy(count: usize) -> Self {
        let mut map = BTreeMap::new();
        map.insert(1, 1);
        Self {
            histogram: ScalarValueHistogram::Int32(histogram_simple::Histogram::new(vec![], 0)),
            count,
            n_distinct: 1,
            mcvs: ScalarValueMCVs::Int32(mcvs::MostCommonValues::new(map, 1)),
        }
    }

    pub fn new(
        histogram: ScalarValueHistogram,
        count: usize,
        n_distinct: usize,
        mcvs: ScalarValueMCVs,
    ) -> Self {
        Self {
            histogram,
            count,
            n_distinct,
            mcvs,
        }
    }

    pub fn frequency(&self, value: &ScalarValue) -> f64 {
        if let (count, 1) = self.mcvs.count(value) {
            count as f64 / self.count as f64
        } else {
            let non_mcv_n_distinct = self.n_distinct - self.mcvs.num_values();
            if non_mcv_n_distinct == 0 {
                0.0
            } else {
                let non_mcv_count = self.count - self.mcvs.total_count();
                (non_mcv_count as f64 / non_mcv_n_distinct as f64) / self.count as f64
            }
        }
    }
}

pub trait Distribution<T> {
    fn cdf(&self, value: &T) -> f64;
}

#[derive(Debug, Clone, PartialEq)]
pub enum ScalarValueMCVs {
    Int32(mcvs::MostCommonValues<i32>),
    Int64(mcvs::MostCommonValues<i64>),
}

impl ScalarValueMCVs {
    pub fn num_values(&self) -> usize {
        match self {
            ScalarValueMCVs::Int32(mcvs) => mcvs.num_values(),
            ScalarValueMCVs::Int64(mcvs) => mcvs.num_values(),
        }
    }

    pub fn total_count(&self) -> usize {
        match self {
            ScalarValueMCVs::Int32(mcvs) => mcvs.total_count(),
            ScalarValueMCVs::Int64(mcvs) => mcvs.total_count(),
        }
    }

    /// Returns the count and number of distinct values (=1) for the given value.
    pub fn count(&self, value: &ScalarValue) -> (usize, usize) {
        match (self, value) {
            (ScalarValueMCVs::Int32(mcvs), ScalarValue::Int32(Some(v))) => mcvs.count(v),
            (ScalarValueMCVs::Int64(mcvs), ScalarValue::Int64(Some(v))) => mcvs.count(v),
            _ => (0, 0),
        }
    }

    /// Returns the count and number of distinct values satisfying the given predicate.
    pub fn count_with(&self, predicate: impl Fn(&ScalarValue) -> bool) -> (usize, usize) {
        match self {
            ScalarValueMCVs::Int32(mcvs) => mcvs.count_with(|v| {
                let sv = ScalarValue::Int32(Some(*v));
                predicate(&sv)
            }),
            ScalarValueMCVs::Int64(mcvs) => mcvs.count_with(|v| {
                let sv = ScalarValue::Int64(Some(*v));
                predicate(&sv)
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ScalarValueHistogram {
    Int32(histogram_simple::Histogram<i32>),
    Int64(histogram_simple::Histogram<i64>),
}

impl Distribution<ScalarValue> for ScalarValueHistogram {
    fn cdf(&self, value: &ScalarValue) -> f64 {
        match (self, value) {
            (ScalarValueHistogram::Int32(histo), ScalarValue::Int32(Some(v))) => histo.cdf(*v),
            (ScalarValueHistogram::Int64(histo), ScalarValue::Int64(Some(v))) => histo.cdf(*v),
            _ => 0.0,
        }
    }
}
