use optd_core::rel_node::Value;
use optd_gungnir::stats::{counter::Counter, tdigest::TDigest};

use super::base_cost::{Distribution, MostCommonValues};

impl Distribution for TDigest {
    fn cdf(&self, value: &Value) -> f64 {
        match value {
            Value::Int8(i) => self.cdf(*i as f64),
            Value::Int16(i) => self.cdf(*i as f64),
            Value::Int32(i) => self.cdf(*i as f64),
            Value::Int64(i) => self.cdf(*i as f64),
            Value::Int128(i) => self.cdf(*i as f64),
            Value::Float(i) => self.cdf(*i.0),
            _ => panic!("Value is not a number"),
        }
    }
}

impl MostCommonValues for Counter<Value> {
    fn freq(&self, value: &Value) -> Option<f64> {
        self.frequencies().get(value).copied()
    }

    fn total_freq(&self) -> f64 {
        self.frequencies().values().sum()
    }

    fn freq_over_pred(&self, pred: Box<dyn Fn(&Value) -> bool>) -> f64 {
        self.frequencies()
            .iter()
            .filter(|(val, _)| pred(val))
            .map(|(_, freq)| freq)
            .sum()
    }

    fn cnt(&self) -> usize {
        self.frequencies().len()
    }
}
