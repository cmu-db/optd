use gungnir::stats::tdigest::TDigest;
use optd_core::rel_node::Value;

use super::base_cost::Distribution;

impl Distribution for TDigest {
    fn cdf(&self, value: &Value) -> f64 {
        match value {
            Value::Int8(i) => self.cdf(*i as f64),
            Value::Int16(i) => self.cdf(*i as f64),
            Value::Int32(i) => self.cdf(*i as f64),
            Value::Int64(i) => self.cdf(*i as f64),
            Value::Int128(i) => self.cdf(*i as f64),
            Value::Float(i) => self.cdf(**i),
            _ => panic!("Value is not a number"),
        }
    }
}
