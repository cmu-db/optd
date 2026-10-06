use std::fmt;

/// A mergeable HyperLogLog distinct-value sketch over canonical byte encodings.
///
/// Compatibility requires identical precision, hash seed, and encoding version. The encoding
/// version belongs to the caller because this crate intentionally does not know planner values.
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HyperLogLog {
    precision: u8,
    hash_seed: u64,
    encoding_version: u16,
    registers: Box<[u8]>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HyperLogLogError {
    InvalidPrecision(u8),
    InvalidRegisterCount {
        precision: u8,
        actual: usize,
    },
    InvalidRegisterValue {
        precision: u8,
        index: usize,
        actual: u8,
    },
    Incompatible,
}

impl fmt::Display for HyperLogLogError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPrecision(precision) => {
                write!(
                    f,
                    "HyperLogLog precision must be in 4..=18, got {precision}"
                )
            }
            Self::InvalidRegisterCount { precision, actual } => write!(
                f,
                "HyperLogLog precision {precision} requires {} registers, got {actual}",
                1usize << precision,
            ),
            Self::InvalidRegisterValue {
                precision,
                index,
                actual,
            } => write!(
                f,
                "HyperLogLog register {index} has rank {actual}, exceeding the maximum for precision {precision}",
            ),
            Self::Incompatible => f.write_str(
                "HyperLogLog sketches have different precision, hash seed, encoding version, or register count",
            ),
        }
    }
}

impl std::error::Error for HyperLogLogError {}

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for HyperLogLog {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(serde::Deserialize)]
        struct SerializedHyperLogLog {
            precision: u8,
            hash_seed: u64,
            encoding_version: u16,
            registers: Vec<u8>,
        }

        let serialized = SerializedHyperLogLog::deserialize(deserializer)?;
        Self::from_registers(
            serialized.precision,
            serialized.hash_seed,
            serialized.encoding_version,
            serialized.registers,
        )
        .map_err(serde::de::Error::custom)
    }
}

impl HyperLogLog {
    pub const DEFAULT_PRECISION: u8 = 12;
    pub const DEFAULT_HASH_SEED: u64 = 0x9e37_79b9_7f4a_7c15;

    pub fn new(encoding_version: u16) -> Self {
        Self::with_config(
            Self::DEFAULT_PRECISION,
            Self::DEFAULT_HASH_SEED,
            encoding_version,
        )
        .expect("the default HyperLogLog configuration is valid")
    }

    pub fn with_config(
        precision: u8,
        hash_seed: u64,
        encoding_version: u16,
    ) -> Result<Self, HyperLogLogError> {
        if !(4..=18).contains(&precision) {
            return Err(HyperLogLogError::InvalidPrecision(precision));
        }
        Ok(Self {
            precision,
            hash_seed,
            encoding_version,
            registers: vec![0; 1usize << precision].into_boxed_slice(),
        })
    }

    pub fn from_registers(
        precision: u8,
        hash_seed: u64,
        encoding_version: u16,
        registers: Vec<u8>,
    ) -> Result<Self, HyperLogLogError> {
        if !(4..=18).contains(&precision) {
            return Err(HyperLogLogError::InvalidPrecision(precision));
        }
        let expected = 1usize << precision;
        if registers.len() != expected {
            return Err(HyperLogLogError::InvalidRegisterCount {
                precision,
                actual: registers.len(),
            });
        }
        let max_rank = 64 - precision + 1;
        if let Some((index, actual)) = registers
            .iter()
            .copied()
            .enumerate()
            .find(|(_, rank)| *rank > max_rank)
        {
            return Err(HyperLogLogError::InvalidRegisterValue {
                precision,
                index,
                actual,
            });
        }
        Ok(Self {
            precision,
            hash_seed,
            encoding_version,
            registers: registers.into_boxed_slice(),
        })
    }

    pub fn precision(&self) -> u8 {
        self.precision
    }

    pub fn hash_seed(&self) -> u64 {
        self.hash_seed
    }

    pub fn encoding_version(&self) -> u16 {
        self.encoding_version
    }

    pub fn registers(&self) -> &[u8] {
        &self.registers
    }

    pub fn is_compatible(&self, other: &Self) -> bool {
        self.precision == other.precision
            && self.hash_seed == other.hash_seed
            && self.encoding_version == other.encoding_version
            && self.registers.len() == other.registers.len()
    }

    pub fn insert(&mut self, encoded_value: &[u8]) {
        self.insert_hash(murmur2_64a(encoded_value, self.hash_seed));
    }

    pub fn merge(&mut self, other: &Self) -> Result<(), HyperLogLogError> {
        if !self.is_compatible(other) {
            return Err(HyperLogLogError::Incompatible);
        }
        for (left, right) in self.registers.iter_mut().zip(other.registers.iter()) {
            *left = (*left).max(*right);
        }
        Ok(())
    }

    pub fn estimate(&self) -> f64 {
        let m = self.registers.len() as f64;
        let alpha = match self.registers.len() {
            16 => 0.673,
            32 => 0.697,
            64 => 0.709,
            _ => 0.7213 / (1.0 + 1.079 / m),
        };
        let inverse_sum = self
            .registers
            .iter()
            .map(|register| 2_f64.powi(-i32::from(*register)))
            .sum::<f64>();
        let raw = alpha * m * m / inverse_sum;
        let zero_registers = self
            .registers
            .iter()
            .filter(|register| **register == 0)
            .count();
        if raw <= 2.5 * m && zero_registers > 0 {
            m * (m / zero_registers as f64).ln()
        } else {
            raw
        }
    }

    fn insert_hash(&mut self, hash: u64) {
        let index_mask = (1_u64 << self.precision) - 1;
        let index = (hash & index_mask) as usize;
        let remaining = hash >> self.precision;
        let max_rank = 64 - u32::from(self.precision) + 1;
        let rank = (remaining.trailing_zeros() + 1).min(max_rank) as u8;
        self.registers[index] = self.registers[index].max(rank);
    }
}

/// MurmurHash64A. A fixed implementation keeps serialized sketches stable across processes.
fn murmur2_64a(bytes: &[u8], seed: u64) -> u64 {
    const M: u64 = 0xc6a4_a793_5bd1_e995;
    const R: u32 = 47;

    let mut hash = seed ^ (bytes.len() as u64).wrapping_mul(M);
    let mut chunks = bytes.chunks_exact(8);
    for chunk in &mut chunks {
        let mut value = u64::from_le_bytes(chunk.try_into().expect("chunk length is eight"));
        value = value.wrapping_mul(M);
        value ^= value >> R;
        value = value.wrapping_mul(M);
        hash ^= value;
        hash = hash.wrapping_mul(M);
    }

    let remainder = chunks.remainder();
    let mut tail = 0_u64;
    for (shift, byte) in remainder.iter().enumerate() {
        tail |= u64::from(*byte) << (shift * 8);
    }
    if !remainder.is_empty() {
        hash ^= tail;
        hash = hash.wrapping_mul(M);
    }

    hash ^= hash >> R;
    hash = hash.wrapping_mul(M);
    hash ^ (hash >> R)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn estimates_distinct_values_and_ignores_duplicates() {
        let mut sketch = HyperLogLog::new(1);
        for value in 0_u64..20_000 {
            sketch.insert(&value.to_le_bytes());
            sketch.insert(&value.to_le_bytes());
        }
        let relative_error = (sketch.estimate() - 20_000.0).abs() / 20_000.0;
        assert!(relative_error < 0.08, "relative error was {relative_error}");
    }

    #[cfg(feature = "serde")]
    #[test]
    fn rejects_malformed_serialized_registers() {
        let wrong_count = r#"{"precision":4,"hash_seed":1,"encoding_version":1,"registers":[0]}"#;
        assert!(serde_json::from_str::<HyperLogLog>(wrong_count).is_err());

        let invalid_rank = format!(
            r#"{{"precision":4,"hash_seed":1,"encoding_version":1,"registers":[{}]}}"#,
            std::iter::repeat_n("62", 16).collect::<Vec<_>>().join(",")
        );
        assert!(serde_json::from_str::<HyperLogLog>(&invalid_rank).is_err());
    }

    #[test]
    fn merges_only_compatible_sketches() {
        let mut left = HyperLogLog::new(1);
        let mut right = HyperLogLog::new(1);
        for value in 0_u64..1_000 {
            left.insert(&value.to_le_bytes());
        }
        for value in 1_000_u64..2_000 {
            right.insert(&value.to_le_bytes());
        }
        left.merge(&right).unwrap();
        assert!((left.estimate() - 2_000.0).abs() / 2_000.0 < 0.1);

        let incompatible = HyperLogLog::new(2);
        assert_eq!(
            left.merge(&incompatible),
            Err(HyperLogLogError::Incompatible)
        );
    }
}
