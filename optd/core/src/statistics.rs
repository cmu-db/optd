use std::sync::Arc;

use optd_sketches::{HyperLogLog, SpaceSaving};

use crate::ScalarValue;

/// Version of the canonical scalar encoding used by optimizer sketches.
pub const SKETCH_VALUE_ENCODING_VERSION: u16 = 1;
/// Version of the framed tuple encoding used by column-group sketches.
pub const TUPLE_VALUE_ENCODING_VERSION: u16 = 1;

/// Canonical, type-tagged scalar bytes used by mergeable sketches.
///
/// Sketch compatibility is intentionally independent of display formatting and catalog JSON.
/// NULL is omitted because distinct-value and frequent-value sketches summarize non-null values.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EncodedScalarValue(Arc<[u8]>);

impl EncodedScalarValue {
    pub fn from_scalar(value: &ScalarValue) -> Option<Self> {
        let mut encoded = Vec::new();
        match value {
            ScalarValue::Null(_) => return None,
            ScalarValue::Boolean(value) => {
                encoded.push(1);
                encoded.push(u8::from(*value));
            }
            ScalarValue::Int32(value) => {
                encoded.push(2);
                encoded.extend_from_slice(&value.to_le_bytes());
            }
            ScalarValue::Int64(value) => {
                encoded.push(3);
                encoded.extend_from_slice(&value.to_le_bytes());
            }
            ScalarValue::Float64(value) => {
                encoded.push(4);
                let bits = if *value == 0.0 {
                    0.0_f64.to_bits()
                } else if value.is_nan() {
                    f64::NAN.to_bits()
                } else {
                    value.to_bits()
                };
                encoded.extend_from_slice(&bits.to_le_bytes());
            }
            ScalarValue::Decimal128 {
                value,
                precision,
                scale,
            } => {
                encoded.push(5);
                encoded.push(*precision);
                encoded.push(*scale as u8);
                encoded.extend_from_slice(&value.to_le_bytes());
            }
            ScalarValue::Date32(value) => {
                encoded.push(6);
                encoded.extend_from_slice(&value.to_le_bytes());
            }
            ScalarValue::Utf8(value) => {
                encoded.push(7);
                encoded.extend_from_slice(value.as_bytes());
            }
            ScalarValue::IntervalMonthDayNano {
                months,
                days,
                nanoseconds,
            } => {
                encoded.push(8);
                encoded.extend_from_slice(&months.to_le_bytes());
                encoded.extend_from_slice(&days.to_le_bytes());
                encoded.extend_from_slice(&nanoseconds.to_le_bytes());
            }
            ScalarValue::IntervalDayTime { days, milliseconds } => {
                encoded.push(9);
                encoded.extend_from_slice(&days.to_le_bytes());
                encoded.extend_from_slice(&milliseconds.to_le_bytes());
            }
        }
        Some(Self(encoded.into()))
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

/// Canonically encodes a non-null tuple of scalar values for a column-group sketch.
///
/// Components remain in logical schema order. Length framing prevents ambiguity between
/// adjacent variable-length values.
pub fn encode_scalar_tuple(values: &[ScalarValue]) -> Option<Vec<u8>> {
    let mut encoded = Vec::new();
    encoded.push(0x54);
    encoded.extend_from_slice(&TUPLE_VALUE_ENCODING_VERSION.to_le_bytes());
    encoded.extend_from_slice(&(u32::try_from(values.len()).ok()?).to_le_bytes());
    for value in values {
        let value = EncodedScalarValue::from_scalar(value)?;
        encoded.extend_from_slice(&(u32::try_from(value.as_bytes().len()).ok()?).to_le_bytes());
        encoded.extend_from_slice(value.as_bytes());
    }
    Some(encoded)
}

/// Query-local sketches attached to one base-column population.
///
/// The same value encoding is shared by both sketches. HLL supplies a reliable NDV estimate;
/// SpaceSaving retains heavy-hitter identities and frequency bounds for skew-aware filters and
/// joins. `population_rows` describes the rows from which both sketches were built.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnSketches {
    /// Version shared by every encoded-value sketch in this payload.
    pub encoding_version: u16,
    pub population_rows: u64,
    pub distinct_values: Option<HyperLogLog>,
    pub frequent_values: Option<SpaceSaving<EncodedScalarValue>>,
}

impl ColumnSketches {
    pub fn new(population_rows: u64) -> Self {
        Self {
            encoding_version: SKETCH_VALUE_ENCODING_VERSION,
            population_rows,
            distinct_values: None,
            frequent_values: None,
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.encoding_version != SKETCH_VALUE_ENCODING_VERSION {
            return Err(format!(
                "unsupported scalar sketch encoding version {} (expected {})",
                self.encoding_version, SKETCH_VALUE_ENCODING_VERSION,
            ));
        }
        if self
            .distinct_values
            .as_ref()
            .is_some_and(|hll| hll.encoding_version() != self.encoding_version)
        {
            return Err("HLL encoding version does not match its column sketch payload".into());
        }
        if self
            .frequent_values
            .as_ref()
            .is_some_and(|frequent| frequent.observations() > self.population_rows)
        {
            return Err("frequent-value observations exceed the sketch population".into());
        }
        Ok(())
    }

    pub fn observe(&mut self, value: &ScalarValue) {
        let Some(encoded) = EncodedScalarValue::from_scalar(value) else {
            return;
        };
        if let Some(hll) = &mut self.distinct_values {
            hll.insert(encoded.as_bytes());
        }
        if let Some(frequent) = &mut self.frequent_values {
            frequent.insert(encoded);
        }
    }

    pub fn hll(mut self) -> Self {
        self.distinct_values = Some(HyperLogLog::new(self.encoding_version));
        self
    }

    pub fn space_saving(
        mut self,
        capacity: usize,
    ) -> Result<Self, optd_sketches::SpaceSavingError> {
        self.frequent_values = Some(SpaceSaving::new(capacity)?);
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn column_sketch_validation_rejects_incompatible_payloads() {
        let mut wrong_version = ColumnSketches::new(1);
        wrong_version.encoding_version += 1;
        assert!(wrong_version.validate().is_err());

        let too_many_observations = ColumnSketches::new(1).space_saving(2).unwrap();
        let mut too_many_observations = too_many_observations;
        too_many_observations.observe(&ScalarValue::Int64(1));
        too_many_observations.observe(&ScalarValue::Int64(2));
        assert!(too_many_observations.validate().is_err());
    }

    #[test]
    fn tuple_encoding_is_framed_and_omits_nulls() {
        let left = encode_scalar_tuple(&[
            ScalarValue::Utf8("1".into()),
            ScalarValue::Utf8("23".into()),
        ])
        .unwrap();
        let right = encode_scalar_tuple(&[
            ScalarValue::Utf8("12".into()),
            ScalarValue::Utf8("3".into()),
        ])
        .unwrap();
        assert_ne!(left, right);
        assert!(encode_scalar_tuple(&[ScalarValue::Null(arrow_schema::DataType::Utf8)]).is_none());
    }

    #[test]
    fn scalar_encoding_is_type_tagged_and_normalizes_float_zero() {
        assert_ne!(
            EncodedScalarValue::from_scalar(&ScalarValue::Int32(1)),
            EncodedScalarValue::from_scalar(&ScalarValue::Int64(1)),
        );
        assert_eq!(
            EncodedScalarValue::from_scalar(&ScalarValue::Float64(-0.0)),
            EncodedScalarValue::from_scalar(&ScalarValue::Float64(0.0)),
        );
        assert!(
            EncodedScalarValue::from_scalar(&ScalarValue::Null(arrow_schema::DataType::Int64))
                .is_none()
        );
    }
}
