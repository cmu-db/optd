use std::fmt;

/// One counter in a [`SpaceSaving`] frequent-items sketch.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrequentItem<T> {
    pub value: T,
    /// Upper estimate of the value's frequency.
    pub frequency: u64,
    /// Maximum over-count introduced when this counter replaced another value.
    pub error: u64,
}

impl<T> FrequentItem<T> {
    pub fn lower_frequency(&self) -> u64 {
        self.frequency.saturating_sub(self.error)
    }
}

/// A fixed-capacity SpaceSaving summary for weighted frequent-item tracking.
#[cfg_attr(
    feature = "serde",
    derive(serde::Serialize),
    serde(bound(serialize = "T: serde::Serialize"))
)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpaceSaving<T> {
    capacity: usize,
    observations: u64,
    entries: Vec<FrequentItem<T>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SpaceSavingError {
    ZeroCapacity,
    TooManyEntries { capacity: usize, actual: usize },
    DuplicateEntries,
    InvalidCounter { index: usize },
    InvalidObservationCount,
    IncompatibleCapacity { left: usize, right: usize },
}

impl fmt::Display for SpaceSavingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroCapacity => f.write_str("SpaceSaving capacity must be greater than zero"),
            Self::TooManyEntries { capacity, actual } => write!(
                f,
                "SpaceSaving capacity {capacity} cannot hold {actual} entries",
            ),
            Self::DuplicateEntries => f.write_str("SpaceSaving entries must have distinct values"),
            Self::InvalidCounter { index } => write!(
                f,
                "SpaceSaving entry {index} has an error greater than its frequency",
            ),
            Self::InvalidObservationCount => f.write_str(
                "SpaceSaving observations must cover every counter's lower frequency bound",
            ),
            Self::IncompatibleCapacity { left, right } => write!(
                f,
                "SpaceSaving sketches have incompatible capacities {left} and {right}",
            ),
        }
    }
}

impl std::error::Error for SpaceSavingError {}

#[cfg(feature = "serde")]
impl<'de, T> serde::Deserialize<'de> for SpaceSaving<T>
where
    T: serde::Deserialize<'de> + Clone + Eq,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(serde::Deserialize)]
        struct SerializedSpaceSaving<T> {
            capacity: usize,
            observations: u64,
            entries: Vec<FrequentItem<T>>,
        }

        let serialized = SerializedSpaceSaving::deserialize(deserializer)?;
        Self::from_entries(
            serialized.capacity,
            serialized.observations,
            serialized.entries,
        )
        .map_err(serde::de::Error::custom)
    }
}

impl<T> SpaceSaving<T>
where
    T: Clone + Eq,
{
    pub fn new(capacity: usize) -> Result<Self, SpaceSavingError> {
        if capacity == 0 {
            return Err(SpaceSavingError::ZeroCapacity);
        }
        Ok(Self {
            capacity,
            observations: 0,
            entries: Vec::with_capacity(capacity),
        })
    }

    pub fn from_entries(
        capacity: usize,
        observations: u64,
        entries: Vec<FrequentItem<T>>,
    ) -> Result<Self, SpaceSavingError> {
        if capacity == 0 {
            return Err(SpaceSavingError::ZeroCapacity);
        }
        if entries.len() > capacity {
            return Err(SpaceSavingError::TooManyEntries {
                capacity,
                actual: entries.len(),
            });
        }
        if entries.iter().enumerate().any(|(index, entry)| {
            entries[..index]
                .iter()
                .any(|prior| prior.value == entry.value)
        }) {
            return Err(SpaceSavingError::DuplicateEntries);
        }
        if let Some((index, _)) = entries
            .iter()
            .enumerate()
            .find(|(_, entry)| entry.error > entry.frequency)
        {
            return Err(SpaceSavingError::InvalidCounter { index });
        }
        let lower_total = entries.iter().fold(0_u64, |total, entry| {
            total.saturating_add(entry.lower_frequency())
        });
        if entries.iter().any(|entry| entry.frequency > observations) || lower_total > observations
        {
            return Err(SpaceSavingError::InvalidObservationCount);
        }
        Ok(Self {
            capacity,
            observations,
            entries,
        })
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn observations(&self) -> u64 {
        self.observations
    }

    pub fn entries(&self) -> &[FrequentItem<T>] {
        &self.entries
    }

    pub fn insert(&mut self, value: T) {
        self.insert_weighted(value, 1);
    }

    pub fn insert_weighted(&mut self, value: T, weight: u64) {
        if weight == 0 {
            return;
        }
        self.observations = self.observations.saturating_add(weight);
        if let Some(entry) = self.entries.iter_mut().find(|entry| entry.value == value) {
            entry.frequency = entry.frequency.saturating_add(weight);
            return;
        }
        if self.entries.len() < self.capacity {
            self.entries.push(FrequentItem {
                value,
                frequency: weight,
                error: 0,
            });
            return;
        }

        let mut minimum_index = 0;
        for index in 1..self.entries.len() {
            if self.entries[index].frequency < self.entries[minimum_index].frequency {
                minimum_index = index;
            }
        }
        let minimum = self.entries[minimum_index].frequency;
        self.entries[minimum_index] = FrequentItem {
            value,
            frequency: minimum.saturating_add(weight),
            error: minimum,
        };
    }

    /// Combines compatible summaries while preserving SpaceSaving error bounds.
    pub fn merge(&mut self, other: &Self) -> Result<(), SpaceSavingError> {
        if self.capacity != other.capacity {
            return Err(SpaceSavingError::IncompatibleCapacity {
                left: self.capacity,
                right: other.capacity,
            });
        }
        let left_minimum = if self.entries.len() == self.capacity {
            self.entries
                .iter()
                .map(|entry| entry.frequency)
                .min()
                .unwrap_or(0)
        } else {
            0
        };
        let right_minimum = if other.entries.len() == other.capacity {
            other
                .entries
                .iter()
                .map(|entry| entry.frequency)
                .min()
                .unwrap_or(0)
        } else {
            0
        };

        let mut merged = Vec::with_capacity(self.entries.len() + other.entries.len());
        for left in &self.entries {
            if let Some(right) = other.estimate(&left.value) {
                merged.push(FrequentItem {
                    value: left.value.clone(),
                    frequency: left.frequency.saturating_add(right.frequency),
                    error: left.error.saturating_add(right.error),
                });
            } else {
                merged.push(FrequentItem {
                    value: left.value.clone(),
                    frequency: left.frequency.saturating_add(right_minimum),
                    error: left.error.saturating_add(right_minimum),
                });
            }
        }
        for right in &other.entries {
            if self.estimate(&right.value).is_none() {
                merged.push(FrequentItem {
                    value: right.value.clone(),
                    frequency: right.frequency.saturating_add(left_minimum),
                    error: right.error.saturating_add(left_minimum),
                });
            }
        }
        merged.sort_by_key(|entry| std::cmp::Reverse(entry.frequency));
        merged.truncate(self.capacity);
        self.entries = merged;
        self.observations = self.observations.saturating_add(other.observations);
        Ok(())
    }

    pub fn estimate(&self, value: &T) -> Option<&FrequentItem<T>> {
        self.entries.iter().find(|entry| &entry.value == value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retains_heavy_items_with_bounded_counts() {
        let mut sketch = match SpaceSaving::new(3) {
            Ok(sketch) => sketch,
            Err(error) => panic!("unexpected constructor error: {error}"),
        };
        for _ in 0..100 {
            sketch.insert("heavy");
        }
        for value in 0..30 {
            sketch.insert(if value % 2 == 0 { "left" } else { "right" });
        }
        let heavy = match sketch.estimate(&"heavy") {
            Some(heavy) => heavy,
            None => panic!("heavy hitter should remain tracked"),
        };
        assert!(heavy.lower_frequency() <= 100);
        assert!(heavy.frequency >= 100);
        assert_eq!(sketch.observations(), 130);
    }

    #[test]
    fn merge_preserves_heavy_items_and_observation_count() {
        let mut left = SpaceSaving::new(3).expect("valid capacity");
        let mut right = SpaceSaving::new(3).expect("valid capacity");
        for _ in 0..50 {
            left.insert("shared");
            right.insert("shared");
        }
        for value in ["a", "b", "c", "d"] {
            left.insert(value);
        }
        for value in ["e", "f", "g", "h"] {
            right.insert(value);
        }

        left.merge(&right).expect("compatible summaries");
        let shared = left.estimate(&"shared").expect("heavy item survives merge");
        assert!(shared.lower_frequency() <= 100);
        assert!(shared.frequency >= 100);
        assert_eq!(left.observations(), 108);
    }

    #[cfg(feature = "serde")]
    #[test]
    fn rejects_malformed_serialized_entries() {
        let zero_capacity = r#"{"capacity":0,"observations":0,"entries":[]}"#;
        assert!(serde_json::from_str::<SpaceSaving<String>>(zero_capacity).is_err());

        let invalid_counter =
            r#"{"capacity":1,"observations":1,"entries":[{"value":"x","frequency":1,"error":2}]}"#;
        assert!(serde_json::from_str::<SpaceSaving<String>>(invalid_counter).is_err());

        let duplicates = r#"{"capacity":2,"observations":2,"entries":[{"value":"x","frequency":1,"error":0},{"value":"x","frequency":1,"error":0}]}"#;
        assert!(serde_json::from_str::<SpaceSaving<String>>(duplicates).is_err());
    }

    #[test]
    fn rejects_incompatible_merges() {
        let mut left = match SpaceSaving::<u8>::new(2) {
            Ok(sketch) => sketch,
            Err(error) => panic!("unexpected constructor error: {error}"),
        };
        let right = match SpaceSaving::<u8>::new(3) {
            Ok(sketch) => sketch,
            Err(error) => panic!("unexpected constructor error: {error}"),
        };
        assert_eq!(
            left.merge(&right),
            Err(SpaceSavingError::IncompatibleCapacity { left: 2, right: 3 })
        );
    }
}
