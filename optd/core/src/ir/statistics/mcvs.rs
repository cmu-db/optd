use std::collections::{BTreeMap, HashMap};

#[derive(Debug, Clone, PartialEq)]
pub struct MostCommonValues<T> {
    mcvs: BTreeMap<T, usize>,
    total_count: usize,
}

impl<T> MostCommonValues<T> {
    pub fn new(mcvs: BTreeMap<T, usize>, total_count: usize) -> Self {
        Self { mcvs, total_count }
    }

    pub fn count(&self, value: &T) -> (usize, usize)
    where
        T: Ord,
    {
        self.mcvs
            .get(value)
            .map(|count| (*count, 1))
            .unwrap_or((0, 0))
    }

    pub fn count_with(&self, predicate: impl Fn(&T) -> bool) -> (usize, usize) {
        self.mcvs
            .iter()
            .filter_map(|(k, v)| if predicate(k) { Some((*v, 1)) } else { None })
            .fold((0, 0), |(acc_count, acc_total), (count, total)| {
                (acc_count + count, acc_total + total)
            })
    }

    pub fn num_values(&self) -> usize {
        self.mcvs.len()
    }

    pub fn total_count(&self) -> usize {
        self.total_count
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mcvs_count_with() {
        let mut mcvs_map = BTreeMap::new();
        mcvs_map.insert("apple", 10);
        mcvs_map.insert("banana", 5);
        mcvs_map.insert("blueberry", 12);
        mcvs_map.insert("orange", 8);

        let mcvs = MostCommonValues::new(mcvs_map, 35);
        assert_eq!(mcvs.num_values(), 4);
        assert_eq!(mcvs.total_count(), 35);

        let (count, total) = mcvs.count_with(|v| v.starts_with('b'));
        assert_eq!(count, 17);
        assert_eq!(total, 2);

        let (count, total) = mcvs.count_with(|v| v.len() > 6);
        assert_eq!(count, 12);
        assert_eq!(total, 1);
    }

    #[test]
    fn test_mcvs_count() {
        let mut mcvs_map = BTreeMap::new();
        mcvs_map.insert("banana", 5);
        mcvs_map.insert("orange", 8);
        let mcvs = MostCommonValues::new(mcvs_map, 13);
        assert_eq!(mcvs.num_values(), 2);
        assert_eq!(mcvs.total_count(), 13);
        let result = mcvs.count(&"banana");
        assert_eq!(result, (5, 1));
        let result = mcvs.count(&"grape");
        assert_eq!(result, (0, 0));
    }
}
