#[derive(Clone, PartialEq)]
pub struct Histogram<T> {
    bins: Vec<Bin<T>>,
    total_count: usize,
}

impl<T: std::fmt::Debug> std::fmt::Debug for Histogram<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Histogram {{")?;

        for (i, bin) in self.bins.iter().enumerate() {
            if i == 0 {
                writeln!(f, "  x <= {:?}: {{ count:{} }},", bin.upper, bin.count,)?;
            } else {
                // Subsequent buckets: (prev_upper, upper]
                let prev_upper = &self.bins[i - 1].upper;
                writeln!(
                    f,
                    "  {:?} < x <= {:?}: {{ count:{}, }},",
                    prev_upper, bin.upper, bin.count,
                )?;
            }
        }

        write!(f, "}}")
    }
}

/// A single bin in a histogram.
#[derive(Debug, Clone, PartialEq)]
pub struct Bin<T> {
    /// Upper bound of the bin.
    pub upper: T,
    /// Count of values in this bin.
    pub count: usize,
}

impl<T> Bin<T> {
    pub fn new(upper: T, count: usize) -> Self {
        Self { upper, count }
    }
}

impl<T> Histogram<T> {
    /// Creates a new histogram with the given bins.
    pub fn new(bins: Vec<Bin<T>>, total_count: usize) -> Self {
        Self { bins, total_count }
    }

    /// Gets the total number of items in the histogram.
    pub fn total_count(&self) -> usize {
        self.total_count
    }

    /// Gets the number of bins in the histogram.
    pub fn num_bins(&self) -> usize {
        self.bins.len()
    }
}

impl<T> Histogram<T>
where
    T: Ord + num_traits::ToPrimitive,
{
    /// Computes the cumulative distribution function (CDF) for a given value,
    /// where CDF(value) = P(X <= value)
    pub fn cdf(&self, value: T) -> f64 {
        if self.total_count == 0 {
            return 0.;
        }

        // Find the first bin where upper > value
        // `partition_point` gives us the index of the first bin where the condition is false
        let index = self.bins.partition_point(|bin| bin.upper <= value);

        if index == 0 {
            return 0.;
        }

        if index == self.bins.len() {
            // Value is > all bins
            return 1.;
        }

        // Value is in bin[index], between bins[index-1].upper and bins[index].upper
        let mut cumulative = self.bins[..index].iter().map(|b| b.count).sum::<usize>() as f64;

        let lower = self.bins[index - 1].upper.to_f64().unwrap();
        let upper = self.bins[index].upper.to_f64().unwrap();
        let val = value.to_f64().unwrap();

        let bin_range = upper - lower;
        let value_range = val - lower;

        cumulative += self.bins[index].count as f64 * (value_range / bin_range);

        cumulative / self.total_count as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_histogram_cdf() {
        let histo = Histogram::new(
            vec![
                Bin::new(10, 5),  // (0, 10]: 5 items
                Bin::new(20, 15), // (10, 20]: 15 items
                Bin::new(30, 10), // (20, 30]: 10 items
            ],
            30,
        );

        assert_eq!(histo.cdf(5), 0.0); // before first bin
        assert_eq!(histo.cdf(10), 5.0 / 30.0); // ~0.167 (end of first bin)
        assert_eq!(histo.cdf(15), 12.5 / 30.0); // ~0.417 (halfway through second bin)
        assert_eq!(histo.cdf(20), 20.0 / 30.0); // ~0.667 (end of second bin)
        assert_eq!(histo.cdf(25), 25.0 / 30.0); // ~0.833 (halfway through third bin)
        assert_eq!(histo.cdf(30), 1.0); // end of all bins
        assert_eq!(histo.cdf(35), 1.0); // beyond all bins
    }
}
