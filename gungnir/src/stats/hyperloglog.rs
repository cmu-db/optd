//----------------------------------------------------//
// This Software is provided to you by...             //
//       _____                         _              //
//      / ____|                       (_)             //
//      | |  __ _   _ _ __   __ _ _ __  _ _ __        //
//      | | |_ | | | | '_ \ / _` | '_ \| | '__|       //
//      | |__| | |_| | | | | (_| | | | | | |          //
//       \_____|\__,_|_| |_|\__, |_| |_|_|_|          //
//                           __/ |                    //
//                          |___/                     //
//                                                    //
// Author: Alexis Schlomer <aschlome@andrew.cmu.edu>  //
//----------------------------------------------------//

//! Implementation of the HyperLogLog data structure as described in Flajolet et al. paper:
//! "HyperLogLog: the analysis of a near-optimal cardinality estimation algorithm" (2007).
//! For more details, refer to:
//! https://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf
//! We modified it by hashing objects into 64-bit values instead of 32-bit ones to reduce the
//! number of collisions and eliminate the need for a large range correction estimator.

use std::{cmp::max, marker::PhantomData};

// Trait to transform any object into a stream of bytes.
// TODO(alexis): Lifetimes, perhaps.
pub trait ByteSerializable {
    fn to_bytes(&self) -> &[u8];
}

// The HyperLogLog (HLL) structure to provide a statistical estimate of NDistinct.
// For safety reasons, HLLs can only count elements of the same ByteSerializable type.
pub struct HyperLogLog<T: ByteSerializable> {
    registers: Vec<u8>, // The buckets to estimate HLL on (i.e. upper p bits).
    precision: u8,      // The precision (p) of our HLL; 4 <= p <= 16.
    m: usize,           // The number of HLL buckets; 2^p.
    alpha: f64,         // The normal HLL multiplier factor.

    hll_type: PhantomData<T>, // A marker to the data type of our HLL (to silent warnings).
}

// Self-contained implementation of the HyperLogLog data structure.
impl<T> HyperLogLog<T>
where
    T: ByteSerializable,
{
    // Creates and initializes a new empty HyperLogLog.
    pub fn new(precision: u8) -> Self {
        assert!(4 <= precision && precision <= 16);

        let m = 1 << precision;
        let alpha = compute_alpha(m);

        HyperLogLog::<T> {
            registers: vec![0; m as usize],
            precision,
            m,
            alpha,

            hll_type: PhantomData,
        }
    }

    // Digests an array of ByteSerializable data into the HLL.
    pub fn aggregate(&mut self, data: &[T]) {
        for d in data {
            let hash = murmur_hash(d.to_bytes(), 0); // TODO(alexis): Setup seed.
            let mask = (1 << (self.precision)) - 1;
            let idx = (hash & mask) as usize; // LSB is bucket discriminator; MSB is zero streak.
            self.registers[idx] = max(self.registers[idx], self.zeros(hash));
        }
    }

    // Merges two HLLs together and returns a new one.
    // Particularly useful for parallel execution.
    // NOTE: Takes ownership of self and other.
    pub fn merge(self, other: HyperLogLog<T>) -> Self {
        assert!(self.precision == other.precision);

        let merged_registers = self
            .registers
            .into_iter()
            .zip(other.registers.into_iter())
            .map(|(x, y)| x.max(y))
            .collect();

        HyperLogLog::<T> {
            registers: merged_registers,
            precision: self.precision,
            m: self.m,
            alpha: self.alpha,

            hll_type: PhantomData,
        }
    }

    // Returns an estimation of the n_distinct seen so far by the HLL.
    pub fn n_distinct(&self) -> u64 {
        let m = self.m as f64;
        let raw_estimate = self.alpha
            * (m * m)
            * self
                .registers
                .iter()
                .fold(0.0, |acc, elem| (1.0 / (1 << elem) as f64) + acc);

        if raw_estimate <= ((5.0 * m) / 2.0) {
            let empty_reg = self.registers.iter().filter(|&elem| *elem == 0).count();
            if empty_reg != 0 {
                (m * (m / (empty_reg as f64)).ln()).round() as u64
            } else {
                raw_estimate.round() as u64
            }
        } else {
            raw_estimate.round() as u64
        }
    }

    // Returns the number of consecutive zeros in hash, starting from LSB.
    fn zeros(&self, hash: u64) -> u8 {
        let max_bit = 64 - self.precision;
        (0..max_bit)
            .take_while(|i| (hash & (1 << (self.precision + i))) == 0)
            .count() as u8
    }
}

// Implementation of the MurmurHash2 function, for 64b outputs, by Austin Appleby (2008).
// Note: Assumes little-endian machines.
fn murmur_hash(bytes: &[u8], seed: u64) -> u64 {
    const M: u64 = 0xc6a4a7935bd1e995;
    const R: u8 = 47;

    let mut hash = seed ^ (bytes.len() as u64).wrapping_mul(M);

    let div = bytes.len() / 8;
    let rem = bytes.len() % 8;

    let whole_part: &[u64] =
        unsafe { std::slice::from_raw_parts(bytes.as_ptr() as *const u64, div) };

    for batch in whole_part {
        let mut k = batch.wrapping_mul(M);
        k ^= k >> R;
        k = k.wrapping_mul(M);

        hash ^= k;
        hash = hash.wrapping_mul(M);
    }

    if rem > 0 {
        for i in 0..rem {
            hash ^= (bytes[div * 8 + i] as u64) << (i * 8);
        }
        hash = hash.wrapping_mul(M);
    }

    hash ^= hash >> R;
    hash = hash.wrapping_mul(M);
    hash ^= hash >> R;
    hash
}

// Computes the alpha HLL parameter based on m.
fn compute_alpha(m: usize) -> f64 {
    match m {
        16 => 0.673,
        32 => 0.697,
        64 => 0.709,
        _ => 0.7213 / (1.0 + 1.079 / (m as f64)),
    }
}

// Start of unit testing section.
#[cfg(test)]
mod tests {
    use super::murmur_hash;

    #[test]
    fn murmur_string() {
        assert_eq!(
            murmur_hash("Hyper🪵🪵 Rules!".as_bytes(), 1257851387),
            1623602735526180105
        );
        assert_eq!(
            murmur_hash(
                "All work and no play makes Jack a dull boy".as_bytes(),
                1111111111
            ),
            1955247671966919985
        );
        assert_eq!(murmur_hash("".as_bytes(), 0), 0);
        assert_eq!(
            murmur_hash("Gungnir™".as_bytes(), 4242424242),
            13329505761566523763
        );
    }

    // TODO(alexis): Test HLL.
}
