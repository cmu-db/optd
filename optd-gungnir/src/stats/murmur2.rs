// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

//! Implementation of the MurmurHash2 function, for 64b outputs, by Austin Appleby (2008).
//! Note: Assumes little-endian machines.

/// Returns the MurmurHash2 (u64) given a stream of bytes and a seed.
pub fn murmur_hash(bytes: &[u8], seed: u64) -> u64 {
    murmur2::murmur64a(bytes, seed)
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
}
