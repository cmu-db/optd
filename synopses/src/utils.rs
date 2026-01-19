use std::hash::Hasher;

const M: u64 = 0xc6a4a7935bd1e995;
const R: u32 = 47;

/// MurmurHash2 64-bit hasher implementation
#[derive(Clone)]
pub struct Murmur2Hash64a {
    hash: u64,
    tail: u64,
    tail_len: usize,
    length: usize,
}

impl Default for Murmur2Hash64a {
    fn default() -> Self {
        Self {
            hash: 0x9747b28c,
            tail: Default::default(),
            tail_len: Default::default(),
            length: Default::default(),
        }
    }
}

impl Murmur2Hash64a {
    /// Creates a new MurmurHash2 64-bit hasher with the given seed
    pub fn new(seed: u64) -> Self {
        Self {
            hash: seed,
            tail: 0,
            tail_len: 0,
            length: 0,
        }
    }

    #[inline]
    fn process_chunk(&mut self, mut k: u64) {
        k = k.wrapping_mul(M);
        k ^= k >> R;
        k = k.wrapping_mul(M);

        self.hash ^= k;
        self.hash = self.hash.wrapping_mul(M);
    }
}

impl Hasher for Murmur2Hash64a {
    fn finish(&self) -> u64 {
        let mut h = self.hash;

        // Mix in the length (critical step that was missing!)
        h ^= (self.length as u64).wrapping_mul(M);

        // Process any remaining bytes in tail
        if self.tail_len > 0 {
            h ^= self.tail;
            h = h.wrapping_mul(M);
        }

        // Final mix
        h ^= h >> R;
        h = h.wrapping_mul(M);
        h ^= h >> R;

        h
    }

    fn write(&mut self, bytes: &[u8]) {
        self.length += bytes.len();
        let mut data = bytes;

        // If we have partial bytes in tail, try to complete a chunk
        if self.tail_len > 0 {
            let needed = 8 - self.tail_len;
            let available = data.len().min(needed);

            for b in data.iter().take(available) {
                self.tail |= (*b as u64) << (self.tail_len * 8);
                self.tail_len += 1;
            }

            data = &data[available..];

            // If we completed a full 8-byte chunk, process it
            if self.tail_len == 8 {
                self.process_chunk(self.tail);
                self.tail = 0;
                self.tail_len = 0;
            }
        }

        // Process 8-byte chunks
        let chunks = data.len() / 8;
        for chunk_bytes in data.chunks_exact(8) {
            let k = u64::from_le_bytes(chunk_bytes.try_into().unwrap());
            self.process_chunk(k);
        }

        // Store remaining bytes in tail
        let processed = chunks * 8;
        let remaining = &data[processed..];
        for (i, &byte) in remaining.iter().enumerate() {
            self.tail |= (byte as u64) << (i * 8);
            self.tail_len += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::hash::Hasher;

    #[test]
    fn test_empty_string() {
        let mut hasher = Murmur2Hash64a::new(0);
        hasher.write(b"");
        let hash = hasher.finish();
        assert_eq!(hash, 0);
    }

    #[test]
    fn test_hello_world() {
        let mut hasher = Murmur2Hash64a::new(0);
        hasher.write(b"Hello, World!");
        let hash = hasher.finish();
        println!("Hash of 'Hello, World!': 0x{:016x}", hash);
    }

    #[test]
    fn test_multiple_writes() {
        let mut hasher1 = Murmur2Hash64a::new(42);
        hasher1.write(b"Hello, ");
        hasher1.write(b"World!");
        let hash1 = hasher1.finish();

        let mut hasher2 = Murmur2Hash64a::new(42);
        hasher2.write(b"Hello, World!");
        let hash2 = hasher2.finish();

        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_different_seeds() {
        let mut hasher1 = Murmur2Hash64a::new(0);
        hasher1.write(b"test");
        let hash1 = hasher1.finish();

        let mut hasher2 = Murmur2Hash64a::new(42);
        hasher2.write(b"test");
        let hash2 = hasher2.finish();

        assert_ne!(hash1, hash2);
    }

    #[test]
    fn test_incremental_hashing() {
        let mut hasher = Murmur2Hash64a::new(0);
        hasher.write(b"a");
        hasher.write(b"b");
        hasher.write(b"c");
        hasher.write(b"d");
        hasher.write(b"e");
        hasher.write(b"f");
        hasher.write(b"g");
        hasher.write(b"h");
        hasher.write(b"i");
        let hash = hasher.finish();
        println!("Incremental hash: 0x{:016x}", hash);
    }

    #[test]
    fn test_odd_byte_boundaries() {
        let mut hasher1 = Murmur2Hash64a::new(0);
        hasher1.write(b"abc");
        hasher1.write(b"defgh");
        hasher1.write(b"ijkl");
        let hash1 = hasher1.finish();

        let mut hasher2 = Murmur2Hash64a::new(0);
        hasher2.write(b"abcdefghijkl");
        let hash2 = hasher2.finish();

        assert_eq!(hash1, hash2);
    }
}
