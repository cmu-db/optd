//! This module provides an encoder that converts alpha-numeric strings
//! into f64 values, designed to maintain the natural ordering of strings.
//!
//! While the encoding is theoretically lossless, in practice, it may suffer
//! from precision loss due to floating-point errors.
//!
//! Non-alpha-numeric characters are relegated to the end of the encoded value,
//! rendering them indistinguishable from one another in this context.

use std::collections::HashMap;

use lazy_static::lazy_static;

// The alphanumerical ordering.
const ALPHANUMERIC_ORDER: [char; 95] = [
    ' ', '!', '"', '#', '$', '%', '&', '\'', '(', ')', '*', '+', ',', '-', '.', '/', ':', ';', '<',
    '=', '>', '?', '@', '[', '\\', ']', '^', '_', '`', '{', '|', '}', '~', '0', '1', '2', '3', '4',
    '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N',
    'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f', 'g',
    'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
];

const PMF: f64 = 1.0 / (ALPHANUMERIC_ORDER.len() as f64);

lazy_static! {
    static ref CDF: HashMap<char, f64> = {
        let length = ALPHANUMERIC_ORDER.len() + 1; // To account for non-alpha-numeric characters.
        let mut cdf = HashMap::with_capacity(length);
        for (index, &char) in ALPHANUMERIC_ORDER.iter().enumerate() {
            cdf.insert(char, (index as f64) / (length as f64));
        }
        cdf
    };
}

pub fn encode(string: &str) -> f64 {
    let mut left = 0.0;
    // 10_000.0 is fairly arbitrary. don't make it f64::MAX though because it causes overflow in
    // other places of the code
    let mut right = 10_000.0;

    for char in string.chars() {
        let cdf = CDF.get(&char).unwrap_or(&1.0);
        let distance = right - left;
        right = left + distance * (cdf + PMF);
        left += distance * cdf;
    }

    left
}

// Start of unit testing section.
#[cfg(test)]
mod tests {
    use super::encode;

    #[test]
    fn encode_tests() {
        assert!(encode("") < encode("abc"));
        assert!(encode("abc") < encode("bcd"));

        assert!(encode("a") < encode("aaa"));
        assert!(encode("!a") < encode("a!"));
        assert!(encode("Alexis") < encode("Schlomer"));

        assert!(encode("Gungnir Rules!") < encode("Schlomer"));
        assert!(encode("Gungnir Rules!") < encode("Schlomer"));

        assert_eq!(encode(" "), encode(" "));
        assert_eq!(encode("Same"), encode("Same"));
        assert!(encode("Nicolas  ") < encode("Nicolas💰💼"));
    }
}
