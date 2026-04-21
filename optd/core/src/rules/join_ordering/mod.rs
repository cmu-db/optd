pub mod dpccp;
pub mod dphyp;
mod subset;

use bitvec::prelude::*;

pub fn extract_bitset(s: &BitVec) -> Vec<usize> {
    s.iter_ones().collect()
}
