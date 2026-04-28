//! Join-ordering algorithms and the pass that applies them to IR join trees.
//!
//! `dphyp` is the production enumeration engine. `dpccp` is currently kept as
//! a test/reference implementation for parity checks on simple graphs.
#[cfg(test)]
pub mod dpccp;
pub mod dphyp;
mod bitset;
#[cfg(test)]
mod fixtures;
#[cfg(test)]
mod format;
mod island;
mod pass;
#[cfg(test)]
mod tests;
mod types;

pub(crate) use bitset::{BitVecSetOpsExt, subsets};
#[cfg(test)]
pub(crate) use bitset::extract_bitset;
#[cfg(test)]
pub(crate) use format::{debug_vertex_set, write_csg_cmp_pairs};
#[cfg(test)]
pub(crate) use island::{JoinIsland, JoinIslandNode, JoinSemantics};
pub use pass::JoinOrderingPass;
pub(crate) use types::{EdgeIndex, EdgeSet, VertexIndex, VertexSet};
