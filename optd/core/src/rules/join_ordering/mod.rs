//! Join-ordering algorithms and the pass that applies them to IR join trees.
//!
//! `dpccp` and `dphyp` are kept as enumeration engines. `pass` owns plan
//! extraction, costing, and reconstruction.
pub mod dpccp;
pub mod dphyp;
mod bitset;
#[cfg(test)]
mod fixtures;
mod format;
mod island;
mod pass;
#[cfg(test)]
mod tests;
mod types;

pub(crate) use bitset::{BitVecSetOpsExt, extract_bitset, subsets};
pub(crate) use format::{debug_vertex_set, write_csg_cmp_pairs};
#[cfg(test)]
pub(crate) use island::{JoinIsland, JoinIslandNode, JoinSemantics};
pub use pass::JoinOrderingPass;
pub(crate) use types::{EdgeIndex, EdgeSet, VertexIndex, VertexSet};
