//! Shared bitset-based index and set aliases used by the join-ordering code.

use bitvec::prelude::*;

/// Index into a graph or hypergraph vertex array.
pub type VertexIndex = usize;

/// Index into a graph or hypergraph edge array.
pub type EdgeIndex = usize;

/// Bitset over vertices.
pub type VertexSet = BitVec;

/// Bitset over edges.
pub type EdgeSet = BitVec;
