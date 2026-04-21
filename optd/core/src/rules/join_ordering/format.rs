//! Small formatting helpers shared by the test/debug-oriented graph printers.

use std::fmt::Debug;
use std::io::{Result, Write};

use super::types::VertexSet;

/// Resolves a vertex bitset into the corresponding vertex payloads.
pub(crate) fn debug_vertex_set<'a, V: Debug>(
    set: &VertexSet,
    mut get_vertex_info: impl FnMut(usize) -> Option<&'a V>,
) -> Vec<&'a V> {
    set.iter_ones()
        .map(|v| get_vertex_info(v).expect("vertex index should be valid"))
        .collect()
}

/// Writes a standard header/footer around a stream of emitted CSG-CMP pairs.
pub(crate) fn write_csg_cmp_pairs<W, P>(
    mut f: W,
    pairs: impl IntoIterator<Item = P>,
    mut write_pair: impl FnMut(&mut W, P) -> Result<()>,
) -> Result<()>
where
    W: Write,
{
    f.write_fmt(format_args!("All csg-cmp pairs:\n"))?;
    let mut n = 0;
    for pair in pairs {
        write_pair(&mut f, pair)?;
        n += 1;
    }
    f.write_fmt(format_args!("Total {} pairs.\n", n))
}
