//! Discovery of maximal contiguous join groups.

use crate::{Operator, OperatorData, QueryContext};

// ---------------------------------------------------------------------------
// Multi-group root collection
// ---------------------------------------------------------------------------

/// Collects all join group roots in bottom-up order.
///
/// A join group is a maximal connected region of [`crate::Join`] and [`crate::CrossProduct`]
/// operators. Its root therefore has no join-like parent. Unary operators such as projections,
/// selections, aggregations, maps, renames, sorts, and limits delimit groups, but traversal
/// continues through them so nested groups are still discovered.
///
/// Children are visited before the current operator, making the returned order suitable for
/// rewrite passes that must optimize nested groups before their enclosing group.
///
/// This helper is public primarily for optimizer composition and diagnostics; it does not build
/// hypergraphs or mutate `ctx`.
pub fn collect_join_group_roots(ctx: &QueryContext, root: Operator) -> Vec<Operator> {
    let mut roots = Vec::new();
    collect_roots_rec(ctx, root, false, &mut roots);
    roots
}

fn collect_roots_rec(
    ctx: &QueryContext,
    op: Operator,
    parent_is_join: bool,
    out: &mut Vec<Operator>,
) {
    let is_join = matches!(
        ctx.operator(op),
        OperatorData::Join(_) | OperatorData::CrossProduct(_)
    );

    // Recurse into children.
    match ctx.operator(op) {
        OperatorData::Join(j) => {
            collect_roots_rec(ctx, j.outer, true, out);
            collect_roots_rec(ctx, j.inner, true, out);
        }
        OperatorData::CrossProduct(cp) => {
            collect_roots_rec(ctx, cp.outer, true, out);
            collect_roots_rec(ctx, cp.inner, true, out);
        }
        OperatorData::Output(o) => collect_roots_rec(ctx, o.input, false, out),
        OperatorData::Projection(p) => collect_roots_rec(ctx, p.input, false, out),
        OperatorData::Selection(s) => collect_roots_rec(ctx, s.input, false, out),
        OperatorData::Sort(s) => collect_roots_rec(ctx, s.input, false, out),
        OperatorData::Limit(l) => collect_roots_rec(ctx, l.input, false, out),
        OperatorData::Map(m) => collect_roots_rec(ctx, m.input, false, out),
        OperatorData::Rename(r) => collect_roots_rec(ctx, r.input, false, out),
        OperatorData::Aggregation(a) => collect_roots_rec(ctx, a.input, false, out),
        _ => {}
    }

    if is_join && !parent_is_join {
        out.push(op);
    }
}
