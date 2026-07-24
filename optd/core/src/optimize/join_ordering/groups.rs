//! Discovery of maximal contiguous join groups.

use crate::{Operator, OperatorData, QueryContext, Relation};

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
    enum Work {
        Visit { op: Operator, parent_is_join: bool },
        Emit(Operator),
    }

    let mut roots = Vec::new();
    let mut work = vec![Work::Visit {
        op: root,
        parent_is_join: false,
    }];

    while let Some(next) = work.pop() {
        match next {
            Work::Visit { op, parent_is_join } => {
                let is_join = matches!(
                    ctx.operator(op),
                    OperatorData::Join(_) | OperatorData::CrossProduct(_)
                );
                if is_join && !parent_is_join {
                    work.push(Work::Emit(op));
                }
                work.extend(
                    ctx.operator(op)
                        .inputs()
                        .into_iter()
                        .rev()
                        .map(|child| Work::Visit {
                            op: child,
                            parent_is_join: is_join,
                        }),
                );
            }
            Work::Emit(op) => roots.push(op),
        }
    }

    roots
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CrossProduct, Scan, TableRef};

    #[test]
    fn root_collection_is_stack_safe_for_a_deep_join_group() {
        const DEPTH: usize = 20_000;

        let mut ctx = QueryContext::new();
        let leaf = OperatorData::Scan(Scan {
            table: TableRef::bare("leaf"),
            columns: vec![],
        })
        .add(&mut ctx);
        let root = (0..DEPTH).fold(leaf, |outer, _| {
            OperatorData::CrossProduct(CrossProduct { outer, inner: leaf }).add(&mut ctx)
        });

        assert_eq!(collect_join_group_roots(&ctx, root), vec![root]);
    }
}
