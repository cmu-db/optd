/// This module contains the main data structures used in decorrelation.
///
/// Following the paper, immutable global state lives in `UnnestingInfo`
/// (outer refs/domain/parent pointer), while mutable fragment-local state lives
/// in `Unnesting` (repr/cclasses).
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::ir::convert::IntoScalar;
use crate::ir::operator::Operator;
use crate::ir::scalar::{BinaryOpKind, ColumnRef, NaryOpKind};
use crate::ir::{Column, ColumnSet, IRContext, Scalar, ScalarKind, ScalarValue};
use crate::utility::union_find::UnionFind;

#[derive(Clone, Debug)]
pub(super) struct UnnestingInfo<'a> {
    /// The columns from this dependent join's outer branch referenced by its
    /// inner branch.
    pub(super) outer_refs: HashSet<Column>,

    /// Local domain for this scope `(domain_repr, domain_op)`.
    /// domain_repr maps each outer column to its domain representation
    /// domain_op is the physical projection operator that projects the domain
    pub(super) domain: (HashMap<Column, Column>, Arc<Operator>),

    /// Parent unnesting state (if this scope is nested)
    pub(super) parent: Option<&'a Unnesting<'a>>,
}

impl<'a> UnnestingInfo<'a> {
    pub(super) fn new(
        outer_refs: HashSet<Column>,
        domain: (HashMap<Column, Column>, Arc<Operator>),
        parent: Option<&'a Unnesting<'a>>,
    ) -> Self {
        Self {
            outer_refs,
            domain,
            parent,
        }
    }
}

#[derive(Debug)]
pub(super) struct Unnesting<'a> {
    /// Shared immutable info for this unnesting scope
    pub(super) info: Arc<UnnestingInfo<'a>>,

    /// Equivalence classes of columns for attribute minimization
    pub(super) cclasses: UnionFind<Column>,

    /// Mapping from outer column to the current representative column
    pub(super) repr: HashMap<Column, Column>,
}

impl<'a> Unnesting<'a> {
    pub(super) fn new(info: Arc<UnnestingInfo<'a>>) -> Self {
        // By default, any outer-ref gets mapped to the domain-ref. Only
        // through attribute minimisation does this get changed to some
        // local column
        let mut repr: HashMap<Column, Column> = info.domain.0.clone();
        let mut cur_info = info.parent.clone();
        while let Some(un) = cur_info {
            repr.extend(un.info.domain.0.clone());
            cur_info = un.info.parent.clone();
        }

        Self {
            repr,
            cclasses: UnionFind::default(),
            info,
        }
    }

    /// Resolve a column by getting its representative
    pub(super) fn resolve_col(&self, col: Column) -> Option<Column> {
        match self.repr.get(&col).is_some() {
            true => Some(self.repr.get(&col).copied().unwrap()),
            false => None,
        }
    }

    /// Checks if a column is an outer ref in this scope or any parent scope
    pub(super) fn is_outer_ref_recursive(&self, col: &Column) -> bool {
        self.info.outer_refs.contains(col)
            || self.info
                .parent
                .as_ref()
                .map(|p| p.is_outer_ref_recursive(col))
                .unwrap_or(false)
    }

    /// Merge equivalence classes from another branch-local state into this one.
    pub(super) fn merge_col_eq_from(&mut self, other: &Unnesting<'_>) {
        for col in other.cclasses.keys() {
            self.cclasses.merge(col, &other.cclasses.find(col));
        }
    }

    /// Update repr mappings for current outer refs if a local equivalent is available. 
    pub(super) fn update_repr_with_available(&mut self, available: &ColumnSet) {
        fn get_domain_repr_recursive(info: &UnnestingInfo<'_>, col: &Column) -> Option<Column> {
            info.domain
                .0
                .get(col)
                .copied()
                .or_else(|| info.parent.and_then(|p| get_domain_repr_recursive(&p.info, col)))
        }

        for c in self.info.outer_refs.iter() {
            if let Some(current) = self.repr.get(&c).copied() {
                let domain_mapping = get_domain_repr_recursive(&self.info, &c);
                if domain_mapping != Some(current) && !available.contains(&current) {
                    if let Some(domain_col) = domain_mapping {
                        self.repr.insert(*c, domain_col);
                    } else {
                        self.repr.remove(&c);
                    }
                }
            }
            let representative = self.cclasses.find(&c);
            if !self.is_outer_ref_recursive(&representative) && available.contains(&representative) {
                self.repr.insert(*c, representative);
            }
        }
    }

    // Based on operators we traverse, we can update the cclasses structure to
    // take advantage of known equivalences. 
    pub(super) fn update_cclasses_equivalences(&mut self, condition: &Arc<Scalar>) {
        match &condition.kind {
            ScalarKind::BinaryOp(bin)
                if bin.op_kind == BinaryOpKind::Eq
                    || bin.op_kind == BinaryOpKind::IsNotDistinctFrom =>
            {
                if let (ScalarKind::ColumnRef(l), ScalarKind::ColumnRef(r)) = (
                    &condition.input_scalars()[0].kind,
                    &condition.input_scalars()[1].kind,
                ) {
                    if self.is_outer_ref_recursive(&l.column) && !self.is_outer_ref_recursive(&r.column) {
                        self.cclasses.merge(&r.column, &l.column);
                    } else {
                        self.cclasses.merge(&l.column, &r.column);
                    };
                }
            }
            ScalarKind::NaryOp(nary) if nary.op_kind == NaryOpKind::And => {
                for child in condition.input_scalars() {
                    self.update_cclasses_equivalences(child);
                }
            }
            _ => {}
        }
    }

    // Rewrite the scalar using representative columns.
    pub(super) fn rewrite_columns(&self, scalar: Arc<Scalar>) -> Arc<Scalar> {
        let rewritten = match &scalar.kind {
            ScalarKind::ColumnRef(cr) => {
                ColumnRef::new(self.repr.get(&cr.column).copied().unwrap_or(cr.column)).into_scalar()
            }
            _ => {
                let new_inputs: Vec<Arc<Scalar>> = scalar
                    .input_scalars()
                    .iter()
                    .map(|s| self.rewrite_columns(s.clone()))
                    .collect();

                if new_inputs != scalar.input_scalars() {
                    Arc::new(scalar.clone_with_inputs(Some(Arc::from(new_inputs)), None))
                } else {
                    scalar
                }
            }
        };
        rewritten.simplify_nary_scalar()
    }
}

/// Compute the operators that access outer references, and the exact columns
/// referenced from outside the subtree.
pub(super) fn compute_accessing_set(op: &Arc<Operator>, ctx: &IRContext) -> (HashSet<*const Operator>, HashSet<Column>) {
    fn compute(
        op: &Arc<Operator>,
        operators: &mut HashSet<*const Operator>,
        outer_refs: &mut HashSet<Column>,
        ctx: &IRContext,
    ) {
        // We say an operator is accessing an outer ref if it uses a column
        // that is not available to it from a downstream operator
        let available = op
            .input_operators()
            .into_iter()
            .fold(ColumnSet::default(), |acc, child| {
                acc | &child.output_columns(ctx)
            });
        let mut non_bound_cols = HashSet::new();
        for scalar in op.input_scalars() {
            for c in (scalar.used_columns() - &available).iter() {
                non_bound_cols.insert(*c);
            }
        }
        if !non_bound_cols.is_empty() {
            operators.insert(Arc::as_ptr(op));
            outer_refs.extend(non_bound_cols);
        }
        op.input_operators()
            .iter()
            .for_each(|c| compute(c, operators, outer_refs, ctx));
    }

    let (mut operators, mut outer_refs) = (HashSet::new(), HashSet::new());
    compute(op, &mut operators, &mut outer_refs, ctx);
    (operators, outer_refs)
}

/// Check if an operator (by pointer) is contained within a subtree.
pub(super) fn is_contained_in(op_ptr: *const Operator, subtree: &Arc<Operator>) -> bool {
    return Arc::as_ptr(subtree) == op_ptr || subtree
        .input_operators()
        .iter()
        .any(|c| is_contained_in(op_ptr, c))
}

pub(super) fn is_true_scalar(scalar: &Arc<Scalar>) -> bool {
    matches!(
        &scalar.kind,
        ScalarKind::Literal(meta) if matches!(meta.value, ScalarValue::Boolean(Some(true)))
    )
}
