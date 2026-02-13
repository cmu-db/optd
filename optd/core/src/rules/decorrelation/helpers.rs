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
    outer_refs: HashSet<Column>,

    /// Local domain for this scope `(domain_repr, domain_op)`.
    /// domain_repr maps each outer column to its domain representation
    /// domain_op is the physical projection operator that projects the domain
    domain: (HashMap<Column, Column>, Arc<Operator>),

    /// Parent unnesting state (if this scope is nested)
    parent: Option<&'a Unnesting<'a>>,
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

    pub(super) fn get_outer_refs(&self) -> &HashSet<Column> {
        &self.outer_refs
    }

    pub(super) fn get_domain_repr(&self) -> &HashMap<Column, Column> {
        &self.domain.0
    }

    pub(super) fn get_domain_op(&self) -> &Arc<Operator> {
        &self.domain.1
    }
}

#[derive(Debug)]
pub(super) struct Unnesting<'a> {
    /// Shared immutable info for this unnesting scope
    info: Arc<UnnestingInfo<'a>>,

    /// Equivalence classes of columns for attribute minimization
    cclasses: UnionFind<Column>,

    /// Mapping from outer column to the current representative column
    repr: HashMap<Column, Column>,
}

impl<'a> Unnesting<'a> {
    pub(super) fn new(info: Arc<UnnestingInfo<'a>>) -> Self {
        Self {
            repr: HashMap::new(),
            cclasses: UnionFind::default(),
            info,
        }
    }

    pub(super) fn get_info(&self) -> Arc<UnnestingInfo<'a>> {
        self.info.clone()
    }

    pub(super) fn get_repr_of(&self, col: &Column) -> Option<&Column> {
        self.repr.get(col)
    }

    pub(super) fn add_repr_of(&mut self, key: Column, val: Column) {
        self.repr.insert(key, val);
    }

    pub(super) fn get_all_repr_values(&self) -> Vec<Column> {
        self.repr.values().copied().collect()
    }

    pub(super) fn domain_repr_recursive(&self, col: Column) -> Option<Column> {
        self
        .info
        .domain
        .0
        .get(&col)
        .copied()
        .or_else(|| self.info.parent.and_then(|p| p.domain_repr_recursive(col)))
    }

    pub(super) fn resolve_mapped_col(&self, col: Column) -> Option<Column> {
        self.get_repr_of(&col)
            .copied()
            .or_else(|| self.domain_repr_recursive(col))
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

    // Returns the subset of domain operators that are required based on current
    // column representatives.
    pub(super) fn required_domain_ops(
        &self,
        seen_outer_refs: &HashSet<Column>,
    ) -> Vec<Arc<Operator>> {
        fn collect_required_domain_ops_from_info<'a>(
            info: &UnnestingInfo<'a>,
            repr: &HashMap<Column, Column>,
            seen_outer_refs: &HashSet<Column>,
            required_domain_ops: &mut Vec<Arc<Operator>>,
        ) {
            if info.domain.0.iter().any(|(outer_ref, _)| {
                seen_outer_refs.contains(outer_ref) && !repr.contains_key(outer_ref)
            }) {
                required_domain_ops.push(info.domain.1.clone());
            }
            if let Some(parent) = info.parent {
                collect_required_domain_ops_from_info(
                    parent.info.as_ref(),
                    repr,
                    seen_outer_refs,
                    required_domain_ops,
                );
            }
        }

        let mut required_domain_ops = Vec::new();
        collect_required_domain_ops_from_info(
            self.info.as_ref(),
            &self.repr,
            seen_outer_refs,
            &mut required_domain_ops,
        );
        required_domain_ops
    }

    /// Map each seen outer-ref to an available equivalent column.
    /// Domain mappings are intentionally not stored in `repr`; they are the
    /// default fallback and are resolved via `domain_repr_recursive`.
    pub(super) fn choose_repr_for_terminal(
        &mut self,
        seen_outer_refs: &HashSet<Column>,
        available: &ColumnSet,
    ) {
        for c in seen_outer_refs {
            if self.info.outer_refs.contains(c) {
                let target_class = self.cclasses.find(c);
                let mut available_cols: Vec<Column> = available.iter().copied().collect();
                available_cols.sort_by_key(|col| col.0);
                if let Some(chosen) = available_cols.into_iter().find(|candidate| {
                    self.cclasses.find(candidate) == target_class
                }) {
                    self.repr.insert(*c, chosen);
                } else {
                    // No local equivalent chosen: fall back to domain/default by
                    // removing any previous replacement entry.
                    self.repr.remove(c);
                }
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
                ColumnRef::new(self.resolve_mapped_col(cr.column).unwrap_or(cr.column)).into_scalar()
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
