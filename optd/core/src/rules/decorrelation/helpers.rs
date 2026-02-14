/// This module contains the main data structures used in decorrelation.
///
/// Following the paper, immutable global state lives in `UnnestingInfo`
/// (outer refs/domain/parent pointer), while mutable fragment-local state lives
/// in `Unnesting` (repr/nested_repr/cclasses).
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::ir::convert::IntoScalar;
use crate::ir::operator::Operator;
use crate::ir::scalar::{BinaryOpKind, ColumnRef, NaryOpKind};
use crate::ir::{Column, ColumnSet, IRContext, Scalar, ScalarKind};
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

    pub(super) fn is_outer_ref(&self, col: &Column) -> bool {
        self.outer_refs.contains(col)
    }

    pub(super) fn get_domain_repr_set(&self) -> &HashMap<Column, Column> {
        &self.domain.0
    }

    pub(super) fn get_domain_repr_of(&self, col: &Column) -> Option<Column> {
        self.domain.0.get(col).cloned()
    }

    pub(super) fn get_domain_op(&self) -> Arc<Operator> {
        self.domain.1.clone()
    }

    pub(super) fn get_parent(&self) -> Option<&'a Unnesting<'a>> {
        self.parent
    }
}

#[derive(Debug)]
pub(super) struct Unnesting<'a> {
    /// Shared immutable info for this unnesting scope
    info: Arc<UnnestingInfo<'a>>,

    /// Equivalence classes of columns for attribute minimization
    cclasses: UnionFind<Column>,

    /// Mapping from outer column to the current representative column, if one
    /// is chosen
    repr: HashMap<Column, Column>,

    /// Representatives discovered for columns that belong to ancestor scopes.
    nested_repr: HashMap<Column, Column>,
}

impl<'a> Unnesting<'a> {
    pub(super) fn new(info: Arc<UnnestingInfo<'a>>) -> Self {
        Self {
            repr: HashMap::new(),
            nested_repr: HashMap::new(),
            cclasses: UnionFind::default(),
            info,
        }
    }

    pub(super) fn get_info(&self) -> Arc<UnnestingInfo<'a>> {
        self.info.clone()
    }

    pub(super) fn get_cclass_of(&self, col: &Column) -> Column {
        self.cclasses.find(col)
    }

    pub(super) fn get_repr_of(&self, col: &Column) -> Option<&Column> {
        self.repr.get(col)
    }

    pub(super) fn get_nested_repr_of(&self, col: &Column) -> Option<&Column> {
        self.nested_repr.get(col)
    }

    pub(super) fn get_nested_repr_set(&self) -> &HashMap<Column, Column> {
        &self.nested_repr
    }

    pub(super) fn get_resolved_repr_of(&self, col: &Column) -> Option<&Column> {
        self.get_repr_of(col)
            .or_else(|| self.get_nested_repr_of(col))
    }

    pub(super) fn has_resolved_repr_for(&self, col: &Column) -> bool {
        self.repr.contains_key(col) || self.nested_repr.contains_key(col)
    }

    pub(super) fn set_scoped_repr_of(&mut self, key: Column, val: Column) {
        if self.info.is_outer_ref(&key) {
            self.repr.insert(key, val);
            self.nested_repr.remove(&key);
        } else if self.is_outer_ref_recursive(&key) {
            self.nested_repr.insert(key, val);
        }
    }

    pub(super) fn clear_scoped_repr_of(&mut self, key: &Column) {
        self.repr.remove(key);
        self.nested_repr.remove(key);
    }

    pub(super) fn get_resolved_repr_values(&self) -> Vec<Column> {
        let mut values: Vec<Column> = self
            .repr
            .values()
            .chain(self.nested_repr.values())
            .copied()
            .collect();
        values.sort_by_key(|c| c.0);
        values.dedup_by_key(|c| c.0);
        values
    }

    pub(super) fn get_nested_repr_entries(&self) -> Vec<(Column, Column)> {
        let mut entries: Vec<(Column, Column)> = self
            .nested_repr
            .iter()
            .map(|(key, val)| (*key, *val))
            .collect();
        entries.sort_by_key(|(key, _)| key.0);
        entries
    }

    pub(super) fn remap_repr_values(&mut self, remap: &HashMap<Column, Column>) {
        for val in self.repr.values_mut() {
            if let Some(mapped) = remap.get(val) {
                *val = *mapped;
            }
        }
        for val in self.nested_repr.values_mut() {
            if let Some(mapped) = remap.get(val) {
                *val = *mapped;
            }
        }
    }

    pub(super) fn resolve_domain_repr_recursive(&self, col: Column) -> Option<Column> {
        self.info.get_domain_repr_of(&col).or_else(|| {
            self.info
                .parent
                .and_then(|p| p.resolve_domain_repr_recursive(col))
        })
    }

    pub(super) fn resolve_col(&self, col: Column) -> Option<Column> {
        self.get_resolved_repr_of(&col)
            .copied()
            .or_else(|| self.resolve_domain_repr_recursive(col))
    }

    pub(super) fn is_outer_ref_recursive(&self, col: &Column) -> bool {
        self.info.is_outer_ref(col)
            || self
                .info
                .parent
                .as_ref()
                .map(|p| p.is_outer_ref_recursive(col))
                .unwrap_or(false)
    }

    pub(super) fn merge_col_eq_from(&mut self, other: &Unnesting<'_>) {
        for col in other.cclasses.keys() {
            self.cclasses.merge(col, &other.cclasses.find(col));
        }
    }

    // Based on operators we traverse, we can update the cclasses structure to
    // take advantage of known equivalences.
    pub(super) fn update_cclasses_equivalences(&mut self, condition: &Arc<Scalar>) {
        match &condition.kind {
            ScalarKind::BinaryOp(bin) if bin.op_kind == BinaryOpKind::Eq => {
                if let (ScalarKind::ColumnRef(l), ScalarKind::ColumnRef(r)) = (
                    &condition.input_scalars()[0].kind,
                    &condition.input_scalars()[1].kind,
                ) {
                    self.cclasses.merge(&l.column, &r.column);
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
                ColumnRef::new(self.resolve_col(cr.column).unwrap_or(cr.column)).into_scalar()
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
pub(super) fn compute_accessing_set(
    op: &Arc<Operator>,
    ctx: &IRContext,
) -> (HashSet<*const Operator>, HashSet<Column>) {
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
            non_bound_cols.extend((scalar.used_columns() - &available).as_hash_set());
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
    return Arc::as_ptr(subtree) == op_ptr
        || subtree
            .input_operators()
            .iter()
            .any(|c| is_contained_in(op_ptr, c));
}
