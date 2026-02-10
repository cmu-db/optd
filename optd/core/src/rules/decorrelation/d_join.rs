/// This module is the main arbitrary unnesting algorithm, based on the paper
/// Improving Unnesting of Complex Queries (BTW 2025)
/// 
/// Gaps:
/// 1 - We currently only support full unnesting, without the "simple unnesting"
///     pass before that. This is logically correct, and anything that can be
///     simply unnested can be fully unnested. However, we can probably shave
///     off a few milliseconds by running a simple unnesting pass first if
///     latency is essential in the future
/// 2 - We compute outer-references on the fly, by checking which columns of a
///     join operator is not bound by downstream columns being produced. This is
///     a much bigger latency concern than (1), since we do potentially O(n)
///     work per operator in the tree, i.e. potentially O(n^2) total work. This
///     should potentially be done in a pass before decorrelation, and we could
///     also consider the indexed algebra from the paper (section 3.1) for
///     accessing sets (which also currently take O(n) work)
/// 3 - The "advanced constructs" from the paper (like CTEs, WITH RECURSIVE, 
///     etc) are unsupported, since many of these constructs don't even have a
///     meaningful IR in optd

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::ir::{Column, Scalar, ScalarKind, ScalarValue};
use crate::ir::IRContext;
use crate::ir::convert::{IntoOperator, IntoScalar};
use crate::ir::operator::{LogicalAggregate, LogicalDependentJoin, LogicalJoin, LogicalProject, LogicalRemap, Operator, OperatorKind};
use crate::ir::scalar::{BinaryOp, BinaryOpKind, ColumnAssign, ColumnRef, List, NaryOp, NaryOpKind};

use super::UnnestingRule;
use super::helpers::{UnnestingInfo, compute_accessing_set};

impl UnnestingRule {
    // Creates the domain for the new unnesting struct
    fn create_domain(
        &self,
        outer_refs: Vec<Column>, 
        outer: Arc<Operator>,
        ctx: &IRContext
    ) -> (HashMap<Column, Column>, Arc<Operator>) {
        // Compute the domain representation
        let mut domain_repr: HashMap<Column, Column> = HashMap::new();
        for c in &outer_refs {
            let fresh = ctx.define_column(ctx.get_column_meta(c).data_type.clone(), None);
            domain_repr.insert(*c, fresh);
        }

        // Compute the domain operator
        let project_scalars: Vec<Arc<Scalar>> = outer_refs
            .iter()
            .map(|c| ColumnRef::new(*c).into_scalar())
            .collect();
        let project_list = List::new(project_scalars.into()).into_scalar();
        let domain_project = LogicalProject::new(outer.clone(), project_list).into_operator();
        let group_keys: Vec<Arc<Scalar>> = outer_refs
            .iter()
            .map(|c| {
                let col_ref = ColumnRef::new(*c).into_scalar();
                ColumnAssign::new(*c, col_ref).into_scalar()
            })
            .collect();
        let group_keys_list = List::new(group_keys.into()).into_scalar();
        let empty_exprs_list = List::new(vec![].into()).into_scalar();
        let domain_distinct =
            LogicalAggregate::new(domain_project, empty_exprs_list, group_keys_list)
                .into_operator();
        let remap_keys: Vec<Arc<Scalar>> = outer_refs
            .iter()
            .map(|c| {
                let target = *domain_repr
                    .get(c)
                    .expect("domain repr missing for outer ref");
                let col_ref = ColumnRef::new(*c).into_scalar();
                ColumnAssign::new(target, col_ref).into_scalar()
            })
            .collect();
        let remap_list = List::new(remap_keys.into()).into_scalar();
        let domain_d = LogicalRemap::new(domain_distinct, remap_list).into_operator();

        (domain_repr, domain_d)
    }

    // This is the main dependent decorrelation join algorithm, following
    // the algorithm from page 11 of the referenced paper
    pub(super) fn d_join_elimination(
        &self,
        join: Arc<Operator>,
        mut parent_unnesting: Option<&mut UnnestingInfo>,
        ctx: &IRContext,
    ) -> Arc<Operator> {
        // In the nested case we have to unnest the left-hand side first
        let dep_join = if let OperatorKind::LogicalDependentJoin(meta) = &join.kind {
            LogicalDependentJoin::borrow_raw_parts(meta, &join.common)
        } else {
            panic!("Unnesting is only run on dependent joins!");
        };
        let mut required_parent_refs = HashSet::new();
        let (new_outer, condition) = if let Some(ref mut pu) = parent_unnesting {
            required_parent_refs = dep_join
                .inner()
                .collect_used_columns()
                .iter()
                .filter(|c| pu.is_outer_ref(c))
                .copied()
                .collect();
            let left_accessing = compute_accessing_set(dep_join.outer(), &pu.access_refs(), ctx);
            let op = self.unnest(
                dep_join.outer().clone(),
                *pu,
                &left_accessing,
                false,
                ctx,
            );
            let cond = pu.rewrite_columns(dep_join.join_cond().clone());
            (op, cond)
        } else {
            (dep_join.outer().clone(), dep_join.join_cond().clone())
        };

        // Create a new unnesting struct
        let mut outer_refs = HashSet::new();
        let outer_outputs = new_outer.output_columns(ctx);
        for c in dep_join.inner().collect_used_columns().iter() {
            if outer_outputs.contains(c) {
                outer_refs.insert(*c);
            } else if let Some(pu) = parent_unnesting.as_deref() {
                let mapped = pu.resolve_mapped_column(*c);
                if outer_outputs.contains(&mapped) {
                    outer_refs.insert(mapped);
                }
            }
        }
        if outer_refs.is_empty() {
            return LogicalJoin::new(*dep_join.join_type(), new_outer, dep_join.inner().clone(), condition.clone()).into_operator()
        }
        let mut outer_refs_vec: Vec<Column> = outer_refs.iter().copied().collect();
        outer_refs_vec.sort_by_key(|c| c.0);
        let (domain_repr, domain_op) = self.create_domain(outer_refs_vec.clone(), new_outer.clone(), ctx);
        let mut info = UnnestingInfo::new_with_parent(
            outer_refs,
            domain_repr.clone(),
            domain_repr,
            domain_op,
            parent_unnesting.as_deref(),
            required_parent_refs,
        );

        // Merge with parent unnesting if needed
        let accessing = compute_accessing_set(dep_join.inner(), &info.access_refs(), ctx);

        // Unnest right-hand side
        info.update_equivalences(&condition);
        let new_inner = self.unnest(
            dep_join.inner().clone(),
            &mut info,
            &accessing,
            false,
            ctx,
        );
        info.update_repr_with_available(&new_inner.output_columns(ctx));
        let mut new_conds = Vec::new();
        for c in &outer_refs_vec {
            let left_ref = ColumnRef::new(*c).into_scalar();
            let right_ref = ColumnRef::new(*info.repr.get(c).unwrap_or(c)).into_scalar();
            if left_ref == right_ref {
                continue;
            }
            new_conds.push(BinaryOp::new(BinaryOpKind::IsNotDistinctFrom, left_ref, right_ref).into_scalar());
        }
        if !matches!(
            &condition.kind,
            ScalarKind::Literal(meta) if matches!(meta.value, ScalarValue::Boolean(Some(true)))
        ) {
            new_conds.push(condition.clone());
        }
        let final_cond = if new_conds.len() == 1 {
            new_conds[0].clone()
        } else {
            NaryOp::new(NaryOpKind::And, new_conds.into()).into_scalar()
        };

        // Done!
        let new_join = LogicalJoin::new(*dep_join.join_type(), new_outer, new_inner, final_cond)
            .into_operator();
        new_join
    }
}
