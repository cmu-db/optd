//! Deterministic join-tree normalization using the query hypergraph.
//!
//! This pass builds a predicate-aware initial tree before cost-based join
//! ordering. It attaches predicates as soon as their hypergraph endpoints are
//! available and preserves cross products for dummy edges.

use std::collections::HashMap;

use crate::hypergraph::{HyperedgeJoinType, NodeSet, QueryHypergraph};
use crate::{
    BinaryOp, Column, Expr, ExprData, Join, NaryOp, Operator, OperatorData, OptimizerContext,
    QueryContext, ScalarValue, build_hypergraph,
};

use super::{
    OptimizeError, OptimizeResult, Pass, PassResult, QueryPass,
    join_ordering::collect_join_group_roots,
};

pub struct JoinTreeNormalize {
    last_run: Option<(usize, u64)>,
}

impl JoinTreeNormalize {
    pub fn new() -> Self {
        Self { last_run: None }
    }
}

impl Default for JoinTreeNormalize {
    fn default() -> Self {
        Self::new()
    }
}

impl Pass for JoinTreeNormalize {
    fn name(&self) -> &'static str {
        "JoinTreeNormalize"
    }
}

impl QueryPass for JoinTreeNormalize {
    fn run(&mut self, ctx: &mut OptimizerContext) -> OptimizeResult<PassResult> {
        let run_key = (
            (&ctx.query as *const QueryContext) as usize,
            ctx.optimizer_run_id,
        );
        if self.last_run == Some(run_key) {
            return Ok(PassResult::Unchanged);
        }

        let Some(root) = ctx.query.root() else {
            return Ok(PassResult::Unchanged);
        };

        let group_roots = collect_join_group_roots(&ctx.query, root);
        if group_roots.is_empty() {
            return Ok(PassResult::Unchanged);
        }

        self.last_run = Some(run_key);

        let replacements =
            group_roots
                .into_iter()
                .try_fold(Vec::new(), |mut replacements, group_root| {
                    let hg = build_hypergraph(&ctx.query, &mut ctx.analyses, group_root);
                    if hg.nodes.len() < 2
                        || !is_supported_group(&hg)
                        || !has_non_true_predicate(&hg, &ctx.query)
                    {
                        return Ok::<_, OptimizeError>(replacements);
                    }
                    if let Some(new_op) = normalize_group(&hg, &mut ctx.query)? {
                        replacements.push((group_root, new_op));
                    }
                    Ok(replacements)
                })?;

        if replacements.is_empty() {
            return Ok(PassResult::Unchanged);
        }

        for (group_root, new_op) in replacements {
            ctx.rewrites.replace(group_root, new_op);
        }

        super::materialize_reachable_rewrites(root, ctx);

        Ok(PassResult::Changed)
    }
}

#[derive(Clone)]
struct Component {
    nodes: NodeSet,
    op: Operator,
    equalities: EqualityClasses,
}

#[derive(Clone, Default)]
struct EqualityClasses {
    parent: HashMap<Column, Column>,
}

impl EqualityClasses {
    fn merged(left: &Self, right: &Self) -> Self {
        let mut merged = left.clone();
        for (&column, &parent) in &right.parent {
            merged.parent.entry(column).or_insert(parent);
        }
        merged
    }

    fn equivalent(&mut self, left: Column, right: Column) -> bool {
        self.find(left) == self.find(right)
    }

    fn union(&mut self, left: Column, right: Column) {
        let left_root = self.find(left);
        let right_root = self.find(right);
        if left_root != right_root {
            self.parent.insert(right_root, left_root);
        }
    }

    fn find(&mut self, column: Column) -> Column {
        let parent = *self.parent.entry(column).or_insert(column);
        if parent == column {
            column
        } else {
            let root = self.find(parent);
            self.parent.insert(column, root);
            root
        }
    }
}

fn is_supported_group(hg: &QueryHypergraph) -> bool {
    hg.edges
        .iter()
        .all(|edge| edge.join_type == HyperedgeJoinType::Inner)
}

fn has_non_true_predicate(hg: &QueryHypergraph, ctx: &QueryContext) -> bool {
    hg.edges
        .iter()
        .filter_map(|edge| edge.predicate)
        .any(|predicate| !is_true_literal(predicate, ctx))
}

fn normalize_group(
    hg: &QueryHypergraph,
    ctx: &mut QueryContext,
) -> OptimizeResult<Option<Operator>> {
    let mut components = hg
        .nodes
        .iter()
        .enumerate()
        .map(|(idx, node)| Component {
            nodes: 1u64 << idx,
            op: node.root,
            equalities: EqualityClasses::default(),
        })
        .collect::<Vec<_>>();

    let mut used_edges = vec![false; hg.edges.len()];

    while components.len() > 1 {
        let Some((left_idx, right_idx)) = choose_component_pair(hg, &components, &used_edges, ctx)
        else {
            return Err(OptimizeError::PassError {
                pass: "JoinTreeNormalize",
                message: "could not connect join group components".to_string(),
            });
        };

        let edge_indices =
            collect_connecting_edges(hg, &components, &used_edges, left_idx, right_idx);
        for edge_idx in &edge_indices {
            used_edges[*edge_idx] = true;
        }

        let left = components[left_idx].clone();
        let right = components[right_idx].clone();
        let (op, equalities) = build_join_input(hg, &edge_indices, &left, &right, ctx);
        let merged = Component {
            nodes: left.nodes | right.nodes,
            op,
            equalities,
        };

        let (remove_first, remove_second) = if left_idx > right_idx {
            (left_idx, right_idx)
        } else {
            (right_idx, left_idx)
        };
        components.remove(remove_first);
        components.remove(remove_second);
        components.push(merged);
    }

    Ok(Some(components[0].op))
}

fn choose_component_pair(
    hg: &QueryHypergraph,
    components: &[Component],
    used_edges: &[bool],
    ctx: &QueryContext,
) -> Option<(usize, usize)> {
    choose_component_pair_with(hg, components, used_edges, true, ctx)
        .or_else(|| choose_component_pair_with(hg, components, used_edges, false, ctx))
}

fn choose_component_pair_with(
    hg: &QueryHypergraph,
    components: &[Component],
    used_edges: &[bool],
    require_real_predicate: bool,
    ctx: &QueryContext,
) -> Option<(usize, usize)> {
    for (edge_idx, edge) in hg.edges.iter().enumerate() {
        if used_edges[edge_idx] {
            continue;
        }
        if require_real_predicate
            && edge
                .predicate
                .is_none_or(|predicate| is_true_literal(predicate, ctx))
        {
            continue;
        }
        for left_idx in 0..components.len() {
            for right_idx in 0..components.len() {
                if left_idx == right_idx {
                    continue;
                }
                if connects(
                    edge.left,
                    edge.right,
                    components[left_idx].nodes,
                    components[right_idx].nodes,
                ) {
                    return Some((left_idx, right_idx));
                }
            }
        }
    }
    None
}

fn collect_connecting_edges(
    hg: &QueryHypergraph,
    components: &[Component],
    used_edges: &[bool],
    left_idx: usize,
    right_idx: usize,
) -> Vec<usize> {
    let left_nodes = components[left_idx].nodes;
    let right_nodes = components[right_idx].nodes;
    hg.edges
        .iter()
        .enumerate()
        .filter(|(idx, edge)| {
            !used_edges[*idx] && connects(edge.left, edge.right, left_nodes, right_nodes)
        })
        .map(|(idx, _)| idx)
        .collect()
}

fn connects(edge_left: NodeSet, edge_right: NodeSet, left: NodeSet, right: NodeSet) -> bool {
    ((edge_left & left == edge_left) && (edge_right & right == edge_right))
        || ((edge_left & right == edge_left) && (edge_right & left == edge_right))
}

fn build_join_input(
    hg: &QueryHypergraph,
    edge_indices: &[usize],
    outer: &Component,
    inner: &Component,
    ctx: &mut QueryContext,
) -> (Operator, EqualityClasses) {
    let mut equalities = EqualityClasses::merged(&outer.equalities, &inner.equalities);
    let mut predicates = Vec::new();

    for predicate in edge_indices
        .iter()
        .filter_map(|idx| hg.edges[*idx].predicate)
        .filter(|predicate| !is_true_literal(*predicate, ctx))
    {
        let Some((left_col, right_col)) = column_equality(predicate, ctx) else {
            predicates.push(predicate);
            continue;
        };

        if !equalities.equivalent(left_col, right_col) {
            equalities.union(left_col, right_col);
        }
        predicates.push(predicate);
    }

    if predicates.is_empty() {
        return (
            OperatorData::CrossProduct(crate::CrossProduct {
                outer: outer.op,
                inner: inner.op,
            })
            .add(ctx),
            equalities,
        );
    }

    let on = make_and(&mut predicates, ctx);
    (
        OperatorData::Join(Join {
            join_type: crate::JoinType::Inner,
            on,
            outer: outer.op,
            inner: inner.op,
        })
        .add(ctx),
        equalities,
    )
}

fn make_and(exprs: &mut Vec<Expr>, ctx: &mut QueryContext) -> Expr {
    if exprs.len() == 1 {
        exprs.remove(0)
    } else {
        ExprData::Nary {
            op: NaryOp::And,
            exprs: std::mem::take(exprs),
        }
        .add(ctx)
    }
}

fn is_true_literal(expr: Expr, ctx: &QueryContext) -> bool {
    matches!(
        ctx.expr(expr),
        ExprData::Literal(ScalarValue::Boolean(true))
    )
}

fn column_equality(expr: Expr, ctx: &QueryContext) -> Option<(Column, Column)> {
    let ExprData::Binary {
        op: BinaryOp::Eq,
        left,
        right,
    } = ctx.expr(expr)
    else {
        return None;
    };
    let ExprData::ColumnRef(left_col) = ctx.expr(*left) else {
        return None;
    };
    let ExprData::ColumnRef(right_col) = ctx.expr(*right) else {
        return None;
    };
    Some((*left_col, *right_col))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        BinaryOp, ColumnData, CrossProduct, JoinType, OperatorData, PassManager, QueryFormatter,
        Scan, TableRef,
    };
    use arrow_schema::DataType;

    #[test]
    fn normalizes_predicate_before_dummy_cross_product_edge() {
        let mut query = QueryContext::new();
        let a = ColumnData::new("a", DataType::Int64).add(&mut query);
        let b = ColumnData::new("b", DataType::Int64).add(&mut query);
        let c = ColumnData::new("c", DataType::Int64).add(&mut query);
        let scan_a = OperatorData::Scan(Scan {
            table: TableRef::bare("A"),
            columns: vec![a],
        })
        .add(&mut query);
        let scan_b = OperatorData::Scan(Scan {
            table: TableRef::bare("B"),
            columns: vec![b],
        })
        .add(&mut query);
        let scan_c = OperatorData::Scan(Scan {
            table: TableRef::bare("C"),
            columns: vec![c],
        })
        .add(&mut query);
        let cross_ab = OperatorData::CrossProduct(CrossProduct {
            outer: scan_a,
            inner: scan_b,
        })
        .add(&mut query);
        let on_ac = ExprData::Binary {
            op: BinaryOp::Eq,
            left: ExprData::ColumnRef(a).add(&mut query),
            right: ExprData::ColumnRef(c).add(&mut query),
        }
        .add(&mut query);
        let root = OperatorData::Join(Join {
            join_type: JoinType::Inner,
            on: on_ac,
            outer: cross_ab,
            inner: scan_c,
        })
        .add(&mut query);
        query.set_root(root);

        let mut opt = OptimizerContext::new(query);
        let mut pm = PassManager::new();
        pm.add_pass(JoinTreeNormalize::new());
        pm.run(&mut opt)
            .expect("join tree normalize should succeed");

        let root = opt.query.root().expect("root should be set");
        let OperatorData::CrossProduct(cross) = opt.query.operator(root) else {
            panic!("expected cross product root");
        };
        assert!(
            matches!(opt.query.operator(cross.outer), OperatorData::Join(_))
                || matches!(opt.query.operator(cross.inner), OperatorData::Join(_))
        );
    }

    #[test]
    fn preserves_connecting_column_equality_edges() {
        let mut query = QueryContext::new();
        let a = ColumnData::new("a", DataType::Int64).add(&mut query);
        let b = ColumnData::new("b", DataType::Int64).add(&mut query);
        let c = ColumnData::new("c", DataType::Int64).add(&mut query);
        let scan_a = OperatorData::Scan(Scan {
            table: TableRef::bare("A"),
            columns: vec![a],
        })
        .add(&mut query);
        let scan_b = OperatorData::Scan(Scan {
            table: TableRef::bare("B"),
            columns: vec![b],
        })
        .add(&mut query);
        let scan_c = OperatorData::Scan(Scan {
            table: TableRef::bare("C"),
            columns: vec![c],
        })
        .add(&mut query);
        let cross_ab = OperatorData::CrossProduct(CrossProduct {
            outer: scan_a,
            inner: scan_b,
        })
        .add(&mut query);
        let ab = eq(a, b, &mut query);
        let bc = eq(b, c, &mut query);
        let ac = eq(a, c, &mut query);
        let on = ExprData::Nary {
            op: NaryOp::And,
            exprs: vec![ab, bc, ac],
        }
        .add(&mut query);
        let root = OperatorData::Join(Join {
            join_type: JoinType::Inner,
            on,
            outer: cross_ab,
            inner: scan_c,
        })
        .add(&mut query);
        query.set_root(root);

        let mut opt = OptimizerContext::new(query);
        let mut pm = PassManager::new();
        pm.add_pass(JoinTreeNormalize::new());
        pm.run(&mut opt)
            .expect("join tree normalize should succeed");

        let root = opt.query.root().expect("root should be set");
        let join_conditions = join_condition_strings(&opt.query, root);
        assert_eq!(join_conditions.len(), 2, "{join_conditions:?}");
    }

    fn eq(left: Column, right: Column, query: &mut QueryContext) -> Expr {
        ExprData::Binary {
            op: BinaryOp::Eq,
            left: ExprData::ColumnRef(left).add(query),
            right: ExprData::ColumnRef(right).add(query),
        }
        .add(query)
    }

    fn join_condition_strings(query: &QueryContext, op: Operator) -> Vec<String> {
        let formatter = QueryFormatter::new(query);
        let mut conditions = Vec::new();
        collect_join_condition_strings(query, op, &formatter, &mut conditions);
        conditions
    }

    fn collect_join_condition_strings(
        query: &QueryContext,
        op: Operator,
        formatter: &QueryFormatter<'_>,
        conditions: &mut Vec<String>,
    ) {
        match query.operator(op) {
            OperatorData::Join(join) => {
                collect_expr_condition_strings(query, join.on, formatter, conditions);
                collect_join_condition_strings(query, join.outer, formatter, conditions);
                collect_join_condition_strings(query, join.inner, formatter, conditions);
            }
            OperatorData::CrossProduct(cross) => {
                collect_join_condition_strings(query, cross.outer, formatter, conditions);
                collect_join_condition_strings(query, cross.inner, formatter, conditions);
            }
            OperatorData::Projection(projection) => {
                collect_join_condition_strings(query, projection.input, formatter, conditions);
            }
            OperatorData::Selection(selection) => {
                collect_join_condition_strings(query, selection.input, formatter, conditions);
            }
            OperatorData::Output(output) => {
                collect_join_condition_strings(query, output.input, formatter, conditions);
            }
            _ => {}
        }
    }

    fn collect_expr_condition_strings(
        query: &QueryContext,
        expr: Expr,
        formatter: &QueryFormatter<'_>,
        conditions: &mut Vec<String>,
    ) {
        match query.expr(expr) {
            ExprData::Nary {
                op: NaryOp::And,
                exprs,
            } => {
                for expr in exprs {
                    collect_expr_condition_strings(query, *expr, formatter, conditions);
                }
            }
            _ => conditions.push(formatter.format_expr_pub(expr)),
        }
    }
}
