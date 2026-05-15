//! Pass that converts subquery expressions into explicit join operators.
//!
//! Three subquery forms are handled:
//!
//! - `ScalarSubquery` → `JoinType::Single`. The subquery must expose exactly one
//!   column; that column is joined in and the expression is replaced with a
//!   `ColumnRef` to it.
//!
//! - `Exists` / `InSubquery` as the **direct predicate** of a `Selection`
//!   (possibly under a single `NOT`) → `JoinType::LeftSemi` / `JoinType::LeftAnti`.
//!   The `Selection` is replaced by the join; no extra column is produced.
//!
//! - `Exists` / `InSubquery` used in any other expression context →
//!   `JoinType::LeftMark(mark_col)`. A boolean mark column is produced by the join
//!   and the subquery expression is replaced with a `ColumnRef` to it.
//!   If the marker is used only in conjunctive predicates the query optimizer
//!   can usually translate the mark join into semi or anti semi joins.

use arrow_schema::DataType;

use crate::{
    AnalysisContext, AvailableColumns, BinaryOp, Column, ColumnData, ExprData, Join, JoinType,
    Operator, OperatorData, OptimizerContext, Relation, Selection,
    optimize::{OptimizeResult, Pass, PassResult, QueryPass},
};

pub struct SubqueryToJoin;

impl Pass for SubqueryToJoin {
    fn name(&self) -> &'static str {
        "subquery_to_join"
    }
}

impl QueryPass for SubqueryToJoin {
    fn run(&mut self, ctx: &mut OptimizerContext) -> OptimizeResult<PassResult> {
        let Some(root) = ctx.query.root() else {
            return Ok(PassResult::Unchanged);
        };

        // Collect operators in bottom-up order so inner subqueries are processed first.
        let ops = collect_post_order(root, ctx);
        let mut changed = false;

        for op in ops {
            let op = ctx.rewrites.resolve(op);
            if let Some(replacement) = rewrite_operator(op, ctx) {
                ctx.rewrites.replace(op, replacement);
                changed = true;
            }
        }

        // Patch every reachable operator's child pointers through the rewrite map.
        // Collect from the original root (to reach parents of replaced nodes) and
        // the resolved root (to reach newly-inserted nodes).
        if changed {
            let resolved = ctx.rewrites.resolve(root);
            let mut ops = collect_post_order(root, ctx);
            if resolved != root {
                for op in collect_post_order(resolved, ctx) {
                    if !ops.contains(&op) {
                        ops.push(op);
                    }
                }
            }
            for op in ops {
                patch_inputs(op, ctx);
            }
        }

        Ok(if changed {
            PassResult::Changed
        } else {
            PassResult::Unchanged
        })
    }
}

// ---------------------------------------------------------------------------
// Per-operator rewrite
// ---------------------------------------------------------------------------

/// Returns a replacement operator if `op` contained any subquery expressions,
/// otherwise returns `None`.
fn rewrite_operator(op: Operator, ctx: &mut OptimizerContext) -> Option<Operator> {
    match ctx.query.operator(op).clone() {
        OperatorData::Selection(sel) => rewrite_selection(op, sel, ctx),
        OperatorData::Map(map) => {
            let mut input = map.input;
            let mut computations = map.computations.clone();
            let mut any = false;
            for (_, expr) in &mut computations {
                if lift_subqueries_from_expr(*expr, &mut input, ctx) {
                    any = true;
                }
            }
            if any {
                let new_op = OperatorData::Map(crate::Map {
                    computations,
                    input,
                })
                .add(&mut ctx.query);
                Some(new_op)
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Rewrites a `Selection`. If the predicate is a bare `Exists`/`InSubquery`
/// (or its negation), we emit a semi/anti join directly. Otherwise we lift
/// subqueries from the predicate expression into mark joins.
fn rewrite_selection(
    _op: Operator,
    sel: crate::Selection,
    ctx: &mut OptimizerContext,
) -> Option<Operator> {
    // --- direct semi/anti join case ---
    if let Some(join) = try_direct_semijoin(&sel, ctx) {
        return Some(join);
    }

    // --- general case: lift subqueries from predicate into mark/single joins ---
    let mut input = sel.input;
    let predicate = sel.predicate;
    if lift_subqueries_from_expr(predicate, &mut input, ctx) {
        let new_op = OperatorData::Selection(Selection { predicate, input }).add(&mut ctx.query);
        Some(new_op)
    } else {
        None
    }
}

/// If `sel.predicate` is exactly `Exists(subquery)`, `NOT Exists(subquery)`,
/// `InSubquery(expr, subquery)`, or `NOT InSubquery(expr, subquery)`, emit a
/// `LeftSemi` or `LeftAnti` join and return it.
fn try_direct_semijoin(sel: &crate::Selection, ctx: &mut OptimizerContext) -> Option<Operator> {
    // Unwrap single-element And (produced by select_rel's and() helper).
    let predicate = match ctx.query.expr(sel.predicate) {
        ExprData::Nary {
            op: crate::NaryOp::And,
            exprs,
        } if exprs.len() == 1 => exprs[0],
        _ => sel.predicate,
    };
    let (subquery, negated, in_expr) = match ctx.query.expr(predicate) {
        ExprData::Exists { subquery, negated } => (*subquery, *negated, None),
        ExprData::InSubquery {
            expr,
            subquery,
            negated,
        } => (*subquery, *negated, Some(*expr)),
        ExprData::Unary {
            op: crate::UnaryOp::Not,
            expr,
        } => match ctx.query.expr(*expr) {
            ExprData::Exists { subquery, negated } => (*subquery, !negated, None),
            ExprData::InSubquery {
                expr: inner,
                subquery,
                negated,
            } => (*subquery, !negated, Some(*inner)),
            _ => return None,
        },
        _ => return None,
    };

    let join_type = if negated {
        JoinType::LeftAnti
    } else {
        JoinType::LeftSemi
    };

    // Build the join condition.
    let on = match in_expr {
        None => {
            // EXISTS: condition is always true (no predicate needed).
            ExprData::Literal(crate::ScalarValue::Boolean(true)).add(&mut ctx.query)
        }
        Some(lhs_expr) => {
            // IN: condition is lhs = first_column_of_subquery.
            let rhs_col = single_available_column(subquery, ctx);
            let rhs = ExprData::ColumnRef(rhs_col).add(&mut ctx.query);
            ExprData::Binary {
                op: BinaryOp::Eq,
                left: lhs_expr,
                right: rhs,
            }
            .add(&mut ctx.query)
        }
    };

    let join = OperatorData::Join(Join {
        join_type,
        on,
        outer: sel.input,
        inner: subquery,
    })
    .add(&mut ctx.query);

    Some(join)
}

// ---------------------------------------------------------------------------
// Expression-level subquery lifting
// ---------------------------------------------------------------------------

/// Walks `expr` and for each subquery node found, inserts a join above `input`
/// and rewrites the expression in-place to a `ColumnRef`. Returns `true` if
/// any subquery was lifted.
fn lift_subqueries_from_expr(
    expr: crate::Expr,
    input: &mut Operator,
    ctx: &mut OptimizerContext,
) -> bool {
    match ctx.query.expr(expr).clone() {
        ExprData::Exists { subquery, negated } => {
            let mark_col = fresh_mark_column("exists_mark", ctx);
            let join_type = JoinType::LeftMark(mark_col);
            let on = ExprData::Literal(crate::ScalarValue::Boolean(true)).add(&mut ctx.query);
            let join = OperatorData::Join(Join {
                join_type,
                on,
                outer: *input,
                inner: subquery,
            })
            .add(&mut ctx.query);
            *input = join;

            // Replace the Exists expr with ColumnRef(mark_col), possibly negated.
            let col_ref = ExprData::ColumnRef(mark_col).add(&mut ctx.query);
            let replacement = if negated {
                ExprData::Unary {
                    op: crate::UnaryOp::Not,
                    expr: col_ref,
                }
                .add(&mut ctx.query)
            } else {
                col_ref
            };
            *ctx.query.expr_mut(expr) = ctx.query.expr(replacement).clone();
            true
        }
        ExprData::InSubquery {
            expr: lhs,
            subquery,
            negated,
        } => {
            let mark_col = fresh_mark_column("in_mark", ctx);
            let rhs_col = single_available_column(subquery, ctx);
            let rhs = ExprData::ColumnRef(rhs_col).add(&mut ctx.query);
            let on = ExprData::Binary {
                op: BinaryOp::Eq,
                left: lhs,
                right: rhs,
            }
            .add(&mut ctx.query);
            let join = OperatorData::Join(Join {
                join_type: JoinType::LeftMark(mark_col),
                on,
                outer: *input,
                inner: subquery,
            })
            .add(&mut ctx.query);
            *input = join;

            let col_ref = ExprData::ColumnRef(mark_col).add(&mut ctx.query);
            let replacement = if negated {
                ExprData::Unary {
                    op: crate::UnaryOp::Not,
                    expr: col_ref,
                }
                .add(&mut ctx.query)
            } else {
                col_ref
            };
            *ctx.query.expr_mut(expr) = ctx.query.expr(replacement).clone();
            true
        }
        ExprData::ScalarSubquery { subquery } => {
            let scalar_col = single_available_column(subquery, ctx);
            let on = ExprData::Literal(crate::ScalarValue::Boolean(true)).add(&mut ctx.query);
            let join = OperatorData::Join(Join {
                join_type: JoinType::Single,
                on,
                outer: *input,
                inner: subquery,
            })
            .add(&mut ctx.query);
            *input = join;

            *ctx.query.expr_mut(expr) = ExprData::ColumnRef(scalar_col);
            true
        }
        // Recurse into compound expressions.
        ExprData::Unary { op, expr: inner } => {
            if lift_subqueries_from_expr(inner, input, ctx) {
                // inner was rewritten in-place; just re-use the handle.
                *ctx.query.expr_mut(expr) = ExprData::Unary { op, expr: inner };
                true
            } else {
                false
            }
        }
        ExprData::Binary { op, left, right } => {
            let l = lift_subqueries_from_expr(left, input, ctx);
            let r = lift_subqueries_from_expr(right, input, ctx);
            if l || r {
                *ctx.query.expr_mut(expr) = ExprData::Binary { op, left, right };
            }
            l || r
        }
        ExprData::Nary { op, exprs } => {
            let mut any = false;
            for &e in &exprs {
                if lift_subqueries_from_expr(e, input, ctx) {
                    any = true;
                }
            }
            if any {
                *ctx.query.expr_mut(expr) = ExprData::Nary { op, exprs };
            }
            any
        }
        ExprData::CaseWhen {
            when_then,
            else_expr,
        } => {
            let mut any = false;
            for (when, then) in &when_then {
                any |= lift_subqueries_from_expr(*when, input, ctx);
                any |= lift_subqueries_from_expr(*then, input, ctx);
            }
            if let Some(e) = else_expr {
                any |= lift_subqueries_from_expr(e, input, ctx);
            }
            if any {
                *ctx.query.expr_mut(expr) = ExprData::CaseWhen {
                    when_then,
                    else_expr,
                };
            }
            any
        }
        ExprData::ScalarFunction { function, args } => {
            let mut any = false;
            for &a in &args {
                any |= lift_subqueries_from_expr(a, input, ctx);
            }
            if any {
                *ctx.query.expr_mut(expr) = ExprData::ScalarFunction { function, args };
            }
            any
        }
        ExprData::Cast { expr: inner, ty } => {
            if lift_subqueries_from_expr(inner, input, ctx) {
                *ctx.query.expr_mut(expr) = ExprData::Cast { expr: inner, ty };
                true
            } else {
                false
            }
        }
        ExprData::Literal(_) | ExprData::ColumnRef(_) | ExprData::Like { .. } => false,
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Returns the single available column of `subquery`, asserting there is exactly one.
fn single_available_column(subquery: Operator, ctx: &mut OptimizerContext) -> Column {
    let mut analyses = AnalysisContext::new();
    let cols = analyses
        .get::<AvailableColumns>(&ctx.query, subquery)
        .expect("subquery available columns");
    assert_eq!(
        cols.len(),
        1,
        "subquery must expose exactly one column for IN/ScalarSubquery, got {}",
        cols.len()
    );
    cols[0]
}

/// Allocates a fresh boolean mark column.
fn fresh_mark_column(name: &str, ctx: &mut OptimizerContext) -> Column {
    ColumnData::new(name, DataType::Boolean).add(&mut ctx.query)
}

/// Rewrites all child operator pointers in `op` through the rewrite map.
fn patch_inputs(op: Operator, ctx: &mut OptimizerContext) {
    let data = ctx.query.operator(op).clone();
    let patched = match data {
        OperatorData::Selection(mut s) => {
            s.input = ctx.rewrites.resolve(s.input);
            OperatorData::Selection(s)
        }
        OperatorData::Map(mut m) => {
            m.input = ctx.rewrites.resolve(m.input);
            OperatorData::Map(m)
        }
        OperatorData::Join(mut j) => {
            j.outer = ctx.rewrites.resolve(j.outer);
            j.inner = ctx.rewrites.resolve(j.inner);
            OperatorData::Join(j)
        }
        OperatorData::CrossProduct(mut cp) => {
            cp.outer = ctx.rewrites.resolve(cp.outer);
            cp.inner = ctx.rewrites.resolve(cp.inner);
            OperatorData::CrossProduct(cp)
        }
        OperatorData::Sort(mut s) => {
            s.input = ctx.rewrites.resolve(s.input);
            OperatorData::Sort(s)
        }
        OperatorData::Limit(mut l) => {
            l.input = ctx.rewrites.resolve(l.input);
            OperatorData::Limit(l)
        }
        OperatorData::Aggregation(mut a) => {
            a.input = ctx.rewrites.resolve(a.input);
            OperatorData::Aggregation(a)
        }
        OperatorData::Projection(mut p) => {
            p.input = ctx.rewrites.resolve(p.input);
            OperatorData::Projection(p)
        }
        OperatorData::Output(mut o) => {
            o.input = ctx.rewrites.resolve(o.input);
            OperatorData::Output(o)
        }
        OperatorData::Rename(mut r) => {
            r.input = ctx.rewrites.resolve(r.input);
            OperatorData::Rename(r)
        }
        OperatorData::Scan(_) | OperatorData::TableFunction(_) | OperatorData::SingleRow => return,
    };
    *ctx.query.operator_mut(op) = patched;
}

fn collect_post_order(root: Operator, ctx: &OptimizerContext) -> Vec<Operator> {
    let mut visited = std::collections::HashSet::new();
    let mut result = Vec::new();
    post_order(root, ctx, &mut visited, &mut result);
    result
}

fn post_order(
    op: Operator,
    ctx: &OptimizerContext,
    visited: &mut std::collections::HashSet<Operator>,
    result: &mut Vec<Operator>,
) {
    let op = ctx.rewrites.resolve(op);
    if !visited.insert(op) {
        return;
    }
    for child in op.get(&ctx.query).inputs() {
        post_order(child, ctx, visited, result);
    }
    result.push(op);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        BinaryOp, ColumnData, ExprData, JoinType, OperatorData, OptimizerContext, Output,
        Projection, QueryContext, Scan, Selection, TableRef,
        optimize::{PassManager, PassResult, QueryPass},
    };
    use arrow_schema::DataType;

    fn run_pass(ctx: QueryContext) -> QueryContext {
        let mut opt = OptimizerContext::new(ctx);
        let mut pm = PassManager::new(10);
        pm.add_pass(SubqueryToJoin);
        pm.run(&mut opt).unwrap();
        if let Some(root) = opt.query.root() {
            let resolved = opt.rewrites.resolve(root);
            opt.query.set_root(resolved);
        }
        opt.into_query()
    }

    /// EXISTS subquery as direct Selection predicate → LeftSemi join.
    #[test]
    fn exists_direct_selection_becomes_semi_join() {
        let mut ctx = QueryContext::new();
        let uid = ColumnData::new("uid", DataType::Int64).add(&mut ctx);
        let oid = ColumnData::new("oid", DataType::Int64).add(&mut ctx);

        let users = OperatorData::Scan(Scan {
            table: TableRef::bare("users"),
            columns: vec![uid],
        })
        .add(&mut ctx);
        let orders = OperatorData::Scan(Scan {
            table: TableRef::bare("orders"),
            columns: vec![oid],
        })
        .add(&mut ctx);

        let exists = ExprData::Exists {
            subquery: orders,
            negated: false,
        }
        .add(&mut ctx);
        let sel = OperatorData::Selection(Selection {
            predicate: exists,
            input: users,
        })
        .add(&mut ctx);
        let out = OperatorData::Output(Output { input: sel }).add(&mut ctx);
        ctx.set_root(out);

        let ctx = run_pass(ctx);

        // Root → Output → Join(LeftSemi)
        let root = ctx.root().unwrap();
        let OperatorData::Output(o) = ctx.operator(root) else {
            panic!()
        };
        let OperatorData::Join(j) = ctx.operator(o.input) else {
            panic!("expected join, got {:?}", ctx.operator(o.input))
        };
        assert_eq!(j.join_type, JoinType::LeftSemi);
    }

    /// NOT EXISTS → LeftAnti join.
    #[test]
    fn not_exists_direct_selection_becomes_anti_join() {
        let mut ctx = QueryContext::new();
        let uid = ColumnData::new("uid", DataType::Int64).add(&mut ctx);
        let oid = ColumnData::new("oid", DataType::Int64).add(&mut ctx);

        let users = OperatorData::Scan(Scan {
            table: TableRef::bare("users"),
            columns: vec![uid],
        })
        .add(&mut ctx);
        let orders = OperatorData::Scan(Scan {
            table: TableRef::bare("orders"),
            columns: vec![oid],
        })
        .add(&mut ctx);

        let exists = ExprData::Exists {
            subquery: orders,
            negated: true,
        }
        .add(&mut ctx);
        let sel = OperatorData::Selection(Selection {
            predicate: exists,
            input: users,
        })
        .add(&mut ctx);
        ctx.set_root(sel);

        let ctx = run_pass(ctx);

        let root = ctx.root().unwrap();
        let OperatorData::Join(j) = ctx.operator(root) else {
            panic!()
        };
        assert_eq!(j.join_type, JoinType::LeftAnti);
    }

    /// IN subquery as direct Selection predicate → LeftSemi join with equality condition.
    #[test]
    fn in_subquery_direct_selection_becomes_semi_join_with_eq() {
        let mut ctx = QueryContext::new();
        let uid = ColumnData::new("uid", DataType::Int64).add(&mut ctx);
        let oid = ColumnData::new("oid", DataType::Int64).add(&mut ctx);

        let users = OperatorData::Scan(Scan {
            table: TableRef::bare("users"),
            columns: vec![uid],
        })
        .add(&mut ctx);
        let orders = OperatorData::Scan(Scan {
            table: TableRef::bare("orders"),
            columns: vec![oid],
        })
        .add(&mut ctx);

        let uid_ref = ExprData::ColumnRef(uid).add(&mut ctx);
        let in_sub = ExprData::InSubquery {
            expr: uid_ref,
            subquery: orders,
            negated: false,
        }
        .add(&mut ctx);
        let sel = OperatorData::Selection(Selection {
            predicate: in_sub,
            input: users,
        })
        .add(&mut ctx);
        ctx.set_root(sel);

        let ctx = run_pass(ctx);

        let root = ctx.root().unwrap();
        let OperatorData::Join(j) = ctx.operator(root) else {
            panic!()
        };
        assert_eq!(j.join_type, JoinType::LeftSemi);
        // Condition should be an equality.
        let ExprData::Binary { op, .. } = ctx.expr(j.on) else {
            panic!()
        };
        assert_eq!(*op, BinaryOp::Eq);
    }

    /// EXISTS inside a Map computation (non-direct context) → Mark join.
    #[test]
    fn exists_in_map_becomes_mark_join() {
        let mut ctx = QueryContext::new();
        let uid = ColumnData::new("uid", DataType::Int64).add(&mut ctx);
        let oid = ColumnData::new("oid", DataType::Int64).add(&mut ctx);
        let flag = ColumnData::new("flag", DataType::Boolean).add(&mut ctx);

        let users = OperatorData::Scan(Scan {
            table: TableRef::bare("users"),
            columns: vec![uid],
        })
        .add(&mut ctx);
        let orders = OperatorData::Scan(Scan {
            table: TableRef::bare("orders"),
            columns: vec![oid],
        })
        .add(&mut ctx);

        let exists = ExprData::Exists {
            subquery: orders,
            negated: false,
        }
        .add(&mut ctx);
        let map = OperatorData::Map(crate::Map {
            computations: vec![(flag, exists)],
            input: users,
        })
        .add(&mut ctx);
        ctx.set_root(map);

        let ctx = run_pass(ctx);

        let root = ctx.root().unwrap();
        let OperatorData::Map(m) = ctx.operator(root) else {
            panic!()
        };
        // The map's input should now be a Mark join.
        let OperatorData::Join(j) = ctx.operator(m.input) else {
            panic!()
        };
        assert!(matches!(j.join_type, JoinType::LeftMark(_)));
        // The computation expr should now be a ColumnRef to the mark column.
        let (_, expr) = m.computations[0];
        assert!(matches!(ctx.expr(expr), ExprData::ColumnRef(_)));
    }

    /// ScalarSubquery → Single join; expression replaced with ColumnRef.
    #[test]
    fn scalar_subquery_becomes_single_join() {
        let mut ctx = QueryContext::new();
        let uid = ColumnData::new("uid", DataType::Int64).add(&mut ctx);
        let total = ColumnData::new("total", DataType::Float64).add(&mut ctx);

        let users = OperatorData::Scan(Scan {
            table: TableRef::bare("users"),
            columns: vec![uid],
        })
        .add(&mut ctx);
        // Subquery exposes exactly one column.
        let agg_col = ColumnData::new("agg", DataType::Float64).add(&mut ctx);
        let sub_scan = OperatorData::Scan(Scan {
            table: TableRef::bare("orders"),
            columns: vec![agg_col],
        })
        .add(&mut ctx);
        let proj = OperatorData::Projection(Projection {
            columns: vec![agg_col],
            input: sub_scan,
        })
        .add(&mut ctx);

        let scalar_sub = ExprData::ScalarSubquery { subquery: proj }.add(&mut ctx);
        let map = OperatorData::Map(crate::Map {
            computations: vec![(total, scalar_sub)],
            input: users,
        })
        .add(&mut ctx);
        ctx.set_root(map);

        let ctx = run_pass(ctx);

        let root = ctx.root().unwrap();
        let OperatorData::Map(m) = ctx.operator(root) else {
            panic!()
        };
        let OperatorData::Join(j) = ctx.operator(m.input) else {
            panic!()
        };
        assert_eq!(j.join_type, JoinType::Single);
        let (_, expr) = m.computations[0];
        assert!(matches!(ctx.expr(expr), ExprData::ColumnRef(_)));
    }

    /// Plan with no subqueries is unchanged.
    #[test]
    fn no_subquery_is_unchanged() {
        let mut ctx = QueryContext::new();
        let id = ColumnData::new("id", DataType::Int64).add(&mut ctx);
        let scan = OperatorData::Scan(Scan {
            table: TableRef::bare("t"),
            columns: vec![id],
        })
        .add(&mut ctx);
        ctx.set_root(scan);

        let mut opt = OptimizerContext::new(ctx.clone());
        let result = SubqueryToJoin.run(&mut opt).unwrap();
        assert_eq!(result, PassResult::Unchanged);
    }
}
