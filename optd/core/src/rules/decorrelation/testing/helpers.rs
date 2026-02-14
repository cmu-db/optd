use crate::ir::builder::{column_assign, column_ref};
use crate::ir::convert::IntoScalar;
use crate::ir::explain::quick_explain;
use crate::ir::operator::{
    LogicalAggregate, LogicalDependentJoin, LogicalJoin, LogicalOrderBy, LogicalProject,
    LogicalRemap, LogicalSelect, MockScan, Operator, OperatorKind, join::JoinType,
};
use crate::ir::scalar::{
    BinaryOp, BinaryOpKind, ColumnAssign, ColumnRef, List, Literal, NaryOp, NaryOpKind, ScalarKind,
};
use crate::ir::{Column, DataType, IRContext, Scalar, ScalarValue};
use crate::rules::decorrelation::UnnestingRule;
use std::collections::BTreeMap;
use std::sync::Arc;

// --- Helper functions to help create a logical plan ---

pub(super) fn make_cols(ctx: &IRContext, n: usize) -> Vec<Column> {
    (0..n)
        .map(|_| ctx.define_column(DataType::Int32, None))
        .collect()
}

/// Creates a NULL safe equality predicate
pub(super) fn null_safe_eq(
    lhs: Arc<crate::ir::Scalar>,
    rhs: Arc<crate::ir::Scalar>,
) -> Arc<crate::ir::Scalar> {
    BinaryOp::new(BinaryOpKind::IsNotDistinctFrom, lhs, rhs).into_scalar()
}

/// Creates a domain operator and remaps its output columns to new aliases.
pub(super) fn create_domain_with_aliases(
    input: Arc<Operator>,
    from_cols: Vec<Column>,
    to_cols: Vec<Column>,
) -> Arc<Operator> {
    assert_eq!(from_cols.len(), to_cols.len(), "domain remap must be 1:1");
    let project_exprs: Vec<_> = from_cols.iter().map(|c| column_ref(*c)).collect();
    let domain_project = input.logical_project(project_exprs);

    let group_keys: Vec<_> = from_cols
        .iter()
        .map(|c| column_assign(*c, column_ref(*c)))
        .collect();
    let domain_distinct = domain_project.logical_aggregate(std::iter::empty(), group_keys);

    let remap_assigns: Vec<_> = from_cols
        .iter()
        .zip(to_cols.iter())
        .map(|(from, to)| column_assign(*to, column_ref(*from)))
        .collect();
    domain_distinct.logical_remap(remap_assigns)
}

type EvalRow = BTreeMap<Column, Option<i32>>;
type MockData = BTreeMap<usize, Vec<EvalRow>>;

#[derive(Clone, Copy)]
enum EvalValue {
    Int(Option<i32>),
    Bool(Option<bool>),
}

fn collect_mock_schemas(op: &Arc<Operator>, schemas: &mut BTreeMap<usize, Vec<Column>>) {
    if let OperatorKind::MockScan(meta) = &op.kind {
        let scan = MockScan::borrow_raw_parts(meta, &op.common);
        let mut cols: Vec<Column> = scan.spec().mocked_output_columns.iter().copied().collect();
        cols.sort_by_key(|c| c.0);
        if let Some(existing) = schemas.get(scan.mock_id()) {
            assert_eq!(
                existing, &cols,
                "same mock id must expose the same output columns"
            );
        } else {
            schemas.insert(*scan.mock_id(), cols);
        }
    }
    for child in op.input_operators() {
        collect_mock_schemas(child, schemas);
    }
}

fn next_rand(state: &mut u64) -> u64 {
    *state = state
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    *state
}

fn random_value(state: &mut u64) -> Option<i32> {
    match next_rand(state) % 5 {
        0 => None,
        1 => Some(-1),
        2 => Some(0),
        3 => Some(1),
        _ => Some(2),
    }
}

fn generate_mock_data(schemas: &BTreeMap<usize, Vec<Column>>, seed: u64) -> MockData {
    let mut state = seed;
    let mut data = BTreeMap::new();
    for (mock_id, cols) in schemas {
        let row_count = (next_rand(&mut state) % 4) as usize;
        let mut rows = Vec::with_capacity(row_count);
        for _ in 0..row_count {
            let mut row = BTreeMap::new();
            for col in cols {
                row.insert(*col, random_value(&mut state));
            }
            rows.push(row);
        }
        data.insert(*mock_id, rows);
    }
    data
}

fn lookup_col(row: &EvalRow, env: &[EvalRow], col: Column) -> Option<i32> {
    if let Some(v) = row.get(&col) {
        return *v;
    }
    for parent in env.iter().rev() {
        if let Some(v) = parent.get(&col) {
            return *v;
        }
    }
    panic!("unbound column during evaluation: {:?}", col);
}

fn as_bool(v: EvalValue) -> Option<bool> {
    match v {
        EvalValue::Bool(b) => b,
        _ => panic!("expected boolean scalar"),
    }
}

fn as_int(v: EvalValue) -> Option<i32> {
    match v {
        EvalValue::Int(i) => i,
        _ => panic!("expected integer scalar"),
    }
}

fn eval_scalar(s: &Arc<Scalar>, row: &EvalRow, env: &[EvalRow]) -> EvalValue {
    match &s.kind {
        ScalarKind::ColumnRef(meta) => {
            let cr = ColumnRef::borrow_raw_parts(meta, &s.common);
            EvalValue::Int(lookup_col(row, env, *cr.column()))
        }
        ScalarKind::Literal(meta) => {
            let lit = Literal::borrow_raw_parts(meta, &s.common);
            match lit.value() {
                ScalarValue::Boolean(v) => EvalValue::Bool(*v),
                ScalarValue::Int32(v) => EvalValue::Int(*v),
                ScalarValue::Int64(v) => EvalValue::Int(v.map(|x| x as i32)),
                _ => panic!("unsupported literal in semantic test evaluator"),
            }
        }
        ScalarKind::ColumnAssign(meta) => {
            let assign = ColumnAssign::borrow_raw_parts(meta, &s.common);
            eval_scalar(assign.expr(), row, env)
        }
        ScalarKind::BinaryOp(meta) => {
            let bin = BinaryOp::borrow_raw_parts(meta, &s.common);
            match bin.op_kind() {
                BinaryOpKind::Eq => {
                    let l = as_int(eval_scalar(bin.lhs(), row, env));
                    let r = as_int(eval_scalar(bin.rhs(), row, env));
                    EvalValue::Bool(match (l, r) {
                        (Some(a), Some(b)) => Some(a == b),
                        _ => None,
                    })
                }
                BinaryOpKind::IsNotDistinctFrom => {
                    let l = as_int(eval_scalar(bin.lhs(), row, env));
                    let r = as_int(eval_scalar(bin.rhs(), row, env));
                    EvalValue::Bool(Some(l == r))
                }
                BinaryOpKind::Gt => {
                    let l = as_int(eval_scalar(bin.lhs(), row, env));
                    let r = as_int(eval_scalar(bin.rhs(), row, env));
                    EvalValue::Bool(match (l, r) {
                        (Some(a), Some(b)) => Some(a > b),
                        _ => None,
                    })
                }
                BinaryOpKind::Ge => {
                    let l = as_int(eval_scalar(bin.lhs(), row, env));
                    let r = as_int(eval_scalar(bin.rhs(), row, env));
                    EvalValue::Bool(match (l, r) {
                        (Some(a), Some(b)) => Some(a >= b),
                        _ => None,
                    })
                }
                BinaryOpKind::Lt => {
                    let l = as_int(eval_scalar(bin.lhs(), row, env));
                    let r = as_int(eval_scalar(bin.rhs(), row, env));
                    EvalValue::Bool(match (l, r) {
                        (Some(a), Some(b)) => Some(a < b),
                        _ => None,
                    })
                }
                BinaryOpKind::Le => {
                    let l = as_int(eval_scalar(bin.lhs(), row, env));
                    let r = as_int(eval_scalar(bin.rhs(), row, env));
                    EvalValue::Bool(match (l, r) {
                        (Some(a), Some(b)) => Some(a <= b),
                        _ => None,
                    })
                }
                _ => panic!("unsupported binary op in semantic test evaluator"),
            }
        }
        ScalarKind::NaryOp(meta) => {
            let nary = NaryOp::borrow_raw_parts(meta, &s.common);
            match nary.op_kind() {
                NaryOpKind::And => {
                    let mut saw_null = false;
                    for t in nary.terms() {
                        match as_bool(eval_scalar(t, row, env)) {
                            Some(true) => {}
                            Some(false) => return EvalValue::Bool(Some(false)),
                            None => saw_null = true,
                        }
                    }
                    EvalValue::Bool(if saw_null { None } else { Some(true) })
                }
                NaryOpKind::Or => {
                    let mut saw_null = false;
                    for t in nary.terms() {
                        match as_bool(eval_scalar(t, row, env)) {
                            Some(true) => return EvalValue::Bool(Some(true)),
                            Some(false) => {}
                            None => saw_null = true,
                        }
                    }
                    EvalValue::Bool(if saw_null { None } else { Some(false) })
                }
            }
        }
        _ => panic!("unsupported scalar kind in semantic test evaluator"),
    }
}

fn eval_predicate_true(pred: &Arc<Scalar>, row: &EvalRow, env: &[EvalRow]) -> bool {
    matches!(as_bool(eval_scalar(pred, row, env)), Some(true))
}

fn combine_rows(left: &EvalRow, right: &EvalRow) -> EvalRow {
    let mut out = left.clone();
    for (c, v) in right {
        if let Some(existing) = out.get(c) {
            assert_eq!(
                existing, v,
                "column collision with different values during semantic evaluation"
            );
        }
        out.insert(*c, *v);
    }
    out
}

fn eval_list_members(list: &Arc<Scalar>) -> Vec<Arc<Scalar>> {
    list.try_borrow::<List>().unwrap().members().to_vec()
}

fn eval_op(op: &Arc<Operator>, env: &[EvalRow], data: &MockData, ctx: &IRContext) -> Vec<EvalRow> {
    match &op.kind {
        OperatorKind::MockScan(meta) => {
            let scan = MockScan::borrow_raw_parts(meta, &op.common);
            data.get(scan.mock_id()).cloned().unwrap_or_default()
        }
        OperatorKind::LogicalSelect(meta) => {
            let sel = LogicalSelect::borrow_raw_parts(meta, &op.common);
            eval_op(sel.input(), env, data, ctx)
                .into_iter()
                .filter(|r| eval_predicate_true(sel.predicate(), r, env))
                .collect()
        }
        OperatorKind::LogicalProject(meta) => {
            let proj = LogicalProject::borrow_raw_parts(meta, &op.common);
            let members = eval_list_members(proj.projections());
            eval_op(proj.input(), env, data, ctx)
                .into_iter()
                .map(|r| {
                    let mut out = EvalRow::new();
                    for m in &members {
                        if let Ok(assign) = m.try_borrow::<ColumnAssign>() {
                            out.insert(
                                *assign.column(),
                                as_int(eval_scalar(assign.expr(), &r, env)),
                            );
                        } else if let Ok(cr) = m.try_borrow::<ColumnRef>() {
                            out.insert(*cr.column(), lookup_col(&r, env, *cr.column()));
                        } else {
                            panic!("unsupported projection expression");
                        }
                    }
                    out
                })
                .collect()
        }
        OperatorKind::LogicalRemap(meta) => {
            let remap = LogicalRemap::borrow_raw_parts(meta, &op.common);
            let members = eval_list_members(remap.mappings());
            eval_op(remap.input(), env, data, ctx)
                .into_iter()
                .map(|r| {
                    let mut out = EvalRow::new();
                    for m in &members {
                        let assign = m.try_borrow::<ColumnAssign>().unwrap();
                        out.insert(
                            *assign.column(),
                            as_int(eval_scalar(assign.expr(), &r, env)),
                        );
                    }
                    out
                })
                .collect()
        }
        OperatorKind::LogicalAggregate(meta) => {
            let agg = LogicalAggregate::borrow_raw_parts(meta, &op.common);
            let input_rows = eval_op(agg.input(), env, data, ctx);
            let key_members = eval_list_members(agg.keys());
            if key_members.is_empty() {
                return vec![EvalRow::new()];
            }
            let mut groups: BTreeMap<Vec<(Column, Option<i32>)>, EvalRow> = BTreeMap::new();
            for r in input_rows {
                let mut out = EvalRow::new();
                for m in &key_members {
                    if let Ok(assign) = m.try_borrow::<ColumnAssign>() {
                        out.insert(
                            *assign.column(),
                            as_int(eval_scalar(assign.expr(), &r, env)),
                        );
                    } else if let Ok(cr) = m.try_borrow::<ColumnRef>() {
                        out.insert(*cr.column(), lookup_col(&r, env, *cr.column()));
                    } else {
                        panic!("unsupported aggregate key expression");
                    }
                }
                let key: Vec<(Column, Option<i32>)> = out.iter().map(|(c, v)| (*c, *v)).collect();
                groups.entry(key).or_insert(out);
            }
            groups.into_values().collect()
        }
        OperatorKind::LogicalOrderBy(meta) => {
            let order = LogicalOrderBy::borrow_raw_parts(meta, &op.common);
            eval_op(order.input(), env, data, ctx)
        }
        OperatorKind::LogicalJoin(meta) => {
            let join = LogicalJoin::borrow_raw_parts(meta, &op.common);
            let left_rows = eval_op(join.outer(), env, data, ctx);
            let right_rows = eval_op(join.inner(), env, data, ctx);
            match join.join_type() {
                JoinType::Inner => {
                    let mut out = Vec::new();
                    for l in &left_rows {
                        for r in &right_rows {
                            let row = combine_rows(l, r);
                            if eval_predicate_true(join.join_cond(), &row, env) {
                                out.push(row);
                            }
                        }
                    }
                    out
                }
                JoinType::Left => {
                    let right_cols: Vec<Column> =
                        join.inner().output_columns(ctx).iter().copied().collect();
                    let mut out = Vec::new();
                    for l in &left_rows {
                        let mut matched = false;
                        for r in &right_rows {
                            let row = combine_rows(l, r);
                            if eval_predicate_true(join.join_cond(), &row, env) {
                                out.push(row);
                                matched = true;
                            }
                        }
                        if !matched {
                            let mut row = l.clone();
                            for c in &right_cols {
                                row.entry(*c).or_insert(None);
                            }
                            out.push(row);
                        }
                    }
                    out
                }
                _ => panic!("unsupported join type in semantic test evaluator"),
            }
        }
        OperatorKind::LogicalDependentJoin(meta) => {
            let dep = LogicalDependentJoin::borrow_raw_parts(meta, &op.common);
            assert_eq!(*dep.join_type(), JoinType::Inner);
            let outer_rows = eval_op(dep.outer(), env, data, ctx);
            let mut out = Vec::new();
            for l in &outer_rows {
                let mut new_env = env.to_vec();
                new_env.push(l.clone());
                let inner_rows = eval_op(dep.inner(), &new_env, data, ctx);
                for r in &inner_rows {
                    let row = combine_rows(l, r);
                    if eval_predicate_true(dep.join_cond(), &row, env) {
                        out.push(row);
                    }
                }
            }
            out
        }
        _ => panic!("unsupported operator in semantic test evaluator"),
    }
}

fn rows_to_bag(
    rows: Vec<EvalRow>,
    compare_cols: &[Column],
) -> BTreeMap<Vec<(Column, Option<i32>)>, usize> {
    let mut bag = BTreeMap::new();
    for row in rows {
        let key: Vec<(Column, Option<i32>)> = compare_cols
            .iter()
            .map(|c| (*c, row.get(c).copied().unwrap_or(None)))
            .collect();
        *bag.entry(key).or_insert(0) += 1;
    }
    bag
}

fn assert_semantic_equivalence(ctx: &IRContext, input: Arc<Operator>, expected: Arc<Operator>) {
    let mut schemas = BTreeMap::new();
    collect_mock_schemas(&input, &mut schemas);
    collect_mock_schemas(&expected, &mut schemas);
    let mut compare_cols: Vec<Column> = input.output_columns(ctx).iter().copied().collect();
    compare_cols.sort_by_key(|c| c.0);
    for seed in 0..256_u64 {
        let data = generate_mock_data(&schemas, seed);
        let in_rows = rows_to_bag(eval_op(&input, &[], &data, ctx), &compare_cols);
        let ex_rows = rows_to_bag(eval_op(&expected, &[], &data, ctx), &compare_cols);
        if in_rows != ex_rows {
            panic!(
                "semantic mismatch for seed {seed}\nmock data: {data:?}\ninput plan:\n{}\nexpected plan:\n{}\ninput rows: {in_rows:?}\nexpected rows: {ex_rows:?}",
                quick_explain(&input, ctx),
                quick_explain(&expected, ctx),
            );
        }
    }
}

// --- Helper functions to test the rule ---

/// Applies the unnesting rule and asserts the result matches expected.
pub(super) fn assert_unnesting(ctx: &IRContext, input: Arc<Operator>, expected: Arc<Operator>) {
    let res = UnnestingRule::new().apply(input.clone(), &ctx);
    if res != expected {
        panic!(
            "Unnested plan does not match expected plan\n\nactual:\n{}\n\nexpected:\n{}",
            quick_explain(&res, ctx),
            quick_explain(&expected, ctx)
        );
    }
    assert_semantic_equivalence(ctx, input, expected);
}
