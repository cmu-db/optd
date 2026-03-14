use crate::error::Result;
use crate::ir::builder::{column_assign, column_ref};
use crate::ir::convert::IntoScalar;
use crate::ir::explain::quick_explain;
use crate::ir::operator::{
    Aggregate, Join, LogicalDependentJoin, LogicalOrderBy, MockScan, Operator, OperatorKind,
    Project, Select, join::JoinType,
};
use crate::ir::scalar::{
    BinaryOp, BinaryOpKind, ColumnAssign, ColumnRef, List, Literal, NaryOp, NaryOpKind, ScalarKind,
};
use crate::ir::{Column, DataType, IRContext, Scalar, ScalarValue};
use crate::rules::decorrelation::UnnestingRule;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::BTreeMap;
use std::sync::Arc;

// --- Helper functions to help create a logical plan ---

pub(super) fn make_cols(ctx: &IRContext, n: usize) -> Vec<Column> {
    (0..n)
        .map(|_| ctx.define_column(DataType::Int32, None))
        .collect()
}

pub(super) fn null_safe_eq(
    lhs: Arc<crate::ir::Scalar>,
    rhs: Arc<crate::ir::Scalar>,
) -> Arc<crate::ir::Scalar> {
    BinaryOp::new(BinaryOpKind::IsNotDistinctFrom, lhs, rhs).into_scalar()
}

pub(super) fn create_domain_with_aliases(
    input: Arc<Operator>,
    from_cols: Vec<Column>,
    to_cols: Vec<Column>,
) -> Arc<Operator> {
    assert_eq!(from_cols.len(), to_cols.len(), "domain remap must be 1:1");
    let project_exprs: Vec<_> = from_cols.iter().map(|c| column_ref(*c)).collect();

    let table_index = 0;
    let domain_project = input.logical_project(table_index, project_exprs);

    let group_keys: Vec<_> = from_cols
        .iter()
        .map(|c| column_assign(*c, column_ref(*c)))
        .collect();
    let domain_distinct = domain_project.logical_aggregate(0, std::iter::empty(), group_keys);

    let remap_assigns: Vec<_> = from_cols
        .iter()
        .zip(to_cols.iter())
        .map(|(from, to)| column_assign(*to, column_ref(*from)))
        .collect();
    let table_index = 0;
    domain_distinct.logical_remap(table_index)
}

// --- Helper functions to "execute" a plan and check equality ---

/// One row of data keyed by column id; values are nullable i32s.
type Row = BTreeMap<Column, Option<i32>>;

/// Mock scan table data keyed by mock table id.
type MockData = BTreeMap<i64, Vec<Row>>;

/// Runtime value domain used by the mini evaluator.
#[derive(Clone, Copy)]
enum EvalValue {
    Int(Option<i32>),
    Bool(Option<bool>),
}

/// Gather mock scan schemas so random test data can be generated consistently.
fn collect_mock_schemas(op: &Arc<Operator>, schemas: &mut BTreeMap<i64, Vec<Column>>) {
    if let OperatorKind::MockScan(meta) = &op.kind {
        let scan = MockScan::borrow_raw_parts(meta, &op.common);
        let mut cols: Vec<Column> = scan.spec().mocked_output_columns.iter().copied().collect();
        cols.sort_by_key(|c| c.0);
        schemas.insert(*scan.table_index(), cols);
    }
    for child in op.input_operators() {
        collect_mock_schemas(child, schemas);
    }
}

/// Build per-mock scan input rows from discovered schemas and a seed.
fn generate_mock_data(
    schemas: &BTreeMap<i64, Vec<Column>>,
    seed: u64,
    num_rows_low: usize,
    num_rows_high: usize,
) -> MockData {
    /// Draw a random test value with some NULLs to exercise null semantics.
    fn random_value(rng: &mut StdRng) -> Option<i32> {
        match rng.gen_range(0..5) {
            0 => None,
            1 => Some(-1),
            2 => Some(0),
            3 => Some(1),
            _ => Some(2),
        }
    }

    let mut rng = StdRng::seed_from_u64(seed);
    let mut data = BTreeMap::new();
    for (mock_id, cols) in schemas {
        let row_count = rng.gen_range(num_rows_low..num_rows_high);
        let mut rows = Vec::with_capacity(row_count);
        for _ in 0..row_count {
            let mut row = BTreeMap::new();
            for col in cols {
                row.insert(*col, random_value(&mut rng));
            }
            rows.push(row);
        }
        data.insert(*mock_id, rows);
    }
    data
}

/// Resolve a column from the current row, or from outer correlated rows.
fn lookup_col(row: &Row, env: &[Row], col: Column) -> Option<i32> {
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

/// Enforce boolean-typed scalar evaluation in the mini evaluator.
fn as_bool(v: EvalValue) -> Option<bool> {
    match v {
        EvalValue::Bool(b) => b,
        _ => panic!("expected boolean scalar"),
    }
}

/// Enforce integer-typed scalar evaluation in the mini evaluator.
fn as_int(v: EvalValue) -> Option<i32> {
    match v {
        EvalValue::Int(i) => i,
        _ => panic!("expected integer scalar"),
    }
}

/// Evaluate a scalar expression under SQL three-valued logic rules.
fn eval_scalar(s: &Arc<Scalar>, row: &Row, env: &[Row]) -> EvalValue {
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

/// Merge two rows produced by a join pair into one composite row.
fn combine_rows(left: &Row, right: &Row) -> Row {
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

/// Interpret a logical plan over mock data and return produced rows.
fn eval_op(op: &Arc<Operator>, env: &[Row], data: &MockData, ctx: &IRContext) -> Result<Vec<Row>> {
    let rows = match &op.kind {
        OperatorKind::MockScan(meta) => {
            let scan = MockScan::borrow_raw_parts(meta, &op.common);
            data.get(scan.table_index()).cloned().unwrap_or_default()
        }
        OperatorKind::Select(meta) => {
            let sel = Select::borrow_raw_parts(meta, &op.common);
            eval_op(sel.input(), env, data, ctx)?
                .into_iter()
                .filter(|r| matches!(as_bool(eval_scalar(sel.predicate(), r, env)), Some(true)))
                .collect()
        }
        OperatorKind::Project(meta) => {
            let proj = Project::borrow_raw_parts(meta, &op.common);
            let members = proj
                .projections()
                .try_borrow::<List>()
                .unwrap()
                .members()
                .to_vec();
            eval_op(proj.input(), env, data, ctx)?
                .into_iter()
                .map(|r| {
                    let mut out = Row::new();
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
        OperatorKind::LogicalRemap(_meta) => {
            todo!()
            // let remap = LogicalRemap::borrow_raw_parts(meta, &op.common);
            // let members = remap
            //     .mappings()
            //     .try_borrow::<List>()
            //     .unwrap()
            //     .members()
            //     .to_vec();
            // eval_op(remap.input(), env, data, ctx)
            //     .into_iter()
            //     .map(|r| {
            //         let mut out = Row::new();
            //         for m in &members {
            //             let assign = m.try_borrow::<ColumnAssign>().unwrap();
            //             out.insert(
            //                 *assign.column(),
            //                 as_int(eval_scalar(assign.expr(), &r, env)),
            //             );
            //         }
            //         out
            //     })
            //     .collect()
        }
        OperatorKind::Aggregate(meta) => {
            let agg = Aggregate::borrow_raw_parts(meta, &op.common);
            let input_rows = eval_op(agg.input(), env, data, ctx)?;
            let key_members = agg.keys().try_borrow::<List>().unwrap().members().to_vec();
            if key_members.is_empty() {
                return Ok(vec![Row::new()]);
            }
            let mut groups: BTreeMap<Vec<(Column, Option<i32>)>, Row> = BTreeMap::new();
            for r in input_rows {
                let mut out = Row::new();
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
            eval_op(order.input(), env, data, ctx)?
        }
        OperatorKind::Join(meta) => {
            let join = Join::borrow_raw_parts(meta, &op.common);
            let left_rows = eval_op(join.outer(), env, data, ctx)?;
            let right_rows = eval_op(join.inner(), env, data, ctx)?;
            match join.join_type() {
                JoinType::Inner => {
                    let mut out = Vec::new();
                    for l in &left_rows {
                        for r in &right_rows {
                            let row = combine_rows(l, r);
                            if matches!(
                                as_bool(eval_scalar(join.join_cond(), &row, env)),
                                Some(true)
                            ) {
                                out.push(row);
                            }
                        }
                    }
                    out
                }
                JoinType::Left => {
                    let right_cols: Vec<Column> =
                        join.inner().output_columns(ctx)?.iter().copied().collect();
                    let mut out = Vec::new();
                    for l in &left_rows {
                        let mut matched = false;
                        for r in &right_rows {
                            let row = combine_rows(l, r);
                            if matches!(
                                as_bool(eval_scalar(join.join_cond(), &row, env)),
                                Some(true)
                            ) {
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
            let outer_rows = eval_op(dep.outer(), env, data, ctx)?;
            let mut out = Vec::new();
            for l in &outer_rows {
                let mut new_env = env.to_vec();
                new_env.push(l.clone());
                let inner_rows = eval_op(dep.inner(), &new_env, data, ctx)?;
                for r in &inner_rows {
                    let row = combine_rows(l, r);
                    if matches!(as_bool(eval_scalar(dep.join_cond(), &row, env)), Some(true)) {
                        out.push(row);
                    }
                }
            }
            out
        }
        _ => panic!("unsupported operator in semantic test evaluator"),
    };
    Ok(rows)
}

/// Convert rows into a multiset keyed by selected output columns.
fn rows_to_bag(
    rows: Vec<Row>,
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

/// Check semantic equivalence by running both plans over many random datasets.
fn assert_executed_equivalence(
    ctx: &IRContext,
    input: Arc<Operator>,
    expected: Arc<Operator>,
    num_rows_low: usize,
    num_rows_high: usize,
    num_iterations: usize,
) -> Result<()> {
    let mut schemas = BTreeMap::new();
    collect_mock_schemas(&input, &mut schemas);
    collect_mock_schemas(&expected, &mut schemas);
    let mut compare_cols: Vec<Column> = input.output_columns(ctx)?.iter().copied().collect();
    compare_cols.sort_by_key(|c| c.0);
    for seed in 0..num_iterations {
        let data = generate_mock_data(&schemas, seed as u64, num_rows_low, num_rows_high);
        let in_rows = rows_to_bag(eval_op(&input, &[], &data, ctx)?, &compare_cols);
        let ex_rows = rows_to_bag(eval_op(&expected, &[], &data, ctx)?, &compare_cols);
        if in_rows != ex_rows {
            panic!(
                "semantic mismatch for seed {seed}\nmock data: {data:?}\ninput plan:\n{}\nexpected plan:\n{}\ninput rows: {in_rows:?}\nexpected rows: {ex_rows:?}",
                quick_explain(&input, ctx),
                quick_explain(&expected, ctx),
            );
        }
    }
    Ok(())
}

// --- Helper functions to test the rule ---

/// Applies the unnesting rule and asserts the result matches expected.
pub(super) fn assert_unnesting(
    ctx: &IRContext,
    input: Arc<Operator>,
    expected: Arc<Operator>,
) -> Result<()> {
    let res = UnnestingRule::new().apply(input.clone(), &ctx)?;
    if res != expected {
        panic!(
            "Unnested plan does not match expected plan\n\nactual:\n{}\n\nexpected:\n{}",
            quick_explain(&res, ctx),
            quick_explain(&expected, ctx)
        );
    }

    // We execute tests on these plans with tables of size 8-12, 32 times each
    assert_executed_equivalence(ctx, input, expected, 8, 12, 32)
}
