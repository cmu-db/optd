use std::collections::HashMap;
use std::sync::Arc;

use crate::error::Result;
use crate::ir::catalog::{DataSourceId, Field, Schema};
use crate::ir::convert::{IntoOperator, IntoScalar};
use crate::ir::explain::quick_explain;
use crate::ir::operator::{
    Aggregate, DependentJoin, Get, Join, Operator, OperatorKind, OrderBy, Project, Remap, Select,
    join::JoinType,
};
use crate::ir::scalar::{
    BinaryOp, BinaryOpKind, ColumnRef, List, Literal, NaryOp, ScalarKind,
};
use crate::ir::table_ref::TableRef;
use crate::ir::{Column, DataType, IRContext, Scalar};
use crate::rules::decorrelation::UnnestingRule;

// Helpers to just make chaining operators easier in test logic
pub(super) trait TestOperatorExt {
    fn logical_select(self, predicate: Arc<Scalar>) -> Arc<Operator>;
    fn logical_join(
        self,
        inner: Arc<Operator>,
        join_cond: Arc<Scalar>,
        join_type: JoinType,
    ) -> Arc<Operator>;
    fn logical_dependent_join(
        self,
        inner: Arc<Operator>,
        join_cond: Arc<Scalar>,
        join_type: JoinType,
    ) -> Arc<Operator>;
}

impl TestOperatorExt for Arc<Operator> {
    fn logical_select(self, predicate: Arc<Scalar>) -> Arc<Operator> {
        Select::new(self, predicate).into_operator()
    }

    fn logical_join(
        self,
        inner: Arc<Operator>,
        join_cond: Arc<Scalar>,
        join_type: JoinType,
    ) -> Arc<Operator> {
        Join::new(join_type, self, inner, join_cond, None).into_operator()
    }

    fn logical_dependent_join(
        self,
        inner: Arc<Operator>,
        join_cond: Arc<Scalar>,
        join_type: JoinType,
    ) -> Arc<Operator> {
        DependentJoin::new(join_type, self, inner, join_cond).into_operator()
    }
}

// Create a catalog-backed mock binding and return its columns in ordinal order.
pub(super) fn make_cols(ctx: &IRContext, n: usize) -> Vec<Column> {
    let schema = Arc::new(Schema::new(
        (0..n)
            .map(|idx| Field::new(format!("c{idx}"), DataType::Int32, true))
            .collect::<Vec<_>>(),
    ));
    let next_table = ctx.binder.read().unwrap().bindings.len() + 1;
    let table_ref = TableRef::bare(format!("__mock_{next_table}"));
    ctx.catalog.create_table(table_ref.clone(), schema.clone()).unwrap();
    let table_index = ctx.add_binding(Some(table_ref), schema).unwrap();
    (0..n).map(|idx| Column(table_index, idx)).collect()
}

// Build a `Get` over an existing mock binding, preserving the supplied
// projection order.
pub(super) fn mock_scan_with_columns(
    table_id: i64,
    cols: Vec<Column>,
) -> Arc<Operator> {
    assert!(!cols.is_empty(), "mock scan requires at least one column");
    let table_index = cols[0].0;
    let projections = cols
        .iter()
        .map(|col| {
            assert_eq!(col.0, table_index, "mock scan columns must share a binding");
            col.1
        })
        .collect::<Arc<[_]>>();

    Get::logical(DataSourceId(table_id), table_index, projections).into_operator()
}

// Compare two scalars with `IS NOT DISTINCT FROM` so nullable columns match the
// null-safe equality semantics used by decorrelation joins.
pub(super) fn null_safe_eq(
    lhs: Arc<crate::ir::Scalar>,
    rhs: Arc<crate::ir::Scalar>,
) -> Arc<crate::ir::Scalar> {
    BinaryOp::new(BinaryOpKind::IsNotDistinctFrom, lhs, rhs).into_scalar()
}

// Build a project and return both the operator and the fresh output columns it
// creates so tests can keep following the new binding.
pub(super) fn project_with_outputs(
    ctx: &IRContext,
    input: Arc<Operator>,
    projections: impl IntoIterator<Item = Arc<Scalar>>,
) -> Result<(Arc<Operator>, Vec<Column>)> {
    let projections: Vec<_> = projections.into_iter().collect();
    let project = ctx.project(input, projections.clone())?.build();
    let table_index = *project.borrow::<Project>().table_index();
    let outputs = (0..projections.len())
        .map(|idx| Column(table_index, idx))
        .collect();
    Ok((project, outputs))
}

// Build an aggregate and return the fresh key and aggregate output columns
// allocated by the current IR.
pub(super) fn aggregate_with_outputs(
    ctx: &IRContext,
    input: Arc<Operator>,
    exprs: impl IntoIterator<Item = Arc<Scalar>>,
    keys: impl IntoIterator<Item = Arc<Scalar>>,
) -> Result<(Arc<Operator>, Vec<Column>, Vec<Column>)> {
    let exprs: Vec<_> = exprs.into_iter().collect();
    let keys: Vec<_> = keys.into_iter().collect();
    let aggregate = input
        .with_ctx(ctx)
        .logical_aggregate(exprs.clone(), keys.clone())?
        .build();
    let node = aggregate.borrow::<Aggregate>();
    let key_outputs = (0..keys.len())
        .map(|idx| Column(*node.key_table_index(), idx))
        .collect();
    let expr_outputs = (0..exprs.len())
        .map(|idx| Column(*node.aggregate_table_index(), idx))
        .collect();
    Ok((aggregate, key_outputs, expr_outputs))
}

// Build the "domain" aggregate used throughout the unnesting paper and expose
// the aliased key columns that survive above the aggregate boundary.
pub(super) fn create_domain_with_aliases(
    ctx: &IRContext,
    input: Arc<Operator>,
    from_cols: Vec<Column>,
) -> Result<(Arc<Operator>, Vec<Column>)> {
    let (domain, outputs, _exprs) = aggregate_with_outputs(
        ctx,
        input,
        std::iter::empty::<Arc<Scalar>>(),
        from_cols.into_iter().map(ColumnRef::new).map(IntoScalar::into_scalar),
    )?;
    Ok((domain, outputs))
}

// Normalize fresh binding ids away so structural plan assertions stay stable
// across rewrites that allocate different `table_index` values.
fn canonicalize_plan(op: &Arc<Operator>) -> String {
    fn register_table(map: &mut HashMap<i64, usize>, next: &mut usize, table_index: i64) {
        map.entry(table_index).or_insert_with(|| {
            let id = *next;
            *next += 1;
            id
        });
    }

    fn assign_tables(op: &Arc<Operator>, map: &mut HashMap<i64, usize>, next: &mut usize) {
        for child in op.input_operators() {
            assign_tables(child, map, next);
        }
        for scalar in op.input_scalars() {
            for col in scalar.used_columns().iter() {
                register_table(map, next, col.0);
            }
        }
        match &op.kind {
            OperatorKind::Get(meta) => {
                register_table(map, next, *Get::borrow_raw_parts(meta, &op.common).table_index());
            }
            OperatorKind::Project(meta) => {
                register_table(
                    map,
                    next,
                    *Project::borrow_raw_parts(meta, &op.common).table_index(),
                );
            }
            OperatorKind::Aggregate(meta) => {
                let agg = Aggregate::borrow_raw_parts(meta, &op.common);
                register_table(map, next, *agg.key_table_index());
                register_table(map, next, *agg.aggregate_table_index());
            }
            OperatorKind::Remap(meta) => {
                register_table(map, next, *Remap::borrow_raw_parts(meta, &op.common).table_index());
            }
            OperatorKind::Join(meta) => {
                if let JoinType::Mark(col) = Join::borrow_raw_parts(meta, &op.common).join_type() {
                    register_table(map, next, col.0);
                }
            }
            OperatorKind::DependentJoin(meta) => {
                if let JoinType::Mark(col) =
                    DependentJoin::borrow_raw_parts(meta, &op.common).join_type()
                {
                    register_table(map, next, col.0);
                }
            }
            OperatorKind::Select(_)
            | OperatorKind::OrderBy(_)
            | OperatorKind::Limit(_)
            | OperatorKind::EnforcerSort(_)
            | OperatorKind::Subquery(_)
            | OperatorKind::Group(_) => {}
        }
    }

    fn fmt_col(col: Column, map: &HashMap<i64, usize>) -> String {
        format!("t{}.{}", map[&col.0], col.1)
    }

    fn fmt_scalar(s: &Arc<Scalar>, map: &HashMap<i64, usize>) -> String {
        match &s.kind {
            ScalarKind::ColumnRef(meta) => {
                let cr = ColumnRef::borrow_raw_parts(meta, &s.common);
                format!("col({})", fmt_col(*cr.column(), map))
            }
            ScalarKind::Literal(meta) => {
                let lit = Literal::borrow_raw_parts(meta, &s.common);
                format!("lit({:?})", lit.value())
            }
            ScalarKind::BinaryOp(meta) => {
                let bin = BinaryOp::borrow_raw_parts(meta, &s.common);
                format!(
                    "bin({:?},{},{})",
                    bin.op_kind(),
                    fmt_scalar(bin.lhs(), map),
                    fmt_scalar(bin.rhs(), map)
                )
            }
            ScalarKind::NaryOp(meta) => {
                let nary = NaryOp::borrow_raw_parts(meta, &s.common);
                let terms = nary
                    .terms()
                    .iter()
                    .map(|term| fmt_scalar(term, map))
                    .collect::<Vec<_>>()
                    .join(",");
                format!("nary({:?},[{terms}])", nary.op_kind())
            }
            ScalarKind::List(meta) => {
                let list = List::borrow_raw_parts(meta, &s.common);
                let members = list
                    .members()
                    .iter()
                    .map(|member| fmt_scalar(member, map))
                    .collect::<Vec<_>>()
                    .join(",");
                format!("list([{members}])")
            }
            _ => panic!("unsupported scalar kind in canonical plan formatter"),
        }
    }

    fn fmt_join_type(join_type: &JoinType, map: &HashMap<i64, usize>) -> String {
        match join_type {
            JoinType::Inner => "Inner".to_string(),
            JoinType::LeftOuter => "LeftOuter".to_string(),
            JoinType::LeftSemi => "LeftSemi".to_string(),
            JoinType::LeftAnti => "LeftAnti".to_string(),
            JoinType::Single => "Single".to_string(),
            JoinType::Mark(col) => format!("Mark({})", fmt_col(*col, map)),
        }
    }

    fn fmt_op(op: &Arc<Operator>, map: &HashMap<i64, usize>) -> String {
        match &op.kind {
            OperatorKind::Get(meta) => {
                let get = Get::borrow_raw_parts(meta, &op.common);
                let projections = get
                    .projections()
                    .iter()
                    .map(|idx| idx.to_string())
                    .collect::<Vec<_>>()
                    .join(",");
                format!("Get({},[{projections}])", get.data_source_id().0)
            }
            OperatorKind::Select(meta) => {
                let select = Select::borrow_raw_parts(meta, &op.common);
                format!(
                    "Select({},{})",
                    fmt_scalar(select.predicate(), map),
                    fmt_op(select.input(), map)
                )
            }
            OperatorKind::Project(meta) => {
                let project = Project::borrow_raw_parts(meta, &op.common);
                format!(
                    "Project({},{})",
                    fmt_scalar(project.projections(), map),
                    fmt_op(project.input(), map)
                )
            }
            OperatorKind::Aggregate(meta) => {
                let aggregate = Aggregate::borrow_raw_parts(meta, &op.common);
                format!(
                    "Aggregate(keys={},exprs={},input={})",
                    fmt_scalar(aggregate.keys(), map),
                    fmt_scalar(aggregate.exprs(), map),
                    fmt_op(aggregate.input(), map)
                )
            }
            OperatorKind::Remap(meta) => {
                let remap = Remap::borrow_raw_parts(meta, &op.common);
                format!("Remap({})", fmt_op(remap.input(), map))
            }
            OperatorKind::OrderBy(meta) => {
                let order = OrderBy::borrow_raw_parts(meta, &op.common);
                let exprs = order
                    .exprs()
                    .iter()
                    .zip(order.directions().iter())
                    .map(|(expr, asc)| format!("{}:{asc}", fmt_scalar(expr, map)))
                    .collect::<Vec<_>>()
                    .join(",");
                format!("OrderBy([{exprs}],{})", fmt_op(order.input(), map))
            }
            OperatorKind::Join(meta) => {
                let join = Join::borrow_raw_parts(meta, &op.common);
                format!(
                    "Join(type={},cond={},outer={},inner={})",
                    fmt_join_type(join.join_type(), map),
                    fmt_scalar(join.join_cond(), map),
                    fmt_op(join.outer(), map),
                    fmt_op(join.inner(), map)
                )
            }
            OperatorKind::DependentJoin(meta) => {
                let join = DependentJoin::borrow_raw_parts(meta, &op.common);
                format!(
                    "DependentJoin(type={},cond={},outer={},inner={})",
                    fmt_join_type(join.join_type(), map),
                    fmt_scalar(join.join_cond(), map),
                    fmt_op(join.outer(), map),
                    fmt_op(join.inner(), map)
                )
            }
            _ => panic!("unsupported operator in canonical plan formatter"),
        }
    }

    let mut map = HashMap::new();
    let mut next = 0;
    assign_tables(op, &mut map, &mut next);
    fmt_op(op, &map)
}

// Run the unnesting rule and compare the normalized plan shape against the
// expected rewrite.
pub(super) fn assert_unnesting(
    input_ctx: &IRContext,
    input: Arc<Operator>,
    expected_ctx: &IRContext,
    expected: Arc<Operator>,
) -> Result<()> {
    let res = UnnestingRule::new().apply(input.clone(), input_ctx)?;
    let actual = canonicalize_plan(&res);
    let wanted = canonicalize_plan(&expected);
    if actual != wanted {
        panic!(
            "Unnested plan does not match expected plan\n\nactual:\n{}\n\nexpected:\n{}",
            quick_explain(&res, input_ctx),
            quick_explain(&expected, expected_ctx)
        );
    }
    Ok(())
}
