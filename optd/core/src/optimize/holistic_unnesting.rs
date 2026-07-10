//! Holistic unnesting pass for correlated subqueries.
//!
//! This is the v1 optd implementation point for the top-down unnesting strategy
//! described by Neumann, "Improving Unnesting of Complex Queries" (BTW 2025).
//! The implemented subset is deliberately conservative: it decorrelates
//! subquery-derived joins by lifting safe correlated predicates, preserves scalar
//! aggregate semantics with left outer joins, and leaves shapes that need full
//! domain/window/tri-valued marker support unchanged.

use std::collections::{HashMap, HashSet};

use crate::{
    AnalysisContext, AvailableColumns, Column, Expr, ExprData, FreeColumns, Join, JoinType, Map,
    NaryOp, Operator, OperatorData, OptimizerContext, Projection, Relation, Rename, Selection,
    Sort, expr_used_columns,
    optimize::{OptimizeResult, Pass, PassResult, QueryPass},
};

/// Decorrelates subquery-derived joins before later relational rewrites.
pub struct HolisticUnnesting;

impl Pass for HolisticUnnesting {
    fn name(&self) -> &'static str {
        "HolisticUnnesting"
    }
}

impl QueryPass for HolisticUnnesting {
    fn run(&mut self, ctx: &mut OptimizerContext) -> OptimizeResult<PassResult> {
        run_holistic_unnesting(ctx)
    }
}

fn run_holistic_unnesting(ctx: &mut OptimizerContext) -> OptimizeResult<PassResult> {
    let mut changed = false;

    while let Some(root) = ctx.query.root() {
        let sites = collect_dependent_join_sites(root, ctx)?;
        if sites.is_empty() {
            break;
        }

        let access_index = AccessIndex::build(&sites, ctx)?;
        let mut progressed = false;

        for site in &sites {
            let accessing = access_index.accessing(site.join);
            if let Some(rewrite) = rewrite_target_join(root, site, accessing, ctx)? {
                ctx.query.set_root(rewrite.op);
                ctx.analyses.clear();
                changed = true;
                progressed = true;
                break;
            }
        }

        if !progressed {
            break;
        }
    }

    if changed {
        Ok(PassResult::Changed)
    } else {
        Ok(PassResult::Unchanged)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DependentJoinSite {
    join: Operator,
    join_type: JoinType,
    outer: Operator,
    inner: Operator,
    on: Expr,
    outer_refs: Vec<Column>,
    depth: usize,
}

#[derive(Debug, Default, Clone)]
struct AccessIndex {
    accessing: HashMap<Operator, Vec<Operator>>,
}

impl AccessIndex {
    fn build(sites: &[DependentJoinSite], ctx: &mut OptimizerContext) -> OptimizeResult<Self> {
        let mut index = Self::default();
        for site in sites {
            let mut ops = Vec::new();
            collect_accessing_ops(site.inner, &site.outer_refs, ctx, &mut ops)?;
            index.accessing.insert(site.join, ops);
        }
        Ok(index)
    }

    fn accessing(&self, join: Operator) -> &[Operator] {
        self.accessing.get(&join).map(Vec::as_slice).unwrap_or(&[])
    }
}

#[derive(Debug, Clone)]
struct UnnestingState {
    cclasses: ColumnEqClasses,
    repr: HashMap<Column, Column>,
    changed: bool,
}

impl UnnestingState {
    fn for_site(_site: &DependentJoinSite) -> Self {
        Self {
            cclasses: ColumnEqClasses::default(),
            repr: HashMap::new(),
            changed: false,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct ColumnEqClasses {
    parent: HashMap<Column, Column>,
}

impl ColumnEqClasses {
    fn add(&mut self, column: Column) {
        self.parent.entry(column).or_insert(column);
    }

    fn find(&mut self, column: Column) -> Column {
        self.add(column);
        let parent = self.parent[&column];
        if parent == column {
            column
        } else {
            let root = self.find(parent);
            self.parent.insert(column, root);
            root
        }
    }

    fn union(&mut self, left: Column, right: Column) {
        let left_root = self.find(left);
        let right_root = self.find(right);
        if left_root != right_root {
            self.parent.insert(right_root, left_root);
        }
    }

    fn equivalent(&mut self, left: Column, right: Column) -> bool {
        self.find(left) == self.find(right)
    }
}

#[derive(Debug, Clone, Default)]
struct ColumnRewrite {
    repr: HashMap<Column, Column>,
}

impl ColumnRewrite {
    fn rewrite_expr(&self, expr: Expr, ctx: &mut crate::QueryContext) -> Expr {
        match expr.get(ctx).clone() {
            ExprData::ColumnRef(column) => self
                .repr
                .get(&column)
                .copied()
                .map(|replacement| ExprData::ColumnRef(replacement).add(ctx))
                .unwrap_or(expr),
            ExprData::Unary { op, expr: input } => ExprData::Unary {
                op,
                expr: self.rewrite_expr(input, ctx),
            }
            .add(ctx),
            ExprData::Binary { op, left, right } => ExprData::Binary {
                op,
                left: self.rewrite_expr(left, ctx),
                right: self.rewrite_expr(right, ctx),
            }
            .add(ctx),
            ExprData::Nary { op, exprs } => ExprData::Nary {
                op,
                exprs: exprs
                    .into_iter()
                    .map(|expr| self.rewrite_expr(expr, ctx))
                    .collect(),
            }
            .add(ctx),
            ExprData::Cast { expr: input, ty } => ExprData::Cast {
                expr: self.rewrite_expr(input, ctx),
                ty,
            }
            .add(ctx),
            ExprData::CaseWhen {
                when_then,
                else_expr,
            } => ExprData::CaseWhen {
                when_then: when_then
                    .into_iter()
                    .map(|(when, then)| {
                        (self.rewrite_expr(when, ctx), self.rewrite_expr(then, ctx))
                    })
                    .collect(),
                else_expr: else_expr.map(|expr| self.rewrite_expr(expr, ctx)),
            }
            .add(ctx),
            ExprData::ScalarFunction { function, args } => ExprData::ScalarFunction {
                function,
                args: args
                    .into_iter()
                    .map(|expr| self.rewrite_expr(expr, ctx))
                    .collect(),
            }
            .add(ctx),
            ExprData::Like {
                negated,
                expr: input,
                pattern,
                case_insensitive,
            } => ExprData::Like {
                negated,
                expr: self.rewrite_expr(input, ctx),
                pattern: self.rewrite_expr(pattern, ctx),
                case_insensitive,
            }
            .add(ctx),
            ExprData::Literal(_)
            | ExprData::Exists { .. }
            | ExprData::InSubquery { .. }
            | ExprData::ScalarSubquery { .. } => expr,
        }
    }
}

#[derive(Debug, Clone)]
struct UnnestResult {
    op: Operator,
    state: UnnestingState,
    succeeded: bool,
    count_defaults: Vec<Column>,
}

#[derive(Debug, Clone)]
struct OperatorRewriteResult {
    op: Operator,
    count_defaults: Vec<Column>,
}

fn collect_dependent_join_sites(
    root: Operator,
    ctx: &mut OptimizerContext,
) -> OptimizeResult<Vec<DependentJoinSite>> {
    let mut sites = Vec::new();
    collect_dependent_join_sites_inner(root, 0, ctx, &mut sites)?;
    Ok(sites)
}

fn collect_dependent_join_sites_inner(
    op: Operator,
    depth: usize,
    ctx: &mut OptimizerContext,
    sites: &mut Vec<DependentJoinSite>,
) -> OptimizeResult<()> {
    if let OperatorData::Join(join) = op.get(&ctx.query).clone() {
        let outer_available: HashSet<_> = available_columns_for(ctx, join.outer)?
            .into_iter()
            .collect();
        let mut refs: Vec<_> = free_columns_for(ctx, join.inner)?
            .into_iter()
            .filter(|column| outer_available.contains(column))
            .collect();
        if let Ok(used) = expr_used_columns(&ctx.query, &mut ctx.analyses, join.on) {
            let inner_available: HashSet<_> = available_columns_for(ctx, join.inner)?
                .into_iter()
                .collect();
            refs.extend(used.into_iter().filter(|column| {
                outer_available.contains(column) && !inner_available.contains(column)
            }));
        }
        dedup_columns(&mut refs);

        if is_subquery_derived_join(&join.join_type) && !refs.is_empty() {
            sites.push(DependentJoinSite {
                join: op,
                join_type: join.join_type.clone(),
                outer: join.outer,
                inner: join.inner,
                on: join.on,
                outer_refs: refs,
                depth,
            });
        }
    }

    for input in op.get(&ctx.query).inputs() {
        collect_dependent_join_sites_inner(input, depth + 1, ctx, sites)?;
    }

    Ok(())
}

fn collect_accessing_ops(
    op: Operator,
    outer_refs: &[Column],
    ctx: &mut OptimizerContext,
    out: &mut Vec<Operator>,
) -> OptimizeResult<()> {
    let free: HashSet<_> = free_columns_for(ctx, op)?.into_iter().collect();
    if outer_refs.iter().any(|column| free.contains(column)) {
        out.push(op);
    }
    for input in op.get(&ctx.query).inputs() {
        collect_accessing_ops(input, outer_refs, ctx, out)?;
    }
    Ok(())
}

fn build_null_safe_eq(left: Column, right: Column, ctx: &mut crate::QueryContext) -> Expr {
    // Domain/replacement joins compare correlated NULL bindings with SQL
    // `IS NOT DISTINCT FROM` semantics.
    ExprData::Binary {
        op: crate::BinaryOp::IsNotDistinctFrom,
        left: ExprData::ColumnRef(left).add(ctx),
        right: ExprData::ColumnRef(right).add(ctx),
    }
    .add(ctx)
}

fn register_equality_predicate(
    predicate: Expr,
    cclasses: &mut ColumnEqClasses,
    query: &crate::QueryContext,
) {
    match predicate.get(query) {
        ExprData::Binary {
            op: crate::BinaryOp::Eq,
            left,
            right,
        } => {
            if let (ExprData::ColumnRef(left), ExprData::ColumnRef(right)) =
                (left.get(query), right.get(query))
            {
                cclasses.union(*left, *right);
            }
        }
        ExprData::Nary {
            op: NaryOp::And,
            exprs,
        } => {
            for expr in exprs {
                register_equality_predicate(*expr, cclasses, query);
            }
        }
        _ => {}
    }
}

fn dedup_columns(columns: &mut Vec<Column>) {
    let mut seen = HashSet::new();
    columns.retain(|column| seen.insert(*column));
}

fn rewrite_target_join(
    op: Operator,
    site: &DependentJoinSite,
    accessing: &[Operator],
    ctx: &mut OptimizerContext,
) -> OptimizeResult<Option<OperatorRewriteResult>> {
    if op == site.join {
        return eliminate_dependent_join(site, accessing, ctx).map(|result| {
            let _state_changed = result.state.changed;
            if result.succeeded {
                Some(OperatorRewriteResult {
                    op: result.op,
                    count_defaults: result.count_defaults,
                })
            } else {
                None
            }
        });
    }

    let data = op.get(&ctx.query).clone();
    let mut changed = false;
    let mut count_defaults = Vec::new();
    let rebuilt = data.try_map_inputs(|input| {
        if changed {
            return Ok(input);
        }
        let rewritten = rewrite_target_join(input, site, accessing, ctx)?;
        changed = rewritten.is_some();
        if let Some(rewritten) = rewritten {
            count_defaults = rewritten.count_defaults;
            Ok(rewritten.op)
        } else {
            Ok(input)
        }
    })?;

    if changed {
        let rebuilt = default_scalar_count_after_outer_join(rebuilt, &mut count_defaults, ctx);
        Ok(Some(OperatorRewriteResult {
            op: rebuilt.add(&mut ctx.query),
            count_defaults,
        }))
    } else {
        Ok(None)
    }
}

fn eliminate_dependent_join(
    site: &DependentJoinSite,
    accessing: &[Operator],
    ctx: &mut OptimizerContext,
) -> OptimizeResult<UnnestResult> {
    let join = match site.join.get(&ctx.query).clone() {
        OperatorData::Join(join) => join,
        _ => {
            return Ok(UnnestResult {
                op: site.join,
                state: UnnestingState::for_site(site),
                succeeded: false,
                count_defaults: Vec::new(),
            });
        }
    };

    let mut state = UnnestingState::for_site(site);
    let op = if let Some(op) = try_simple_elimination(site, join, &mut state, ctx)? {
        op
    } else if let Some(op) = finish_when_accessing_empty(site, accessing, &mut state, ctx)? {
        op
    } else {
        site.join
    };

    Ok(UnnestResult {
        op,
        succeeded: op != site.join,
        count_defaults: rewrite_count_defaults(op, site.join, &ctx.query),
        state,
    })
}

fn try_simple_elimination(
    _site: &DependentJoinSite,
    join: Join,
    state: &mut UnnestingState,
    ctx: &mut OptimizerContext,
) -> OptimizeResult<Option<Operator>> {
    if !is_subquery_derived_join(&join.join_type) || free_columns_for(ctx, join.inner)?.is_empty() {
        return Ok(None);
    }

    let lift_outer_only = matches!(
        join.join_type,
        JoinType::LeftSemi | JoinType::LeftMark { .. }
    );
    let rewrite =
        lift_correlated_predicates(join.inner, lift_outer_only, &mut state.cclasses, ctx)?;
    if rewrite.predicates.is_empty() {
        return Ok(None);
    }

    let inner = expose_columns(rewrite.input(join.inner), &rewrite.inner_columns, ctx)?;
    for predicate in &rewrite.predicates {
        register_equality_predicate(*predicate, &mut state.cclasses, &ctx.query);
    }
    let join_type = if matches!(join.join_type, JoinType::Single) && rewrite.saw_aggregation {
        JoinType::LeftOuter
    } else {
        join.join_type
    };
    let on = make_and(
        std::iter::once(join.on).chain(rewrite.predicates).collect(),
        &mut ctx.query,
    );

    state.changed = true;

    Ok(Some(
        OperatorData::Join(Join {
            join_type,
            on,
            outer: join.outer,
            inner,
        })
        .add(&mut ctx.query),
    ))
}

fn rewrite_count_defaults(
    rewritten: Operator,
    original: Operator,
    query: &crate::QueryContext,
) -> Vec<Column> {
    if rewritten == original {
        return Vec::new();
    }

    let OperatorData::Join(join) = rewritten.get(query) else {
        return Vec::new();
    };
    if !matches!(join.join_type, JoinType::LeftOuter) {
        return Vec::new();
    }

    collect_count_aggregate_columns(join.inner, query)
}

fn default_scalar_count_after_outer_join(
    operator: OperatorData,
    count_defaults: &mut Vec<Column>,
    ctx: &mut OptimizerContext,
) -> OperatorData {
    if count_defaults.is_empty() {
        return operator;
    }

    match operator {
        OperatorData::Map(mut map) => {
            let observed = map
                .computations
                .iter()
                .any(|(_, expr)| expr_references_any(*expr, count_defaults, ctx));
            map.computations = map
                .computations
                .into_iter()
                .map(|(column, expr)| {
                    (
                        column,
                        coalesce_count_defaults_in_expr(expr, count_defaults, &mut ctx.query),
                    )
                })
                .collect();
            if observed {
                count_defaults.clear();
            }
            OperatorData::Map(map)
        }
        OperatorData::Selection(mut selection) => {
            selection.predicate = coalesce_count_defaults_in_expr(
                selection.predicate,
                count_defaults,
                &mut ctx.query,
            );
            OperatorData::Selection(selection)
        }
        OperatorData::Sort(mut sort) => {
            sort.keys = sort
                .keys
                .into_iter()
                .map(|mut key| {
                    key.expr =
                        coalesce_count_defaults_in_expr(key.expr, count_defaults, &mut ctx.query);
                    key
                })
                .collect();
            OperatorData::Sort(sort)
        }
        OperatorData::Join(mut join) => {
            join.on = coalesce_count_defaults_in_expr(join.on, count_defaults, &mut ctx.query);
            OperatorData::Join(join)
        }
        operator => operator,
    }
}

fn coalesce_count_defaults_in_expr(
    expr: Expr,
    count_defaults: &[Column],
    query: &mut crate::QueryContext,
) -> Expr {
    match expr.get(query).clone() {
        ExprData::ColumnRef(column) if count_defaults.contains(&column) => {
            let value = ExprData::ColumnRef(column).add(query);
            let zero = ExprData::Literal(crate::ScalarValue::Int64(0)).add(query);
            ExprData::ScalarFunction {
                function: crate::ScalarFunction::Coalesce,
                args: vec![value, zero],
            }
            .add(query)
        }
        ExprData::Unary { op, expr: input } => ExprData::Unary {
            op,
            expr: coalesce_count_defaults_in_expr(input, count_defaults, query),
        }
        .add(query),
        ExprData::Binary { op, left, right } => ExprData::Binary {
            op,
            left: coalesce_count_defaults_in_expr(left, count_defaults, query),
            right: coalesce_count_defaults_in_expr(right, count_defaults, query),
        }
        .add(query),
        ExprData::Nary { op, exprs } => ExprData::Nary {
            op,
            exprs: exprs
                .into_iter()
                .map(|expr| coalesce_count_defaults_in_expr(expr, count_defaults, query))
                .collect(),
        }
        .add(query),
        ExprData::Cast { expr: input, ty } => ExprData::Cast {
            expr: coalesce_count_defaults_in_expr(input, count_defaults, query),
            ty,
        }
        .add(query),
        ExprData::CaseWhen {
            when_then,
            else_expr,
        } => ExprData::CaseWhen {
            when_then: when_then
                .into_iter()
                .map(|(when, then)| {
                    (
                        coalesce_count_defaults_in_expr(when, count_defaults, query),
                        coalesce_count_defaults_in_expr(then, count_defaults, query),
                    )
                })
                .collect(),
            else_expr: else_expr
                .map(|else_expr| coalesce_count_defaults_in_expr(else_expr, count_defaults, query)),
        }
        .add(query),
        ExprData::ScalarFunction { function, args } => ExprData::ScalarFunction {
            function,
            args: args
                .into_iter()
                .map(|expr| coalesce_count_defaults_in_expr(expr, count_defaults, query))
                .collect(),
        }
        .add(query),
        ExprData::Like {
            negated,
            expr: input,
            pattern,
            case_insensitive,
        } => ExprData::Like {
            negated,
            expr: coalesce_count_defaults_in_expr(input, count_defaults, query),
            pattern: coalesce_count_defaults_in_expr(pattern, count_defaults, query),
            case_insensitive,
        }
        .add(query),
        ExprData::Literal(_)
        | ExprData::ColumnRef(_)
        | ExprData::Exists { .. }
        | ExprData::InSubquery { .. }
        | ExprData::ScalarSubquery { .. } => expr,
    }
}

fn expr_references_any(expr: Expr, columns: &[Column], ctx: &mut OptimizerContext) -> bool {
    expr_used_columns(&ctx.query, &mut ctx.analyses, expr)
        .map(|used| used.into_iter().any(|column| columns.contains(&column)))
        .unwrap_or(false)
}

fn collect_count_aggregate_columns(op: Operator, query: &crate::QueryContext) -> Vec<Column> {
    match op.get(query) {
        OperatorData::Aggregation(aggregation) => aggregation
            .aggregates
            .iter()
            .filter_map(|(column, aggregate)| is_count_aggregate(aggregate).then_some(*column))
            .collect(),
        data => data
            .inputs()
            .into_iter()
            .flat_map(|input| collect_count_aggregate_columns(input, query))
            .collect(),
    }
}

fn finish_when_accessing_empty(
    site: &DependentJoinSite,
    accessing: &[Operator],
    state: &mut UnnestingState,
    ctx: &mut OptimizerContext,
) -> OptimizeResult<Option<Operator>> {
    if !accessing.is_empty() {
        return Ok(None);
    }

    let OperatorData::Join(join) = site.join.get(&ctx.query).clone() else {
        return Ok(None);
    };
    let inner_available: HashSet<_> = available_columns_for(ctx, join.inner)?
        .into_iter()
        .collect();
    for outer_ref in &site.outer_refs {
        let replacement = inner_available
            .iter()
            .copied()
            .find(|candidate| state.cclasses.equivalent(*outer_ref, *candidate));
        if let Some(replacement) = replacement {
            state.repr.insert(*outer_ref, replacement);
        }
    }

    if state.repr.len() == site.outer_refs.len() {
        let rewrite = ColumnRewrite {
            repr: state.repr.clone(),
        };
        let on = rewrite.rewrite_expr(join.on, &mut ctx.query);
        let predicates = site
            .outer_refs
            .iter()
            .filter_map(|outer_ref| {
                state
                    .repr
                    .get(outer_ref)
                    .map(|replacement| (*outer_ref, *replacement))
            })
            .map(|(outer_ref, replacement)| {
                build_null_safe_eq(outer_ref, replacement, &mut ctx.query)
            })
            .collect::<Vec<_>>();
        let on = make_and(
            std::iter::once(on).chain(predicates).collect(),
            &mut ctx.query,
        );
        return Ok(Some(
            OperatorData::Join(Join {
                join_type: join.join_type,
                on,
                outer: join.outer,
                inner: join.inner,
            })
            .add(&mut ctx.query),
        ));
    }
    Ok(None)
}

#[derive(Debug, Default)]
struct InnerRewrite {
    input: Option<Operator>,
    predicates: Vec<Expr>,
    inner_columns: Vec<Column>,
    saw_aggregation: bool,
}

impl InnerRewrite {
    fn unchanged(input: Operator) -> Self {
        Self {
            input: Some(input),
            ..Self::default()
        }
    }

    fn input(&self, fallback: Operator) -> Operator {
        self.input.unwrap_or(fallback)
    }

    fn merge_child(&mut self, child: InnerRewrite) {
        self.predicates.extend(child.predicates);
        extend_unique_columns(&mut self.inner_columns, child.inner_columns);
        self.saw_aggregation |= child.saw_aggregation;
    }
}

fn lift_correlated_predicates(
    op: Operator,
    lift_outer_only: bool,
    cclasses: &mut ColumnEqClasses,
    ctx: &mut OptimizerContext,
) -> OptimizeResult<InnerRewrite> {
    match op.get(&ctx.query).clone() {
        OperatorData::Selection(selection) => {
            lift_from_selection(op, selection, lift_outer_only, cclasses, ctx)
        }
        OperatorData::Aggregation(aggregation) => {
            let child =
                lift_correlated_predicates(aggregation.input, lift_outer_only, cclasses, ctx)?;
            if child.predicates.is_empty() {
                return Ok(InnerRewrite::unchanged(op));
            }

            let input = child.input(aggregation.input);
            let mut keys = aggregation.keys;
            expose_columns_as_grouping_keys(&mut keys, &child.inner_columns, &mut ctx.query);
            let new_op = OperatorData::Aggregation(crate::Aggregation {
                keys,
                aggregates: aggregation.aggregates,
                input,
            })
            .add(&mut ctx.query);

            Ok(InnerRewrite {
                input: Some(new_op),
                saw_aggregation: true,
                ..child
            })
        }
        OperatorData::Map(map) => lift_from_unary(
            op,
            map.input,
            ctx,
            |input, ctx| {
                OperatorData::Map(Map {
                    computations: map.computations,
                    input,
                })
                .add(ctx)
            },
            lift_outer_only,
            cclasses,
        ),
        OperatorData::Projection(projection) => {
            lift_from_projection(op, projection, ctx, lift_outer_only, cclasses)
        }
        OperatorData::Sort(sort) => lift_from_unary(
            op,
            sort.input,
            ctx,
            |input, ctx| {
                OperatorData::Sort(Sort {
                    keys: sort.keys,
                    input,
                })
                .add(ctx)
            },
            lift_outer_only,
            cclasses,
        ),
        OperatorData::Limit(_) => Ok(InnerRewrite::unchanged(op)),
        OperatorData::Rename(rename) => lift_from_unary(
            op,
            rename.input,
            ctx,
            |input, ctx| {
                OperatorData::Rename(Rename {
                    alias: rename.alias,
                    defs: rename.defs,
                    input,
                })
                .add(ctx)
            },
            lift_outer_only,
            cclasses,
        ),
        OperatorData::Join(join) if matches!(join.join_type, JoinType::Inner) => lift_from_binary(
            op,
            join.outer,
            join.inner,
            ctx,
            |outer, inner, ctx| {
                OperatorData::Join(Join {
                    join_type: join.join_type,
                    on: join.on,
                    outer,
                    inner,
                })
                .add(ctx)
            },
            lift_outer_only,
            cclasses,
        ),
        OperatorData::Join(join) if matches!(join.join_type, JoinType::Single) => lift_from_binary(
            op,
            join.outer,
            join.inner,
            ctx,
            |outer, inner, ctx| {
                OperatorData::Join(Join {
                    join_type: join.join_type,
                    on: join.on,
                    outer,
                    inner,
                })
                .add(ctx)
            },
            false,
            cclasses,
        ),
        OperatorData::Join(_) => Ok(InnerRewrite::unchanged(op)),
        OperatorData::CrossProduct(cross) => lift_from_binary(
            op,
            cross.outer,
            cross.inner,
            ctx,
            |outer, inner, ctx| {
                OperatorData::CrossProduct(crate::CrossProduct { outer, inner }).add(ctx)
            },
            lift_outer_only,
            cclasses,
        ),
        _ => Ok(InnerRewrite::unchanged(op)),
    }
}

fn lift_from_projection(
    original: Operator,
    projection: Projection,
    ctx: &mut OptimizerContext,
    lift_outer_only: bool,
    cclasses: &mut ColumnEqClasses,
) -> OptimizeResult<InnerRewrite> {
    let child = lift_correlated_predicates(projection.input, lift_outer_only, cclasses, ctx)?;
    if child.predicates.is_empty() {
        return Ok(InnerRewrite::unchanged(original));
    }

    let input = child.input(projection.input);
    let mut columns = projection.columns;
    extend_unique_columns(&mut columns, child.inner_columns.iter().copied());
    let rebuilt = OperatorData::Projection(Projection { columns, input }).add(&mut ctx.query);

    Ok(InnerRewrite {
        input: Some(rebuilt),
        ..child
    })
}

fn expose_columns_as_grouping_keys(
    keys: &mut Vec<Expr>,
    columns: &[Column],
    query: &mut crate::QueryContext,
) {
    for column in columns {
        if !keys.iter().any(
            |key| matches!(key.get(query), ExprData::ColumnRef(existing) if existing == column),
        ) {
            keys.push(ExprData::ColumnRef(*column).add(query));
        }
    }
}

fn lift_from_selection(
    original: Operator,
    selection: Selection,
    lift_outer_only: bool,
    cclasses: &mut ColumnEqClasses,
    ctx: &mut OptimizerContext,
) -> OptimizeResult<InnerRewrite> {
    register_equality_predicate(selection.predicate, cclasses, &ctx.query);
    let child = lift_correlated_predicates(selection.input, lift_outer_only, cclasses, ctx)?;
    let input = child.input(selection.input);
    let input_columns = available_columns_for(ctx, input)?;
    let conjuncts = split_conjuncts(selection.predicate, &ctx.query);
    let mut keep = Vec::new();
    let mut lift = Vec::new();
    let mut inner_columns = Vec::new();

    for conjunct in conjuncts {
        let used = expr_used_columns(&ctx.query, &mut ctx.analyses, conjunct).unwrap_or_default();
        let uses_input = used.iter().any(|column| input_columns.contains(column));
        let uses_outer = used.iter().any(|column| !input_columns.contains(column));
        if uses_input && uses_outer {
            extend_unique_columns(
                &mut inner_columns,
                used.into_iter()
                    .filter(|column| input_columns.contains(column))
                    .collect::<Vec<_>>(),
            );
            lift.push(conjunct);
        } else if uses_outer && lift_outer_only {
            lift.push(conjunct);
        } else {
            keep.push(conjunct);
        }
    }

    if lift.is_empty() && child.predicates.is_empty() {
        return Ok(InnerRewrite::unchanged(original));
    }

    let input = if keep.is_empty() {
        input
    } else {
        OperatorData::Selection(Selection {
            predicate: make_and(keep, &mut ctx.query),
            input,
        })
        .add(&mut ctx.query)
    };

    let mut result = InnerRewrite {
        input: Some(input),
        predicates: lift,
        inner_columns,
        ..InnerRewrite::default()
    };
    result.merge_child(child);
    Ok(result)
}

fn lift_from_unary(
    original: Operator,
    input: Operator,
    ctx: &mut OptimizerContext,
    rebuild: impl FnOnce(Operator, &mut crate::QueryContext) -> Operator,
    lift_outer_only: bool,
    cclasses: &mut ColumnEqClasses,
) -> OptimizeResult<InnerRewrite> {
    let child = lift_correlated_predicates(input, lift_outer_only, cclasses, ctx)?;
    if child.predicates.is_empty() {
        return Ok(InnerRewrite::unchanged(original));
    }

    let new_input = child.input(input);
    let rebuilt = rebuild(new_input, &mut ctx.query);
    Ok(InnerRewrite {
        input: Some(rebuilt),
        ..child
    })
}

fn lift_from_binary(
    original: Operator,
    outer: Operator,
    inner: Operator,
    ctx: &mut OptimizerContext,
    rebuild: impl FnOnce(Operator, Operator, &mut crate::QueryContext) -> Operator,
    lift_outer_only: bool,
    cclasses: &mut ColumnEqClasses,
) -> OptimizeResult<InnerRewrite> {
    let left = lift_correlated_predicates(outer, lift_outer_only, cclasses, ctx)?;
    let right = lift_correlated_predicates(inner, lift_outer_only, cclasses, ctx)?;
    if left.predicates.is_empty() && right.predicates.is_empty() {
        return Ok(InnerRewrite::unchanged(original));
    }

    let rebuilt = rebuild(left.input(outer), right.input(inner), &mut ctx.query);
    let mut result = InnerRewrite {
        input: Some(rebuilt),
        ..InnerRewrite::default()
    };
    result.merge_child(left);
    result.merge_child(right);
    Ok(result)
}

fn expose_columns(
    op: Operator,
    columns: &[Column],
    ctx: &mut OptimizerContext,
) -> OptimizeResult<Operator> {
    // Lifted correlation predicates move from inside the subquery input to the
    // parent join condition, so any inner-side ColumnRef they use must be
    // produced by the rewritten join.inner. This is a handle-level requirement:
    // Projection can drop a Column, and Aggregation can only output grouping
    // keys or aggregate results. It is not connector name disambiguation.
    if columns.is_empty() {
        return Ok(op);
    }
    if columns_available(ctx, op, columns)? {
        return Ok(op);
    }

    match op.get(&ctx.query).clone() {
        OperatorData::Aggregation(mut aggregation) => {
            let input = expose_columns(aggregation.input, columns, ctx)?;
            aggregation.input = input;
            expose_columns_as_grouping_keys(&mut aggregation.keys, columns, &mut ctx.query);
            Ok(OperatorData::Aggregation(aggregation).add(&mut ctx.query))
        }
        OperatorData::Projection(mut projection) => {
            let input = expose_columns(projection.input, columns, ctx)?;
            projection.input = input;
            for column in columns {
                if !projection.columns.contains(column) {
                    projection.columns.push(*column);
                }
            }
            Ok(OperatorData::Projection(projection).add(&mut ctx.query))
        }
        OperatorData::Map(mut map) => {
            map.input = expose_columns(map.input, columns, ctx)?;
            Ok(OperatorData::Map(map).add(&mut ctx.query))
        }
        OperatorData::Selection(mut selection) => {
            selection.input = expose_columns(selection.input, columns, ctx)?;
            Ok(OperatorData::Selection(selection).add(&mut ctx.query))
        }
        OperatorData::Sort(mut sort) => {
            sort.input = expose_columns(sort.input, columns, ctx)?;
            Ok(OperatorData::Sort(sort).add(&mut ctx.query))
        }
        OperatorData::Limit(mut limit) => {
            limit.input = expose_columns(limit.input, columns, ctx)?;
            Ok(OperatorData::Limit(limit).add(&mut ctx.query))
        }
        _ => Ok(op),
    }
}

fn free_columns_for(ctx: &mut OptimizerContext, op: Operator) -> OptimizeResult<Vec<Column>> {
    ctx.analyses
        .get::<FreeColumns>(&ctx.query, op)
        .map_err(|error| crate::OptimizeError::PassError {
            pass: "HolisticUnnesting",
            message: error.to_string(),
        })
}

fn available_columns_for(ctx: &mut OptimizerContext, op: Operator) -> OptimizeResult<Vec<Column>> {
    ctx.analyses
        .get::<AvailableColumns>(&ctx.query, op)
        .map_err(|error| crate::OptimizeError::PassError {
            pass: "HolisticUnnesting",
            message: error.to_string(),
        })
}

fn columns_available(
    ctx: &mut OptimizerContext,
    op: Operator,
    columns: &[Column],
) -> OptimizeResult<bool> {
    let available = available_columns_for(ctx, op)?;
    Ok(columns.iter().all(|column| available.contains(column)))
}

fn is_count_aggregate(aggregate: &crate::AggregateExpr) -> bool {
    matches!(
        aggregate,
        crate::AggregateExpr::CountStar
            | crate::AggregateExpr::Func {
                func: crate::AggregateFunction::Count,
                ..
            }
    )
}

fn split_conjuncts(expr: Expr, query: &crate::QueryContext) -> Vec<Expr> {
    match expr.get(query).clone() {
        ExprData::Nary {
            op: NaryOp::And,
            exprs,
        } => exprs
            .into_iter()
            .flat_map(|expr| split_conjuncts(expr, query))
            .collect(),
        _ => vec![expr],
    }
}

fn make_and(mut exprs: Vec<Expr>, query: &mut crate::QueryContext) -> Expr {
    exprs.retain(|expr| !is_true_literal(*expr, query));
    match exprs.len() {
        0 => ExprData::Literal(crate::ScalarValue::Boolean(true)).add(query),
        1 => exprs.remove(0),
        _ => ExprData::Nary {
            op: NaryOp::And,
            exprs,
        }
        .add(query),
    }
}

fn is_true_literal(expr: Expr, query: &crate::QueryContext) -> bool {
    matches!(
        expr.get(query),
        ExprData::Literal(crate::ScalarValue::Boolean(true))
    )
}

fn extend_unique_columns(columns: &mut Vec<Column>, incoming: impl IntoIterator<Item = Column>) {
    for column in incoming {
        if !columns.contains(&column) {
            columns.push(column);
        }
    }
}

/// Returns subquery-derived joins whose inner input still depends on outer
/// columns, according to [`FreeColumns`].
pub fn correlated_subquery_joins(
    query: &crate::QueryContext,
    root: Operator,
    analyses: &mut AnalysisContext,
) -> OptimizeResult<Vec<CorrelatedSubqueryJoin>> {
    let mut out = Vec::new();
    collect_correlated_subquery_joins(query, root, analyses, &mut out)?;
    Ok(out)
}

/// A subquery-derived join input that still contains outer references.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CorrelatedSubqueryJoin {
    pub join: Operator,
    pub join_type: JoinType,
    pub inner: Operator,
    pub free_columns: Vec<Column>,
}

fn collect_correlated_subquery_joins(
    query: &crate::QueryContext,
    op: Operator,
    analyses: &mut AnalysisContext,
    out: &mut Vec<CorrelatedSubqueryJoin>,
) -> OptimizeResult<()> {
    if let OperatorData::Join(join) = op.get(query)
        && is_subquery_derived_join(&join.join_type)
    {
        let free_columns = analyses
            .get::<FreeColumns>(query, join.inner)
            .map_err(|error| crate::OptimizeError::PassError {
                pass: "HolisticUnnesting",
                message: error.to_string(),
            })?;

        if !free_columns.is_empty() {
            out.push(CorrelatedSubqueryJoin {
                join: op,
                join_type: join.join_type.clone(),
                inner: join.inner,
                free_columns,
            });
        }
    }

    for input in op.get(query).inputs() {
        collect_correlated_subquery_joins(query, input, analyses, out)?;
    }

    Ok(())
}

fn is_subquery_derived_join(join_type: &JoinType) -> bool {
    matches!(
        join_type,
        JoinType::Single | JoinType::LeftSemi | JoinType::LeftAnti | JoinType::LeftMark { .. }
    )
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::{
        AggregateExpr, AggregateFunction, Aggregation, AvailableColumns, BinaryOp, ColumnData,
        ExprData, FreeColumns, Join, JoinType, Limit, NaryOp, OperatorData, PassResult,
        QueryContext, Rename, ScalarValue, Scan, Selection, TableRef,
        optimize::{
            OperatorRewriteAdaptor, PassManager, QueryPass,
            holistic_unnesting::correlated_subquery_joins,
        },
    };
    use arrow_schema::DataType;

    fn expr_contains_coalesce(expr: crate::Expr, query: &QueryContext) -> bool {
        match expr.get(query) {
            ExprData::ScalarFunction {
                function: crate::ScalarFunction::Coalesce,
                ..
            } => true,
            ExprData::Unary { expr, .. } => expr_contains_coalesce(*expr, query),
            ExprData::Binary { left, right, .. } => {
                expr_contains_coalesce(*left, query) || expr_contains_coalesce(*right, query)
            }
            ExprData::Nary { exprs, .. } => exprs
                .iter()
                .any(|expr| expr_contains_coalesce(*expr, query)),
            ExprData::Cast { expr, .. } => expr_contains_coalesce(*expr, query),
            ExprData::CaseWhen {
                when_then,
                else_expr,
            } => {
                when_then.iter().any(|(when, then)| {
                    expr_contains_coalesce(*when, query) || expr_contains_coalesce(*then, query)
                }) || else_expr.is_some_and(|else_expr| expr_contains_coalesce(else_expr, query))
            }
            ExprData::ScalarFunction { args, .. } => {
                args.iter().any(|expr| expr_contains_coalesce(*expr, query))
            }
            ExprData::Like { expr, pattern, .. } => {
                expr_contains_coalesce(*expr, query) || expr_contains_coalesce(*pattern, query)
            }
            ExprData::Literal(_)
            | ExprData::ColumnRef(_)
            | ExprData::Exists { .. }
            | ExprData::InSubquery { .. }
            | ExprData::ScalarSubquery { .. } => false,
        }
    }

    #[test]
    fn column_eq_classes_tracks_transitive_equivalence() {
        let mut query = QueryContext::new();
        let a = ColumnData::new("a", DataType::Int64).add(&mut query);
        let b = ColumnData::new("b", DataType::Int64).add(&mut query);
        let c = ColumnData::new("c", DataType::Int64).add(&mut query);

        let mut classes = super::ColumnEqClasses::default();
        classes.union(a, b);
        classes.union(b, c);

        assert!(classes.equivalent(a, c));
    }

    #[test]
    fn null_safe_equality_builder_uses_distinctness_comparison() {
        let mut query = QueryContext::new();
        let left_col = ColumnData::new("left", DataType::Int64).add(&mut query);
        let right_col = ColumnData::new("right", DataType::Int64).add(&mut query);

        let expr = super::build_null_safe_eq(left_col, right_col, &mut query);

        let ExprData::Binary { op, left, right } = query.expr(expr) else {
            panic!("expected binary expression");
        };
        assert_eq!(*op, BinaryOp::IsNotDistinctFrom);
        assert!(matches!(query.expr(*left), ExprData::ColumnRef(c) if *c == left_col));
        assert!(matches!(query.expr(*right), ExprData::ColumnRef(c) if *c == right_col));
    }

    #[test]
    fn column_rewrite_replaces_column_refs_append_only() {
        let mut query = QueryContext::new();
        let original = ColumnData::new("a", DataType::Int64).add(&mut query);
        let replacement = ColumnData::new("b", DataType::Int64).add(&mut query);
        let literal = ExprData::Literal(ScalarValue::Int64(5)).add(&mut query);
        let expr = ExprData::Binary {
            op: BinaryOp::Eq,
            left: ExprData::ColumnRef(original).add(&mut query),
            right: literal,
        }
        .add(&mut query);

        let rewrite = super::ColumnRewrite {
            repr: HashMap::from([(original, replacement)]),
        };
        let rewritten = rewrite.rewrite_expr(expr, &mut query);

        assert_ne!(expr, rewritten);
        assert!(matches!(
            rewritten.get(&query),
            ExprData::Binary { left, .. }
                if matches!(left.get(&query), ExprData::ColumnRef(column) if *column == replacement)
        ));
    }

    #[test]
    fn dependent_join_sites_are_collected_top_down() {
        // Shape:
        //
        //   SingleJoin(outer,
        //     Selection(middle = outer,
        //       LeftSemiJoin(middle, Selection(inner = middle, inner_scan))))
        //
        // The holistic pass processes one reachable dependent join at a time,
        // so discovery order must be outermost-first. Rewriting the nested
        // semi join before the parent scalar join can leave stale handles and
        // misses the intended top-down correlated scope.
        let mut query = QueryContext::new();
        let outer_col = ColumnData::new("outer_a", DataType::Int64).add(&mut query);
        let middle_col = ColumnData::new("middle_a", DataType::Int64).add(&mut query);
        let inner_col = ColumnData::new("inner_a", DataType::Int64).add(&mut query);

        let outer = OperatorData::Scan(Scan {
            table: TableRef::bare("outer_t"),
            columns: vec![outer_col],
        })
        .add(&mut query);
        let middle_scan = OperatorData::Scan(Scan {
            table: TableRef::bare("middle_t"),
            columns: vec![middle_col],
        })
        .add(&mut query);
        let inner_scan = OperatorData::Scan(Scan {
            table: TableRef::bare("inner_t"),
            columns: vec![inner_col],
        })
        .add(&mut query);

        let inner_predicate = ExprData::Binary {
            op: BinaryOp::Eq,
            left: ExprData::ColumnRef(inner_col).add(&mut query),
            right: ExprData::ColumnRef(middle_col).add(&mut query),
        }
        .add(&mut query);
        let inner_selection = OperatorData::Selection(Selection {
            predicate: inner_predicate,
            input: inner_scan,
        })
        .add(&mut query);
        let true_expr = ExprData::Literal(ScalarValue::Boolean(true)).add(&mut query);
        let nested_join = OperatorData::Join(Join {
            join_type: JoinType::LeftSemi,
            on: true_expr,
            outer: middle_scan,
            inner: inner_selection,
        })
        .add(&mut query);

        let outer_predicate = ExprData::Binary {
            op: BinaryOp::Eq,
            left: ExprData::ColumnRef(middle_col).add(&mut query),
            right: ExprData::ColumnRef(outer_col).add(&mut query),
        }
        .add(&mut query);
        let middle_selection = OperatorData::Selection(Selection {
            predicate: outer_predicate,
            input: nested_join,
        })
        .add(&mut query);
        let root = OperatorData::Join(Join {
            join_type: JoinType::Single,
            on: true_expr,
            outer,
            inner: middle_selection,
        })
        .add(&mut query);
        query.set_root(root);

        let mut opt = crate::test_optimizer_context(query);
        let sites = super::collect_dependent_join_sites(root, &mut opt).unwrap();

        assert_eq!(sites.len(), 2);
        assert_eq!(sites[0].join, root);
        assert_eq!(sites[1].join, nested_join);
        assert!(sites[0].depth < sites[1].depth);
    }

    #[test]
    fn reports_single_join_inner_free_columns() {
        // Inspection helper regression: a scalar subquery lowered to
        // `SingleJoin(outer, Selection(inner = outer, inner_scan))` should be
        // reported as correlated because its inner side has a free outer
        // column. This helper powers explain/debug assertions outside the pass.
        let mut query = QueryContext::new();
        let outer_col = ColumnData::new("outer_a", DataType::Int64).add(&mut query);
        let inner_col = ColumnData::new("inner_a", DataType::Int64).add(&mut query);

        let outer = OperatorData::Scan(Scan {
            table: TableRef::bare("outer_t"),
            columns: vec![outer_col],
        })
        .add(&mut query);
        let inner_scan = OperatorData::Scan(Scan {
            table: TableRef::bare("inner_t"),
            columns: vec![inner_col],
        })
        .add(&mut query);

        let predicate = ExprData::Binary {
            op: BinaryOp::Eq,
            left: ExprData::ColumnRef(inner_col).add(&mut query),
            right: ExprData::ColumnRef(outer_col).add(&mut query),
        }
        .add(&mut query);
        let inner = OperatorData::Selection(Selection {
            predicate,
            input: inner_scan,
        })
        .add(&mut query);

        let on = ExprData::Literal(ScalarValue::Boolean(true)).add(&mut query);
        let join = OperatorData::Join(Join {
            join_type: JoinType::Single,
            on,
            outer,
            inner,
        })
        .add(&mut query);
        query.set_root(join);

        let mut analyses = crate::test_analyses(&query);
        let correlated = correlated_subquery_joins(&query, join, &mut analyses).unwrap();
        assert_eq!(correlated.len(), 1);
        assert_eq!(correlated[0].join, join);
        assert_eq!(correlated[0].join_type, JoinType::Single);
        assert_eq!(correlated[0].inner, inner);
        assert_eq!(correlated[0].free_columns, vec![outer_col]);
    }

    #[test]
    fn ignores_decorrelated_subquery_join() {
        // The same inspection helper must ignore already-decorrelated
        // subquery-derived joins. A semi join whose condition references both
        // inputs is normal relational join state; the inner scan itself has no
        // free outer columns.
        let mut query = QueryContext::new();
        let outer_col = ColumnData::new("outer_a", DataType::Int64).add(&mut query);
        let inner_col = ColumnData::new("inner_a", DataType::Int64).add(&mut query);

        let outer = OperatorData::Scan(Scan {
            table: TableRef::bare("outer_t"),
            columns: vec![outer_col],
        })
        .add(&mut query);
        let inner = OperatorData::Scan(Scan {
            table: TableRef::bare("inner_t"),
            columns: vec![inner_col],
        })
        .add(&mut query);

        let on = ExprData::Binary {
            op: BinaryOp::Eq,
            left: ExprData::ColumnRef(outer_col).add(&mut query),
            right: ExprData::ColumnRef(inner_col).add(&mut query),
        }
        .add(&mut query);
        let join = OperatorData::Join(Join {
            join_type: JoinType::LeftSemi,
            on,
            outer,
            inner,
        })
        .add(&mut query);
        query.set_root(join);

        let mut analyses = crate::test_analyses(&query);
        let correlated = correlated_subquery_joins(&query, join, &mut analyses).unwrap();
        assert!(correlated.is_empty());
    }

    #[test]
    fn unnests_correlated_scalar_aggregate_as_left_outer_join() {
        // Shape:
        //
        //   SingleJoin(outer,
        //     Aggregation([], min(value),
        //       Selection(inner_key = outer_key, inner_scan)))
        //
        // The scalar aggregate must become a left outer join and the lifted
        // inner key must be added to the aggregate grouping keys. Otherwise the
        // join condition would refer to a column the aggregate no longer
        // produces, and empty correlated matches would lose scalar-subquery
        // NULL semantics.
        let mut query = QueryContext::new();
        let outer_key = ColumnData::new("outer_key", DataType::Int64).add(&mut query);
        let inner_key = ColumnData::new("inner_key", DataType::Int64).add(&mut query);
        let inner_value = ColumnData::new("inner_value", DataType::Int64).add(&mut query);
        let min_value = ColumnData::new("min_value", DataType::Int64).add(&mut query);

        let outer = OperatorData::Scan(Scan {
            table: TableRef::bare("outer_t"),
            columns: vec![outer_key],
        })
        .add(&mut query);
        let inner_scan = OperatorData::Scan(Scan {
            table: TableRef::bare("inner_t"),
            columns: vec![inner_key, inner_value],
        })
        .add(&mut query);
        let predicate = ExprData::Binary {
            op: BinaryOp::Eq,
            left: ExprData::ColumnRef(inner_key).add(&mut query),
            right: ExprData::ColumnRef(outer_key).add(&mut query),
        }
        .add(&mut query);
        let inner_selection = OperatorData::Selection(Selection {
            predicate,
            input: inner_scan,
        })
        .add(&mut query);
        let aggregate = OperatorData::Aggregation(Aggregation {
            keys: vec![],
            aggregates: vec![(
                min_value,
                AggregateExpr::Func {
                    func: AggregateFunction::Min,
                    arg: ExprData::ColumnRef(inner_value).add(&mut query),
                    distinct: false,
                },
            )],
            input: inner_selection,
        })
        .add(&mut query);
        let on = ExprData::Literal(ScalarValue::Boolean(true)).add(&mut query);
        let join = OperatorData::Join(Join {
            join_type: JoinType::Single,
            on,
            outer,
            inner: aggregate,
        })
        .add(&mut query);
        query.set_root(join);

        let mut opt = crate::test_optimizer_context(query);
        let result = super::HolisticUnnesting.run(&mut opt).unwrap();
        assert_eq!(result, PassResult::Changed);

        let root = opt.query.root().unwrap();
        let OperatorData::Join(join) = root.get(&opt.query) else {
            panic!("root should be a join");
        };
        assert_eq!(join.join_type, JoinType::LeftOuter);
        assert!(
            opt.analyses
                .get::<FreeColumns>(&opt.query, join.inner)
                .unwrap()
                .is_empty()
        );

        let OperatorData::Aggregation(aggregation) = join.inner.get(&opt.query) else {
            panic!("inner should remain an aggregation");
        };
        assert_eq!(aggregation.keys.len(), 1);
        assert!(matches!(
            aggregation.keys[0].get(&opt.query),
            ExprData::ColumnRef(column) if *column == inner_key
        ));
    }

    #[test]
    fn unnests_correlated_scalar_count_aggregate_as_left_outer_join() {
        // COUNT is the scalar aggregate edge case: after decorrelation the
        // grouped aggregate produces no row for an outer key with no matches.
        // The pass rewrites the scalar join as a left outer join so later
        // expression handling can default COUNT outputs to zero at the parent
        // map/projection layer.
        let mut query = QueryContext::new();
        let outer_key = ColumnData::new("outer_key", DataType::Int64).add(&mut query);
        let inner_key = ColumnData::new("inner_key", DataType::Int64).add(&mut query);
        let count_value = ColumnData::new("count_value", DataType::Int64).add(&mut query);

        let outer = OperatorData::Scan(Scan {
            table: TableRef::bare("outer_t"),
            columns: vec![outer_key],
        })
        .add(&mut query);
        let inner_scan = OperatorData::Scan(Scan {
            table: TableRef::bare("inner_t"),
            columns: vec![inner_key],
        })
        .add(&mut query);
        let predicate = ExprData::Binary {
            op: BinaryOp::Eq,
            left: ExprData::ColumnRef(inner_key).add(&mut query),
            right: ExprData::ColumnRef(outer_key).add(&mut query),
        }
        .add(&mut query);
        let inner_selection = OperatorData::Selection(Selection {
            predicate,
            input: inner_scan,
        })
        .add(&mut query);
        let aggregate = OperatorData::Aggregation(Aggregation {
            keys: vec![],
            aggregates: vec![(count_value, AggregateExpr::CountStar)],
            input: inner_selection,
        })
        .add(&mut query);
        let on = ExprData::Literal(ScalarValue::Boolean(true)).add(&mut query);
        let join = OperatorData::Join(Join {
            join_type: JoinType::Single,
            on,
            outer,
            inner: aggregate,
        })
        .add(&mut query);
        query.set_root(join);

        let mut opt = crate::test_optimizer_context(query);
        let result = super::HolisticUnnesting.run(&mut opt).unwrap();
        assert_eq!(result, PassResult::Changed);

        let root = opt.query.root().unwrap();
        let OperatorData::Join(join) = root.get(&opt.query) else {
            panic!("root should be a join");
        };
        assert_eq!(join.join_type, JoinType::LeftOuter);
        assert!(
            opt.analyses
                .get::<FreeColumns>(&opt.query, join.inner)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn defaults_scalar_count_before_selection_predicate_observes_it() {
        // COUNT defaulting must happen anywhere the scalar result is observed,
        // not only in projection maps. A filter such as
        // `(SELECT count(*) ...) = 0` should compare `COALESCE(count, 0)` after
        // the scalar aggregate is decorrelated into a left outer join.
        let mut query = QueryContext::new();
        let outer_key = ColumnData::new("outer_key", DataType::Int64).add(&mut query);
        let inner_key = ColumnData::new("inner_key", DataType::Int64).add(&mut query);
        let count_value = ColumnData::new("count_value", DataType::Int64).add(&mut query);

        let outer = OperatorData::Scan(Scan {
            table: TableRef::bare("outer_t"),
            columns: vec![outer_key],
        })
        .add(&mut query);
        let inner_scan = OperatorData::Scan(Scan {
            table: TableRef::bare("inner_t"),
            columns: vec![inner_key],
        })
        .add(&mut query);
        let correlated = ExprData::Binary {
            op: BinaryOp::Eq,
            left: ExprData::ColumnRef(inner_key).add(&mut query),
            right: ExprData::ColumnRef(outer_key).add(&mut query),
        }
        .add(&mut query);
        let inner_selection = OperatorData::Selection(Selection {
            predicate: correlated,
            input: inner_scan,
        })
        .add(&mut query);
        let aggregate = OperatorData::Aggregation(Aggregation {
            keys: vec![],
            aggregates: vec![(count_value, AggregateExpr::CountStar)],
            input: inner_selection,
        })
        .add(&mut query);
        let on = ExprData::Literal(ScalarValue::Boolean(true)).add(&mut query);
        let scalar_join = OperatorData::Join(Join {
            join_type: JoinType::Single,
            on,
            outer,
            inner: aggregate,
        })
        .add(&mut query);
        let predicate = ExprData::Binary {
            op: BinaryOp::Eq,
            left: ExprData::ColumnRef(count_value).add(&mut query),
            right: ExprData::Literal(ScalarValue::Int64(0)).add(&mut query),
        }
        .add(&mut query);
        let root = OperatorData::Selection(Selection {
            predicate,
            input: scalar_join,
        })
        .add(&mut query);
        query.set_root(root);

        let mut opt = crate::test_optimizer_context(query);
        let result = super::HolisticUnnesting.run(&mut opt).unwrap();
        assert_eq!(result, PassResult::Changed);

        let root = opt.query.root().unwrap();
        let OperatorData::Selection(selection) = root.get(&opt.query) else {
            panic!("root should remain a selection");
        };
        assert!(expr_contains_coalesce(selection.predicate, &opt.query));
        let OperatorData::Join(join) = selection.input.get(&opt.query) else {
            panic!("selection input should be the decorrelated join");
        };
        assert_eq!(join.join_type, JoinType::LeftOuter);
    }

    #[test]
    fn unnests_correlated_semijoin_by_lifting_inner_predicate() {
        // Basic cheap path:
        //
        //   LeftSemiJoin(outer, Selection(inner_key = outer_key, inner_scan))
        //
        // The pass should remove the inner selection, move the equality into
        // the semi-join condition, and leave an inner input with no free outer
        // columns.
        let mut query = QueryContext::new();
        let outer_key = ColumnData::new("outer_key", DataType::Int64).add(&mut query);
        let inner_key = ColumnData::new("inner_key", DataType::Int64).add(&mut query);

        let outer = OperatorData::Scan(Scan {
            table: TableRef::bare("outer_t"),
            columns: vec![outer_key],
        })
        .add(&mut query);
        let inner_scan = OperatorData::Scan(Scan {
            table: TableRef::bare("inner_t"),
            columns: vec![inner_key],
        })
        .add(&mut query);
        let predicate = ExprData::Binary {
            op: BinaryOp::Eq,
            left: ExprData::ColumnRef(inner_key).add(&mut query),
            right: ExprData::ColumnRef(outer_key).add(&mut query),
        }
        .add(&mut query);
        let inner = OperatorData::Selection(Selection {
            predicate,
            input: inner_scan,
        })
        .add(&mut query);
        let on = ExprData::Literal(ScalarValue::Boolean(true)).add(&mut query);
        let join = OperatorData::Join(Join {
            join_type: JoinType::LeftSemi,
            on,
            outer,
            inner,
        })
        .add(&mut query);
        query.set_root(join);

        let mut opt = crate::test_optimizer_context(query);
        let result = super::HolisticUnnesting.run(&mut opt).unwrap();
        assert_eq!(result, PassResult::Changed);

        let root = opt.query.root().unwrap();
        let OperatorData::Join(join) = root.get(&opt.query) else {
            panic!("root should be a join");
        };
        assert_eq!(join.join_type, JoinType::LeftSemi);
        assert_eq!(join.inner, inner_scan);
        assert!(matches!(
            join.on.get(&opt.query),
            ExprData::Binary {
                op: BinaryOp::Eq,
                ..
            }
        ));
        assert!(
            opt.analyses
                .get::<FreeColumns>(&opt.query, join.inner)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn lifts_outer_only_predicate_for_exists_style_semijoin() {
        // Outer-only predicates inside EXISTS are safe to move into the semi join
        // condition: the marker is only asking whether the inner side has any row
        // for an outer binding that also satisfies the outer predicate.
        let mut query = QueryContext::new();
        let outer_key = ColumnData::new("outer_key", DataType::Int64).add(&mut query);
        let inner_key = ColumnData::new("inner_key", DataType::Int64).add(&mut query);

        let outer = OperatorData::Scan(Scan {
            table: TableRef::bare("outer_t"),
            columns: vec![outer_key],
        })
        .add(&mut query);
        let inner_scan = OperatorData::Scan(Scan {
            table: TableRef::bare("inner_t"),
            columns: vec![inner_key],
        })
        .add(&mut query);
        let outer_only = ExprData::Binary {
            op: BinaryOp::Gt,
            left: ExprData::ColumnRef(outer_key).add(&mut query),
            right: ExprData::Literal(ScalarValue::Int64(10)).add(&mut query),
        }
        .add(&mut query);
        let inner = OperatorData::Selection(Selection {
            predicate: outer_only,
            input: inner_scan,
        })
        .add(&mut query);
        let on = ExprData::Literal(ScalarValue::Boolean(true)).add(&mut query);
        let join = OperatorData::Join(Join {
            join_type: JoinType::LeftSemi,
            on,
            outer,
            inner,
        })
        .add(&mut query);
        query.set_root(join);

        let mut opt = crate::test_optimizer_context(query);
        let result = super::HolisticUnnesting.run(&mut opt).unwrap();
        assert_eq!(result, PassResult::Changed);

        let root = opt.query.root().unwrap();
        let OperatorData::Join(join) = root.get(&opt.query) else {
            panic!("root should be a join");
        };
        assert_eq!(join.join_type, JoinType::LeftSemi);
        assert_eq!(join.inner, inner_scan);
        assert!(matches!(
            join.on.get(&opt.query),
            ExprData::Binary {
                op: BinaryOp::Gt,
                ..
            }
        ));
    }

    #[test]
    fn does_not_lift_outer_only_predicate_for_scalar_join() {
        // For scalar joins, an outer-only filter inside the subquery changes the
        // difference between "no row" and a NULL scalar result. v1 leaves this
        // shape correlated until a full domain-based rule handles it.
        let mut query = QueryContext::new();
        let outer_key = ColumnData::new("outer_key", DataType::Int64).add(&mut query);
        let inner_key = ColumnData::new("inner_key", DataType::Int64).add(&mut query);

        let outer = OperatorData::Scan(Scan {
            table: TableRef::bare("outer_t"),
            columns: vec![outer_key],
        })
        .add(&mut query);
        let inner_scan = OperatorData::Scan(Scan {
            table: TableRef::bare("inner_t"),
            columns: vec![inner_key],
        })
        .add(&mut query);
        let outer_only = ExprData::Binary {
            op: BinaryOp::Gt,
            left: ExprData::ColumnRef(outer_key).add(&mut query),
            right: ExprData::Literal(ScalarValue::Int64(10)).add(&mut query),
        }
        .add(&mut query);
        let inner = OperatorData::Selection(Selection {
            predicate: outer_only,
            input: inner_scan,
        })
        .add(&mut query);
        let on = ExprData::Literal(ScalarValue::Boolean(true)).add(&mut query);
        let join = OperatorData::Join(Join {
            join_type: JoinType::Single,
            on,
            outer,
            inner,
        })
        .add(&mut query);
        query.set_root(join);

        let mut opt = crate::test_optimizer_context(query);
        let result = super::HolisticUnnesting.run(&mut opt).unwrap();
        assert_eq!(result, PassResult::Unchanged);
        assert_eq!(opt.query.root(), Some(join));
    }

    #[test]
    fn does_not_lift_correlated_predicate_through_non_inner_join_boundaries() {
        // Pulling a correlated filter through a null-supplying, semi/anti, or
        // scalar boundary can change cardinality and NULL semantics. v1 only
        // recurses through cross products and ordinary inner joins.
        for join_type in [JoinType::LeftOuter, JoinType::LeftSemi, JoinType::LeftAnti] {
            let mut query = QueryContext::new();
            let outer_key = ColumnData::new("outer_key", DataType::Int64).add(&mut query);
            let inner_key = ColumnData::new("inner_key", DataType::Int64).add(&mut query);
            let sibling_key = ColumnData::new("sibling_key", DataType::Int64).add(&mut query);

            let outer = OperatorData::Scan(Scan {
                table: TableRef::bare("outer_t"),
                columns: vec![outer_key],
            })
            .add(&mut query);
            let inner_scan = OperatorData::Scan(Scan {
                table: TableRef::bare("inner_t"),
                columns: vec![inner_key],
            })
            .add(&mut query);
            let sibling = OperatorData::Scan(Scan {
                table: TableRef::bare("sibling_t"),
                columns: vec![sibling_key],
            })
            .add(&mut query);
            let correlated = ExprData::Binary {
                op: BinaryOp::Eq,
                left: ExprData::ColumnRef(inner_key).add(&mut query),
                right: ExprData::ColumnRef(outer_key).add(&mut query),
            }
            .add(&mut query);
            let filtered_inner = OperatorData::Selection(Selection {
                predicate: correlated,
                input: inner_scan,
            })
            .add(&mut query);
            let true_expr = ExprData::Literal(ScalarValue::Boolean(true)).add(&mut query);
            let boundary = OperatorData::Join(Join {
                join_type,
                on: true_expr,
                outer: filtered_inner,
                inner: sibling,
            })
            .add(&mut query);
            let root = OperatorData::Join(Join {
                join_type: JoinType::LeftSemi,
                on: true_expr,
                outer,
                inner: boundary,
            })
            .add(&mut query);
            query.set_root(root);

            let mut opt = crate::test_optimizer_context(query);
            let result = super::HolisticUnnesting.run(&mut opt).unwrap();
            assert_eq!(result, PassResult::Unchanged);
            assert_eq!(opt.query.root(), Some(root));
        }
    }

    #[test]
    fn preserves_unchanged_selection_when_sibling_lifts_predicate() {
        // Regression for recursive lifting through binary operators:
        //
        //   LeftSemiJoin(outer,
        //     CrossProduct(
        //       Selection(local_order_filter, orders),
        //       Selection(correlated_shipment_filter, shipments)))
        //
        // Rewriting the correlated right sibling must not replace the unchanged
        // left Selection with its input scan; otherwise the local filter is
        // silently dropped. DataFusion SQL cannot currently express this exact
        // correlated-in-one-binary-child shape through the executable path, so
        // this test builds the IR directly.
        let mut query = QueryContext::new();
        let outer_key = ColumnData::new("outer_key", DataType::Int64).add(&mut query);
        let order_key = ColumnData::new("order_key", DataType::Int64).add(&mut query);
        let order_total = ColumnData::new("order_total", DataType::Int64).add(&mut query);
        let shipment_key = ColumnData::new("shipment_key", DataType::Int64).add(&mut query);

        let outer = OperatorData::Scan(Scan {
            table: TableRef::bare("outer_t"),
            columns: vec![outer_key],
        })
        .add(&mut query);
        let orders = OperatorData::Scan(Scan {
            table: TableRef::bare("orders"),
            columns: vec![order_key, order_total],
        })
        .add(&mut query);
        let local_predicate = ExprData::Binary {
            op: BinaryOp::Gt,
            left: ExprData::ColumnRef(order_total).add(&mut query),
            right: ExprData::Literal(ScalarValue::Int64(1000)).add(&mut query),
        }
        .add(&mut query);
        let filtered_orders = OperatorData::Selection(Selection {
            predicate: local_predicate,
            input: orders,
        })
        .add(&mut query);

        let shipments = OperatorData::Scan(Scan {
            table: TableRef::bare("shipments"),
            columns: vec![shipment_key],
        })
        .add(&mut query);
        let correlated_predicate = ExprData::Binary {
            op: BinaryOp::Eq,
            left: ExprData::ColumnRef(shipment_key).add(&mut query),
            right: ExprData::ColumnRef(outer_key).add(&mut query),
        }
        .add(&mut query);
        let filtered_shipments = OperatorData::Selection(Selection {
            predicate: correlated_predicate,
            input: shipments,
        })
        .add(&mut query);
        let inner = OperatorData::CrossProduct(crate::CrossProduct {
            outer: filtered_orders,
            inner: filtered_shipments,
        })
        .add(&mut query);
        let on = ExprData::Literal(ScalarValue::Boolean(true)).add(&mut query);
        let join = OperatorData::Join(Join {
            join_type: JoinType::LeftSemi,
            on,
            outer,
            inner,
        })
        .add(&mut query);
        query.set_root(join);

        let mut opt = crate::test_optimizer_context(query);
        let result = super::HolisticUnnesting.run(&mut opt).unwrap();
        assert_eq!(result, PassResult::Changed);

        let root = opt.query.root().unwrap();
        let OperatorData::Join(join) = root.get(&opt.query) else {
            panic!("root should be a join");
        };
        let OperatorData::CrossProduct(cross) = join.inner.get(&opt.query) else {
            panic!("inner should remain a cross product");
        };
        assert!(
            matches!(cross.outer.get(&opt.query), OperatorData::Selection(selection) if selection.input == orders),
            "non-correlated selection on the unchanged sibling must be preserved"
        );
    }

    #[test]
    fn does_not_lift_correlated_predicate_through_limit() {
        // LIMIT is intentionally a hard boundary for v1. Pulling
        // `inner_key = outer_key` above `LIMIT 1` changes which row is visible
        // for each outer binding; proper support needs the deferred
        // window-function rewrite instead.
        let mut query = QueryContext::new();
        let outer_key = ColumnData::new("outer_key", DataType::Int64).add(&mut query);
        let inner_key = ColumnData::new("inner_key", DataType::Int64).add(&mut query);

        let outer = OperatorData::Scan(Scan {
            table: TableRef::bare("outer_t"),
            columns: vec![outer_key],
        })
        .add(&mut query);
        let inner_scan = OperatorData::Scan(Scan {
            table: TableRef::bare("inner_t"),
            columns: vec![inner_key],
        })
        .add(&mut query);
        let predicate = ExprData::Binary {
            op: BinaryOp::Eq,
            left: ExprData::ColumnRef(inner_key).add(&mut query),
            right: ExprData::ColumnRef(outer_key).add(&mut query),
        }
        .add(&mut query);
        let selection = OperatorData::Selection(Selection {
            predicate,
            input: inner_scan,
        })
        .add(&mut query);
        let limit = OperatorData::Limit(Limit {
            fetch: Some(1),
            offset: 0,
            input: selection,
        })
        .add(&mut query);
        let on = ExprData::Literal(ScalarValue::Boolean(true)).add(&mut query);
        let join = OperatorData::Join(Join {
            join_type: JoinType::LeftSemi,
            on,
            outer,
            inner: limit,
        })
        .add(&mut query);
        query.set_root(join);

        let mut opt = crate::test_optimizer_context(query);
        let result = super::HolisticUnnesting.run(&mut opt).unwrap();
        assert_eq!(result, PassResult::Unchanged);
        assert_eq!(opt.query.root(), Some(join));
        assert_eq!(
            correlated_subquery_joins(&opt.query, join, &mut opt.analyses).unwrap()[0].free_columns,
            vec![outer_key]
        );
    }

    #[test]
    fn keeps_distinct_inner_key_handle_when_field_metadata_conflicts() {
        // Regression for self-correlations where outer and inner columns have
        // identical relation/name metadata but different handles, as in TPC-H
        // self-subqueries. The lifted join condition must use the inner handle
        // exposed by the aggregate, not accidentally match or reuse the outer
        // handle by display name.
        let mut query = QueryContext::new();
        let outer_key =
            ColumnData::with_qualifier("ps_partkey", DataType::Int64, "partsupp").add(&mut query);
        let inner_key =
            ColumnData::with_qualifier("ps_partkey", DataType::Int64, "partsupp").add(&mut query);
        let inner_value = ColumnData::with_qualifier("ps_supplycost", DataType::Int64, "partsupp")
            .add(&mut query);
        let min_value = ColumnData::new("min_value", DataType::Int64).add(&mut query);

        let outer = OperatorData::Scan(Scan {
            table: TableRef::bare("outer_partsupp"),
            columns: vec![outer_key],
        })
        .add(&mut query);
        let inner_scan = OperatorData::Scan(Scan {
            table: TableRef::bare("inner_partsupp"),
            columns: vec![inner_key, inner_value],
        })
        .add(&mut query);
        let predicate = ExprData::Binary {
            op: BinaryOp::Eq,
            left: ExprData::ColumnRef(outer_key).add(&mut query),
            right: ExprData::ColumnRef(inner_key).add(&mut query),
        }
        .add(&mut query);
        let inner_selection = OperatorData::Selection(Selection {
            predicate,
            input: inner_scan,
        })
        .add(&mut query);
        let aggregate = OperatorData::Aggregation(Aggregation {
            keys: vec![],
            aggregates: vec![(
                min_value,
                AggregateExpr::Func {
                    func: AggregateFunction::Min,
                    arg: ExprData::ColumnRef(inner_value).add(&mut query),
                    distinct: false,
                },
            )],
            input: inner_selection,
        })
        .add(&mut query);
        let on = ExprData::Literal(ScalarValue::Boolean(true)).add(&mut query);
        let join = OperatorData::Join(Join {
            join_type: JoinType::Single,
            on,
            outer,
            inner: aggregate,
        })
        .add(&mut query);
        query.set_root(join);

        let mut opt = crate::test_optimizer_context(query);
        let result = super::HolisticUnnesting.run(&mut opt).unwrap();
        assert_eq!(result, PassResult::Changed);

        let root = opt.query.root().unwrap();
        let OperatorData::Join(join) = root.get(&opt.query) else {
            panic!("root should be a join");
        };
        let inner_columns = opt
            .analyses
            .get::<AvailableColumns>(&opt.query, join.inner)
            .unwrap();
        assert_ne!(outer_key, inner_key);
        assert!(inner_columns.contains(&inner_key));
        assert!(matches!(
            join.on.get(&opt.query),
            ExprData::Binary { right, .. }
                if matches!(right.get(&opt.query), ExprData::ColumnRef(column) if *column == inner_key)
        ));
    }

    #[test]
    fn unnests_scalar_aggregate_through_map_and_projection_wrappers() {
        // Shape:
        //
        //   SingleJoin(outer,
        //     Projection(scaled_avg,
        //       Map(scaled_avg = 0.2 * avg,
        //         Aggregation([], avg(value),
        //           Selection(inner_key = outer_key, inner_scan)))))
        //
        // The aggregate grouping key introduced by lifting must survive the
        // map/projection wrappers. This mirrors TPC-H style scalar aggregate
        // expressions such as `0.2 * avg(...)`.
        let mut query = QueryContext::new();
        let outer_key = ColumnData::new("p_partkey", DataType::Int64).add(&mut query);
        let outer_quantity = ColumnData::new("l_quantity", DataType::Int64).add(&mut query);
        let inner_key = ColumnData::new("l_partkey", DataType::Int64).add(&mut query);
        let inner_quantity = ColumnData::new("l2_quantity", DataType::Int64).add(&mut query);
        let avg_quantity = ColumnData::new("avg_quantity", DataType::Float64).add(&mut query);
        let scaled_avg = ColumnData::new("scaled_avg", DataType::Float64).add(&mut query);

        let outer = OperatorData::Scan(Scan {
            table: TableRef::bare("joined"),
            columns: vec![outer_key, outer_quantity],
        })
        .add(&mut query);
        let inner_scan = OperatorData::Scan(Scan {
            table: TableRef::bare("lineitem"),
            columns: vec![inner_key, inner_quantity],
        })
        .add(&mut query);
        let predicate = ExprData::Binary {
            op: BinaryOp::Eq,
            left: ExprData::ColumnRef(inner_key).add(&mut query),
            right: ExprData::ColumnRef(outer_key).add(&mut query),
        }
        .add(&mut query);
        let inner_selection = OperatorData::Selection(Selection {
            predicate,
            input: inner_scan,
        })
        .add(&mut query);
        let aggregate = OperatorData::Aggregation(Aggregation {
            keys: vec![],
            aggregates: vec![(
                avg_quantity,
                AggregateExpr::Func {
                    func: AggregateFunction::Avg,
                    arg: ExprData::ColumnRef(inner_quantity).add(&mut query),
                    distinct: false,
                },
            )],
            input: inner_selection,
        })
        .add(&mut query);
        let scaled_expr = ExprData::Binary {
            op: BinaryOp::Multiply,
            left: ExprData::Literal(ScalarValue::Float64(0.2)).add(&mut query),
            right: ExprData::ColumnRef(avg_quantity).add(&mut query),
        }
        .add(&mut query);
        let map = OperatorData::Map(crate::Map {
            computations: vec![(scaled_avg, scaled_expr)],
            input: aggregate,
        })
        .add(&mut query);
        let projection = OperatorData::Projection(crate::Projection {
            columns: vec![scaled_avg],
            input: map,
        })
        .add(&mut query);
        let on = ExprData::Literal(ScalarValue::Boolean(true)).add(&mut query);
        let join = OperatorData::Join(Join {
            join_type: JoinType::Single,
            on,
            outer,
            inner: projection,
        })
        .add(&mut query);
        query.set_root(join);

        let mut opt = crate::test_optimizer_context(query);
        let result = super::HolisticUnnesting.run(&mut opt).unwrap();
        assert_eq!(result, PassResult::Changed);

        let root = opt.query.root().unwrap();
        let OperatorData::Join(join) = root.get(&opt.query) else {
            panic!("root should be a join");
        };
        assert_eq!(join.join_type, JoinType::LeftOuter);
        assert!(
            opt.analyses
                .get::<FreeColumns>(&opt.query, join.inner)
                .unwrap()
                .is_empty()
        );
        let inner_columns = opt
            .analyses
            .get::<AvailableColumns>(&opt.query, join.inner)
            .unwrap();
        assert!(inner_columns.contains(&inner_key));
    }

    #[test]
    fn unnests_mark_join_with_renamed_inner_then_mark_to_semi_can_fire() {
        // Mark-join pipeline regression:
        //
        //   Selection(marker,
        //     LeftMarkJoin(outer,
        //       Selection(l2.orderkey = outer.orderkey AND l2.suppkey <> outer.suppkey,
        //         Rename(l2, lineitem))))
        //
        // HolisticUnnesting must preserve renamed inner handles while lifting
        // both predicates into the mark join. That leaves a decorrelated shape
        // where the following MarkJoinToSemiJoin pass can convert the marker
        // filter into a regular semi join.
        let mut query = QueryContext::new();
        let outer_orderkey = ColumnData::new("l_orderkey", DataType::Int64).add(&mut query);
        let outer_suppkey = ColumnData::new("l_suppkey", DataType::Int64).add(&mut query);
        let base_orderkey = ColumnData::new("l_orderkey", DataType::Int64).add(&mut query);
        let base_suppkey = ColumnData::new("l_suppkey", DataType::Int64).add(&mut query);
        let inner_orderkey =
            ColumnData::with_qualifier("l_orderkey", DataType::Int64, "l2").add(&mut query);
        let inner_suppkey =
            ColumnData::with_qualifier("l_suppkey", DataType::Int64, "l2").add(&mut query);
        let marker = ColumnData::new("exists_mark", DataType::Boolean).add(&mut query);

        let outer = OperatorData::Scan(Scan {
            table: TableRef::bare("outer_lineitem"),
            columns: vec![outer_orderkey, outer_suppkey],
        })
        .add(&mut query);
        let inner_scan = OperatorData::Scan(Scan {
            table: TableRef::bare("lineitem"),
            columns: vec![base_orderkey, base_suppkey],
        })
        .add(&mut query);
        let rename = OperatorData::Rename(Rename {
            alias: "l2".to_string(),
            defs: vec![
                (inner_orderkey, base_orderkey),
                (inner_suppkey, base_suppkey),
            ],
            input: inner_scan,
        })
        .add(&mut query);
        let same_order = ExprData::Binary {
            op: BinaryOp::Eq,
            left: ExprData::ColumnRef(inner_orderkey).add(&mut query),
            right: ExprData::ColumnRef(outer_orderkey).add(&mut query),
        }
        .add(&mut query);
        let different_supplier = ExprData::Binary {
            op: BinaryOp::NotEq,
            left: ExprData::ColumnRef(inner_suppkey).add(&mut query),
            right: ExprData::ColumnRef(outer_suppkey).add(&mut query),
        }
        .add(&mut query);
        let predicate = ExprData::Nary {
            op: NaryOp::And,
            exprs: vec![same_order, different_supplier],
        }
        .add(&mut query);
        let inner = OperatorData::Selection(Selection {
            predicate,
            input: rename,
        })
        .add(&mut query);
        let on = ExprData::Literal(ScalarValue::Boolean(true)).add(&mut query);
        let mark_join = OperatorData::Join(Join {
            join_type: JoinType::LeftMark {
                marker,
                nullable: false,
            },
            on,
            outer,
            inner,
        })
        .add(&mut query);
        let marker_expr = ExprData::ColumnRef(marker).add(&mut query);
        let selection = OperatorData::Selection(Selection {
            predicate: marker_expr,
            input: mark_join,
        })
        .add(&mut query);
        query.set_root(selection);

        let mut opt = crate::test_optimizer_context(query);
        let mut pm = PassManager::new();
        pm.add_pass(super::HolisticUnnesting);
        pm.add_pass(OperatorRewriteAdaptor::new(crate::MarkJoinToSemiJoin));
        pm.run(&mut opt).unwrap();

        let root = opt.query.root().unwrap();
        let OperatorData::Join(join) = root.get(&opt.query) else {
            panic!("root should become a join");
        };
        assert_eq!(join.join_type, JoinType::LeftSemi);
        assert!(matches!(join.on.get(&opt.query), ExprData::Nary { .. }));
        assert!(
            opt.analyses
                .get::<FreeColumns>(&opt.query, join.inner)
                .unwrap()
                .is_empty()
        );
    }
}
