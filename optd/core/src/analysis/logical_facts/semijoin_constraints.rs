//! Exact preserved-side consequences of a successful semi join.
//!
//! This imports only logical predicates that hold for every RHS row, not estimated
//! domains, row counts, or join selectivity. Anti/mark/outer joins must not use it.

use crate::{BinaryOp, Expr, ExprData, NaryOp, QueryContext, ScalarValue};

use super::LogicalFacts;

pub(super) fn semi_join_facts(
    left: &LogicalFacts,
    right: &LogicalFacts,
    on: Expr,
    ctx: &QueryContext,
) -> LogicalFacts {
    let mut facts = left.clone();
    facts.constraints.contradictory |= right.constraints.contradictory;
    transfer_conjunct(&mut facts, right, on, ctx);
    facts.constraints.normalize();
    facts
}

fn transfer_conjunct(facts: &mut LogicalFacts, right: &LogicalFacts, on: Expr, ctx: &QueryContext) {
    match on.get(ctx) {
        ExprData::Nary {
            op: NaryOp::And,
            exprs,
        } => {
            for expr in exprs {
                transfer_conjunct(facts, right, *expr, ctx);
            }
        }
        ExprData::Literal(ScalarValue::Boolean(false) | ScalarValue::Null(_)) => {
            facts.constraints.contradictory = true;
        }
        ExprData::Binary {
            op: BinaryOp::Eq | BinaryOp::IsNotDistinctFrom,
            left,
            right: rhs,
        } => {
            let (ExprData::ColumnRef(a), ExprData::ColumnRef(b)) = (left.get(ctx), rhs.get(ctx))
            else {
                return;
            };
            // Require unambiguous input ownership, including for self-join aliases.
            let pair = if facts.lineage.contains_key(a)
                && !right.lineage.contains_key(a)
                && right.lineage.contains_key(b)
                && !facts.lineage.contains_key(b)
            {
                Some((*a, *b))
            } else if facts.lineage.contains_key(b)
                && !right.lineage.contains_key(b)
                && right.lineage.contains_key(a)
                && !facts.lineage.contains_key(a)
            {
                Some((*b, *a))
            } else {
                None
            };
            let Some((left_column, right_column)) = pair else {
                return;
            };
            let left_value = facts.lineage[&left_column].clone();
            let right_value = right.constraints.canonical(&right.lineage[&right_column]);
            let ordinary = matches!(
                on.get(ctx),
                ExprData::Binary {
                    op: BinaryOp::Eq,
                    ..
                }
            );
            if ordinary || right.constraints.non_null_values.contains(&right_value) {
                facts
                    .constraints
                    .non_null_values
                    .insert(facts.constraints.canonical(&left_value));
            }
            // Cross-type comparisons can involve coercion. Do not transfer a literal or
            // numeric interval without an exact type match (or a future coercion proof).
            if ctx.column(left_column).ty != ctx.column(right_column).ty {
                return;
            }
            if let Some(literal) = right.constraints.literal_equalities.get(&right_value)
                && literal.data_type() == ctx.column(left_column).ty
                && !matches!(literal, ScalarValue::Null(_))
                && !matches!(literal, ScalarValue::Float64(value) if !value.is_finite())
            {
                facts
                    .constraints
                    .add_literal(left_value.clone(), literal.clone());
            }
            if let Some(range) = right.constraints.ranges.get(&right_value) {
                let canonical = facts.constraints.canonical(&left_value);
                let target = facts.constraints.ranges.entry(canonical).or_default();
                if let Some((value, inclusive)) = range.lower {
                    target.tighten_lower(value, inclusive);
                }
                if let Some((value, inclusive)) = range.upper {
                    target.tighten_upper(value, inclusive);
                }
            }
        }
        // OR, casts, computed equalities, and arbitrary predicates need their own proof.
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Column, ColumnData};
    use arrow_schema::DataType;

    struct Fixture {
        ctx: QueryContext,
        left: LogicalFacts,
        right: LogicalFacts,
        left_key: Column,
        left_ref: Expr,
        right_ref: Expr,
    }

    impl Fixture {
        fn new(left_type: DataType, right_type: DataType) -> Self {
            let mut ctx = QueryContext::new();
            let left_key = ColumnData::new("left", left_type).add(&mut ctx);
            let right_key = ColumnData::new("right", right_type).add(&mut ctx);
            let left_ref = ExprData::ColumnRef(left_key).add(&mut ctx);
            let right_ref = ExprData::ColumnRef(right_key).add(&mut ctx);
            let left = LogicalFacts {
                lineage: [(left_key, super::super::ValueId::Derived(left_key))]
                    .into_iter()
                    .collect(),
                ..LogicalFacts::default()
            };
            let right = LogicalFacts {
                lineage: [(right_key, super::super::ValueId::Derived(right_key))]
                    .into_iter()
                    .collect(),
                ..LogicalFacts::default()
            };
            Self {
                ctx,
                left,
                right,
                left_key,
                left_ref,
                right_ref,
            }
        }

        fn constrain_right(&mut self, op: BinaryOp, literal: ScalarValue) {
            let right = ExprData::Literal(literal).add(&mut self.ctx);
            let predicate = ExprData::Binary {
                op,
                left: self.right_ref,
                right,
            }
            .add(&mut self.ctx);
            self.right.apply_predicate(predicate, &self.ctx);
        }

        fn on(&mut self, op: BinaryOp) -> Expr {
            ExprData::Binary {
                op,
                left: self.left_ref,
                right: self.right_ref,
            }
            .add(&mut self.ctx)
        }

        fn derive(&self, on: Expr) -> LogicalFacts {
            semi_join_facts(&self.left, &self.right, on, &self.ctx)
        }
    }

    #[test]
    fn transfers_conjunctive_ranges_and_rejects_conflicting_left_constraints() {
        let mut f = Fixture::new(DataType::Int64, DataType::Int64);
        f.constrain_right(BinaryOp::Gt, ScalarValue::Int64(5));
        f.constrain_right(BinaryOp::LtEq, ScalarValue::Int64(9));
        let equality = f.on(BinaryOp::Eq);
        let on = ExprData::Nary {
            op: NaryOp::And,
            exprs: vec![
                equality,
                ExprData::Literal(ScalarValue::Boolean(true)).add(&mut f.ctx),
            ],
        }
        .add(&mut f.ctx);
        let facts = f.derive(on);
        let key = facts.canonical_value_for_column(f.left_key).unwrap();
        assert_eq!(facts.constraints.ranges[&key].lower, Some((5.0, false)));
        assert_eq!(facts.constraints.ranges[&key].upper, Some((9.0, true)));
        assert!(facts.constraints.non_null_values.contains(&key));
        assert_eq!(facts.lineage.len(), 1);
        // Intersect with an existing left restriction; no matching row can survive.
        f.left.constraints.add_literal(key, ScalarValue::Int64(2));
        assert!(f.derive(on).constraints.contradictory);
    }

    #[test]
    fn non_null_consequences_distinguish_equality_from_null_safe_equality() {
        let mut f = Fixture::new(DataType::Int64, DataType::Int64);
        let null_safe = f.on(BinaryOp::IsNotDistinctFrom);
        let ordinary = f.on(BinaryOp::Eq);
        assert!(f.derive(null_safe).constraints.non_null_values.is_empty());
        assert_eq!(f.derive(ordinary).constraints.non_null_values.len(), 1);
        f.constrain_right(BinaryOp::Gt, ScalarValue::Int64(0));
        assert_eq!(f.derive(null_safe).constraints.non_null_values.len(), 1);
    }

    #[test]
    fn refuses_or_and_computed_join_keys() {
        let mut f = Fixture::new(DataType::Int64, DataType::Int64);
        f.constrain_right(BinaryOp::Eq, ScalarValue::Int64(5));
        let equality = f.on(BinaryOp::Eq);
        let on = ExprData::Nary {
            op: NaryOp::Or,
            exprs: vec![
                equality,
                ExprData::Literal(ScalarValue::Boolean(true)).add(&mut f.ctx),
            ],
        }
        .add(&mut f.ctx);
        assert!(f.derive(on).constraints.literal_equalities.is_empty());
        let cast = ExprData::Cast {
            expr: f.left_ref,
            ty: DataType::Int64,
        }
        .add(&mut f.ctx);
        let computed = ExprData::Binary {
            op: BinaryOp::Eq,
            left: cast,
            right: f.right_ref,
        }
        .add(&mut f.ctx);
        assert!(f.derive(computed).constraints.literal_equalities.is_empty());
    }

    #[test]
    fn refuses_cross_type_and_non_finite_literal_transfer() {
        for (left_type, right_type, value) in [
            (
                DataType::Int64,
                DataType::Float64,
                ScalarValue::Float64(5.0),
            ),
            (DataType::Float64, DataType::Float64, ScalarValue::Int64(5)),
            (
                DataType::Float64,
                DataType::Float64,
                ScalarValue::Float64(f64::NAN),
            ),
        ] {
            let mut f = Fixture::new(left_type, right_type);
            f.constrain_right(BinaryOp::Eq, value);
            let on = f.on(BinaryOp::Eq);
            assert!(f.derive(on).constraints.literal_equalities.is_empty());
        }
    }

    #[test]
    fn empty_rhs_or_false_join_is_a_logical_contradiction() {
        let mut f = Fixture::new(DataType::Int64, DataType::Int64);
        let on = f.on(BinaryOp::Eq);
        f.right.constraints.contradictory = true;
        assert!(f.derive(on).constraints.contradictory);
        f.right.constraints.contradictory = false;
        for value in [
            ScalarValue::Boolean(false),
            ScalarValue::Null(DataType::Boolean),
        ] {
            let on = ExprData::Literal(value).add(&mut f.ctx);
            assert!(f.derive(on).constraints.contradictory);
        }
    }
}
