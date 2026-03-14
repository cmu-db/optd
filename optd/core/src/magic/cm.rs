use crate::ir::{
    cost::{Cost, CostModel},
    operator::*,
    scalar::List,
};
use snafu::whatever;

pub struct MagicCostModel;

impl MagicCostModel {
    const MAGIC_COMPUTATION_FACTOR: f64 = 5f64;

    fn sort_cost(input_card: crate::ir::properties::Cardinality) -> Cost {
        Cost::UNIT * input_card.as_f64() * input_card.as_f64().ln_1p().max(1.0)
    }

    fn scan_cost(card: crate::ir::properties::Cardinality) -> Cost {
        Cost::UNIT * card * 2f64
    }

    fn nl_join_cost(
        outer_card: crate::ir::properties::Cardinality,
        inner_card: crate::ir::properties::Cardinality,
    ) -> Cost {
        outer_card.as_f64()
            * inner_card.as_f64()
            * MagicCostModel::MAGIC_COMPUTATION_FACTOR
            * Cost::UNIT
            + outer_card * Cost::UNIT
    }

    fn filter_cost(input_card: crate::ir::properties::Cardinality) -> Cost {
        input_card.as_f64() * Self::MAGIC_COMPUTATION_FACTOR * Cost::UNIT
    }

    fn project_cost(input_card: crate::ir::properties::Cardinality) -> Cost {
        input_card.as_f64() * Self::MAGIC_COMPUTATION_FACTOR * Cost::UNIT
    }

    fn hash_aggregate_cost(
        input_card: crate::ir::properties::Cardinality,
        num_exprs: usize,
    ) -> Cost {
        input_card.as_f64() * num_exprs as f64 * Self::MAGIC_COMPUTATION_FACTOR * Cost::UNIT
    }
}

impl CostModel for MagicCostModel {
    fn compute_operator_cost(
        &self,
        op: &crate::ir::Operator,
        ctx: &crate::ir::IRContext,
    ) -> crate::error::Result<crate::ir::cost::Cost> {
        use crate::ir::OperatorKind;
        match &op.kind {
            OperatorKind::Group(_) => whatever!("cannot compute cost for Group operator"),
            OperatorKind::Get(_) => {
                let card = op.cardinality(ctx);
                Ok(Self::scan_cost(card))
            }
            OperatorKind::Join(meta) => {
                let join = Join::borrow_raw_parts(meta, &op.common);
                match join.implementation() {
                    Some(JoinImplementation::Hash(_)) => {
                        let build_card = join.build_side().unwrap().cardinality(ctx);
                        let probe_card = join.probe_side().unwrap().cardinality(ctx);
                        let cost = (build_card.as_f64() * 2. + probe_card.as_f64()) * Cost::UNIT;
                        Ok(cost)
                    }
                    _ => {
                        let outer_card = join.outer().cardinality(ctx);
                        let inner_card = join.inner().cardinality(ctx);
                        Ok(Self::nl_join_cost(outer_card, inner_card))
                    }
                }
            }
            OperatorKind::LogicalDependentJoin(meta) => {
                let join = LogicalDependentJoin::borrow_raw_parts(meta, &op.common);
                let outer_card = join.outer().cardinality(ctx);
                let inner_card = join.inner().cardinality(ctx);
                Ok(Self::nl_join_cost(outer_card, inner_card))
            }
            OperatorKind::Select(meta) => {
                let filter = Select::borrow_raw_parts(meta, &op.common);
                let input_card = filter.input().cardinality(ctx);
                Ok(Self::filter_cost(input_card))
            }
            OperatorKind::LogicalLimit(_meta) => Ok(Cost::ZERO),
            OperatorKind::Project(meta) => {
                let project = Project::borrow_raw_parts(meta, &op.common);
                let input_card = project.input().cardinality(ctx);
                Ok(Self::project_cost(input_card))
            }
            OperatorKind::Aggregate(meta) => match meta.implementation {
                None | Some(AggregateImplementation::Hash) => {
                    let agg = Aggregate::borrow_raw_parts(meta, &op.common);
                    let num_exprs = agg.exprs().borrow::<List>().members().len();
                    let input_card = agg.input().cardinality(ctx);
                    Ok(Self::hash_aggregate_cost(input_card, num_exprs))
                }
            },
            OperatorKind::LogicalOrderBy(meta) => {
                let order_by = LogicalOrderBy::borrow_raw_parts(meta, &op.common);
                let input_card = order_by.input().cardinality(ctx);
                Ok(Self::sort_cost(input_card))
            }
            OperatorKind::LogicalSubquery(_) => Ok(Cost::ZERO),
            OperatorKind::LogicalRemap(_) => Ok(Cost::ZERO),
            OperatorKind::EnforcerSort(_) => {
                let input_card = op.input_operators()[0].cardinality(ctx);
                Ok(Self::sort_cost(input_card))
            }
            OperatorKind::MockScan(meta) => {
                meta.spec
                    .mocked_operator_cost
                    .ok_or_else(|| crate::error::Error::Whatever {
                        message: "mock scan is missing operator cost".into(),
                        source: None,
                    })
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::ir::{
        builder::{boolean, column_ref, int64},
        convert::{IntoOperator, IntoScalar},
        cost::Cost,
        operator::{join::JoinType, *},
        properties::{TupleOrdering, TupleOrderingDirection},
        scalar::{List, Literal},
        *,
    };

    #[test]
    fn sort_at_lower_cardinality_is_cheaper() {
        let ctx = IRContext::with_empty_magic();

        let m1 = ctx.mock_scan(1, 2, 100.);

        let m2 = ctx.mock_scan(1, 2, 100.);

        let join_cond = Literal::new(ScalarValue::Boolean(Some(true))).into_scalar();

        let tuple_ordering =
            TupleOrdering::from_iter([(Column(1, 0), TupleOrderingDirection::Asc)]);

        // plan with sort on top.
        let op1 = EnforcerSort::new(
            tuple_ordering.clone(),
            Join::nested_loop(JoinType::Inner, m1.clone(), m2.clone(), join_cond.clone())
                .into_operator(),
        )
        .into_operator();

        // plan with sort passed down.
        let op2 = Join::nested_loop(
            JoinType::Inner,
            EnforcerSort::new(tuple_ordering, m1).into_operator(),
            m2,
            join_cond.clone(),
        )
        .into_operator();

        let op1_cost = ctx.cm.compute_total_cost(&op1, &ctx).unwrap();
        let op2_cost = ctx.cm.compute_total_cost(&op2, &ctx).unwrap();
        println!("cost1: {op1_cost:?}, cost2: {op2_cost:?}");
        assert!(op1_cost > op2_cost);
    }

    #[test]
    fn logical_cost_matches_physical_counterparts() {
        let ctx = IRContext::with_empty_magic();

        let schema = catalog::Schema::new(vec![
            catalog::Field::new("t1.c0", DataType::Int32, false),
            catalog::Field::new("t1.c1", DataType::Int32, false),
        ]);
        let logical_get = ctx.logical_get(1, &schema, None);
        let physical_scan = ctx.table_scan(1, &schema, None);
        assert_eq!(
            ctx.cm.compute_operator_cost(&logical_get, &ctx).unwrap(),
            ctx.cm.compute_operator_cost(&physical_scan, &ctx).unwrap()
        );

        let outer = ctx.mock_scan(1, 2, 100.);
        let inner = ctx.mock_scan(2, 2, 50.);
        let join_cond = Literal::new(ScalarValue::Boolean(Some(true))).into_scalar();
        let logical_join = Join::logical(
            JoinType::Inner,
            outer.clone(),
            inner.clone(),
            join_cond.clone(),
        )
        .into_operator();
        let physical_join = Join::nested_loop(
            JoinType::Inner,
            outer.clone(),
            inner.clone(),
            join_cond.clone(),
        )
        .into_operator();
        assert_eq!(
            ctx.cm.compute_operator_cost(&logical_join, &ctx).unwrap(),
            ctx.cm.compute_operator_cost(&physical_join, &ctx).unwrap()
        );

        let logical_dep_join = LogicalDependentJoin::new(
            JoinType::Inner,
            outer.clone(),
            inner.clone(),
            join_cond.clone(),
        )
        .into_operator();
        assert_eq!(
            ctx.cm
                .compute_operator_cost(&logical_dep_join, &ctx)
                .unwrap(),
            ctx.cm.compute_operator_cost(&physical_join, &ctx).unwrap()
        );

        let logical_select = Select::new(outer.clone(), join_cond.clone()).into_operator();
        let physical_filter = Select::new(outer.clone(), join_cond.clone()).into_operator();
        assert_eq!(
            ctx.cm.compute_operator_cost(&logical_select, &ctx).unwrap(),
            ctx.cm
                .compute_operator_cost(&physical_filter, &ctx)
                .unwrap()
        );

        let projections =
            List::new(vec![column_ref(Column(1, 0)), column_ref(Column(1, 1))].into())
                .into_scalar();
        let logical_project = Project::new(3, outer.clone(), projections.clone()).into_operator();
        let physical_project = Project::new(3, outer.clone(), projections.clone()).into_operator();
        assert_eq!(
            ctx.cm
                .compute_operator_cost(&logical_project, &ctx)
                .unwrap(),
            ctx.cm
                .compute_operator_cost(&physical_project, &ctx)
                .unwrap()
        );

        let exprs = List::new(vec![column_ref(Column(1, 1))].into()).into_scalar();
        let keys = List::new(vec![column_ref(Column(1, 0))].into()).into_scalar();
        let logical_agg =
            Aggregate::logical(4, outer.clone(), exprs.clone(), keys.clone()).into_operator();
        let physical_agg = Aggregate::hash(4, outer.clone(), exprs, keys).into_operator();
        assert_eq!(
            ctx.cm.compute_operator_cost(&logical_agg, &ctx).unwrap(),
            ctx.cm.compute_operator_cost(&physical_agg, &ctx).unwrap()
        );

        let ordering = TupleOrdering::from_iter([(Column(1, 0), TupleOrderingDirection::Asc)]);
        let logical_order_by = LogicalOrderBy::new(
            outer.clone(),
            vec![(column_ref(Column(1, 0)), TupleOrderingDirection::Asc)],
        )
        .into_operator();
        let enforcer_sort = EnforcerSort::new(ordering, outer).into_operator();
        assert_eq!(
            ctx.cm
                .compute_operator_cost(&logical_order_by, &ctx)
                .unwrap(),
            ctx.cm.compute_operator_cost(&enforcer_sort, &ctx).unwrap()
        );
    }

    #[test]
    fn remaining_logical_operators_have_costs() {
        let ctx = IRContext::with_empty_magic();
        let input = ctx.mock_scan(1, 2, 100.);

        let limit = LogicalLimit::new(input.clone(), int64(10), int64(20)).into_operator();
        let subquery = LogicalSubquery::new(input.clone()).into_operator();
        let remap = LogicalRemap::new(2, input).into_operator();

        assert_eq!(
            ctx.cm.compute_operator_cost(&limit, &ctx).unwrap(),
            Cost::ZERO
        );
        assert_eq!(
            ctx.cm.compute_operator_cost(&subquery, &ctx).unwrap(),
            Cost::ZERO
        );
        assert_eq!(
            ctx.cm.compute_operator_cost(&remap, &ctx).unwrap(),
            Cost::ZERO
        );
        assert!(
            ctx.cm
                .compute_operator_cost(
                    &Select::new(ctx.mock_scan(3, 1, 10.), boolean(true)).into_operator(),
                    &ctx
                )
                .is_ok()
        );
    }
}
