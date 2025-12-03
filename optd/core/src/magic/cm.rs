use crate::ir::{
    cost::{Cost, CostModel},
    operator::*,
    scalar::List,
};

pub struct MagicCostModel;

impl MagicCostModel {
    const MAGIC_COMPUTATION_FACTOR: f64 = 5f64;
}

impl CostModel for MagicCostModel {
    fn compute_operator_cost(
        &self,
        op: &crate::ir::Operator,
        ctx: &crate::ir::IRContext,
    ) -> Option<crate::ir::cost::Cost> {
        use crate::ir::OperatorKind;
        match &op.kind {
            OperatorKind::Group(_) => None,
            OperatorKind::LogicalGet(_) => None,
            OperatorKind::LogicalJoin(_) => None,
            OperatorKind::LogicalSelect(_) => None,
            OperatorKind::LogicalProject(_) => None,
            OperatorKind::LogicalAggregate(_) => None,
            OperatorKind::LogicalOrderBy(_) => None,
            OperatorKind::LogicalRemap(_) => Some(Cost::UNIT),
            OperatorKind::EnforcerSort(_) => {
                let input_card = op.input_operators()[0].cardinality(ctx);
                let cost = Cost::UNIT * input_card.as_f64() * input_card.as_f64().ln_1p().max(1.0);
                Some(cost)
            }
            OperatorKind::PhysicalTableScan(_) => {
                let card = op.cardinality(ctx);
                Some(Cost::UNIT * card * 2f64)
            }
            OperatorKind::PhysicalNLJoin(meta) => {
                let join = PhysicalNLJoin::borrow_raw_parts(meta, &op.common);
                let outer_card = join.outer().cardinality(ctx);
                let inner_card = join.inner().cardinality(ctx);
                let cost = outer_card.as_f64()
                    * inner_card.as_f64()
                    * MagicCostModel::MAGIC_COMPUTATION_FACTOR
                    * Cost::UNIT
                    + outer_card * Cost::UNIT;
                Some(cost)
            }
            OperatorKind::PhysicalHashJoin(meta) => {
                let join = PhysicalHashJoin::borrow_raw_parts(meta, &op.common);
                let build_card = join.build_side().cardinality(ctx);
                let probe_card = join.probe_side().cardinality(ctx);
                let cost = (build_card.as_f64() * 2. + probe_card.as_f64()) * Cost::UNIT;
                Some(cost)
            }
            OperatorKind::PhysicalFilter(meta) => {
                let filter = PhysicalFilter::borrow_raw_parts(meta, &op.common);
                let input_card = filter.input().cardinality(ctx);
                let cost = input_card.as_f64() * Self::MAGIC_COMPUTATION_FACTOR * Cost::UNIT;
                Some(cost)
            }
            OperatorKind::PhysicalProject(meta) => {
                let project = PhysicalProject::borrow_raw_parts(meta, &op.common);
                let input_card = project.input().cardinality(ctx);
                let cost = input_card.as_f64() * Self::MAGIC_COMPUTATION_FACTOR * Cost::UNIT;
                Some(cost)
            }
            OperatorKind::MockScan(meta) => meta.spec.mocked_operator_cost,
            OperatorKind::PhysicalHashAggregate(meta) => {
                let agg = PhysicalHashAggregate::borrow_raw_parts(meta, &op.common);
                let num_exprs = agg.exprs().borrow::<List>().members().len();
                let input_card = agg.input().cardinality(ctx);
                let cost = input_card.as_f64()
                    * num_exprs as f64
                    * Self::MAGIC_COMPUTATION_FACTOR
                    * Cost::UNIT;
                Some(cost)
            }
            OperatorKind::LogicalLimit(logical_limit_metadata) => {
                let limit = LogicalLimit::borrow_raw_parts(logical_limit_metadata, &op.common)
                    .limit()
                    .clone() as f64;
                let cost = limit * Self::MAGIC_COMPUTATION_FACTOR * Cost::UNIT;
                Some(cost)
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::ir::{
        convert::{IntoOperator, IntoScalar},
        operator::{join::JoinType, *},
        properties::{TupleOrdering, TupleOrderingDirection},
        scalar::Literal,
        *,
    };

    #[test]
    fn sort_at_lower_cardinality_is_cheaper() {
        let ctx = IRContext::with_empty_magic();

        let m1 = ctx.mock_scan(1, vec![0, 1], 100.);

        let m2 = ctx.mock_scan(1, vec![2, 3], 100.);

        let join_cond = Literal::new(ScalarValue::Boolean(Some(true))).into_scalar();

        let tuple_ordering = TupleOrdering::from_iter([(Column(0), TupleOrderingDirection::Asc)]);

        // plan with sort on top.
        let op1 = EnforcerSort::new(
            tuple_ordering.clone(),
            PhysicalNLJoin::new(JoinType::Inner, m1.clone(), m2.clone(), join_cond.clone())
                .into_operator(),
        )
        .into_operator();

        // plan with sort passed down.
        let op2 = PhysicalNLJoin::new(
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
}
