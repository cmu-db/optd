use crate::ir::{
    cost::{Cost, CostModel},
    operator::*,
    properties::{Cardinality, GetProperty},
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
            OperatorKind::EnforcerSort(_) => {
                let input_card = op.input_operators()[0].get_property::<Cardinality>(ctx);
                let cost = Cost::UNIT * input_card.as_f64() * input_card.as_f64().ln_1p().max(1.0);
                Some(cost)
            }
            OperatorKind::PhysicalTableScan(_) => {
                let card = op.get_property::<Cardinality>(ctx);
                Some(Cost::UNIT * card * 2f64)
            }
            OperatorKind::PhysicalNLJoin(meta) => {
                let join = PhysicalNLJoinBorrowed::from_raw_parts(meta, &op.common);
                let outer_card = join.outer().get_property::<Cardinality>(ctx);
                let inner_card = join.inner().get_property::<Cardinality>(ctx);
                let cost = outer_card.as_f64()
                    * inner_card.as_f64()
                    * MagicCostModel::MAGIC_COMPUTATION_FACTOR
                    * Cost::UNIT
                    + outer_card * Cost::UNIT;
                Some(cost)
            }
            OperatorKind::PhysicalFilter(meta) => {
                let filter = PhysicalFilterBorrowed::from_raw_parts(meta, &op.common);
                let input_card = filter.input().get_property::<Cardinality>(ctx);
                let cost = input_card.as_f64() * Self::MAGIC_COMPUTATION_FACTOR * Cost::UNIT;
                Some(cost)
            }
            #[cfg(test)]
            OperatorKind::MockScan(meta) => meta.spec.mocked_operator_cost,
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

        let m1 = {
            let spec = MockSpec::new_test_only(vec![0, 1], 100.);
            MockScan::with_mock_spec(1, spec).into_operator()
        };

        let m2 = {
            let spec = MockSpec::new_test_only(vec![2, 3], 100.);
            MockScan::with_mock_spec(2, spec).into_operator()
        };

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
        println!("cost1: {:?}, cost2: {:?}", op1_cost, op2_cost);
        assert!(op1_cost > op2_cost);
    }
}
