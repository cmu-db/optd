use serde::{de::DeserializeOwned, Serialize};

use crate::{
    cost::{
        base_cost::{
            stats::{Distribution, MostCommonValues},
            UNIMPLEMENTED_SEL,
        },
        OptCostModel,
    },
    plan_nodes::{
        OptRelNode, OptRelNodeTyp, PhysicalColumnRefExpr, PhysicalConstantExpr, PhysicalInListExpr,
    },
    properties::column_ref::{BaseTableColumnRef, BaseTableColumnRefs, ColumnRef},
};

impl<
        M: MostCommonValues + Serialize + DeserializeOwned,
        D: Distribution + Serialize + DeserializeOwned,
    > OptCostModel<M, D>
{
    /// Only support colA in (val1, val2, val3) where colA is a column ref and
    /// val1, val2, val3 are constants.
    pub(super) fn get_in_list_selectivity(
        &self,
        expr: &PhysicalInListExpr,
        column_refs: &BaseTableColumnRefs,
    ) -> f64 {
        let child = expr.child();

        // Check child is a column ref.
        if !matches!(child.typ(), OptRelNodeTyp::PhysicalColumnRef) {
            return UNIMPLEMENTED_SEL;
        }

        // Check all expressions in the list are constants.
        let list_exprs = expr.list().to_vec();
        if list_exprs
            .iter()
            .any(|expr| !matches!(expr.typ(), OptRelNodeTyp::PhysicalConstant(_)))
        {
            return UNIMPLEMENTED_SEL;
        }

        // Convert child and const expressions to concrete types.
        let col_ref_idx = PhysicalColumnRefExpr::from_rel_node(child.into_rel_node())
            .unwrap()
            .index();
        let list_exprs = list_exprs
            .into_iter()
            .map(|expr| {
                PhysicalConstantExpr::from_rel_node(expr.into_rel_node())
                    .expect("we already checked all list elements are constants")
            })
            .collect::<Vec<_>>();
        let negated = expr.negated();

        if let ColumnRef::BaseTableColumnRef(BaseTableColumnRef { table, col_idx }) =
            &column_refs[col_ref_idx]
        {
            let in_sel = list_exprs
                .iter()
                .map(|expr| {
                    self.get_column_equality_selectivity(table, *col_idx, &expr.value(), true)
                })
                .sum::<f64>()
                .min(1.0);
            if negated {
                1.0 - in_sel
            } else {
                in_sel
            }
        } else {
            // Child is a derived column.
            UNIMPLEMENTED_SEL
        }
    }
}

#[cfg(test)]
mod tests {
    use optd_core::rel_node::Value;

    use crate::{
        cost::base_cost::tests::{
            create_one_column_cost_model, in_list, TestDistribution, TestMostCommonValues,
            TestPerColumnStats, TABLE1_NAME,
        },
        properties::column_ref::ColumnRef,
    };

    #[test]
    fn test_in_list() {
        let cost_model = create_one_column_cost_model(TestPerColumnStats::new(
            TestMostCommonValues::new(vec![(Value::Int32(1), 0.8), (Value::Int32(2), 0.2)]),
            2,
            0.0,
            Some(TestDistribution::empty()),
        ));
        let column_refs = vec![ColumnRef::base_table_column_ref(
            String::from(TABLE1_NAME),
            0,
        )];
        assert_approx_eq::assert_approx_eq!(
            cost_model
                .get_in_list_selectivity(&in_list(0, vec![Value::Int32(1)], false), &column_refs),
            0.8
        );
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_in_list_selectivity(
                &in_list(0, vec![Value::Int32(1), Value::Int32(2)], false),
                &column_refs
            ),
            1.0
        );
        assert_approx_eq::assert_approx_eq!(
            cost_model
                .get_in_list_selectivity(&in_list(0, vec![Value::Int32(3)], false), &column_refs),
            0.0
        );
        assert_approx_eq::assert_approx_eq!(
            cost_model
                .get_in_list_selectivity(&in_list(0, vec![Value::Int32(1)], true), &column_refs),
            0.2
        );
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_in_list_selectivity(
                &in_list(0, vec![Value::Int32(1), Value::Int32(2)], true),
                &column_refs
            ),
            0.0
        );
        assert_approx_eq::assert_approx_eq!(
            cost_model
                .get_in_list_selectivity(&in_list(0, vec![Value::Int32(3)], true), &column_refs),
            1.0
        );
    }
}
