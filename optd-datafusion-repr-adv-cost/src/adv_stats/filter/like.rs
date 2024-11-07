use datafusion::arrow::array::StringArray;
use datafusion::arrow::compute::like;
use optd_datafusion_repr::plan_nodes::{
    ColumnRefPred, ConstantPred, DfPredType, DfReprPredNode, LikePred,
};
use optd_datafusion_repr::properties::column_ref::{
    BaseTableColumnRef, BaseTableColumnRefs, ColumnRef,
};
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::adv_stats::stats::{ColumnCombValue, Distribution, MostCommonValues};
use crate::adv_stats::{AdvStats, UNIMPLEMENTED_SEL};

// Used for estimating pattern selectivity character-by-character. These numbers
// are not used on their own. Depending on the characters in the pattern, the
// selectivity is multiplied by these factors.
//
// See `FULL_WILDCARD_SEL` and `FIXED_CHAR_SEL` in Postgres.
const FULL_WILDCARD_SEL_FACTOR: f64 = 5.0;
const FIXED_CHAR_SEL_FACTOR: f64 = 0.2;

impl<
        M: MostCommonValues + Serialize + DeserializeOwned,
        D: Distribution + Serialize + DeserializeOwned,
    > AdvStats<M, D>
{
    /// Compute the selectivity of a (NOT) LIKE expression.
    ///
    /// The logic is somewhat similar to Postgres but different. Postgres first estimates the
    /// histogram part of the population and then add up data for any MCV values. If the
    /// histogram is large enough, it just uses the number of matches in the histogram,
    /// otherwise it estimates the fixed prefix and remainder of pattern separately and
    /// combine them.
    ///
    /// Our approach is simpler and less selective. Firstly, we don't use histogram. The selectivity
    /// is composed of MCV frequency and non-MCV selectivity. MCV frequency is computed by
    /// adding up frequencies of MCVs that match the pattern. Non-MCV  selectivity is computed
    /// in the same way that Postgres computes selectivity for the wildcard part of the pattern.
    pub(super) fn get_like_selectivity(
        &self,
        like_expr: &LikePred,
        column_refs: &BaseTableColumnRefs,
    ) -> f64 {
        let child = like_expr.child();

        // Check child is a column ref.
        if !matches!(child.typ, DfPredType::ColumnRef) {
            return UNIMPLEMENTED_SEL;
        }

        // Check pattern is a constant.
        let pattern = like_expr.pattern();
        if !matches!(pattern.typ, DfPredType::Constant(_)) {
            return UNIMPLEMENTED_SEL;
        }

        let col_ref_idx = ColumnRefPred::from_pred_node(child).unwrap().index();

        if let ColumnRef::BaseTableColumnRef(BaseTableColumnRef { table, col_idx }) =
            &column_refs[col_ref_idx]
        {
            let pattern = ConstantPred::from_pred_node(pattern)
                .expect("we already checked pattern is a constant")
                .value()
                .as_str();

            // Compute the selectivity exculuding MCVs.
            // See Postgres `like_selectivity`.
            let non_mcv_sel = pattern
                .chars()
                .fold(1.0, |acc, c| {
                    if c == '%' {
                        acc * FULL_WILDCARD_SEL_FACTOR
                    } else {
                        acc * FIXED_CHAR_SEL_FACTOR
                    }
                })
                .min(1.0);

            // Compute the selectivity in MCVs.
            let column_stats = self.get_column_comb_stats(table, &[*col_idx]);
            let (mcv_freq, null_frac) = if let Some(column_stats) = column_stats {
                let pred = Box::new(move |val: &ColumnCombValue| {
                    let string =
                        StringArray::from(vec![val[0].as_ref().unwrap().as_str().as_ref()]);
                    let pattern = StringArray::from(vec![pattern.as_ref()]);
                    like(&string, &pattern).unwrap().value(0)
                });
                (
                    column_stats.mcvs.freq_over_pred(pred),
                    column_stats.null_frac,
                )
            } else {
                (0.0, 0.0)
            };

            let result = non_mcv_sel + mcv_freq;

            if like_expr.negated() {
                1.0 - result - null_frac
            } else {
                result
            }
            // Postgres clamps the result after histogram and before MCV. See Postgres
            // `patternsel_common`.
            .clamp(0.0001, 0.9999)
        } else {
            UNIMPLEMENTED_SEL
        }
    }
}

#[cfg(test)]
mod tests {
    use optd_datafusion_repr::properties::column_ref::ColumnRef;
    use optd_datafusion_repr::Value;

    use crate::adv_stats::filter::like::{FIXED_CHAR_SEL_FACTOR, FULL_WILDCARD_SEL_FACTOR};
    use crate::adv_stats::tests::{
        create_one_column_cost_model, like, TestDistribution, TestMostCommonValues,
        TestPerColumnStats, TABLE1_NAME,
    };

    #[test]
    fn test_like_no_nulls() {
        let cost_model = create_one_column_cost_model(TestPerColumnStats::new(
            TestMostCommonValues::new(vec![
                (Value::String("abcd".into()), 0.1),
                (Value::String("abc".into()), 0.1),
            ]),
            2,
            0.0,
            Some(TestDistribution::empty()),
        ));
        let column_refs = vec![ColumnRef::base_table_column_ref(
            String::from(TABLE1_NAME),
            0,
        )];
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_like_selectivity(&like(0, "%abcd%", false), &column_refs),
            0.1 + FULL_WILDCARD_SEL_FACTOR.powi(2) * FIXED_CHAR_SEL_FACTOR.powi(4)
        );
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_like_selectivity(&like(0, "%abc%", false), &column_refs),
            0.1 + 0.1 + FULL_WILDCARD_SEL_FACTOR.powi(2) * FIXED_CHAR_SEL_FACTOR.powi(3)
        );
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_like_selectivity(&like(0, "%abc%", true), &column_refs),
            1.0 - (0.1 + 0.1 + FULL_WILDCARD_SEL_FACTOR.powi(2) * FIXED_CHAR_SEL_FACTOR.powi(3))
        );
    }

    #[test]
    fn test_like_with_nulls() {
        let null_frac = 0.5;
        let cost_model = create_one_column_cost_model(TestPerColumnStats::new(
            TestMostCommonValues::new(vec![(Value::String("abcd".into()), 0.1)]),
            2,
            null_frac,
            Some(TestDistribution::empty()),
        ));
        let column_refs = vec![ColumnRef::base_table_column_ref(
            String::from(TABLE1_NAME),
            0,
        )];
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_like_selectivity(&like(0, "%abcd%", false), &column_refs),
            0.1 + FULL_WILDCARD_SEL_FACTOR.powi(2) * FIXED_CHAR_SEL_FACTOR.powi(4)
        );
        assert_approx_eq::assert_approx_eq!(
            cost_model.get_like_selectivity(&like(0, "%abcd%", true), &column_refs),
            1.0 - (0.1 + FULL_WILDCARD_SEL_FACTOR.powi(2) * FIXED_CHAR_SEL_FACTOR.powi(4))
                - null_frac
        );
    }
}
