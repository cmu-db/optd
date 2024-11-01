use optd_core::cascades::{CascadesOptimizer, RelNodeContext};
use serde::{de::DeserializeOwned, Serialize};

use crate::adv_stats::stats::{Distribution, MostCommonValues};
use optd_datafusion_repr::plan_nodes::{ConstantExpr, ConstantType, OptRelNode, OptRelNodeTyp};

use super::{AdvStats, DEFAULT_UNK_SEL};

impl<
        M: MostCommonValues + Serialize + DeserializeOwned,
        D: Distribution + Serialize + DeserializeOwned,
    > AdvStats<M, D>
{
    pub(crate) fn get_limit_row_cnt(
        &self,
        child_row_cnt: f64,
        context: Option<RelNodeContext>,
        optimizer: Option<&CascadesOptimizer<OptRelNodeTyp>>,
    ) -> f64 {
        if let (Some(context), Some(optimizer)) = (context, optimizer) {
            let fetch_expr = optimizer
                .get_predicate_binding(context.children_group_ids[2])
                .expect("no expression found?");
            assert!(
                matches!(
                    fetch_expr.typ,
                    OptRelNodeTyp::Constant(ConstantType::UInt64)
                ),
                "fetch type can only be UInt64"
            );
            let fetch = ConstantExpr::from_rel_node(fetch_expr)
                .unwrap()
                .value()
                .as_u64();
            // u64::MAX represents None
            if fetch == u64::MAX {
                child_row_cnt
            } else {
                child_row_cnt.min(fetch as f64)
            }
        } else {
            (child_row_cnt * DEFAULT_UNK_SEL).max(1.0)
        }
    }
}
