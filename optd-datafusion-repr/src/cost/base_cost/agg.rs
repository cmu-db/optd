use std::sync::Arc;

use optd_core::{
    cascades::{CascadesOptimizer, RelNodeContext},
    cost::Cost,
    rel_node::RelNode,
};
use serde::{de::DeserializeOwned, Serialize};

use crate::{
    cost::{
        base_cost::stats::{Distribution, MostCommonValues},
        base_cost::DEFAULT_NUM_DISTINCT,
    },
    plan_nodes::{ExprList, OptRelNode, OptRelNodeTyp},
    properties::column_ref::{ColumnRef, ColumnRefPropertyBuilder},
};

use super::{OptCostModel, DEFAULT_UNK_SEL};

impl<
        M: MostCommonValues + Serialize + DeserializeOwned,
        D: Distribution + Serialize + DeserializeOwned,
    > OptCostModel<M, D>
{
    pub(super) fn get_agg_cost(
        &self,
        children: &[Cost],
        context: Option<RelNodeContext>,
        optimizer: Option<&CascadesOptimizer<OptRelNodeTyp>>,
    ) -> Cost {
        let child_row_cnt = Self::row_cnt(&children[0]);
        let row_cnt = self.get_agg_row_cnt(context, optimizer, child_row_cnt);
        let (_, compute_cost_1, _) = Self::cost_tuple(&children[1]);
        let (_, compute_cost_2, _) = Self::cost_tuple(&children[2]);
        Self::cost(
            row_cnt,
            child_row_cnt * (compute_cost_1 + compute_cost_2),
            0.0,
        )
    }

    fn get_agg_row_cnt(
        &self,
        context: Option<RelNodeContext>,
        optimizer: Option<&CascadesOptimizer<OptRelNodeTyp>>,
        child_row_cnt: f64,
    ) -> f64 {
        if let (Some(context), Some(optimizer)) = (context, optimizer) {
            let group_by_id = context.children_group_ids[2];
            let mut group_by_exprs: Vec<Arc<RelNode<OptRelNodeTyp>>> =
                optimizer.get_all_group_bindings(group_by_id, false);
            assert!(
                group_by_exprs.len() == 1,
                "ExprList expression should be the only expression in the GROUP BY group"
            );
            let group_by = group_by_exprs.pop().unwrap();
            let group_by = ExprList::from_rel_node(group_by).unwrap();
            if group_by.is_empty() {
                1.0
            } else {
                // Multiply the n-distinct of all the group by columns.
                // TODO: improve with multi-dimensional n-distinct
                let base_table_col_refs = optimizer
                    .get_property_by_group::<ColumnRefPropertyBuilder>(context.group_id, 1);
                base_table_col_refs
                    .iter()
                    .take(group_by.len())
                    .map(|col_ref| match col_ref {
                        ColumnRef::BaseTableColumnRef { table, col_idx } => {
                            let table_stats = self.per_table_stats_map.get(table);
                            let column_stats = table_stats.and_then(|table_stats| {
                                table_stats.column_comb_stats.get(&vec![*col_idx])
                            });

                            if let Some(column_stats) = column_stats {
                                column_stats.ndistinct as f64
                            } else {
                                // The column type is not supported or stats are missing.
                                DEFAULT_NUM_DISTINCT as f64
                            }
                        }
                        ColumnRef::Derived => DEFAULT_NUM_DISTINCT as f64,
                        _ => panic!(
                            "GROUP BY base table column ref must either be derived or base table"
                        ),
                    })
                    .product()
            }
        } else {
            (child_row_cnt * DEFAULT_UNK_SEL).max(1.0)
        }
    }
}
