use optd_core::cascades::{CascadesOptimizer, RelNodeContext};
use serde::{de::DeserializeOwned, Serialize};

use crate::adv_stats::{
    stats::{Distribution, MostCommonValues},
    DEFAULT_NUM_DISTINCT,
};
use optd_datafusion_repr::{
    plan_nodes::{ExprList, OptRelNode, OptRelNodeTyp},
    properties::column_ref::{BaseTableColumnRef, ColumnRef, ColumnRefPropertyBuilder},
};

use super::{AdvStats, DEFAULT_UNK_SEL};

impl<
        M: MostCommonValues + Serialize + DeserializeOwned,
        D: Distribution + Serialize + DeserializeOwned,
    > AdvStats<M, D>
{
    pub(crate) fn get_agg_row_cnt(
        &self,
        context: Option<RelNodeContext>,
        optimizer: Option<&CascadesOptimizer<OptRelNodeTyp>>,
        child_row_cnt: f64,
    ) -> f64 {
        if let (Some(context), Some(optimizer)) = (context, optimizer) {
            let group_by_id = context.children_group_ids[2];
            let group_by = optimizer
                .get_predicate_binding(group_by_id)
                .expect("no expression found?");
            let group_by = ExprList::from_rel_node(group_by).unwrap();
            if group_by.is_empty() {
                1.0
            } else {
                // Multiply the n-distinct of all the group by columns.
                // TODO: improve with multi-dimensional n-distinct
                let group_col_refs = optimizer
                    .get_property_by_group::<ColumnRefPropertyBuilder>(context.group_id, 1);
                group_col_refs
                    .base_table_column_refs()
                    .iter()
                    .take(group_by.len())
                    .map(|col_ref| match col_ref {
                        ColumnRef::BaseTableColumnRef(BaseTableColumnRef { table, col_idx }) => {
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
