// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

use optd_datafusion_repr::plan_nodes::{ArcDfPredNode, DfReprPredNode, ListPred};
use optd_datafusion_repr::properties::column_ref::{
    BaseTableColumnRef, ColumnRef, GroupColumnRefs,
};
use serde::de::DeserializeOwned;
use serde::Serialize;

use super::AdvStats;
use crate::adv_stats::stats::{Distribution, MostCommonValues};
use crate::adv_stats::DEFAULT_NUM_DISTINCT;

impl<
        M: MostCommonValues + Serialize + DeserializeOwned,
        D: Distribution + Serialize + DeserializeOwned,
    > AdvStats<M, D>
{
    pub(crate) fn get_agg_row_cnt(
        &self,
        group_by: ArcDfPredNode,
        output_col_refs: GroupColumnRefs,
    ) -> f64 {
        let group_by = ListPred::from_pred_node(group_by).unwrap();
        if group_by.is_empty() {
            1.0
        } else {
            // Multiply the n-distinct of all the group by columns.
            // TODO: improve with multi-dimensional n-distinct
            output_col_refs
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
    }
}
