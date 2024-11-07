// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

mod dummy_cost;
mod tpch_catalog;

use std::sync::Arc;

use optd_core::heuristics::{ApplyOrder, HeuristicsOptimizer};
use optd_core::rules::Rule;

use self::tpch_catalog::TpchCatalog;
use crate::plan_nodes::DfNodeType;
use crate::properties::schema::SchemaPropertyBuilder;

/// Create a "dummy" optimizer preloaded with the TPC-H catalog for testing
/// Note: Only provides the schema property currently
pub fn new_test_optimizer(
    rule: Arc<dyn Rule<DfNodeType, HeuristicsOptimizer<DfNodeType>>>,
) -> HeuristicsOptimizer<DfNodeType> {
    let dummy_catalog = Arc::new(TpchCatalog);

    HeuristicsOptimizer::new_with_rules(
        vec![rule],
        ApplyOrder::TopDown,
        Arc::new([Box::new(SchemaPropertyBuilder::new(dummy_catalog))]),
    )
}
