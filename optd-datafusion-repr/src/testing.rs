mod dummy_cost;
mod tpch_catalog;

use std::sync::Arc;

use optd_core::{
    heuristics::{ApplyOrder, HeuristicsOptimizer},
    rules::Rule,
};

use crate::{plan_nodes::OptRelNodeTyp, properties::schema::SchemaPropertyBuilder};

use self::tpch_catalog::TpchCatalog;

/// Create a "dummy" optimizer preloaded with the TPC-H catalog for testing
/// Note: Only provides the schema property currently
pub fn new_test_optimizer(
    rule: Arc<dyn Rule<OptRelNodeTyp, HeuristicsOptimizer<OptRelNodeTyp>>>,
) -> HeuristicsOptimizer<OptRelNodeTyp> {
    let dummy_catalog = Arc::new(TpchCatalog);

    HeuristicsOptimizer::new_with_rules(
        vec![rule],
        ApplyOrder::TopDown,
        Arc::new([Box::new(SchemaPropertyBuilder::new(dummy_catalog))]),
    )
}
