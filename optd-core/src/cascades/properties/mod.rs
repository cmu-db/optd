use serde::{Deserialize, Serialize};

use crate::cascades::ir::OperatorData;

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, Default)]
pub struct PhysicalProperties {
    sort_order: SortProperty,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, Default)]
pub struct SortProperty {
    /// Each tuple is a column index, direction pair.
    /// e.g. vec![(0, Asc), (1, Desc)]
    pub sort_orders: Vec<(OperatorData, OperatorData)>,
}
