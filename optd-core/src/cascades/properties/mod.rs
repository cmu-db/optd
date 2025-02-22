use serde::{Deserialize, Serialize};

use crate::cascades::ir::OperatorData;

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub enum PhysicalProperty {
    Sorted(SortProperty),
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct SortProperty {
    /// Each tuple is a column index, direction pair.
    /// e.g. vec![(0, Asc), (1, Desc)]
    pub sort_orders: Vec<(OperatorData, OperatorData)>,
}
