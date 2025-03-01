use crate::values::OperatorData;

use super::groups::{RelationalGroupId, ScalarGroupId};

#[derive(Clone, Debug, PartialEq)]
pub enum PartialLogicalPlan {
    PartialMaterialized {
        tag: String,
        values: Vec<OperatorData>,
        relational_children: Vec<Vec<Box<PartialLogicalPlan>>>,
        scalar_children: Vec<Vec<Box<PartialScalarPlan>>>,
    },
    UnMaterialized(RelationalGroupId),
}

#[derive(Clone, Debug, PartialEq)]
pub enum PartialScalarPlan {
    PartialMaterialized {
        tag: String,
        values: Vec<OperatorData>,
        scalar_children: Vec<Vec<Box<PartialScalarPlan>>>,
    },
    UnMaterialized(ScalarGroupId),
}
