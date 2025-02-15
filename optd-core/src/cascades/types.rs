use crate::values::OptdValue;

use super::groups::{RelationalGroupId, ScalarGroupId};

#[derive(Clone, Debug, PartialEq)]
pub enum PartialLogicalPlan {
    PartialMaterialized {
        tag: String,
        values: Vec<OptdValue>,
        relational_children: Vec<Vec<Box<PartialLogicalPlan>>>,
        scalar_children: Vec<Vec<Box<PartialScalarPlan>>>,
    },
    UnMaterialized(RelationalGroupId),
}

#[derive(Clone, Debug, PartialEq)]
pub enum PartialScalarPlan {
    PartialMaterialized {
        tag: String,
        values: Vec<OptdValue>,
        scalar_children: Vec<Vec<Box<PartialScalarPlan>>>,
    },
    UnMaterialized(ScalarGroupId),
}
