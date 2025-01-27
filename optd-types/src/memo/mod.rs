use crate::expression::{relational::logical::LogicalExpr, Expr};
use crate::plan::partial_logical_plan::PartialLogicalPlan;
use crate::plan::partial_physical_plan::PartialPhysicalPlan;
use crate::{GroupId, LogicalExprId};

pub mod rule;

pub struct Memo;
mod implementation;
