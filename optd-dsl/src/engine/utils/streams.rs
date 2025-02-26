use futures::Stream;
use optd_core::cascades::ir::PartialLogicalPlan;

use crate::analyzer::hir::Value;

use super::errors::Error;

pub type ValueStream = Box<dyn Stream<Item = Result<Value, Error>> + Send + Unpin>;
pub type VecValueStream = Box<dyn Stream<Item = Result<Vec<Value>, Error>> + Send + Unpin>;
pub type PartialLogicalPlanStream =
    Box<dyn Stream<Item = Result<PartialLogicalPlan, Error>> + Send + Unpin>;
