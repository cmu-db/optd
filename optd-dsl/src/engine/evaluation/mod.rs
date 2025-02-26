use futures::Stream;

use crate::analyzer::hir::Value;

use super::errors::EngineError;

mod core_eval;
mod expr_eval;
mod op_eval;
mod stream_cache;

pub type ValueStream = Box<dyn Stream<Item = Result<Value, EngineError>> + Send + Unpin>;
pub type VecValueStream = Box<dyn Stream<Item = Result<Vec<Value>, EngineError>> + Send + Unpin>;
