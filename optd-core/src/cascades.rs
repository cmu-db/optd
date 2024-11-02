//! The core cascades optimizer implementation.

mod memo;
mod optimizer;
mod tasks;

pub use memo::{Memo, NaiveMemo};
pub use optimizer::{CascadesOptimizer, ExprId, GroupId, OptimizerProperties, RelNodeContext};
use tasks::Task;
