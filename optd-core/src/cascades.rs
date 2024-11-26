// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

//! The core cascades optimizer implementation.

mod memo;
mod optimizer;
mod tasks;

pub use memo::{
    ArcMemoPlanNode, Group, GroupInfo, Memo, MemoPlanNode, NaiveMemo, Winner, WinnerInfo,
};
pub use optimizer::{
    CascadesOptimizer, ExprId, GroupId, OptimizerProperties, PredId, RelNodeContext,
};
use tasks::Task;
