use futures::channel::mpsc;

use crate::{
    cir::{GoalId, LogicalPlan, PhysicalPlan},
    memo::Memoize,
    optimizer::Optimizer,
};

use super::TaskId;

/// Task data for optimizing a logical plan.
pub(super) struct OptimizePlanTask {
    /// The logical plan to be optimized.
    pub plan: LogicalPlan,

    /// Channel to send the optimized physical plan back to the caller.
    pub response_tx: mpsc::Sender<PhysicalPlan>,

    /// The only dependency to get the best plans from.
    pub optimize_goal_in: TaskId,
}

impl OptimizePlanTask {
    pub fn new(
        plan: LogicalPlan,
        response_tx: mpsc::Sender<PhysicalPlan>,
        optimize_goal_in: TaskId,
    ) -> Self {
        Self {
            plan,
            response_tx,
            optimize_goal_in,
        }
    }
}

impl<M: Memoize> Optimizer<M> {
    pub fn create_optimize_plan_task(
        &mut self,
        plan: LogicalPlan,
        response_tx: mpsc::Sender<PhysicalPlan>,
    ) -> TaskId {

        // Do ingestion
        // logical_expresison, group_id
        // goal_id = group + empty prop create if not exists
        // ensure_goal_optimize exists, create subtasks
        // return

        todo!()
    }
}
