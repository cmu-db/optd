use tracing::trace;

use crate::{
    cascades::{
        memo::{GroupInfo, Winner},
        optimizer::{ExprId, RelNodeContext},
        CascadesOptimizer, GroupId,
    },
    cost::Cost,
    rel_node::RelNodeTyp,
};

use super::{optimize_group::OptimizeGroupTask, Task};

pub struct OptimizeInputsTask {
    parent_task_id: Option<usize>,
    task_id: usize,
    expr_id: ExprId,
    cost_limit: Option<isize>,
    iteration: usize,
}

impl OptimizeInputsTask {
    pub fn new(
        parent_task_id: Option<usize>,
        task_id: usize,
        expr_id: ExprId,
        cost_limit: Option<isize>,
    ) -> Self {
        Self {
            parent_task_id,
            task_id,
            expr_id,
            cost_limit,
            iteration: 0,
        }
    }

    fn new_continue_iteration(&self, optimizer: &CascadesOptimizer<impl RelNodeTyp>) -> Self {
        Self {
            parent_task_id: Some(self.task_id),
            task_id: optimizer.get_next_task_id(),
            expr_id: self.expr_id,
            cost_limit: self.cost_limit,
            iteration: self.iteration + 1,
        }
    }
}

fn get_input_cost<T: RelNodeTyp>(
    children: &[GroupId],
    optimizer: &CascadesOptimizer<T>,
) -> Vec<Cost> {
    let zero_cost = optimizer.cost().zero();
    let mut input_cost = Vec::with_capacity(children.len());
    for &child in children.iter() {
        let group = optimizer.get_group_info(child);
        if let Some(ref winner) = group.winner {
            if !winner.impossible {
                // the full winner case
                input_cost.push(winner.cost.clone());
                continue;
            }
        }
        input_cost.push(zero_cost.clone());
    }
    input_cost
}

fn update_winner<T: RelNodeTyp>(expr_id: ExprId, optimizer: &CascadesOptimizer<T>) {
    let cost = optimizer.cost();
    let expr = optimizer.get_expr_memoed(expr_id);
    let group_id = optimizer.get_group_id(expr_id);

    // Calculate cost
    let context = RelNodeContext {
        expr_id,
        group_id,
        children_group_ids: expr.children.clone(),
    };
    let input_cost = get_input_cost(&expr.children, optimizer);
    let total_cost = cost.sum(
        &cost.compute_cost(
            &expr.typ,
            &expr.data,
            &input_cost,
            Some(context.clone()),
            Some(optimizer),
        ),
        &input_cost,
    );

    let group_id = optimizer.get_group_id(expr_id);
    let group_info = optimizer.get_group_info(group_id);

    // Update best cost for group if desired
    let mut update_cost = false;
    if let Some(winner) = &group_info.winner {
        if winner.impossible || winner.cost > total_cost {
            update_cost = true;
        }
    } else {
        update_cost = true;
    }
    // TODO: Deciding the winner and constructing the struct should
    // be performed in the memotable
    if update_cost {
        optimizer.update_group_info(
            group_id,
            GroupInfo {
                winner: Some(Winner {
                    impossible: false,
                    expr_id,
                    cost: total_cost.clone(),
                }),
            },
        );
    }
}

/// TODO
///
/// Pseudocode:
/// function OptInputs(expr, rule, limit)
///     childExpr ← expr.GetNextInput()
///     if childExpr is null then
///         memo.UpdateBestPlan(expr)
///         return
///     tasks.Push(OptInputs(expr, limit))
///     UpdateCostBound(expr)
///     limit ← UpdateCostLimit(expr, limit)
///     tasks.Push(OptGrp(GetGroup(childExpr), limit))
impl<T: RelNodeTyp> Task<T> for OptimizeInputsTask {
    fn execute(&self, optimizer: &CascadesOptimizer<T>) {
        let expr = optimizer.get_expr_memoed(self.expr_id);
        let group_id = optimizer.get_group_id(self.expr_id);
        // TODO: add typ to more traces and iteration to traces below
        trace!(task_id = self.task_id, parent_task_id = self.parent_task_id, event = "task_begin", task = "optimize_inputs", iteration = %self.iteration, group_id = %group_id, expr_id = %self.expr_id, expr = %expr);
        let next_child_expr = expr.children.get(self.iteration);
        if let None = next_child_expr {
            // TODO: If we want to support interrupting the optimizer, it might
            // behoove us to update the winner more often than this.
            update_winner(self.expr_id, optimizer);
            trace!(task_id = self.task_id, parent_task_id = self.parent_task_id, event = "task_finish", task = "optimize_inputs", iteration = %self.iteration, group_id = %group_id, expr_id = %self.expr_id, expr = %expr);
            return;
        }
        let next_child_expr = next_child_expr.unwrap();

        //TODO(parallel): Task dependency
        //TODO: Should be able to add multiple tasks at once
        optimizer.push_task(Box::new(self.new_continue_iteration(optimizer)));
        // TODO updatecostbound (involves cost limit)
        let new_limit = None; // TODO: How do we update cost limit
        optimizer.push_task(Box::new(OptimizeGroupTask::new(
            Some(self.task_id),
            optimizer.get_next_task_id(),
            *next_child_expr,
            new_limit,
        )));
        trace!(task_id = self.task_id, parent_task_id = self.parent_task_id, event = "task_finish", task = "optimize_inputs", iteration = %self.iteration, group_id = %group_id, expr_id = %self.expr_id, expr = %expr);
    }
}
