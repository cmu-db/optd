use anyhow::Result;
use tracing::trace;

use crate::{
    cascades::{
        memo::{GroupInfo, Winner},
        optimizer::ExprId,
        tasks::OptimizeGroupTask,
        CascadesOptimizer, GroupId,
    },
    cost::Cost,
    rel_node::RelNodeTyp,
};

use super::Task;

#[derive(Debug, Clone)]
struct ContinueTask {
    next_group_idx: usize,
    input_cost: Vec<Cost>,
    return_from_optimize_group: bool,
}

pub struct OptimizeInputsTask {
    expr_id: ExprId,
    continue_from: Option<ContinueTask>,
    pruning: bool,
}

impl OptimizeInputsTask {
    pub fn new(expr_id: ExprId, pruning: bool) -> Self {
        Self {
            expr_id,
            continue_from: None,
            pruning,
        }
    }

    fn continue_from(&self, cont: ContinueTask, pruning: bool) -> Self {
        Self {
            expr_id: self.expr_id,
            continue_from: Some(cont),
            pruning,
        }
    }

    /// first invoke of this task, compute the cost of children
    fn first_invoke<T: RelNodeTyp>(
        &self,
        children: &[GroupId],
        optimizer: &mut CascadesOptimizer<T>,
    ) -> Vec<Cost> {
        let zero_cost = optimizer.cost().zero();
        let mut input_cost = Vec::new();
        input_cost.reserve(children.len());
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

    fn should_terminate(&self, cost_so_far: f64, upper_bound: Option<f64>) -> bool {
        if !self.pruning {
            return false;
        }
        if upper_bound.is_none() {
            return false;
        }
        let upper_bound = upper_bound.unwrap();
        if cost_so_far >= upper_bound {
            trace!(
                event = "optimize_inputs_pruning",
                task = "optimize_inputs_pruning",
                cost_so_far = cost_so_far,
                upper_bound = upper_bound
            );
            return true;
        }
        return false;
    }

    fn update_winner<T: RelNodeTyp>(
        &self,
        cost_so_far: &Cost,
        optimizer: &mut CascadesOptimizer<T>,
    ) {
        let cost = optimizer.cost();
        let group_id = optimizer.get_group_id(self.expr_id);
        let group_info = optimizer.get_group_info(group_id);
        let mut update_cost = false;
        if let Some(ref winner) = group_info.winner {
            if winner.impossible || &winner.cost > cost_so_far {
                update_cost = true;
            }
        } else {
            update_cost = true;
        }
        if update_cost {
            optimizer.update_group_info(
                group_id,
                GroupInfo {
                    winner: Some(Winner {
                        impossible: false,
                        expr_id: self.expr_id,
                        cost: cost_so_far.clone(),
                    }),
                },
            );
        }
    }
}

impl<T: RelNodeTyp> Task<T> for OptimizeInputsTask {
    fn execute(&self, optimizer: &mut CascadesOptimizer<T>) -> Result<Vec<Box<dyn Task<T>>>> {
        trace!(event = "task_begin", task = "optimize_inputs", expr_id = %self.expr_id, continue_from = ?self.continue_from);
        let expr = optimizer.get_expr_memoed(self.expr_id);
        let children = &expr.children;
        let cost = optimizer.cost();

        if let Some(ContinueTask {
            next_group_idx,
            mut input_cost,
            return_from_optimize_group,
        }) = self.continue_from.clone()
        {
            if self.should_terminate(
                cost.compute_cost(&expr.typ, &expr.data, &input_cost).0[0],
                optimizer.ctx.upper_bound,
            ) {
                trace!(event = "task_finish", task = "optimize_inputs", expr_id = %self.expr_id);
                return Ok(vec![]);
            }
            let next_group_idx = next_group_idx;
            if next_group_idx < children.len() {
                let group_id = children[next_group_idx];
                let group_idx = next_group_idx;
                let group_info = optimizer.get_group_info(group_id);
                let mut has_full_winner = false;
                if let Some(ref winner) = group_info.winner {
                    if !winner.impossible {
                        input_cost[group_idx] = winner.cost.clone();
                        has_full_winner = true;
                        if self.should_terminate(
                            cost.compute_cost(&expr.typ, &expr.data, &input_cost).0[0],
                            optimizer.ctx.upper_bound,
                        ) {
                            trace!(event = "task_finish", task = "optimize_inputs", expr_id = %self.expr_id);
                            return Ok(vec![]);
                        }
                    }
                }
                if !has_full_winner {
                    if !return_from_optimize_group {
                        trace!(event = "task_yield", task = "optimize_inputs", expr_id = %self.expr_id, group_idx = %group_idx);
                        return Ok(vec![
                            Box::new(self.continue_from(
                                ContinueTask {
                                    next_group_idx: next_group_idx,
                                    input_cost,
                                    return_from_optimize_group: true,
                                },
                                self.pruning,
                            )) as Box<dyn Task<T>>,
                            Box::new(OptimizeGroupTask::new(group_id)) as Box<dyn Task<T>>,
                        ]);
                    } else {
                        if let Some(ref winner) = group_info.winner {
                            if winner.impossible {
                                optimizer.update_group_info(
                                    group_id,
                                    GroupInfo {
                                        winner: Some(Winner {
                                            impossible: true,
                                            ..Default::default()
                                        }),
                                    },
                                );
                                trace!(event = "task_finish", task = "optimize_inputs", expr_id = %self.expr_id);
                                return Ok(vec![]);
                            }
                        }
                        optimizer.update_group_info(
                            group_id,
                            GroupInfo {
                                winner: Some(Winner {
                                    impossible: true,
                                    ..Default::default()
                                }),
                            },
                        );
                        trace!(event = "task_finish", task = "optimize_inputs", expr_id = %self.expr_id);
                        return Ok(vec![]);
                    }
                }
                trace!(event = "task_yield", task = "optimize_inputs", expr_id = %self.expr_id, group_idx = %group_idx);
                return Ok(vec![Box::new(self.continue_from(
                    ContinueTask {
                        next_group_idx: group_idx + 1,
                        input_cost,
                        return_from_optimize_group: false,
                    },
                    self.pruning,
                )) as Box<dyn Task<T>>]);
            } else {
                self.update_winner(
                    &cost.compute_cost(&expr.typ, &expr.data, &input_cost),
                    optimizer,
                );
                trace!(event = "task_finish", task = "optimize_inputs", expr_id = %self.expr_id);
                return Ok(vec![]);
            }
        } else {
            let input_cost = self.first_invoke(&children, optimizer);
            trace!(event = "task_yield", task = "optimize_inputs", expr_id = %self.expr_id);
            return Ok(vec![Box::new(self.continue_from(
                ContinueTask {
                    next_group_idx: 0,
                    input_cost,
                    return_from_optimize_group: false,
                },
                self.pruning,
            )) as Box<dyn Task<T>>]);
        }
    }
}
