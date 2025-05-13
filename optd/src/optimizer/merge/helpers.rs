use crate::{
    cir::LogicalExpressionId,
    memo::Memo,
    optimizer::{Optimizer, errors::OptimizeError, tasks::TaskId},
};
use hashbrown::{HashMap, HashSet};

impl<M: Memo> Optimizer<M> {
    /// Update a group exploration task with new logical expressions, if any.
    ///
    /// This function performs the following:
    /// 1. Computes newly discovered expressions by subtracting existing ones.
    /// 2. For each fork task in the group, launches continuation tasks for each new expression.
    /// 3. If the task is the principal, launches transform tasks for each new expression and rule.
    /// 4. Updates the task's dispatched expressions with the full input set.
    ///
    /// # Arguments
    /// * `task_id` - The ID of the group exploration task to update.
    /// * `all_logical_exprs` - The complete set of logical expressions known for this group.
    /// * `principal` - Whether this task is the principal one (responsible for launching transforms).
    pub(super) async fn update_group_explore(
        &mut self,
        task_id: TaskId,
        all_logical_exprs: &HashSet<LogicalExpressionId>,
        principal: bool,
    ) -> Result<(), OptimizeError> {
        let new_exprs = self.compute_new_expressions(task_id, all_logical_exprs);

        if !new_exprs.is_empty() {
            let (group_id, fork_tasks) = {
                let task = self.get_explore_group_task(task_id).unwrap();
                (task.group_id, task.fork_logical_out.clone())
            };

            for &fork_task_id in &fork_tasks {
                let continuation = self
                    .get_fork_logical_task(fork_task_id)
                    .unwrap()
                    .continuation
                    .clone();

                let continuation_tasks =
                    self.create_logical_cont_tasks(&new_exprs, fork_task_id, &continuation);

                self.get_fork_logical_task_mut(fork_task_id)
                    .unwrap()
                    .continue_with_logical_in
                    .extend(continuation_tasks);
            }

            if principal {
                let transform_tasks = self.create_transform_tasks(&new_exprs, group_id, task_id);
                self.get_explore_group_task_mut(task_id)
                    .unwrap()
                    .transform_expr_in
                    .extend(transform_tasks);
            }

            self.get_explore_group_task_mut(task_id)
                .unwrap()
                .dispatched_exprs = all_logical_exprs.clone();
        }

        Ok(())
    }

    /// Compute new expressions for a group exploration task.
    fn compute_new_expressions(
        &self,
        task_id: TaskId,
        all_exprs: &HashSet<LogicalExpressionId>,
    ) -> HashSet<LogicalExpressionId> {
        let task = self.get_explore_group_task(task_id).unwrap();
        all_exprs
            .difference(&task.dispatched_exprs)
            .copied()
            .collect()
    }

    /// Consolidate a group exploration task into a principal task.
    ///
    /// - A merge in the memo may cause several group exploration tasks to refer to
    ///   the same underlying group. This function consolidates all such secondary
    ///   tasks into a principal one.
    ///
    /// - This involves:
    ///   - Moving all outgoing fork and goal tasks from secondary to principal.
    ///   - Updating each such task's `explore_group_in` to point to the principal.
    ///   - Deleting the secondary task.
    ///
    /// # Arguments
    /// * `principal_task_id` - The ID of the task to retain as canonical.
    /// * `secondary_task_ids` - All other task IDs to merge into the principal.
    pub(super) async fn consolidate_group_explore(
        &mut self,
        principal_task_id: TaskId,
        secondary_task_ids: &[TaskId],
    ) {
        secondary_task_ids.iter().for_each(|&task_id| {
            let task = self.get_explore_group_task_mut(task_id).unwrap();

            // Move out the task sets before deletion.
            let fork_tasks = std::mem::take(&mut task.fork_logical_out);
            let goal_tasks = std::mem::take(&mut task.optimize_goal_out);

            self.delete_task(task_id);

            fork_tasks.into_iter().for_each(|fork_id| {
                let fork_task = self.get_fork_logical_task_mut(fork_id).unwrap();
                fork_task.explore_group_in = principal_task_id;
            });

            goal_tasks.into_iter().for_each(|goal_id| {
                let goal_task = self.get_optimize_goal_task_mut(goal_id).unwrap();
                goal_task.explore_group_in = principal_task_id;
            });
        });
    }

    /// Deduplicate dispatched expressions for a group exploration task.
    ///
    /// - During optimization, logical expressions in a group may get merged
    ///   in the memo structure. As a result, multiple expressions in the same
    ///   group might now resolve to the same canonical form.
    ///
    /// - This function:
    ///   - Maps each expression to its current representative.
    ///   - Identifies and prunes redundant (duplicate) expressions.
    ///   - Updates or deletes related transform and fork/continuation tasks
    ///     accordingly.
    ///
    /// # Arguments
    /// * `task_id` - The ID of the group exploration task to deduplicate.
    pub(super) async fn dedup_group_explore(
        &mut self,
        task_id: TaskId,
    ) -> Result<(), OptimizeError> {
        let task = self.get_explore_group_task_mut(task_id).unwrap();
        let old_exprs = std::mem::take(&mut task.dispatched_exprs);
        let transform_ids: Vec<_> = task.transform_expr_in.iter().copied().collect();
        let fork_ids: Vec<_> = task.fork_logical_out.iter().copied().collect();

        let expr_to_repr = self.map_to_representatives(&old_exprs).await?;

        let (unique_reprs, to_delete) = {
            let mut seen = HashSet::new();
            let mut dups = vec![];

            for (&expr_id, &repr_id) in &expr_to_repr {
                if !seen.insert(repr_id) {
                    dups.push(expr_id);
                }
            }

            (seen, dups)
        };

        self.process_transform_tasks(&transform_ids, &expr_to_repr, &to_delete);
        self.process_fork_tasks(&fork_ids, &expr_to_repr, &to_delete);

        let task = self.get_explore_group_task_mut(task_id).unwrap();
        task.dispatched_exprs = unique_reprs;

        Ok(())
    }

    /// For each logical expression, find its current representative in the memo.
    async fn map_to_representatives(
        &mut self,
        exprs: &HashSet<LogicalExpressionId>,
    ) -> Result<HashMap<LogicalExpressionId, LogicalExpressionId>, OptimizeError> {
        let mut expr_to_repr = HashMap::with_capacity(exprs.len());
        for expr_id in exprs {
            let repr_id = self
                .memo
                .find_repr_logical_expr_id(*expr_id)
                .await
                .map_err(OptimizeError::MemoError)?;
            expr_to_repr.insert(*expr_id, repr_id);
        }
        Ok(expr_to_repr)
    }

    /// Update or delete transform tasks based on deduplicated expressions.
    fn process_transform_tasks(
        &mut self,
        tasks: &[TaskId],
        expr_to_repr: &HashMap<LogicalExpressionId, LogicalExpressionId>,
        to_delete: &[LogicalExpressionId],
    ) {
        for &task_id in tasks {
            let expr_id = self
                .get_transform_expression_task(task_id)
                .unwrap()
                .expression_id;

            if to_delete.contains(&expr_id) {
                self.delete_task(task_id);
            } else {
                self.get_transform_expression_task_mut(task_id)
                    .unwrap()
                    .expression_id = expr_to_repr[&expr_id];
            }
        }
    }

    /// Update or delete continuation tasks spawned by fork tasks.
    fn process_fork_tasks(
        &mut self,
        tasks: &[TaskId],
        expr_to_repr: &HashMap<LogicalExpressionId, LogicalExpressionId>,
        to_delete: &[LogicalExpressionId],
    ) {
        for &task_id in tasks {
            let continue_ids = self
                .get_fork_logical_task(task_id)
                .unwrap()
                .continue_with_logical_in
                .clone(); // Clone to avoid mutable borrow conflict.

            for cont_id in continue_ids {
                let expr_id = self
                    .get_continue_with_logical_task(cont_id)
                    .unwrap()
                    .expression_id;

                if to_delete.contains(&expr_id) {
                    self.delete_task(cont_id);
                } else {
                    self.get_continue_with_logical_task_mut(cont_id)
                        .unwrap()
                        .expression_id = expr_to_repr[&expr_id];
                }
            }
        }
    }
}
