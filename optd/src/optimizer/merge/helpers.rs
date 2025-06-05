use crate::{
    cir::LogicalExpressionId,
    memo::Memo,
    optimizer::{Optimizer, tasks::TaskId},
};
use hashbrown::{HashMap, HashSet};

impl<M: Memo> Optimizer<M> {
    /// Update a group exploration task with new logical expressions, if any.
    ///
    /// This function performs the following:
    /// 1. Computes newly discovered expressions by subtracting existing ones.
    /// 2. For each fork task in the group, launches continuation tasks for each new expression.
    /// 3. If the task is the principal, launches transform tasks for each new expression and rule.
    /// 4. For all related optimize goal tasks, launch implement tasks for each new expression.
    ///
    /// *NOTE*: This happens before merging goals. While this might be slightly inefficient, as
    ///         we might implement twice for the same "soon-to-be" merged goals. However it keeps
    ///         the code cleaner and easier to understand. Also, the performance impact is negligible,
    ///         as once we merge the goals, we effectively delete the implement tasks and its
    ///         associated jobs will *not* be launched.
    ///
    /// 5. Updates the task's dispatched expressions with the full input set.
    ///
    /// # Arguments
    /// * `task_id` - The ID of the group exploration task to update.
    /// * `all_logical_exprs` - The complete set of logical expressions known for this group.
    /// * `principal` - Whether this task is the principal one (responsible for launching transforms).
    #[tracing::instrument(
        level = "debug",
        skip_all,
        name = "update_tasks_for_merged_group",
        fields(task_id = ?task_id, principal),
        target = "optd::optimizer::merge"
    )]
    pub(super) async fn update_tasks(
        &mut self,
        task_id: TaskId,
        all_logical_exprs: &HashSet<LogicalExpressionId>,
        principal: bool,
    ) -> Result<(), M::MemoError> {
        let new_exprs = self.compute_new_expressions(task_id, all_logical_exprs);

        if new_exprs.is_empty() {
            return Ok(());
        }
        tracing::debug!(
            target: "optd::optimizer::merge",
            num_new_exprs = new_exprs.len(),
            "Updating task with new expressions"
        );

        let (group_id, fork_tasks, optimize_goal_tasks) = {
            let task = self.get_explore_group_task(task_id).unwrap();
            (
                task.group_id,
                task.fork_logical_out.clone(),
                task.optimize_goal_out.clone(),
            )
        };

        // For each fork task, create continuation tasks for each new expression.
        fork_tasks.iter().for_each(|&fork_task_id| {
            let continuation = self
                .get_fork_logical_task(fork_task_id)
                .unwrap()
                .continuation
                .clone();

            let continuation_tasks =
                self.create_logical_cont_tasks(&new_exprs, group_id, fork_task_id, &continuation);

            tracing::trace!(
                target: "optd::optimizer::merge",
                "Adding {} continuation tasks to fork task {}", continuation_tasks.len(), fork_task_id.0
            );
            self.get_fork_logical_task_mut(fork_task_id)
                .unwrap()
                .continue_with_logical_in
                .extend(continuation_tasks);
        });

        // For each optimize goal task, create implement tasks for each new expression.
        optimize_goal_tasks.iter().for_each(|&optimize_goal_id| {
            let goal_id = self
                .get_optimize_goal_task(optimize_goal_id)
                .unwrap()
                .goal_id;

            let implement_tasks =
                self.create_implement_tasks(&new_exprs, goal_id, optimize_goal_id);

            tracing::trace!(
                target: "optd::optimizer::merge",
                "Adding {} implement tasks to optimize goal task {}", implement_tasks.len(), optimize_goal_id.0
            );
            self.get_optimize_goal_task_mut(optimize_goal_id)
                .unwrap()
                .implement_expression_in
                .extend(implement_tasks);
        });

        // For the principal task, create transform tasks for each new expression.
        // We could always do it, but this is a straightforward optimization.
        if principal {
            let transform_tasks = self.create_transform_tasks(&new_exprs, group_id, task_id);

            tracing::trace!(
                target: "optd::optimizer::merge",
                "Adding {} transform tasks to explore group task {}", transform_tasks.len(), task_id.0
            );
            self.get_explore_group_task_mut(task_id)
                .unwrap()
                .transform_expr_in
                .extend(transform_tasks);
        }

        self.get_explore_group_task_mut(task_id)
            .unwrap()
            .dispatched_exprs = all_logical_exprs.clone();

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
    #[tracing::instrument(
        level = "debug",
        skip_all,
        name = "consolidate_explore_group_tasks",
        fields(principal_task_id = ?principal_task_id, num_secondaries = secondary_task_ids.len()),
        target = "optd::optimizer::merge"
    )]
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

            tracing::trace!(
                target: "optd::optimizer::merge",
                "Deleting secondary explore group task {} and moving {} fork and {} goal tasks", task_id.0, fork_tasks.len(), goal_tasks.len()
            );
            self.delete_task(task_id);

            fork_tasks.into_iter().for_each(|fork_id| {
                let fork_task = self.get_fork_logical_task_mut(fork_id).unwrap();
                fork_task.explore_group_in = principal_task_id;
            });

            goal_tasks.into_iter().for_each(|goal_id| {
                let goal_task = self.get_optimize_goal_task_mut(goal_id).unwrap();
                goal_task.explore_group_in = principal_task_id;
            });

            // *NOTE*: No need to consolidate transformations as all missing
            // expressions have already been added during the update phase.
        });
    }

    /// Consolidate a goal optimization task into a principal task.
    ///
    /// - Similar to the group exploration consolidation, a merge in the memo may cause
    ///   several goal optimization tasks to refer to the same underlying goal. This function
    ///   consolidates all such secondary tasks into a principal one.
    ///
    /// - This involves:
    ///   - Moving all outgoing optimize goal tasks, incoming optimize goal tasks, and
    ///     optimize plan tasks from secondary to principal.
    ///   - Updating each such task's references to point to the principal task.
    ///   - Deleting the secondary tasks.
    ///
    /// # Arguments
    /// * `principal_task_id` - The ID of the task to retain as canonical.
    /// * `secondary_task_ids` - All other task IDs to merge into the principal.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        name = "consolidate_optimize_goal_tasks",
        fields(principal_task_id = ?principal_task_id, num_secondaries = secondary_task_ids.len()),
        target = "optd::optimizer::merge"
    )]
    pub(super) async fn consolidate_goal_optimize(
        &mut self,
        principal_task_id: TaskId,
        secondary_task_ids: &[TaskId],
    ) {
        secondary_task_ids.iter().for_each(|&task_id| {
            let task = self.get_optimize_goal_task_mut(task_id).unwrap();

            // Move out the task sets before deletion.
            let optimize_out_tasks = std::mem::take(&mut task.optimize_goal_out);
            let optimize_in_tasks = std::mem::take(&mut task.optimize_goal_in);
            let optimize_plan_tasks = std::mem::take(&mut task.optimize_plan_out);

            tracing::trace!(
                target: "optd::optimizer::merge",
                "Deleting secondary optimize goal task {} and moving dependencies", task_id.0
            );
            self.delete_task(task_id);

            optimize_out_tasks.into_iter().for_each(|optimize_id| {
                let optimize_goal_task = self.get_optimize_goal_task_mut(optimize_id).unwrap();
                optimize_goal_task.optimize_goal_in.remove(&task_id);
                optimize_goal_task
                    .optimize_goal_in
                    .insert(principal_task_id);
            });

            optimize_in_tasks.into_iter().for_each(|optimize_id| {
                let optimize_goal_task = self.get_optimize_goal_task_mut(optimize_id).unwrap();
                optimize_goal_task.optimize_goal_out.remove(&task_id);
                optimize_goal_task
                    .optimize_goal_out
                    .insert(principal_task_id);
            });

            optimize_plan_tasks.into_iter().for_each(|optimize_id| {
                let optimize_plan_task = self.get_optimize_plan_task_mut(optimize_id).unwrap();
                optimize_plan_task.optimize_goal_in = Some(principal_task_id);
            });

            // *NOTE*: No need to consolidate implementations as all missing
            // expressions have already been added during the update phase.
        });
    }

    /// Deduplicate dispatched expressions for a group.
    ///
    /// - During optimization, logical expressions in a group may get merged
    ///   in the memo structure. As a result, multiple expressions in the same
    ///   group might now resolve to the same canonical form.
    ///
    /// - This function:
    ///   - Maps each expression to its current representative.
    ///   - Identifies and prunes redundant (duplicate) expressions.
    ///   - Updates or deletes related transform, implement and continuation
    ///     tasks accordingly.
    ///
    /// # Arguments
    /// * `task_id` - The ID of the group exploration task to deduplicate.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        name = "deduplicate_group_tasks",
        fields(task_id = ?task_id),
        target = "optd::optimizer::merge"
    )]
    pub(super) async fn dedup_tasks(&mut self, task_id: TaskId) -> Result<(), M::MemoError> {
        let task = self.get_explore_group_task_mut(task_id).unwrap();
        let old_exprs = std::mem::take(&mut task.dispatched_exprs);
        let transform_ids: Vec<_> = task.transform_expr_in.iter().copied().collect();
        let fork_ids: Vec<_> = task.fork_logical_out.iter().copied().collect();
        let optimize_ids: Vec<_> = task.optimize_goal_out.iter().copied().collect();

        let expr_to_repr = self.map_to_representatives(&old_exprs).await?;

        let (unique_reprs, to_delete) = {
            let mut seen = HashSet::new();
            let mut dups = vec![];

            for (&expr_id, &repr_id) in &expr_to_repr {
                if !seen.insert(repr_id) {
                    dups.push(expr_id);
                }
            }

            tracing::trace!(
                target: "optd::optimizer::merge",
                "Deduplicating tasks: {} unique expressions, {} duplicates to delete", seen.len(), dups.len()
            );
            (seen, dups)
        };

        self.dedup_transform_tasks(&transform_ids, &expr_to_repr, &to_delete);
        self.dedup_continue_tasks(&fork_ids, &expr_to_repr, &to_delete);
        self.dedup_implement_tasks(&optimize_ids, &expr_to_repr, &to_delete);

        let task = self.get_explore_group_task_mut(task_id).unwrap();
        task.dispatched_exprs = unique_reprs;

        Ok(())
    }

    /// For each logical expression, find its current representative in the memo.
    async fn map_to_representatives(
        &mut self,
        exprs: &HashSet<LogicalExpressionId>,
    ) -> Result<HashMap<LogicalExpressionId, LogicalExpressionId>, M::MemoError> {
        let mut expr_to_repr = HashMap::with_capacity(exprs.len());
        for expr_id in exprs {
            let repr_id = self.memo.find_repr_logical_expr_id(*expr_id).await?;
            expr_to_repr.insert(*expr_id, repr_id);
        }
        Ok(expr_to_repr)
    }

    /// Update or delete transform tasks based on deduplicated expressions.
    fn dedup_transform_tasks(
        &mut self,
        transform_ids: &[TaskId],
        expr_to_repr: &HashMap<LogicalExpressionId, LogicalExpressionId>,
        to_delete: &[LogicalExpressionId],
    ) {
        for &transform_id in transform_ids {
            let expr_id = self
                .get_transform_expression_task(transform_id)
                .unwrap()
                .expression_id;

            if to_delete.contains(&expr_id) {
                self.delete_task(transform_id);
            } else {
                self.get_transform_expression_task_mut(transform_id)
                    .unwrap()
                    .expression_id = expr_to_repr[&expr_id];
            }
        }
    }

    /// Update or delete continuation tasks spawned by fork tasks.
    fn dedup_continue_tasks(
        &mut self,
        fork_ids: &[TaskId],
        expr_to_repr: &HashMap<LogicalExpressionId, LogicalExpressionId>,
        to_delete: &[LogicalExpressionId],
    ) {
        for &fork_id in fork_ids {
            let continue_ids = self
                .get_fork_logical_task(fork_id)
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

    /// Update or delete implement tasks based on deduplicated expressions.
    fn dedup_implement_tasks(
        &mut self,
        optimize_goals: &[TaskId],
        expr_to_repr: &HashMap<LogicalExpressionId, LogicalExpressionId>,
        to_delete: &[LogicalExpressionId],
    ) {
        for &optimize_goal in optimize_goals {
            let implement_ids = self
                .get_optimize_goal_task(optimize_goal)
                .unwrap()
                .implement_expression_in
                .clone(); // Clone to avoid mutable borrow conflict.

            for implement_id in implement_ids {
                let expr_id = self
                    .get_implement_expression_task(implement_id)
                    .unwrap()
                    .expression_id;

                if to_delete.contains(&expr_id) {
                    self.delete_task(implement_id);
                } else {
                    self.get_implement_expression_task_mut(implement_id)
                        .unwrap()
                        .expression_id = expr_to_repr[&expr_id];
                }
            }
        }
    }
}
