use std::collections::{HashMap, HashSet};

use crate::{
    cir::LogicalExpressionId,
    memo::Memo,
    optimizer::{Optimizer, errors::OptimizeError, tasks::TaskId},
};

impl<M: Memo> Optimizer<M> {
    /// Deduplicate dispatched expressions for a group exploration task.
    ///
    /// - Logical expressions may be merged in the memo, so multiple dispatched expressions may now
    ///   point to the same representative. We want to avoid redundant work.
    /// - This function ensures we only explore unique representatives and prunes redundant transform
    ///   and continuation tasks.
    pub(super) async fn dedup_group_explore(
        &mut self,
        task_id: TaskId,
    ) -> Result<(), OptimizeError> {
        // Take ownership and clone to avoid conflicts with mutable borrow.
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
                    dups.push(expr_id); // We've already seen this repr, so prune the original expr.
                }
            }

            (seen, dups)
        };

        self.process_transform_tasks(&transform_ids, &expr_to_repr, &to_delete);
        self.process_fork_tasks(&fork_ids, &expr_to_repr, &to_delete);

        let task = self.get_explore_group_task_mut(task_id).unwrap();
        // Update the task with the deduplicated expressions.
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
    ///
    /// - If the expression was deduplicated away, remove the task.
    /// - Otherwise, update it to the new representative ID.
    fn process_transform_tasks(
        &mut self,
        tasks: &[TaskId],
        expr_to_repr: &HashMap<LogicalExpressionId, LogicalExpressionId>,
        to_delete: &Vec<LogicalExpressionId>,
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
    ///
    /// - Continuation tasks rely on dispatched expressions; if deduplicated, remove them.
    /// - Otherwise, update them to reflect their canonical expression.
    fn process_fork_tasks<'a>(
        &mut self,
        tasks: &[TaskId],
        expr_to_repr: &HashMap<LogicalExpressionId, LogicalExpressionId>,
        to_delete: &Vec<LogicalExpressionId>,
    ) {
        for &task_id in tasks {
            let continue_ids = self
                .get_fork_logical_task(task_id)
                .unwrap()
                .continue_with_logical_in
                .clone(); // Clone to avoid borrow conflicts when mutating `self`.

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
