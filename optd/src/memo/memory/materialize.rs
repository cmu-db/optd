use super::MemoryMemo;
use crate::{
    cir::{
        Child, Goal, GoalId, LogicalExpression, LogicalExpressionId, PhysicalExpression,
        PhysicalExpressionId,
    },
    memo::{Materialize, MemoError, Representative, error::MemoResult},
};
use std::collections::HashSet;

impl Materialize for MemoryMemo {
    async fn get_goal_id(&mut self, goal: &Goal) -> MemoResult<GoalId> {
        // Check if the goal is already in the memo table.
        let goal = self.remap_goal(goal).await?;
        if let Some(&goal_id) = self.goal_to_id.get(&goal) {
            return Ok(goal_id);
        }

        // Otherwise, create a new entry in the memo table.
        let goal_id = GoalId(self.next_shared_id());
        self.id_to_goal.insert(goal_id, goal.clone());
        self.goal_to_id.insert(goal.clone(), goal_id);

        Ok(goal_id)
    }

    async fn materialize_goal(&self, goal_id: GoalId) -> MemoResult<Goal> {
        let repr_goal_id = self.find_repr_goal_id(goal_id).await?;
        self.id_to_goal
            .get(&repr_goal_id)
            .ok_or(MemoError::GoalNotFound(repr_goal_id))
            .cloned()
    }

    async fn get_logical_expr_id(
        &mut self,
        logical_expr: &LogicalExpression,
    ) -> MemoResult<LogicalExpressionId> {
        use Child::*;

        // Check if the expression is already in the memo table.
        let remapped_expr = self.remap_logical_expr(logical_expr).await?;
        if let Some(&expr_id) = self.logical_expr_to_id.get(&remapped_expr) {
            return Ok(expr_id);
        }

        // Otherwise, create a new entry in the memo table.
        let expr_id = LogicalExpressionId(self.next_shared_id());
        self.id_to_logical_expr
            .insert(expr_id, remapped_expr.clone());
        self.logical_expr_to_id
            .insert(remapped_expr.clone(), expr_id);

        // Update the logical expression to group index.
        remapped_expr
            .children
            .iter()
            .flat_map(|child| match child {
                Singleton(group_id) => vec![*group_id],
                VarLength(group_ids) => group_ids.clone(),
            })
            .for_each(|group_id| {
                self.group_referencing_exprs_index
                    .entry(group_id)
                    .or_insert_with(HashSet::new)
                    .insert(expr_id);
            });

        Ok(expr_id)
    }

    async fn materialize_logical_expr(
        &self,
        logical_expr_id: LogicalExpressionId,
    ) -> MemoResult<LogicalExpression> {
        let repr_expr_id = self.find_repr_logical_expr_id(logical_expr_id).await?;
        self.id_to_logical_expr
            .get(&repr_expr_id)
            .ok_or(MemoError::LogicalExprNotFound(repr_expr_id))
            .cloned()
    }

    async fn get_physical_expr_id(
        &mut self,
        _physical_expr: &PhysicalExpression,
    ) -> MemoResult<PhysicalExpressionId> {
        todo!()
    }

    async fn materialize_physical_expr(
        &self,
        _physical_expr_id: PhysicalExpressionId,
    ) -> MemoResult<PhysicalExpression> {
        todo!()
    }
}

// Remapping helpers to get canonical expressions, goals, etc.
impl MemoryMemo {
    pub(super) async fn remap_goal(&mut self, goal: &Goal) -> MemoResult<Goal> {
        let Goal(group_id, logical_expr) = goal;
        let group_id = self.find_repr_group_id(*group_id).await?;
        Ok(Goal(group_id, logical_expr.clone()))
    }

    pub(super) async fn remap_logical_expr(
        &mut self,
        logical_expr: &LogicalExpression,
    ) -> MemoResult<LogicalExpression> {
        use Child::*;

        let mut remapped_children = Vec::with_capacity(logical_expr.children.len());

        for child in &logical_expr.children {
            let remapped = match child {
                Singleton(group_id) => {
                    let repr = self.find_repr_group_id(*group_id).await?;
                    Singleton(repr)
                }
                VarLength(group_ids) => {
                    let mut reprs = Vec::with_capacity(group_ids.len());
                    for group_id in group_ids {
                        reprs.push(self.find_repr_group_id(*group_id).await?);
                    }
                    VarLength(reprs)
                }
            };
            remapped_children.push(remapped);
        }

        Ok(LogicalExpression {
            tag: logical_expr.tag.clone(),
            data: logical_expr.data.clone(),
            children: remapped_children,
        })
    }
}
