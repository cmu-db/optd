#![allow(unused)]
use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::Hash;

use crate::cir::cost_is_better;

use super::Memoize;
use super::*;

#[derive(Default)]
pub struct MockMemo {
    groups: HashMap<GroupId, GroupState>,
    logical_exprs: HashMap<LogicalExpressionId, LogicalExpression>,
    logical_expr_node_to_id_index: HashMap<LogicalExpression, LogicalExpressionId>,
    physical_exprs: HashMap<PhysicalExpressionId, (PhysicalExpression, Option<Cost>)>,
    goals: HashMap<GoalId, GoalState>,
    goal_node_to_id_index: HashMap<Goal, GoalId>,
    member_subscribers: HashMap<GoalMemberId, HashSet<GoalId>>,
    physical_expr_node_to_id_index: HashMap<PhysicalExpression, PhysicalExpressionId>,

    logical_expr_group_index: HashMap<LogicalExpressionId, GroupId>,
    best_optimized_physical_expr_index: HashMap<GoalId, (PhysicalExpressionId, Cost)>,

    next_shared_id: i64,
}

struct GroupState {
    properties: Option<LogicalProperties>,
    logical_exprs: HashSet<LogicalExpressionId>,
}

impl GroupState {
    fn new(logical_expr_id: LogicalExpressionId) -> Self {
        let mut logical_exprs = HashSet::new();
        logical_exprs.insert(logical_expr_id);
        Self {
            properties: None,
            logical_exprs,
        }
    }
}

struct GoalState {
    goal: Goal,
    members: HashSet<GoalMemberId>,
}

impl GoalState {
    fn new(goal: Goal) -> Self {
        Self {
            goal,
            members: HashSet::new(),
        }
    }
}

impl Memoize for MockMemo {
    async fn get_logical_properties(
        &self,
        group_id: GroupId,
    ) -> MemoizeResult<Option<LogicalProperties>> {
        let group = self
            .groups
            .get(&group_id)
            .ok_or_else(|| MemoizeError::GroupNotFound(group_id))?;

        Ok(group.properties.clone())
    }

    async fn set_logical_properties(
        &mut self,
        group_id: GroupId,
        props: LogicalProperties,
    ) -> MemoizeResult<()> {
        let group = self
            .groups
            .get_mut(&group_id)
            .ok_or_else(|| MemoizeError::GroupNotFound(group_id))?;

        group.properties = Some(props);
        Ok(())
    }

    async fn get_all_logical_exprs(
        &self,
        group_id: GroupId,
    ) -> MemoizeResult<Vec<LogicalExpressionId>> {
        let group = self
            .groups
            .get(&group_id)
            .ok_or_else(|| MemoizeError::GroupNotFound(group_id))?;

        Ok(group.logical_exprs.iter().cloned().collect())
    }

    async fn get_any_logical_expr(&self, group_id: GroupId) -> MemoizeResult<LogicalExpressionId> {
        let group = self
            .groups
            .get(&group_id)
            .ok_or_else(|| MemoizeError::GroupNotFound(group_id))?;

        group
            .logical_exprs
            .iter()
            .next()
            .cloned()
            .ok_or_else(|| MemoizeError::NoLogicalExprInGroup(group_id))
    }

    async fn find_logical_expr_group(
        &self,
        logical_expr_id: LogicalExpressionId,
    ) -> MemoizeResult<Option<GroupId>> {
        let maybe_group_id = self.logical_expr_group_index.get(&logical_expr_id).cloned();
        Ok(maybe_group_id)
    }

    async fn create_group(
        &mut self,
        logical_expr_id: LogicalExpressionId,
    ) -> MemoizeResult<GroupId> {
        let group_id = self.next_group_id();
        let group = GroupState::new(logical_expr_id);
        self.groups.insert(group_id, group);
        self.logical_expr_group_index
            .insert(logical_expr_id, group_id);

        Ok(group_id)
    }

    async fn merge_groups(
        &mut self,
        group_id_1: GroupId,
        group_id_2: GroupId,
    ) -> MemoizeResult<MergeResult> {
        todo!()
    }

    async fn get_best_optimized_physical_expr(
        &self,
        goal_id: GoalId,
    ) -> MemoizeResult<Option<(PhysicalExpressionId, Cost)>> {
        let maybe_best_costed = self
            .best_optimized_physical_expr_index
            .get(&goal_id)
            .cloned();
        Ok(maybe_best_costed)
    }

    async fn get_all_goal_members(&self, goal_id: GoalId) -> MemoizeResult<Vec<GoalMemberId>> {
        let goal_state = self
            .goals
            .get(&goal_id)
            .ok_or_else(|| MemoizeError::GoalNotFound(goal_id))?;
        Ok(goal_state.members.iter().cloned().collect())
    }

    async fn add_goal_member(
        &mut self,
        goal_id: GoalId,
        member: GoalMemberId,
    ) -> MemoizeResult<bool> {
        let goal_state = self
            .goals
            .get_mut(&goal_id)
            .ok_or_else(|| MemoizeError::GoalNotFound(goal_id))?;

        let is_new = goal_state.members.insert(member);
        if is_new {
            // Create a new subscriber for the member (initialize the set if it doesn't exist).
            self.member_subscribers
                .entry(member)
                .or_default()
                .insert(goal_id);

            let new_member_cost = match member {
                GoalMemberId::PhysicalExpressionId(physical_expr_id) => self
                    .get_physical_expr_cost(physical_expr_id)
                    .await?
                    .map(|c| (physical_expr_id, c)),
                GoalMemberId::GoalId(member_goal_id) => {
                    self.get_best_optimized_physical_expr(member_goal_id)
                        .await?
                }
            };

            let mut subscribers = VecDeque::new();
            subscribers.push_back(goal_id);

            self.propagate_new_member_cost(new_member_cost, subscribers)
                .await?;
        }
        Ok(is_new)
    }

    async fn get_physical_expr_cost(
        &self,
        physical_expr_id: PhysicalExpressionId,
    ) -> MemoizeResult<Option<Cost>> {
        let (_, cost) = self
            .physical_exprs
            .get(&physical_expr_id)
            .ok_or_else(|| MemoizeError::PhysicalExprNotFound(physical_expr_id))?;
        Ok(*cost)
    }

    async fn update_physical_expr_cost(
        &mut self,
        physical_expr_id: PhysicalExpressionId,
        new_cost: Cost,
    ) -> MemoizeResult<bool> {
        let (_, cost_mut) = self
            .physical_exprs
            .get_mut(&physical_expr_id)
            .ok_or_else(|| MemoizeError::PhysicalExprNotFound(physical_expr_id))?;
        let is_better = cost_mut
            .replace(new_cost)
            .map(|old_cost| new_cost < old_cost)
            .unwrap_or(true);

        if is_better {
            let mut subscribers = VecDeque::new();
            // keep propagating the new cost to all subscribers.
            if let Some(subscriber_goal_ids) = self
                .member_subscribers
                .get(&&GoalMemberId::PhysicalExpressionId(physical_expr_id))
                .map(|goals| goals.iter().cloned())
            {
                subscribers.extend(subscriber_goal_ids);
            }
            // propagate the new cost to all subscribers.
            self.propagate_new_member_cost(Some((physical_expr_id, new_cost)), subscribers)
                .await?;
        }
        Ok(is_better)
    }

    async fn get_transformation_status(
        &self,
        logical_expr_id: LogicalExpressionId,
        rule: &TransformationRule,
    ) -> MemoizeResult<Status> {
        todo!()
    }

    async fn set_transformation_clean(
        &mut self,
        logical_expr_id: LogicalExpressionId,
        rule: &TransformationRule,
    ) -> MemoizeResult<()> {
        todo!()
    }

    async fn get_implementation_status(
        &self,
        logical_expr_id: LogicalExpressionId,
        goal_id: GoalId,
        rule: &ImplementationRule,
    ) -> MemoizeResult<Status> {
        todo!()
    }

    async fn set_implementation_clean(
        &mut self,
        logical_expr_id: LogicalExpressionId,
        goal_id: GoalId,
        rule: &ImplementationRule,
    ) -> MemoizeResult<()> {
        todo!()
    }

    async fn get_cost_status(
        &self,
        physical_expr_id: PhysicalExpressionId,
    ) -> MemoizeResult<Status> {
        Ok(Status::Clean)
    }

    async fn set_cost_clean(
        &mut self,
        physical_expr_id: PhysicalExpressionId,
    ) -> MemoizeResult<()> {
        Ok(())
    }

    async fn add_transformation_dependency(
        &mut self,
        logical_expr_id: LogicalExpressionId,
        rule: &TransformationRule,
        group_id: GroupId,
    ) -> MemoizeResult<()> {
        Ok(())
    }

    async fn add_implementation_dependency(
        &mut self,
        logical_expr_id: LogicalExpressionId,
        goal_id: GoalId,
        rule: &ImplementationRule,
        group_id: GroupId,
    ) -> MemoizeResult<()> {
        Ok(())
    }

    async fn add_cost_dependency(
        &mut self,
        physical_expr_id: PhysicalExpressionId,
        goal_id: GoalId,
    ) -> MemoizeResult<()> {
        Ok(())
    }

    async fn get_goal_id(&mut self, goal: &Goal) -> MemoizeResult<GoalId> {
        if let Some(goal_id) = self.goal_node_to_id_index.get(goal).cloned() {
            return Ok(goal_id);
        }
        let goal_id = self.next_goal_id();
        self.goal_node_to_id_index.insert(goal.clone(), goal_id);
        self.goals.insert(goal_id, GoalState::new(goal.clone()));
        Ok(goal_id)
    }

    async fn materialize_goal(&self, goal_id: GoalId) -> MemoizeResult<Goal> {
        let goal = self
            .goals
            .get(&goal_id)
            .ok_or_else(|| MemoizeError::GoalNotFound(goal_id))?
            .goal
            .clone();
        Ok(goal)
    }

    async fn get_logical_expr_id(
        &mut self,
        logical_expr: &LogicalExpression,
    ) -> MemoizeResult<LogicalExpressionId> {
        if let Some(logical_expr_id) = self
            .logical_expr_node_to_id_index
            .get(logical_expr)
            .cloned()
        {
            return Ok(logical_expr_id);
        }
        let logical_expr_id = self.next_logical_expr_id();
        self.logical_expr_node_to_id_index
            .insert(logical_expr.clone(), logical_expr_id);
        self.logical_exprs
            .insert(logical_expr_id, logical_expr.clone());
        Ok(logical_expr_id)
    }

    async fn materialize_logical_expr(
        &self,
        logical_expr_id: LogicalExpressionId,
    ) -> MemoizeResult<LogicalExpression> {
        let logical_expr = self
            .logical_exprs
            .get(&logical_expr_id)
            .ok_or_else(|| MemoizeError::LogicalExprNotFound(logical_expr_id))?
            .clone();
        Ok(logical_expr)
    }

    async fn get_physical_expr_id(
        &mut self,
        physical_expr: &PhysicalExpression,
    ) -> MemoizeResult<PhysicalExpressionId> {
        if let Some(physical_expr_id) = self
            .physical_expr_node_to_id_index
            .get(physical_expr)
            .cloned()
        {
            return Ok(physical_expr_id);
        }
        let physical_expr_id = self.next_physical_expr_id();
        self.physical_expr_node_to_id_index
            .insert(physical_expr.clone(), physical_expr_id);
        self.physical_exprs
            .insert(physical_expr_id, (physical_expr.clone(), None));
        Ok(physical_expr_id)
    }

    async fn materialize_physical_expr(
        &self,
        physical_expr_id: PhysicalExpressionId,
    ) -> MemoizeResult<PhysicalExpression> {
        let physical_expr = self
            .physical_exprs
            .get(&physical_expr_id)
            .ok_or_else(|| MemoizeError::PhysicalExprNotFound(physical_expr_id))?
            .0
            .clone();
        Ok(physical_expr)
    }

    async fn find_repr_group(&self, group_id: GroupId) -> MemoizeResult<GroupId> {
        Ok(group_id)
    }

    async fn find_repr_goal(&self, goal_id: GoalId) -> MemoizeResult<GoalId> {
        Ok(goal_id)
    }

    async fn find_repr_logical_expr(
        &self,
        logical_expr_id: LogicalExpressionId,
    ) -> MemoizeResult<LogicalExpressionId> {
        Ok(logical_expr_id)
    }

    async fn find_repr_physical_expr(
        &self,
        physical_expr_id: PhysicalExpressionId,
    ) -> MemoizeResult<PhysicalExpressionId> {
        Ok(physical_expr_id)
    }
}

impl MockMemo {
    fn next_group_id(&mut self) -> GroupId {
        let group_id = GroupId(self.next_shared_id);
        self.next_shared_id += 1;
        group_id
    }

    fn next_physical_expr_id(&mut self) -> PhysicalExpressionId {
        let physical_expr_id = PhysicalExpressionId(self.next_shared_id);
        self.next_shared_id += 1;
        physical_expr_id
    }

    fn next_logical_expr_id(&mut self) -> LogicalExpressionId {
        let logical_expr_id = LogicalExpressionId(self.next_shared_id);
        self.next_shared_id += 1;
        logical_expr_id
    }

    fn next_goal_id(&mut self) -> GoalId {
        let goal_id = GoalId(self.next_shared_id);
        self.next_shared_id += 1;
        goal_id
    }

    // TODO(yuchen): make this thing return a list of goal ids that have their best cost updated.
    async fn propagate_new_member_cost(
        &mut self,
        new_member_cost: Option<(PhysicalExpressionId, Cost)>,
        mut subscribers: VecDeque<GoalId>,
    ) -> MemoizeResult<()> {
        while let Some(goal_id) = subscribers.pop_front() {
            let current_best = self.get_best_optimized_physical_expr(goal_id).await?;

            if cost_is_better(new_member_cost, current_best) {
                // Update the best cost for the goal.
                self.best_optimized_physical_expr_index
                    .insert(goal_id, new_member_cost.unwrap());

                // keep propagating the new cost to all subscribers.
                if let Some(subscriber_goal_ids) = self
                    .member_subscribers
                    .get(&GoalMemberId::GoalId(goal_id))
                    .map(|goals| goals.iter().cloned().collect::<Vec<_>>())
                {
                    for subscriber_goal_id in subscriber_goal_ids {
                        subscribers.push_back(subscriber_goal_id);
                    }
                }
            }
        }

        Ok(())
    }
}
