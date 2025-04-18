use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet, VecDeque};

use super::Memoize;
use super::merge_repr::Representative;
use super::*;

/// An in-memory implementation of the memo table.
#[derive(Default)]
pub struct MemoryMemo {
    /// Group id to state.
    groups: HashMap<GroupId, GroupState>,

    /// Logical expression id to node.
    logical_exprs: HashMap<LogicalExpressionId, LogicalExpression>,
    /// Logical expression node to id.
    logical_expr_node_to_id_index: HashMap<LogicalExpression, LogicalExpressionId>,
    /// A mapping from logical expression id to group id.
    logical_expr_group_index: HashMap<LogicalExpressionId, GroupId>,

    /// Physical expression id to node.
    physical_exprs: HashMap<PhysicalExpressionId, (PhysicalExpression, Option<Cost>)>,
    /// Physical expression node to id.
    physical_expr_node_to_id_index: HashMap<PhysicalExpression, PhysicalExpressionId>,

    /// Goal id to state.
    goals: HashMap<GoalId, GoalState>,
    /// Goal node to id.
    goal_node_to_id_index: HashMap<Goal, GoalId>,

    /// A mapping from goal member to the set of goal ids that depend on it.
    member_subscribers: HashMap<GoalMemberId, HashSet<GoalId>>,

    /// best optimized physical expression for each goal id.
    best_optimized_physical_expr_index: HashMap<GoalId, (PhysicalExpressionId, Cost)>,

    /// The shared next unique id to be used for goals, groups, logical expressions, and physical expressions.
    next_shared_id: i64,

    repr_group: Representative<GroupId>,
    repr_goal: Representative<GoalId>,
    repr_logical_expr: Representative<LogicalExpressionId>,
    repr_physical_expr: Representative<PhysicalExpressionId>,

    transform_dependency: HashMap<LogicalExpressionId, HashMap<TransformationRule, RuleDependency>>,
    implement_dependency:
        HashMap<LogicalExpressionId, HashMap<(GoalId, ImplementationRule), RuleDependency>>,
    cost_dependency: HashMap<PhysicalExpressionId, CostDependency>,
}

struct RuleDependency {
    group_ids: HashSet<GroupId>,
    status: Status,
}

impl RuleDependency {
    fn new(status: Status) -> Self {
        let group_ids = HashSet::new();
        Self { group_ids, status }
    }
}

struct CostDependency {
    goal_ids: HashSet<GoalId>,
    status: Status,
}

impl CostDependency {
    fn new(status: Status) -> Self {
        let goal_ids = HashSet::new();
        Self { goal_ids, status }
    }
}

/// State of a group in the memo structure.
struct GroupState {
    /// The logical properties of the group, might be `None` if it hasn't been derived yet.
    properties: Option<LogicalProperties>,
    logical_exprs: HashSet<LogicalExpressionId>,
    goals: HashSet<GoalId>,
}

impl GroupState {
    fn new(logical_expr_id: LogicalExpressionId) -> Self {
        let mut logical_exprs = HashSet::new();
        logical_exprs.insert(logical_expr_id);
        Self {
            properties: None,
            logical_exprs,
            goals: HashSet::new(),
        }
    }
}

struct GoalState {
    /// The set of members that are part of this goal.
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

impl Memoize for MemoryMemo {
    async fn get_logical_properties(
        &self,
        group_id: GroupId,
    ) -> MemoizeResult<Option<LogicalProperties>> {
        let group_id = self.find_repr_group(group_id).await?;
        let group = self
            .groups
            .get(&group_id)
            .ok_or(MemoizeError::GroupNotFound(group_id))?;

        Ok(group.properties.clone())
    }

    async fn set_logical_properties(
        &mut self,
        group_id: GroupId,
        props: LogicalProperties,
    ) -> MemoizeResult<()> {
        let group_id = self.find_repr_group(group_id).await?;
        let group = self
            .groups
            .get_mut(&group_id)
            .ok_or(MemoizeError::GroupNotFound(group_id))?;

        group.properties = Some(props);
        Ok(())
    }

    async fn get_all_logical_exprs(
        &self,
        group_id: GroupId,
    ) -> MemoizeResult<Vec<LogicalExpressionId>> {
        let group_id = self.find_repr_group(group_id).await?;
        let group = self
            .groups
            .get(&group_id)
            .ok_or(MemoizeError::GroupNotFound(group_id))?;

        Ok(group.logical_exprs.iter().cloned().collect())
    }

    async fn get_any_logical_expr(&self, group_id: GroupId) -> MemoizeResult<LogicalExpressionId> {
        let group_id = self.find_repr_group(group_id).await?;
        let group = self
            .groups
            .get(&group_id)
            .ok_or(MemoizeError::GroupNotFound(group_id))?;

        group
            .logical_exprs
            .iter()
            .next()
            .cloned()
            .ok_or(MemoizeError::NoLogicalExprInGroup(group_id))
    }

    async fn find_logical_expr_group(
        &self,
        logical_expr_id: LogicalExpressionId,
    ) -> MemoizeResult<Option<GroupId>> {
        let logical_expr_id = self.find_repr_logical_expr(logical_expr_id).await?;
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
    ) -> MemoizeResult<Option<MergeResult>> {
        // our strategy is to always merge group 2 into group 1.
        let group_id_1 = self.find_repr_group(group_id_1).await?;
        let group_id_2 = self.find_repr_group(group_id_2).await?;

        if group_id_1 == group_id_2 {
            return Ok(None);
        }
        let mut result = MergeResult::default();

        let group_2_state = self.groups.remove(&group_id_2).unwrap();
        let group_2_exprs = group_2_state.logical_exprs.iter().cloned().collect();
        // let goal_2_goals = group_2_state.goals.iter().cloned().collect();

        let group1_state = self.groups.get_mut(&group_id_1).unwrap();
        let group1_exprs = group1_state.logical_exprs.iter().cloned().collect();
        // let group_1_goals = group1_state.goals.iter().cloned().collect();

        for logical_expr_id in group_2_state.logical_exprs {
            // Update the logical expression to point to the new group id.
            let old_group_id = self
                .logical_expr_group_index
                .insert(logical_expr_id, group_id_1);
            assert!(old_group_id.is_some());
            group1_state.logical_exprs.insert(logical_expr_id);
        }
        let mut merge_group_result = MergeGroupResult::new(group_id_1);
        merge_group_result
            .merged_groups
            .insert(group_id_1, group1_exprs);
        merge_group_result
            .merged_groups
            .insert(group_id_2, group_2_exprs);

        self.repr_group.merge(&group_id_2, &group_id_1);

        result.group_merges.push(merge_group_result);

        // TODO(yuchen): one-level memo goal merges.

        Ok(Some(result))
    }

    async fn get_best_optimized_physical_expr(
        &self,
        goal_id: GoalId,
    ) -> MemoizeResult<Option<(PhysicalExpressionId, Cost)>> {
        let goal_id = self.find_repr_goal(goal_id).await?;
        let maybe_best_costed = self
            .best_optimized_physical_expr_index
            .get(&goal_id)
            .cloned();
        Ok(maybe_best_costed)
    }

    async fn get_all_goal_members(&self, goal_id: GoalId) -> MemoizeResult<Vec<GoalMemberId>> {
        let goal_id = self.find_repr_goal(goal_id).await?;
        let goal_state = self.goals.get(&goal_id).unwrap();
        Ok(goal_state.members.iter().cloned().collect())
    }

    async fn add_goal_member(
        &mut self,
        goal_id: GoalId,
        member: GoalMemberId,
    ) -> MemoizeResult<Option<ForwardResult>> {
        let goal_id = self.find_repr_goal(goal_id).await?;
        let member = self.find_repr_goal_member(member).await?;
        let goal_state = self.goals.get_mut(&goal_id).unwrap();

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

            let Some((physical_expr_id, cost)) = new_member_cost else {
                return Ok(None);
            };
            let mut subscribers = VecDeque::new();
            subscribers.push_back(goal_id);
            let mut result = ForwardResult::new(physical_expr_id, cost);
            // propagate the new cost to all subscribers.
            self.propagate_new_member_cost(subscribers, &mut result)
                .await?;
            if result.goals_forwarded.is_empty() {
                // No goals were forwarded, so we can return None.
                Ok(None)
            } else {
                // Some goals were forwarded, so we return the result.
                Ok(Some(result))
            }
        } else {
            Ok(None)
        }
    }

    async fn get_physical_expr_cost(
        &self,
        physical_expr_id: PhysicalExpressionId,
    ) -> MemoizeResult<Option<Cost>> {
        let physical_expr_id = self.find_repr_physical_expr(physical_expr_id).await?;
        let (_, maybe_cost) = self
            .physical_exprs
            .get(&physical_expr_id)
            .ok_or(MemoizeError::PhysicalExprNotFound(physical_expr_id))?;
        Ok(*maybe_cost)
    }

    async fn update_physical_expr_cost(
        &mut self,
        physical_expr_id: PhysicalExpressionId,
        new_cost: Cost,
    ) -> MemoizeResult<Option<ForwardResult>> {
        let physical_expr_id = self.find_repr_physical_expr(physical_expr_id).await?;
        let (_, cost_mut) = self
            .physical_exprs
            .get_mut(&physical_expr_id)
            .ok_or(MemoizeError::PhysicalExprNotFound(physical_expr_id))?;
        let is_better = cost_mut
            .replace(new_cost)
            .map(|old_cost| new_cost < old_cost)
            .unwrap_or(true);

        if is_better {
            let mut subscribers = VecDeque::new();
            // keep propagating the new cost to all subscribers.
            if let Some(subscriber_goal_ids) = self
                .member_subscribers
                .get(&GoalMemberId::PhysicalExpressionId(physical_expr_id))
                .map(|goals| goals.iter().cloned())
            {
                subscribers.extend(subscriber_goal_ids);
            }

            let mut result = ForwardResult::new(physical_expr_id, new_cost);
            // propagate the new cost to all subscribers.
            self.propagate_new_member_cost(subscribers, &mut result)
                .await?;
            if result.goals_forwarded.is_empty() {
                // No goals were forwarded, so we can return None.
                Ok(None)
            } else {
                // Some goals were forwarded, so we return the result.
                Ok(Some(result))
            }
        } else {
            Ok(None)
        }
    }

    async fn get_transformation_status(
        &self,
        logical_expr_id: LogicalExpressionId,
        rule: &TransformationRule,
    ) -> MemoizeResult<Status> {
        let logical_expr_id = self.find_repr_logical_expr(logical_expr_id).await?;
        let status = self
            .transform_dependency
            .get(&logical_expr_id)
            .and_then(|status_map| status_map.get(rule))
            .map(|dep| dep.status.clone())
            .unwrap_or(Status::Dirty);
        Ok(status)
    }

    async fn set_transformation_clean(
        &mut self,
        logical_expr_id: LogicalExpressionId,
        rule: &TransformationRule,
    ) -> MemoizeResult<()> {
        let logical_expr_id = self.find_repr_logical_expr(logical_expr_id).await?;
        let status_map = self
            .transform_dependency
            .entry(logical_expr_id)
            .or_default();
        match status_map.entry(rule.clone()) {
            Entry::Occupied(occupied_entry) => {
                let dep = occupied_entry.into_mut();
                dep.status = Status::Clean;
            }
            Entry::Vacant(vacant) => {
                vacant.insert(RuleDependency::new(Status::Clean));
            }
        }
        Ok(())
    }

    async fn get_implementation_status(
        &self,
        logical_expr_id: LogicalExpressionId,
        goal_id: GoalId,
        rule: &ImplementationRule,
    ) -> MemoizeResult<Status> {
        let logical_expr_id = self.find_repr_logical_expr(logical_expr_id).await?;
        let goal_id = self.find_repr_goal(goal_id).await?;
        let status = self
            .implement_dependency
            .get(&logical_expr_id)
            .and_then(|status_map| status_map.get(&(goal_id, rule.clone())))
            .map(|dep| dep.status.clone())
            .unwrap_or(Status::Dirty);
        Ok(status)
    }

    async fn set_implementation_clean(
        &mut self,
        logical_expr_id: LogicalExpressionId,
        goal_id: GoalId,
        rule: &ImplementationRule,
    ) -> MemoizeResult<()> {
        let logical_expr_id = self.find_repr_logical_expr(logical_expr_id).await?;
        let status_map = self
            .implement_dependency
            .entry(logical_expr_id)
            .or_default();
        match status_map.entry((goal_id, rule.clone())) {
            Entry::Occupied(occupied_entry) => {
                let dep = occupied_entry.into_mut();
                dep.status = Status::Clean;
            }
            Entry::Vacant(vacant) => {
                vacant.insert(RuleDependency::new(Status::Clean));
            }
        }
        Ok(())
    }

    async fn get_cost_status(
        &self,
        physical_expr_id: PhysicalExpressionId,
    ) -> MemoizeResult<Status> {
        let physical_expr_id = self.find_repr_physical_expr(physical_expr_id).await?;
        let status = self
            .cost_dependency
            .get(&physical_expr_id)
            .map(|dep| dep.status.clone())
            .unwrap_or(Status::Dirty);
        Ok(status)
    }

    async fn set_cost_clean(
        &mut self,
        physical_expr_id: PhysicalExpressionId,
    ) -> MemoizeResult<()> {
        let physical_expr_id = self.find_repr_physical_expr(physical_expr_id).await?;

        let entry = self.cost_dependency.entry(physical_expr_id);

        match entry {
            Entry::Occupied(occupied) => {
                let dep = occupied.into_mut();
                dep.status = Status::Clean;
            }
            Entry::Vacant(vacant) => {
                vacant.insert(CostDependency::new(Status::Clean));
            }
        }

        Ok(())
    }

    async fn add_transformation_dependency(
        &mut self,
        logical_expr_id: LogicalExpressionId,
        rule: &TransformationRule,
        group_id: GroupId,
    ) -> MemoizeResult<()> {
        let logical_expr_id = self.find_repr_logical_expr(logical_expr_id).await?;
        let group_id = self.find_repr_group(group_id).await?;
        let status_map = self
            .transform_dependency
            .entry(logical_expr_id)
            .or_default();

        match status_map.entry(rule.clone()) {
            Entry::Occupied(occupied_entry) => {
                let dep = occupied_entry.into_mut();
                dep.group_ids.insert(group_id);
            }
            Entry::Vacant(vacant) => {
                let mut dep = RuleDependency::new(Status::Dirty);
                dep.group_ids.insert(group_id);
                vacant.insert(dep);
            }
        }

        Ok(())
    }

    async fn add_implementation_dependency(
        &mut self,
        logical_expr_id: LogicalExpressionId,
        goal_id: GoalId,
        rule: &ImplementationRule,
        group_id: GroupId,
    ) -> MemoizeResult<()> {
        let logical_expr_id = self.find_repr_logical_expr(logical_expr_id).await?;
        let group_id = self.find_repr_group(group_id).await?;
        let goal_id = self.find_repr_goal(goal_id).await?;

        let status_map = self
            .implement_dependency
            .entry(logical_expr_id)
            .or_default();

        match status_map.entry((goal_id, rule.clone())) {
            Entry::Occupied(occupied) => {
                let dep = occupied.into_mut();
                dep.group_ids.insert(group_id);
            }
            Entry::Vacant(vacant) => {
                let mut dep = RuleDependency::new(Status::Dirty);
                dep.group_ids.insert(group_id);
                vacant.insert(dep);
            }
        }

        Ok(())
    }

    async fn add_cost_dependency(
        &mut self,
        physical_expr_id: PhysicalExpressionId,
        goal_id: GoalId,
    ) -> MemoizeResult<()> {
        let physical_expr_id = self.find_repr_physical_expr(physical_expr_id).await?;
        let goal_id = self.find_repr_goal(goal_id).await?;

        match self.cost_dependency.entry(physical_expr_id) {
            Entry::Occupied(occupied) => {
                let dep = occupied.into_mut();
                dep.goal_ids.insert(goal_id);
            }
            Entry::Vacant(vacant) => {
                let mut dep = CostDependency::new(Status::Dirty);
                dep.goal_ids.insert(goal_id);
                vacant.insert(dep);
            }
        }

        Ok(())
    }

    async fn get_goal_id(&mut self, goal: &Goal) -> MemoizeResult<GoalId> {
        if let Some(goal_id) = self.goal_node_to_id_index.get(goal).cloned() {
            return Ok(goal_id);
        }
        let goal_id = self.next_goal_id();
        self.goal_node_to_id_index.insert(goal.clone(), goal_id);
        self.goals.insert(goal_id, GoalState::new(goal.clone()));

        let Goal(group_id, _) = goal;
        self.groups.get_mut(group_id).unwrap().goals.insert(goal_id);
        Ok(goal_id)
    }

    async fn materialize_goal(&self, goal_id: GoalId) -> MemoizeResult<Goal> {
        let state = self
            .goals
            .get(&goal_id)
            .ok_or(MemoizeError::GoalNotFound(goal_id))?;

        Ok(state.goal.clone())
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
        let logical_expr_id = self.find_repr_logical_expr(logical_expr_id).await?;
        let logical_expr = self
            .logical_exprs
            .get(&logical_expr_id)
            .ok_or(MemoizeError::LogicalExprNotFound(logical_expr_id))?;
        Ok(logical_expr.clone())
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
        let physical_expr_id = self.find_repr_physical_expr(physical_expr_id).await?;
        let (physical_expr, _) = self
            .physical_exprs
            .get(&physical_expr_id)
            .ok_or(MemoizeError::PhysicalExprNotFound(physical_expr_id))?;
        Ok(physical_expr.clone())
    }

    async fn find_repr_group(&self, group_id: GroupId) -> MemoizeResult<GroupId> {
        let repr_group_id = self.repr_group.find(&group_id);
        Ok(repr_group_id)
    }

    async fn find_repr_goal(&self, goal_id: GoalId) -> MemoizeResult<GoalId> {
        let repr_goal_id = self.repr_goal.find(&goal_id);
        Ok(repr_goal_id)
    }

    async fn find_repr_logical_expr(
        &self,
        logical_expr_id: LogicalExpressionId,
    ) -> MemoizeResult<LogicalExpressionId> {
        let repr_expr_id = self.repr_logical_expr.find(&logical_expr_id);
        Ok(repr_expr_id)
    }

    async fn find_repr_physical_expr(
        &self,
        physical_expr_id: PhysicalExpressionId,
    ) -> MemoizeResult<PhysicalExpressionId> {
        let repr_expr_id = self.repr_physical_expr.find(&physical_expr_id);
        Ok(repr_expr_id)
    }
}

impl MemoryMemo {
    /// Generates a new group id.
    fn next_group_id(&mut self) -> GroupId {
        let group_id = GroupId(self.next_shared_id);
        self.next_shared_id += 1;
        group_id
    }

    /// Generates a new physical expression id.
    fn next_physical_expr_id(&mut self) -> PhysicalExpressionId {
        let physical_expr_id = PhysicalExpressionId(self.next_shared_id);
        self.next_shared_id += 1;
        physical_expr_id
    }

    /// Generates a new logical expression id.
    fn next_logical_expr_id(&mut self) -> LogicalExpressionId {
        let logical_expr_id = LogicalExpressionId(self.next_shared_id);
        self.next_shared_id += 1;
        logical_expr_id
    }

    /// Generates a new goal id.
    fn next_goal_id(&mut self) -> GoalId {
        let goal_id = GoalId(self.next_shared_id);
        self.next_shared_id += 1;
        goal_id
    }

    /// Propagates the new costed member physical expression to all subscribers.
    async fn propagate_new_member_cost(
        &mut self,
        mut subscribers: VecDeque<GoalId>,
        result: &mut ForwardResult,
    ) -> MemoizeResult<()> {
        while let Some(goal_id) = subscribers.pop_front() {
            let current_best = self.get_best_optimized_physical_expr(goal_id).await?;

            let is_better = current_best
                .map(|(_, cost)| result.best_cost < cost)
                .unwrap_or(true);

            if is_better {
                // Update the best cost for the goal.
                self.best_optimized_physical_expr_index
                    .insert(goal_id, (result.physical_expr_id, result.best_cost));

                result.goals_forwarded.insert(goal_id);

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

    /// Find the representative of a goal member.
    ///
    /// This reduces down to finding representative physical expr or goal id.
    async fn find_repr_goal_member(&self, member: GoalMemberId) -> MemoizeResult<GoalMemberId> {
        match member {
            GoalMemberId::PhysicalExpressionId(physical_expr_id) => {
                let physical_expr_id = self.find_repr_physical_expr(physical_expr_id).await?;
                Ok(GoalMemberId::PhysicalExpressionId(physical_expr_id))
            }
            GoalMemberId::GoalId(goal_id) => {
                let goal_id = self.find_repr_goal(goal_id).await?;
                Ok(GoalMemberId::GoalId(goal_id))
            }
        }
    }
}
