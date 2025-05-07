use super::*;
use crate::cir::*;
use async_recursion::async_recursion;
use std::collections::{HashMap, HashSet, VecDeque, hash_map::Entry};

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

    /// Dependent logical expression ids for each group id.
    /// This is used to quickly find all the logical expressions that have a child equal to the group id, which is the key.
    /// Dependent here does not mean the dependency stuff that we have in the memo table
    group_dependent_logical_exprs: HashMap<GroupId, HashSet<LogicalExpressionId>>,

    /// Physical expression id to node.
    physical_exprs: HashMap<PhysicalExpressionId, (PhysicalExpression, Option<Cost>)>,
    /// Physical expression node to id.
    physical_expr_node_to_id_index: HashMap<PhysicalExpression, PhysicalExpressionId>,

    /// Dependent physical expression ids for each goal id.
    /// This is used to quickly find all the physical expressions that have a child equal to the goal id, which is the key.
    /// Dependent here does not mean the dependency stuff that we have in the memo table
    goal_dependent_physical_exprs: HashMap<GoalId, HashSet<PhysicalExpressionId>>,

    /// Dependent physical expression ids for each physical expression id.
    /// This is used to quickly find all the physical expressions that have a child equal to the physical expression id, which is the key.
    physical_expr_dependent_physical_exprs:
        HashMap<PhysicalExpressionId, HashSet<PhysicalExpressionId>>,

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

    repr_group: UnionFind<GroupId>,
    repr_goal: UnionFind<GoalId>,
    repr_logical_expr: UnionFind<LogicalExpressionId>,
    repr_physical_expr: UnionFind<PhysicalExpressionId>,

    transform_dependency: HashMap<LogicalExpressionId, HashMap<TransformationRule, RuleDependency>>,
    implement_dependency:
        HashMap<LogicalExpressionId, HashMap<(GoalId, ImplementationRule), RuleDependency>>,
    cost_dependency: HashMap<PhysicalExpressionId, CostDependency>,
}

struct RuleDependency {
    group_ids: HashSet<GroupId>,
    status: TaskStatus,
}

impl RuleDependency {
    fn new(status: TaskStatus) -> Self {
        let group_ids = HashSet::new();
        Self { group_ids, status }
    }
}

struct CostDependency {
    goal_ids: HashSet<GoalId>,
    status: TaskStatus,
}

impl CostDependency {
    fn new(status: TaskStatus) -> Self {
        let goal_ids = HashSet::new();
        Self { goal_ids, status }
    }
}

/// State of a group in the memo structure.
struct GroupState {
    /// The logical properties of the group, might be `None` if it hasn't been derived yet.
    properties: LogicalProperties,
    logical_exprs: HashSet<LogicalExpressionId>,
    goals: HashSet<GoalId>,
}

impl GroupState {
    fn new(logical_expr_id: LogicalExpressionId) -> Self {
        let mut logical_exprs = HashSet::new();
        logical_exprs.insert(logical_expr_id);
        Self {
            properties: LogicalProperties(None),
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

impl Representative for MemoryMemo {
    async fn find_repr_group(&self, group_id: GroupId) -> GroupId {
        self.repr_group.find(&group_id)
    }

    async fn find_repr_goal(&self, goal_id: GoalId) -> GoalId {
        self.repr_goal.find(&goal_id)
    }

    async fn find_repr_logical_expr(
        &self,
        logical_expr_id: LogicalExpressionId,
    ) -> LogicalExpressionId {
        self.repr_logical_expr.find(&logical_expr_id)
    }

    async fn find_repr_physical_expr(
        &self,
        physical_expr_id: PhysicalExpressionId,
    ) -> PhysicalExpressionId {
        self.repr_physical_expr.find(&physical_expr_id)
    }
}

impl Materialize for MemoryMemo {
    async fn get_goal_id(&mut self, goal: &Goal) -> MemoResult<GoalId> {
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

    async fn materialize_goal(&self, goal_id: GoalId) -> MemoResult<Goal> {
        let state = self
            .goals
            .get(&goal_id)
            .ok_or(MemoError::GoalNotFound(goal_id))?;

        Ok(state.goal.clone())
    }

    async fn get_logical_expr_id(
        &mut self,
        logical_expr: &LogicalExpression,
    ) -> MemoResult<LogicalExpressionId> {
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

        for child in logical_expr.children.iter() {
            match child {
                Child::Singleton(group_id) => {
                    self.group_dependent_logical_exprs
                        .entry(*group_id)
                        .or_default()
                        .insert(logical_expr_id);
                }
                Child::VarLength(group_ids) => {
                    for group_id in group_ids.iter() {
                        self.group_dependent_logical_exprs
                            .entry(*group_id)
                            .or_default()
                            .insert(logical_expr_id);
                    }
                }
            }
        }
        Ok(logical_expr_id)
    }

    async fn materialize_logical_expr(
        &self,
        logical_expr_id: LogicalExpressionId,
    ) -> MemoResult<LogicalExpression> {
        let logical_expr_id = self.find_repr_logical_expr(logical_expr_id).await;
        let logical_expr = self
            .logical_exprs
            .get(&logical_expr_id)
            .ok_or(MemoError::LogicalExprNotFound(logical_expr_id))?;
        Ok(logical_expr.clone())
    }

    async fn get_physical_expr_id(
        &mut self,
        physical_expr: &PhysicalExpression,
    ) -> MemoResult<PhysicalExpressionId> {
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

        for child in physical_expr.children.iter() {
            match child {
                Child::Singleton(goal_member_id) => {
                    if let GoalMemberId::GoalId(goal_id) = goal_member_id {
                        self.goal_dependent_physical_exprs
                            .entry(*goal_id)
                            .or_default()
                            .insert(physical_expr_id);
                    }
                }
                Child::VarLength(goal_member_ids) => {
                    for goal_member_id in goal_member_ids.iter() {
                        match goal_member_id {
                            GoalMemberId::GoalId(goal_id) => {
                                self.goal_dependent_physical_exprs
                                    .entry(*goal_id)
                                    .or_default()
                                    .insert(physical_expr_id);
                            }
                            GoalMemberId::PhysicalExpressionId(child_physical_expr_id) => {
                                self.physical_expr_dependent_physical_exprs
                                    .entry(*child_physical_expr_id)
                                    .or_default()
                                    .insert(physical_expr_id);
                            }
                        }
                    }
                }
            }
        }
        Ok(physical_expr_id)
    }

    async fn materialize_physical_expr(
        &self,
        physical_expr_id: PhysicalExpressionId,
    ) -> MemoResult<PhysicalExpression> {
        let physical_expr_id = self.find_repr_physical_expr(physical_expr_id).await;
        let (physical_expr, _) = self
            .physical_exprs
            .get(&physical_expr_id)
            .ok_or(MemoError::PhysicalExprNotFound(physical_expr_id))?;
        Ok(physical_expr.clone())
    }
}

impl Memo for MemoryMemo {
    async fn merge_groups(
        &mut self,
        group_id_1: GroupId,
        group_id_2: GroupId,
    ) -> MemoResult<Option<MergeProducts>> {
        self.merge_groups_helper(group_id_1, group_id_2).await
    }

    async fn get_logical_properties(&self, group_id: GroupId) -> MemoResult<LogicalProperties> {
        let group_id = self.find_repr_group(group_id).await;
        let group = self
            .groups
            .get(&group_id)
            .ok_or(MemoError::GroupNotFound(group_id))?;

        Ok(group.properties.clone())
    }

    async fn set_logical_properties(
        &mut self,
        group_id: GroupId,
        props: LogicalProperties,
    ) -> MemoResult<()> {
        let group_id = self.find_repr_group(group_id).await;
        let group = self
            .groups
            .get_mut(&group_id)
            .ok_or(MemoError::GroupNotFound(group_id))?;

        group.properties = props;
        Ok(())
    }

    async fn get_all_logical_exprs(
        &self,
        group_id: GroupId,
    ) -> MemoResult<Vec<LogicalExpressionId>> {
        let group_id = self.find_repr_group(group_id).await;
        let group = self
            .groups
            .get(&group_id)
            .ok_or(MemoError::GroupNotFound(group_id))?;

        Ok(group.logical_exprs.iter().cloned().collect())
    }

    async fn get_any_logical_expr(&self, group_id: GroupId) -> MemoResult<LogicalExpressionId> {
        let group_id = self.find_repr_group(group_id).await;
        let group = self
            .groups
            .get(&group_id)
            .ok_or(MemoError::GroupNotFound(group_id))?;

        group
            .logical_exprs
            .iter()
            .next()
            .cloned()
            .ok_or(MemoError::NoLogicalExprInGroup(group_id))
    }

    async fn find_logical_expr_group(
        &self,
        logical_expr_id: LogicalExpressionId,
    ) -> MemoResult<Option<GroupId>> {
        let logical_expr_id = self.find_repr_logical_expr(logical_expr_id).await;
        let maybe_group_id = self.logical_expr_group_index.get(&logical_expr_id).cloned();
        Ok(maybe_group_id)
    }

    async fn create_group(
        &mut self,
        logical_expr_id: LogicalExpressionId,
        props: &LogicalProperties,
    ) -> MemoResult<GroupId> {
        let group_id = self.next_group_id();
        let group = GroupState::new(logical_expr_id);
        self.groups.insert(group_id, group);
        self.logical_expr_group_index
            .insert(logical_expr_id, group_id);

        Ok(group_id)
    }

    async fn get_best_optimized_physical_expr(
        &self,
        goal_id: GoalId,
    ) -> MemoResult<Option<(PhysicalExpressionId, Cost)>> {
        let goal_id = self.find_repr_goal(goal_id).await;
        let maybe_best_costed = self
            .best_optimized_physical_expr_index
            .get(&goal_id)
            .cloned();
        Ok(maybe_best_costed)
    }

    async fn get_all_goal_members(&self, goal_id: GoalId) -> MemoResult<Vec<GoalMemberId>> {
        let goal_id = self.find_repr_goal(goal_id).await;
        let goal_state = self.goals.get(&goal_id).unwrap();
        Ok(goal_state.members.iter().cloned().collect())
    }

    async fn add_goal_member(
        &mut self,
        goal_id: GoalId,
        member: GoalMemberId,
    ) -> MemoResult<Option<PropagateBestExpression>> {
        let goal_id = self.find_repr_goal(goal_id).await;
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
            let mut result = PropagateBestExpression::new(physical_expr_id, cost);
            // propagate the new cost to all subscribers.
            self.propagate_new_member_cost(subscribers, &mut result)
                .await?;
            if result.goals_propagated_to.is_empty() {
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
    ) -> MemoResult<Option<Cost>> {
        let physical_expr_id = self.find_repr_physical_expr(physical_expr_id).await;
        let (_, maybe_cost) = self
            .physical_exprs
            .get(&physical_expr_id)
            .ok_or(MemoError::PhysicalExprNotFound(physical_expr_id))?;
        Ok(*maybe_cost)
    }

    async fn update_physical_expr_cost(
        &mut self,
        physical_expr_id: PhysicalExpressionId,
        new_cost: Cost,
    ) -> MemoResult<Option<PropagateBestExpression>> {
        let physical_expr_id = self.find_repr_physical_expr(physical_expr_id).await;
        let (_, cost_mut) = self
            .physical_exprs
            .get_mut(&physical_expr_id)
            .ok_or(MemoError::PhysicalExprNotFound(physical_expr_id))?;
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

            let mut result = PropagateBestExpression::new(physical_expr_id, new_cost);
            // propagate the new cost to all subscribers.
            self.propagate_new_member_cost(subscribers, &mut result)
                .await?;
            if result.goals_propagated_to.is_empty() {
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
}

impl MemoryMemo {
    /// Creates a new logical expression with the same children but with the children being the representative group ids.
    async fn create_repr_logical_expr(
        &mut self,
        logical_expr: LogicalExpression,
    ) -> MemoResult<LogicalExpression> {
        let mut repr_logical_expr = logical_expr.clone();
        let mut new_children = Vec::new();

        for child in repr_logical_expr.children.iter() {
            match child {
                Child::Singleton(group_id) => {
                    let repr_group_id = self.find_repr_group(*group_id).await;
                    new_children.push(Child::Singleton(repr_group_id));
                }
                Child::VarLength(group_ids) => {
                    let new_group_ids = group_ids
                        .iter()
                        .map(|group_id| {
                            let self_ref = &self;
                            // TODO(Sarvesh): this is a hack to get the repr group id, i'm sure there's a better way to do this.
                            async move { self_ref.find_repr_group(*group_id).await }
                        })
                        .collect::<Vec<_>>();

                    let new_group_ids = futures::future::join_all(new_group_ids)
                        .await
                        .into_iter()
                        .collect();

                    new_children.push(Child::VarLength(new_group_ids));
                }
            }
        }
        repr_logical_expr.children = new_children;
        Ok(repr_logical_expr)
    }

    /// Creates a new physical expression with the same children but with the children being the representative group ids.
    async fn create_repr_physical_expr(
        &mut self,
        physical_expr: PhysicalExpression,
    ) -> MemoResult<PhysicalExpression> {
        let mut repr_physical_expr = physical_expr.clone();
        let mut new_children = Vec::new();

        for child in repr_physical_expr.children.iter() {
            match child {
                Child::Singleton(goal_member_id) => {
                    if let GoalMemberId::GoalId(goal_id) = goal_member_id {
                        let repr_goal_id = self.find_repr_goal(*goal_id).await;
                        new_children.push(Child::Singleton(GoalMemberId::GoalId(repr_goal_id)));
                    } else {
                        new_children.push(Child::Singleton(*goal_member_id));
                    }
                }
                Child::VarLength(goal_member_ids) => {
                    let mut new_goal_member_ids = Vec::new();
                    for goal_member_id in goal_member_ids.iter() {
                        match goal_member_id {
                            GoalMemberId::GoalId(goal_id) => {
                                let repr_goal_id = self.find_repr_goal(*goal_id).await;
                                new_goal_member_ids.push(GoalMemberId::GoalId(repr_goal_id));
                            }
                            GoalMemberId::PhysicalExpressionId(physical_expr_id) => {
                                let repr_physical_expr_id =
                                    self.find_repr_physical_expr(*physical_expr_id).await;
                                new_goal_member_ids.push(GoalMemberId::PhysicalExpressionId(
                                    repr_physical_expr_id,
                                ));
                            }
                        }
                    }
                    new_children.push(Child::VarLength(new_goal_member_ids));
                }
            }
        }
        repr_physical_expr.children = new_children;
        Ok(repr_physical_expr)
    }

    /// Recursively merges physical expressions.
    #[async_recursion]
    async fn merge_physical_exprs(
        &mut self,
        physical_expr_id: PhysicalExpressionId,
    ) -> MemoResult<Vec<PhysicalMergeProduct>> {
        let (physical_expr, _cost) = self.physical_exprs.get(&physical_expr_id).unwrap();
        let repr_physical_expr = self
            .create_repr_physical_expr(physical_expr.clone())
            .await?;
        let repr_physical_expr_id = self.get_physical_expr_id(&repr_physical_expr).await?;

        // merge the physical exprs
        self.repr_physical_expr
            .merge(&physical_expr_id, &repr_physical_expr_id);

        let subscribers = self
            .member_subscribers
            .remove(&GoalMemberId::PhysicalExpressionId(physical_expr_id));
        if let Some(subscribers) = subscribers {
            // add the subscribers to the repr physical expr id
            self.member_subscribers
                .entry(GoalMemberId::PhysicalExpressionId(repr_physical_expr_id))
                .or_default()
                .extend(subscribers);
        }

        let mut results = Vec::new();
        results.push(PhysicalMergeProduct {
            repr_physical_expr: repr_physical_expr_id,
            non_repr_physical_exprs: physical_expr_id,
        });

        let dependent_physical_exprs = self
            .physical_expr_dependent_physical_exprs
            .get(&physical_expr_id);
        if let Some(dependent_physical_exprs) = dependent_physical_exprs {
            let dependent_physical_exprs =
                dependent_physical_exprs.iter().cloned().collect::<Vec<_>>();
            for dependent_physical_expr_id in dependent_physical_exprs {
                // TODO(Sarvesh): handle async recursion
                let merge_physical_expr_result = self
                    .merge_physical_exprs(dependent_physical_expr_id)
                    .await?;
                results.extend(merge_physical_expr_result);
            }
        }

        Ok(results)
    }

    /// Merges two goals into a single goal.
    async fn merge_goals_helper(
        &mut self,
        goal_id1: GoalId,
        goal_id2: GoalId,
    ) -> MemoResult<(GoalMergeProduct, Vec<PhysicalMergeProduct>)> {
        let goal_2 = self.goals.remove(&goal_id2).unwrap();
        let goal_1 = self.goals.get(&goal_id1).unwrap();

        self.repr_goal.merge(&goal_id2, &goal_id1);

        let mut merged_goal_result = GoalMergeProduct {
            best_expr: None,
            repr_goal_id: goal_id1,
            non_repr_goal_id: goal_id2,
            repr_goal_seen_best_expr_before_merge: false,
            non_repr_goal_seen_best_expr_before_merge: false,
            members: HashSet::default(),
        };

        let best_expr_goal1 = self.get_best_optimized_physical_expr(goal_id1).await?;
        let best_expr_goal2 = self.get_best_optimized_physical_expr(goal_id2).await?;

        let best_expr = match (best_expr_goal1, best_expr_goal2) {
            (Some(best_expr_goal1), Some(best_expr_goal2)) => {
                Some(if best_expr_goal1.1 < best_expr_goal2.1 {
                    best_expr_goal1
                } else {
                    best_expr_goal2
                })
            }
            (Some(best_expr_goal1), None) => Some(best_expr_goal1),
            (None, Some(best_expr_goal2)) => Some(best_expr_goal2),
            (None, None) => None,
        };

        if let Some(best_expr) = best_expr {
            merged_goal_result.best_expr = Some(best_expr);
        }

        merged_goal_result.repr_goal_seen_best_expr_before_merge = {
            if let Some(best_expr_goal1) = best_expr_goal1 {
                if let Some(best_expr_goal2) = best_expr_goal2 {
                    // goal 1 and goal 2 both had expr, return true if goal 1's is better or equal to goal 2's
                    best_expr_goal1.1 <= best_expr_goal2.1
                } else {
                    // goal 1 had a best expr before merge but goal 2 didn't
                    true
                }
            } else {
                // neither goal had a best expr before merge
                false
            }
        };

        merged_goal_result.non_repr_goal_seen_best_expr_before_merge = {
            if let Some(best_expr_goal2) = best_expr_goal2 {
                if let Some(best_expr_goal1) = best_expr_goal1 {
                    // goal 2 and goal 1 both had expr, return true if goal 2's is better or equal to goal 1's
                    best_expr_goal2.1 <= best_expr_goal1.1
                } else {
                    // goal 2 had a best expr before merge but goal 1 didn't
                    true
                }
            } else {
                // neither goal had a best expr before merge
                false
            }
        };

        merged_goal_result.members = goal_1.members.iter().cloned().collect();
        merged_goal_result
            .members
            .extend(goal_2.members.iter().cloned());

        let forward_result = self
            .add_goal_member(goal_id1, GoalMemberId::GoalId(goal_id2))
            .await?;
        if let Some(forward_result) = forward_result {
            // change the subscribers of goal 2 to now be subscribed to goal 1
            let subscribers = self
                .member_subscribers
                .remove(&GoalMemberId::GoalId(goal_id2));
            if let Some(subscribers) = subscribers {
                self.member_subscribers
                    .entry(GoalMemberId::GoalId(goal_id1))
                    .or_default()
                    .extend(&subscribers);
                let mut temp_forward_result = PropagateBestExpression::new(
                    forward_result.physical_expr_id,
                    forward_result.best_cost,
                );
                let vec_deque_subscribers =
                    VecDeque::from(subscribers.into_iter().collect::<Vec<_>>());
                self.propagate_new_member_cost(vec_deque_subscribers, &mut temp_forward_result)
                    .await?;
            }
        }

        // Now, we need to update all the physical exprs that depend on goal 2 to now depend on goal 1.
        let goal_2_dependent_physical_exprs =
            match self.goal_dependent_physical_exprs.get(&goal_id2) {
                None => HashSet::default(),
                Some(exprs) => exprs.clone(),
            };

        let mut results = Vec::new();
        for physical_expr_id in goal_2_dependent_physical_exprs {
            let merge_physical_expr_result = self.merge_physical_exprs(physical_expr_id).await?;
            results.extend(merge_physical_expr_result);
        }

        Ok((merged_goal_result, results))
    }

    #[async_recursion]
    async fn merge_groups_helper(
        &mut self,
        group_id_1: GroupId,
        group_id_2: GroupId,
    ) -> MemoResult<Option<MergeProducts>> {
        // our strategy is to always merge group 2 into group 1.
        let group_id_1 = self.find_repr_group(group_id_1).await;
        let group_id_2 = self.find_repr_group(group_id_2).await;

        if group_id_1 == group_id_2 {
            return Ok(None);
        }
        let mut result = MergeProducts::default();
        let group_2_state = self.groups.remove(&group_id_2).unwrap();
        let group_2_exprs: Vec<LogicalExpressionId> =
            group_2_state.logical_exprs.iter().cloned().collect();

        let group1_state = self.groups.get_mut(&group_id_1).unwrap();
        let group1_exprs: Vec<LogicalExpressionId> =
            group1_state.logical_exprs.iter().cloned().collect();

        for logical_expr_id in group_2_state.logical_exprs {
            // Update the logical expression to point to the new group id.
            let old_group_id = self
                .logical_expr_group_index
                .insert(logical_expr_id, group_id_1);
            assert!(old_group_id.is_some());
            group1_state.logical_exprs.insert(logical_expr_id);
        }
        let mut merge_group_result = GroupMergeProduct::new(group_id_1, group_id_2);
        merge_group_result
            .all_exprs_in_merged_group
            .extend(group1_exprs.iter().cloned());
        merge_group_result
            .all_exprs_in_merged_group
            .extend(group_2_exprs.iter().cloned());

        self.repr_group.merge(&group_id_2, &group_id_1);

        merge_group_result.repr_group_exprs.extend(group1_exprs);
        merge_group_result
            .non_repr_group_exprs
            .extend(group_2_exprs);

        result.group_merges.push(merge_group_result);

        // So now, we have to find out all the goals that belong to both groups but contain the same properties.

        let group_1_goals = group1_state.goals.iter().cloned().collect::<HashSet<_>>();
        let group_2_goals = group_2_state.goals.iter().cloned().collect::<HashSet<_>>();

        for goal_id1 in group_1_goals.iter() {
            for goal_id2 in group_2_goals.iter() {
                let goal_1 = self.goals.get(goal_id1).unwrap();
                let goal_2 = self.goals.get(goal_id2).unwrap();
                let goal_1_props = &goal_1.goal.1;
                let goal_2_props = &goal_2.goal.1;
                if goal_1_props == goal_2_props {
                    let (merged_goal_result, merge_physical_expr_results) =
                        self.merge_goals_helper(*goal_id1, *goal_id2).await?;
                    result.goal_merges.push(merged_goal_result);
                    result
                        .physical_expr_merges
                        .extend(merge_physical_expr_results);
                }
            }
        }

        // Let's check for cascading merges now.
        let logical_expr_with_group_2_as_child =
            match self.group_dependent_logical_exprs.get(&group_id_2) {
                None => HashSet::default(),
                Some(exprs) => exprs.clone(),
            };

        for logical_expr_id in logical_expr_with_group_2_as_child.iter() {
            let logical_expr = self.logical_exprs.get(logical_expr_id).unwrap();
            let repr_logical_expr = self.create_repr_logical_expr(logical_expr.clone()).await?;
            let repr_logical_expr_id = self.get_logical_expr_id(&repr_logical_expr).await?;
            // merge the logical exprs
            self.repr_logical_expr
                .merge(logical_expr_id, &repr_logical_expr_id);

            let parent_group_id = self.logical_expr_group_index.get(logical_expr_id).unwrap();
            let parent_group_state = self.groups.get_mut(parent_group_id).unwrap();
            // We remove the stale logical expr from the parent group.
            parent_group_state.logical_exprs.remove(logical_expr_id);

            // is the repr logical expr already part of a group?
            if let Some(repr_parent_group_id) =
                self.logical_expr_group_index.get(&repr_logical_expr_id)
            {
                // the repr logical expr is part of a group, so
                let parent_group_id = self.logical_expr_group_index.get(logical_expr_id).unwrap();
                if repr_parent_group_id != parent_group_id {
                    // we have another merge to do
                    // do a cascading merge between repr_parent_group_id and parent_group_id
                    let merge_result = self
                        .merge_groups_helper(*repr_parent_group_id, *parent_group_id)
                        .await?;
                    // merge the cascading merge result with the current result.
                    if let Some(merge_result) = merge_result {
                        result.group_merges.extend(merge_result.group_merges);
                        result
                            .physical_expr_merges
                            .extend(merge_result.physical_expr_merges);
                        result.goal_merges.extend(merge_result.goal_merges);
                    }
                }
            } else {
                // the repr logical expr is not part of a group, so we add it to the parent group.
                // We add the new repr logical expr to the parent group.
                parent_group_state
                    .logical_exprs
                    .insert(repr_logical_expr_id);
                // we update the index
                self.logical_expr_group_index
                    .insert(repr_logical_expr_id, *parent_group_id);
            }
        }

        Ok(Some(result))
    }

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
        result: &mut PropagateBestExpression,
    ) -> MemoResult<()> {
        while let Some(goal_id) = subscribers.pop_front() {
            let current_best = self.get_best_optimized_physical_expr(goal_id).await?;

            let is_better = current_best
                .map(|(_, cost)| result.best_cost < cost)
                .unwrap_or(true);

            if is_better {
                // Update the best cost for the goal.
                self.best_optimized_physical_expr_index
                    .insert(goal_id, (result.physical_expr_id, result.best_cost));

                result.goals_propagated_to.insert(goal_id);

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
    async fn find_repr_goal_member(&self, member: GoalMemberId) -> MemoResult<GoalMemberId> {
        match member {
            GoalMemberId::PhysicalExpressionId(physical_expr_id) => {
                let physical_expr_id = self.find_repr_physical_expr(physical_expr_id).await;
                Ok(GoalMemberId::PhysicalExpressionId(physical_expr_id))
            }
            GoalMemberId::GoalId(goal_id) => {
                let goal_id = self.find_repr_goal(goal_id).await;
                Ok(GoalMemberId::GoalId(goal_id))
            }
        }
    }
}

impl TaskGraphState for MemoryMemo {
    async fn get_transformation_status(
        &self,
        logical_expr_id: LogicalExpressionId,
        rule: &TransformationRule,
    ) -> MemoResult<TaskStatus> {
        let logical_expr_id = self.find_repr_logical_expr(logical_expr_id).await;
        let status = self
            .transform_dependency
            .get(&logical_expr_id)
            .and_then(|status_map| status_map.get(rule))
            .map(|dep| dep.status)
            .unwrap_or(TaskStatus::Dirty);
        Ok(status)
    }

    async fn set_transformation_clean(
        &mut self,
        logical_expr_id: LogicalExpressionId,
        rule: &TransformationRule,
    ) -> MemoResult<()> {
        let logical_expr_id = self.find_repr_logical_expr(logical_expr_id).await;
        let status_map = self
            .transform_dependency
            .entry(logical_expr_id)
            .or_default();
        match status_map.entry(rule.clone()) {
            Entry::Occupied(occupied_entry) => {
                let dep = occupied_entry.into_mut();
                dep.status = TaskStatus::Clean;
            }
            Entry::Vacant(vacant) => {
                vacant.insert(RuleDependency::new(TaskStatus::Clean));
            }
        }
        Ok(())
    }

    async fn get_implementation_status(
        &self,
        logical_expr_id: LogicalExpressionId,
        goal_id: GoalId,
        rule: &ImplementationRule,
    ) -> MemoResult<TaskStatus> {
        let logical_expr_id = self.find_repr_logical_expr(logical_expr_id).await;
        let goal_id = self.find_repr_goal(goal_id).await;
        let status = self
            .implement_dependency
            .get(&logical_expr_id)
            .and_then(|status_map| status_map.get(&(goal_id, rule.clone())))
            .map(|dep| dep.status)
            .unwrap_or(TaskStatus::Dirty);
        Ok(status)
    }

    async fn set_implementation_clean(
        &mut self,
        logical_expr_id: LogicalExpressionId,
        goal_id: GoalId,
        rule: &ImplementationRule,
    ) -> MemoResult<()> {
        let logical_expr_id = self.find_repr_logical_expr(logical_expr_id).await;
        let status_map = self
            .implement_dependency
            .entry(logical_expr_id)
            .or_default();
        match status_map.entry((goal_id, rule.clone())) {
            Entry::Occupied(occupied_entry) => {
                let dep = occupied_entry.into_mut();
                dep.status = TaskStatus::Clean;
            }
            Entry::Vacant(vacant) => {
                vacant.insert(RuleDependency::new(TaskStatus::Clean));
            }
        }
        Ok(())
    }

    async fn get_cost_status(
        &self,
        physical_expr_id: PhysicalExpressionId,
    ) -> MemoResult<TaskStatus> {
        let physical_expr_id = self.find_repr_physical_expr(physical_expr_id).await;
        let status = self
            .cost_dependency
            .get(&physical_expr_id)
            .map(|dep| dep.status)
            .unwrap_or(TaskStatus::Dirty);
        Ok(status)
    }

    async fn set_cost_clean(&mut self, physical_expr_id: PhysicalExpressionId) -> MemoResult<()> {
        let physical_expr_id = self.find_repr_physical_expr(physical_expr_id).await;

        let entry = self.cost_dependency.entry(physical_expr_id);

        match entry {
            Entry::Occupied(occupied) => {
                let dep = occupied.into_mut();
                dep.status = TaskStatus::Clean;
            }
            Entry::Vacant(vacant) => {
                vacant.insert(CostDependency::new(TaskStatus::Clean));
            }
        }

        Ok(())
    }

    async fn add_transformation_dependency(
        &mut self,
        logical_expr_id: LogicalExpressionId,
        rule: &TransformationRule,
        group_id: GroupId,
    ) -> MemoResult<()> {
        let logical_expr_id = self.find_repr_logical_expr(logical_expr_id).await;
        let group_id = self.find_repr_group(group_id).await;
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
                let mut dep = RuleDependency::new(TaskStatus::Dirty);
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
    ) -> MemoResult<()> {
        let logical_expr_id = self.find_repr_logical_expr(logical_expr_id).await;
        let group_id = self.find_repr_group(group_id).await;
        let goal_id = self.find_repr_goal(goal_id).await;

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
                let mut dep = RuleDependency::new(TaskStatus::Dirty);
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
    ) -> MemoResult<()> {
        let physical_expr_id = self.find_repr_physical_expr(physical_expr_id).await;
        let goal_id = self.find_repr_goal(goal_id).await;

        match self.cost_dependency.entry(physical_expr_id) {
            Entry::Occupied(occupied) => {
                let dep = occupied.into_mut();
                dep.goal_ids.insert(goal_id);
            }
            Entry::Vacant(vacant) => {
                let mut dep = CostDependency::new(TaskStatus::Dirty);
                dep.goal_ids.insert(goal_id);
                vacant.insert(dep);
            }
        }

        Ok(())
    }
}
