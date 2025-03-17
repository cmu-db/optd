use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
};

use crate::{
    cir::{
        expressions::{LogicalExpression, OptimizedExpression, PhysicalExpression},
        goal::{Cost, Goal},
        group::GroupId,
        properties::{LogicalProperties, PhysicalProperties},
        rules::{ImplementationRule, TransformationRule},
    },
    error::Error,
    optimizer::memo::*,
};

#[derive(Debug, Default)]
pub struct MemoryMemo {
    /// All the groups in the memo.
    groups: HashMap<GroupId, GroupState>,
    /// Maps logical expression id to its group id.
    logical_expr_id_to_group_id: HashMap<LogicalExpressionId, GroupId>,
    /// Maps logical expression id to its logical expression node.
    logical_expr_id_to_node: HashMap<LogicalExpressionId, LogicalExpression>,
    /// Maps logical expression to its id.
    logical_expr_node_to_id: HashMap<LogicalExpression, LogicalExpressionId>,

    /// All the goals in the memo.
    goals: HashMap<GoalId, GoalState>,
    /// Maps physical expression id to its goal id.
    physical_expr_id_to_goal_id: HashMap<PhysicalExpressionId, GoalId>,
    /// Maps goal to its id.
    goal_node_to_id: HashMap<Goal, GoalId>,
    /// Maps physical expression id to its physical expression node.
    /// The cost is `None` if the physical expression is not yet costed.
    physical_expr_id_to_node: HashMap<PhysicalExpressionId, (PhysicalExpression, Option<Cost>)>,
    /// Maps physical expression to its id.
    physical_expr_node_to_id: HashMap<PhysicalExpression, PhysicalExpressionId>,

    /// Tracks the status of applying a transformation rule on a logical expression.
    transformation_status: HashMap<(LogicalExpressionId, TransformationRule), Status>,
    /// Tracks the status of applying an implementation rule on a logical expression.
    implementation_status: HashMap<(LogicalExpressionId, ImplementationRule), Status>,
    /// Tracks the status of costing a physical expression.
    costing_status: HashMap<PhysicalExpressionId, Status>,

    /// Counter for generating unique group, goal, and expression ids.
    uuid_counter: i64,
}

impl MemoryMemo {
    /// Creates a new empty memo.
    pub fn new() -> Self {
        MemoryMemo {
            uuid_counter: 0,
            ..Default::default()
        }
    }
}

impl Memoize for MemoryMemo {
    async fn get_logical_properties(&self, group_id: GroupId) -> MemoizeResult<LogicalProperties> {
        let group = self.get_group_state(group_id)?;
        return Ok(group.properties.clone());
    }

    async fn get_all_logical_exprs(
        &self,
        group_id: GroupId,
    ) -> MemoizeResult<Vec<LogicalExpression>> {
        // group_id -> group -> all logical expression ids -> all logical expressions
        let group = self.get_group_state(group_id)?;

        return Ok(group
            .logical_exprs
            .iter()
            .map(|logical_expr_id| self.get_logical_expr(logical_expr_id).clone())
            .collect());
    }

    async fn find_logical_expr(
        &self,
        logical_expr: &LogicalExpression,
    ) -> MemoizeResult<Option<GroupId>> {
        let maybe_group_id = self
            .get_logical_expr_id(logical_expr)
            .map(|logical_expr_id| self.get_group_id_of(&logical_expr_id));
        Ok(maybe_group_id)
    }

    async fn create_group(
        &mut self,
        logical_expr: &LogicalExpression,
        props: &LogicalProperties,
    ) -> MemoizeResult<GroupId> {
        let logical_expr_id = self.insert_logical_expr(logical_expr);
        let group_id = self.create_group_inner(std::iter::once(logical_expr_id), props.clone());
        Ok(group_id)
    }

    async fn merge_groups(
        &mut self,
        group_1: GroupId,
        group_2: GroupId,
    ) -> MemoizeResult<Vec<MergeResult>> {
        let mut merge_results = Vec::new();
        self.merge_groups_inner(group_1, group_2, &mut merge_results)?;
        Ok(merge_results)
    }

    async fn get_best_optimized_physical_expr(
        &self,
        goal: &Goal,
    ) -> MemoizeResult<Option<OptimizedExpression>> {
        let goal_state = self.get_goal_state(goal)?;
        let best_optimized_expr =
            goal_state
                .best_optimized_expr_id
                .as_ref()
                .map(|best_optimized_expr_id| {
                    let (physical_expr, cost) = self
                        .get_physical_expr_with_cost(best_optimized_expr_id)
                        .clone();

                    OptimizedExpression(
                        physical_expr,
                        cost.expect("best optimized expression should have a cost"),
                    )
                });
        Ok(best_optimized_expr)
    }

    async fn get_all_physical_exprs(&self, goal: &Goal) -> MemoizeResult<Vec<PhysicalExpression>> {
        let goal_state = self.get_goal_state(goal)?;

        Ok(goal_state
            .physical_exprs
            .iter()
            .map(|physical_expr_id| {
                let (physical_expr, _) = self.get_physical_expr_with_cost(physical_expr_id);
                physical_expr.clone()
            })
            .collect())
    }

    async fn find_physical_expr(
        &self,
        physical_expr: &PhysicalExpression,
    ) -> MemoizeResult<Option<Goal>> {
        let maybe_goal = self
            .physical_expr_node_to_id
            .get(physical_expr)
            .map(|physical_expr_id| self.get_goal_state_of(physical_expr_id).goal.clone());
        Ok(maybe_goal)
    }

    async fn create_goal(&mut self, physical_expr: &PhysicalExpression) -> MemoizeResult<Goal> {
        let physical_expr_id = self.insert_physical_expr(physical_expr);
        let goal = self.create_goal_inner(physical_expr_id);
        Ok(goal)
    }

    async fn merge_goals(
        &mut self,
        goal_1: &Goal,
        goal_2: &Goal,
    ) -> MemoizeResult<Vec<MergeResult>> {
        todo!()
    }

    async fn get_equivalent_goals(
        &self,
        goal: &Goal,
    ) -> MemoizeResult<HashMap<GroupId, Vec<PhysicalProperties>>> {
        todo!()
    }

    async fn add_optimized_physical_expr(
        &mut self,
        goal: &Goal,
        OptimizedExpression(physical_expr, cost): &OptimizedExpression,
    ) -> MemoizeResult<bool> {
        let physical_expr_id = self
            .physical_expr_node_to_id
            .get(physical_expr)
            .expect("physical expression should be registered in memo");

        let (_, physical_expr_cost) = self
            .physical_expr_id_to_node
            .get_mut(physical_expr_id)
            .expect("physical expression id should have a mapping node");

        physical_expr_cost.replace(cost.clone());

        let goal_id = self
            .goal_node_to_id
            .get(goal)
            .expect("goal should be registered in memo");

        let goal_state = self
            .goals
            .get_mut(goal_id)
            .expect("goal id should have a mapping state");

        assert!(goal_state.physical_exprs.contains(physical_expr_id));

        let current_best_cost = goal_state
            .best_optimized_expr_id
            .map(|best_optimized_expr_id| {
                self.physical_expr_id_to_node
                    .get(&best_optimized_expr_id)
                    .expect("best optimized expression id should have a mapping node")
                    .1
                    .expect("best optimized expression should have a cost")
            })
            .unwrap_or(Cost(f64::MAX));

        if cost < &current_best_cost {
            goal_state.best_optimized_expr_id = Some(*physical_expr_id);
            return Ok(true);
        }

        Ok(false)
    }

    async fn get_transformation_status(
        &self,
        logical_expr: &LogicalExpression,
        rule: &TransformationRule,
    ) -> MemoizeResult<Status> {
        let logical_expr_id = self
            .get_logical_expr_id(logical_expr)
            .ok_or_else(|| MemoizeError::LogicalExprNotFound(logical_expr.clone()))?;

        let status = self
            .transformation_status
            .get(&(logical_expr_id, rule.clone()))
            .cloned()
            .unwrap_or(Status::Dirty);

        Ok(status)
    }

    async fn set_transformation_status(
        &mut self,
        logical_expr: &LogicalExpression,
        rule: &TransformationRule,
        status: Status,
    ) -> MemoizeResult<()> {
        let logical_expr_id = self
            .get_logical_expr_id(logical_expr)
            .ok_or_else(|| MemoizeError::LogicalExprNotFound(logical_expr.clone()))?;

        self.transformation_status
            .insert((logical_expr_id, rule.clone()), status);
        Ok(())
    }

    async fn get_implementation_status(
        &self,
        logical_expr: &LogicalExpression,
        rule: &ImplementationRule,
    ) -> MemoizeResult<Status> {
        let logical_expr_id = self
            .get_logical_expr_id(logical_expr)
            .ok_or_else(|| MemoizeError::LogicalExprNotFound(logical_expr.clone()))?;

        let status = self
            .implementation_status
            .get(&(logical_expr_id, rule.clone()))
            .cloned()
            .unwrap_or(Status::Dirty);

        Ok(status)
    }

    async fn set_implementation_status(
        &mut self,
        logical_expr: &LogicalExpression,
        rule: &ImplementationRule,
        status: Status,
    ) -> MemoizeResult<()> {
        let logical_expr_id = self
            .get_logical_expr_id(logical_expr)
            .ok_or_else(|| MemoizeError::LogicalExprNotFound(logical_expr.clone()))?;

        self.implementation_status
            .insert((logical_expr_id, rule.clone()), status);
        Ok(())
    }

    async fn get_costing_status(
        &self,
        physical_expr: &PhysicalExpression,
    ) -> MemoizeResult<Status> {
        let physical_expr_id = self
            .get_physical_expr_id(physical_expr)
            .ok_or_else(|| MemoizeError::PhysicalExprNotFound(physical_expr.clone()))?;

        let status = self
            .costing_status
            .get(&physical_expr_id)
            .cloned()
            .unwrap_or(Status::Dirty);

        Ok(status)
    }

    async fn set_costing_status(
        &mut self,
        physical_expr: &PhysicalExpression,
        status: Status,
    ) -> MemoizeResult<()> {
        let physical_expr_id = self
            .get_physical_expr_id(physical_expr)
            .ok_or_else(|| MemoizeError::PhysicalExprNotFound(physical_expr.clone()))?;

        self.costing_status.insert(physical_expr_id, status);

        // Also need to invalidate the best optimized expression for the goal if needed.
        if status == Status::Dirty {
            self.invalidate_best_optimized_expr(&physical_expr_id);
        }

        Ok(())
    }
}

impl MemoryMemo {
    fn get_group_state(&self, group_id: GroupId) -> MemoizeResult<&GroupState> {
        let group_state = self
            .groups
            .get(&group_id)
            .ok_or_else(|| MemoizeError::GroupNotFound(group_id))?;
        Ok(group_state)
    }

    fn remove_group_state(&mut self, group_id: GroupId) -> MemoizeResult<GroupState> {
        let group_state = self
            .groups
            .remove(&group_id)
            .ok_or_else(|| MemoizeError::GroupNotFound(group_id))?;
        Ok(group_state)
    }

    fn get_group_id_of(&self, logical_expr_id: &LogicalExpressionId) -> GroupId {
        *self
            .logical_expr_id_to_group_id
            .get(logical_expr_id)
            .expect("logical expression id should have a mapping group id")
    }

    fn get_goal_state_of(&self, physical_expr_id: &PhysicalExpressionId) -> &GoalState {
        let goal_id = self
            .physical_expr_id_to_goal_id
            .get(physical_expr_id)
            .expect("physical expression id should have a mapping goal id");

        self.goals
            .get(goal_id)
            .expect("goal id should have a mapping state")
    }

    fn get_goal_state(&self, goal: &Goal) -> MemoizeResult<&GoalState> {
        let goal_id = self
            .goal_node_to_id
            .get(goal)
            .ok_or_else(|| Error::Memo(MemoizeError::GoalNotFound(goal.clone())))?;

        let goal_state = self
            .goals
            .get(goal_id)
            .expect("goal id should have a mapping state");

        Ok(goal_state)
    }

    // logical expression id is only used in the memo internally.
    // so we can safely unwrap here.
    fn get_logical_expr(&self, logical_expr_id: &LogicalExpressionId) -> &LogicalExpression {
        self.logical_expr_id_to_node
            .get(logical_expr_id)
            .expect("logical expression id should have a mapping node")
    }

    fn get_logical_expr_id(&self, logical_expr: &LogicalExpression) -> Option<LogicalExpressionId> {
        self.logical_expr_node_to_id.get(logical_expr).copied()
    }

    fn get_physical_expr_with_cost(
        &self,
        best_optimized_expr_id: &PhysicalExpressionId,
    ) -> &(PhysicalExpression, Option<Cost>) {
        self.physical_expr_id_to_node
            .get(best_optimized_expr_id)
            .expect("physical expression id should have a mapping node")
    }

    fn get_physical_expr_id(
        &self,
        physical_expr: &PhysicalExpression,
    ) -> Option<PhysicalExpressionId> {
        self.physical_expr_node_to_id.get(physical_expr).copied()
    }

    fn insert_logical_expr(&mut self, logical_expr: &LogicalExpression) -> LogicalExpressionId {
        let logical_expr_id = self.next_logical_expression_id();
        self.logical_expr_id_to_node
            .insert(logical_expr_id, logical_expr.clone());
        self.logical_expr_node_to_id
            .insert(logical_expr.clone(), logical_expr_id);
        logical_expr_id
    }

    fn insert_physical_expr(&mut self, physical_expr: &PhysicalExpression) -> PhysicalExpressionId {
        let physical_expr_id = self.next_physical_expression_id();
        self.physical_expr_id_to_node
            .insert(physical_expr_id, (physical_expr.clone(), None));
        self.physical_expr_node_to_id
            .insert(physical_expr.clone(), physical_expr_id);
        physical_expr_id
    }

    /// Creates a singleton group and links the logical expression with the group.
    fn create_group_inner(
        &mut self,
        logical_expr_ids: impl Iterator<Item = LogicalExpressionId>,
        props: LogicalProperties,
    ) -> GroupId {
        let mut group = GroupState::new(props);
        let group_id = self.next_group_id();

        for logical_expr_id in logical_expr_ids {
            // Links the logical expression with the group.
            group.add_logical_expr(logical_expr_id);
            self.logical_expr_id_to_group_id
                .insert(logical_expr_id, group_id);
        }
        self.groups.insert(group_id, group);
        group_id
    }

    /// Creates a singleton goal with default physical properties and links the physical expression with the goal.
    fn create_goal_inner(&mut self, physical_expr_id: PhysicalExpressionId) -> Goal {
        // Creates an empty group with default logical property.
        let group = GroupState::new(LogicalProperties::default());
        let group_id = self.next_group_id();
        self.groups.insert(group_id, group);

        // Creates a goal with the group id.
        let goal = Goal(group_id, PhysicalProperties::default());
        let goal_state = GoalState::singleton(physical_expr_id, goal.clone());
        let goal_id = self.next_goal_id();
        self.goals.insert(goal_id, goal_state);
        self.goal_node_to_id.insert(goal.clone(), goal_id);

        // Links the physical expression with the goal.
        self.physical_expr_id_to_goal_id
            .insert(physical_expr_id, goal_id);

        goal
    }

    fn invalidate_best_optimized_expr(&mut self, best_optimized_expr_id: &PhysicalExpressionId) {
        let (_, physical_expr_cost) = self
            .physical_expr_id_to_node
            .get_mut(best_optimized_expr_id)
            .expect("physical expression id should have a mapping node");

        // Set the cost to None to indicate that the expression needs to be re-costed.
        physical_expr_cost.take();

        let goal_id = self
            .physical_expr_id_to_goal_id
            .get(best_optimized_expr_id)
            .expect("physical expression id should have a mapping goal id");

        let goal_state = self
            .goals
            .get_mut(goal_id)
            .expect("goal id should have a mapping state");

        if goal_state.best_optimized_expr_id == Some(*best_optimized_expr_id) {
            // Recompute best optimized expr in the goal with min cost.
            let best_after_invalidation = goal_state
                .physical_exprs
                .iter()
                .filter_map(|id| {
                    self.physical_expr_id_to_node
                        .get(&id)
                        .expect("physical expression id should have a mapping node")
                        .1
                        .map(|cost| (id, cost))
                })
                .min_by(|(_, c1), (_, c2)| {
                    c1.partial_cmp(c2).unwrap_or(std::cmp::Ordering::Greater)
                })
                .map(|(id, _)| *id);
            goal_state.best_optimized_expr_id = best_after_invalidation;
        }
    }

    fn merge_groups_inner(
        &mut self,
        group_1: GroupId,
        group_2: GroupId,
        merge_results: &mut Vec<MergeResult>,
    ) -> MemoizeResult<()> {
        todo!()
    }

    /// Gets the next group id.
    fn next_group_id(&mut self) -> GroupId {
        let id = self.uuid_counter;
        self.uuid_counter += 1;
        GroupId(id)
    }

    /// Gets the next goal id.
    fn next_goal_id(&mut self) -> GoalId {
        let id = self.uuid_counter;
        self.uuid_counter += 1;
        GoalId(id)
    }

    /// Gets the next logical expression id.
    fn next_logical_expression_id(&mut self) -> LogicalExpressionId {
        let id = self.uuid_counter;
        self.uuid_counter += 1;
        LogicalExpressionId(id)
    }

    /// Gets the next physical expression id.
    fn next_physical_expression_id(&mut self) -> PhysicalExpressionId {
        let id = self.uuid_counter;
        self.uuid_counter += 1;
        PhysicalExpressionId(id)
    }
}

/// Unique identifier for a logical expression internal to the memo.
#[derive(Debug, Clone, Copy, PartialEq, Hash, Eq)]
struct LogicalExpressionId(i64);

/// Unique identifier for a physical expression internal to the memo.
#[derive(Debug, Clone, Copy, PartialEq, Hash, Eq)]
struct PhysicalExpressionId(i64);

/// Unique identifier for a goal internal to the memo.
#[derive(Debug, Clone, Copy, PartialEq, Hash, Eq)]
struct GoalId(i64);

#[derive(Debug)]
struct GroupState {
    /// All equivalent logical expressions in the group.
    logical_exprs: HashSet<LogicalExpressionId>,
    /// The logical properties shared by all logical expressions in the group.
    properties: LogicalProperties,
}

impl GroupState {
    fn new(properties: LogicalProperties) -> Self {
        GroupState {
            logical_exprs: HashSet::new(),
            properties,
        }
    }

    fn add_logical_expr(&mut self, logical_expr_id: LogicalExpressionId) {
        self.logical_exprs.insert(logical_expr_id);
    }
}

#[derive(Debug)]
struct GoalState {
    /// All equivalent physical expressions in the goal.
    physical_exprs: HashSet<PhysicalExpressionId>,
    /// The best optimized physical expression for the goal.
    /// This field needs to be invalidated when we  decide to re-cost the best optimized expression.
    best_optimized_expr_id: Option<PhysicalExpressionId>,
    /// The optimization goal.
    goal: Goal,
}

impl GoalState {
    fn singleton(physical_expr_id: PhysicalExpressionId, goal: Goal) -> Self {
        let mut physical_exprs = HashSet::new();
        physical_exprs.insert(physical_expr_id);

        GoalState {
            physical_exprs,
            best_optimized_expr_id: None,
            goal,
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::cir::{
        operators::{Child, Operator, OperatorData},
        properties::PropertiesData,
    };

    use super::*;

    fn create_leaf<T>(tag: &str, data: &str) -> Operator<T> {
        Operator {
            tag: tag.to_string(),
            data: vec![OperatorData::String(data.to_string())],
            children: vec![],
        }
    }

    fn create_inner<T>(tag: &str, children: Vec<T>, data: &str) -> Operator<T> {
        Operator {
            tag: tag.to_string(),
            data: vec![],
            children: children
                .into_iter()
                .map(|child| Child::Singleton(child))
                .collect(),
        }
    }

    fn logical_prop_with_schema_length(schema_len: i64) -> LogicalProperties {
        LogicalProperties(Some(PropertiesData::Int64(schema_len)))
    }

    fn physical_prop_with_sort_order(column_index: i64, sort_order: i64) -> PhysicalProperties {
        PhysicalProperties(Some(PropertiesData::Array(vec![
            PropertiesData::Int64(column_index),
            PropertiesData::Int64(sort_order),
        ])))
    }
}
