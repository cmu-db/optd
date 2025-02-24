import dataclasses
from typing import List, Optional, Tuple


class JoinSet:
    def __init__(self):
        self.tasks = set()

    def spawn(self, task_func):
        self.tasks.add(task_func)

    def join_all(self):
        pass


class JoinHandle:
    def __init__(self):
        pass

    def join(self):
        pass


class Scheduler:
    def __init__() -> None:
        pass

    def spawn(self, task_func) -> JoinHandle:
        pass

    def join_set(self) -> JoinSet:
        pass


RelationGroupId = int
ScalarGroupId = int
GoalId = int
LogicalExpressionId = int
PhysicalExpressionId = int
ScalarExpressionId = int
Cost = float

XformRuleId = int
ImplRuleId = int


@dataclasses.dataclass
class Statistics:
    row_count: int


class PartialLogicalPlan:
    pass


class PartialPhysicalPlan:
    pass


@dataclasses.dataclass
class LogicalExpression:
    id: LogicalExpressionId
    children_relations: List[GoalId]
    children_scalars: List[ScalarGroupId]
    values: List[str]

    def into_partial(self) -> PartialLogicalPlan:
        pass


@dataclasses.dataclass
class PhysicalExpression:
    id: PhysicalExpressionId
    children_goals: List[GoalId]
    children_scalars: List[ScalarGroupId]
    values: List[str]


@dataclasses.dataclass
class ScalarExpression:
    id: ScalarExpressionId
    children_scalars: List[ScalarGroupId]
    values: List[str]


@dataclasses.dataclass
class PhysicalProperties:
    pass


@dataclasses.dataclass
class RelationGroup:
    id: RelationGroupId
    exploration_status: str


@dataclasses.dataclass
class Goal:
    id: GoalId
    group_id: RelationGroupId
    required_props: PhysicalProperties
    optimization_status: str


class CostModel:
    def __init__(self):
        pass

    def compute_statistics(
        self, physical_expr: PhysicalExpression, children_statistics
    ) -> Statistics:
        pass

    def compute_operaton_cost(self, physical_expr: PhysicalExpression) -> Cost:
        pass


class Memo:
    def __init__(self):
        pass

    def create_goal(
        self, group_id: RelationGroupId, required_props: PhysicalProperties
    ) -> Goal:
        pass

    def update_goal_status(self, goal_id: GoalId, status: str):
        pass

    def get_logical_expr_exploration_status(
        self, logical_expr_id: LogicalExpressionId
    ) -> str:
        pass

    def update_logical_expr_exploration_status(
        self, logical_expr_id: LogicalExpressionId, status: str
    ):
        pass

    def get_relation_group(self, group_id: RelationGroupId) -> RelationGroup:
        pass

    def get_all_logical_exprs_in_group(
        self, group_id: RelationGroupId
    ) -> List[LogicalExpression]:
        pass

    def get_all_physical_exprs_in_goal(
        self, goal_id: GoalId
    ) -> List[PhysicalExpression]:
        pass

    def get_winner_physical_expr_in_goal(
        self, goal_id: GoalId
    ) -> Optional[Tuple[PhysicalExpression, Cost, Statistics]]:
        pass

    def add_logical_expr_to_group(
        logical_expr: LogicalExpression, group_id: RelationGroupId
    ) -> RelationGroupId:
        pass

    def add_physical_expr_to_goal(
        self, physical_expr: PhysicalExpression, goal_id: GoalId
    ) -> GoalId:
        pass

    def update_physical_expr_cost_and_statistics(
        self, physical_expr_id: PhysicalExpressionId, cost: Cost, statistics: Statistics
    ):
        pass

    def xform_rule_applied(
        self, xform_rule_id: XformRuleId, logical_expr: LogicalExpression
    ) -> bool:
        pass

    def impl_rule_applied(
        self, xform_rule_id: XformRuleId, logical_expr: LogicalExpression
    ) -> bool:
        pass

    def impl_rule_top_matches(
        self, xform_rule_id: XformRuleId, logical_expr: LogicalExpression
    ) -> bool:
        pass

    def impl_rule_top_matches(
        self, impl_rule_id: ImplRuleId, physical_expr: PhysicalExpression
    ) -> bool:
        pass


class Intepreter:
    def __init__(self, memo: Memo):
        self.memo = memo

    def match_and_apply_xform_rule(
        self, xform_rule_id: XformRuleId, partial_logical_plan: PartialLogicalPlan
    ) -> List[PartialLogicalPlan]:
        pass

    def match_and_apply_impl_rule(
        self,
        impl_rule_id: ImplRuleId,
        partial_logical_plan: PartialLogicalPlan,
        goal_id: GoalId,
    ) -> List[PartialPhysicalPlan]:
        pass


class TaskContext:
    def __init__(self, memo: Memo, scheduler: Scheduler, cost_model: CostModel):
        self.memo = memo
        self.scheduler = scheduler
        self.cost_model = cost_model
        self.xform_rules = []
        self.impl_rules = []
        self.intepreter = Intepreter(memo)

    def optimize_goal(
        self, group: RelationGroupId, required_physical_props: PhysicalProperties
    ):
        goal = self.memo.create_goal(group, required_physical_props)
        if goal.optimization_status == "optimized":
            return
        elif goal.optimization_status == "pending":
            # wait for the goal to be optimized
            return

        assert goal.optimization_status == "not_optimized"
        self.memo.update_goal_status(goal.id, "pending")

        optimize_inputs_tasks = self.scheduler.join_set()
        physical_exprs = self.memo.get_all_physical_exprs_in_goal(goal.id)
        for physical_expr in physical_exprs:
            optimize_inputs_tasks.spawn(
                lambda: self.optimize_physical_expr(
                    physical_expr.children_goals[0],
                    PhysicalProperties(),
                )
            )

        optimize_inputs_tasks.join_all()

        logical_exprs = self.memo.get_all_logical_exprs_in_group(group)
        for logical_expr in logical_exprs:
            self.optimize_logical_expr(logical_expr, goal.id)

    def explore_group(self, group_id: RelationGroupId):
        logical_exprs = self.memo.get_all_logical_exprs_in_group(group_id)

        explore_logical_expr_tasks = self.scheduler.join_set()
        for logical_expr in logical_exprs:
            explore_logical_expr_tasks.spawn(
                lambda: self.explore_logical_expr(logical_expr)
            )
        explore_logical_expr_tasks.join_all()

    def explore_logical_expr(self, logical_expr: LogicalExpression):
        xform_rule_tasks = self.scheduler.join_set()

        for xform_rule_id in self.xform_rules:
            if self.memo.impl_rule_top_matches(
                xform_rule_id, logical_expr
            ) and not self.memo.xform_rule_applied(xform_rule_id, logical_expr):
                explore_children_group_tasks = self.scheduler.join_set()
                explore_children_group_tasks = self.scheduler.join_set()
                for child_group_id in logical_expr.children_relations:
                    explore_children_group_tasks.spawn(
                        lambda: self.explore_group(child_group_id)
                    )
                explore_children_group_tasks.join_all()
                xform_rule_tasks.spawn(
                    lambda: self.apply_xform_rule(xform_rule_id, logical_expr)
                )

        new_logical_expressions: List[LogicalExpression] = xform_rule_tasks.join_all()

        explore_new_logical_expr_tasks = self.scheduler.join_set()
        for new_logical_expr in new_logical_expressions:
            explore_new_logical_expr_tasks.spawn(
                lambda: self.explore_logical_expr(new_logical_expr)
            )

    def optimize_logical_expr(self, logical_expr: LogicalExpression, goal_id: GoalId):
        impl_rule_tasks = self.scheduler.join_set()

        for impl_rule_id in self.impl_rules:
            if self.memo.impl_rule_top_matches(
                impl_rule_id, logical_expr
            ) and not self.memo.impl_rule_applied(impl_rule_id, logical_expr):
                explore_children_group_tasks = self.scheduler.join_set()
                for child_group_id in logical_expr.children_relations:
                    explore_children_group_tasks.spawn(
                        lambda: self.explore_group(child_group_id)
                    )
                explore_children_group_tasks.join_all()
                impl_rule_tasks.spawn(
                    lambda: self.apply_impl_rule(impl_rule_id, logical_expr)
                )

        new_physical_expressions: List[PhysicalExpression] = impl_rule_tasks.join_all()

        optimize_new_physical_expr_tasks = self.scheduler.join_set()
        for new_physical_expr in new_physical_expressions:
            optimize_new_physical_expr_tasks.spawn(
                lambda: self.optimize_physical_expr(new_physical_expr)
            )

    def apply_xform_rule(
        self, xform_rule_id: XformRuleId, logical_expr: LogicalExpression
    ) -> List[LogicalExpression]:
        partial_logical_plans = self.intepreter.match_and_apply_xform_rule(
            xform_rule_id, logical_expr.into_partial()
        )

        new_logical_exprs = []
        for partial_logical_plan in partial_logical_plans:
            new_logical_expr = self.ingest_partial_logical_plan(partial_logical_plan)
            if new_logical_expr:
                new_logical_exprs.append(new_logical_expr)

        return new_logical_exprs

    def apply_impl_rule(
        self,
        impl_rule_id: ImplRuleId,
        logical_expr: LogicalExpression,
        goal_id: GoalId,
    ) -> List[PhysicalExpression]:
        partial_physical_plans = self.intepreter.match_and_apply_impl_rule(
            impl_rule_id,
            logical_expr.into_partial(),
            goal_id,
        )

        new_physical_exprs = []
        for partial_physical_plan in partial_physical_plans:
            new_physical_expr = self.ingest_partial_physical_plan(partial_physical_plan)
            if new_physical_expr:
                new_physical_exprs.append(new_physical_expr)
        return new_physical_exprs

    def ingest_partial_logical_plan(
        self, partial_plan: PartialLogicalPlan
    ) -> Optional[LogicalExpression]:
        pass

    def ingest_partial_physical_plan(
        self, partial_plan: PartialPhysicalPlan
    ) -> Optional[PhysicalExpression]:
        pass

    # Called `optimize_inputs` in the original code
    def optimize_physical_expr(self, physical_expr: PhysicalExpression):
        # TODO: integrate pruning

        for child_goal_id in physical_expr.children_goals:
            child_goal = self.memo.get_goal(child_goal_id)
            handle = self.scheduler.spawn(
                lambda: self.optimize_goal(
                    child_goal.group_id, child_goal.required_props
                )
            )
            handle.join()

        children_winner_physical_exprs = []
        children_costs = []
        children_statistics = []
        for child_goal_id in physical_expr.children_goals:
            (
                winner_physical_expr,
                cost,
                statistics,
            ) = self.memo.get_winner_physical_expr_in_goal(child_goal_id)

            children_winner_physical_exprs.append(winner_physical_expr)
            children_costs.append(cost)
            children_statistics.append(statistics)

        statistics = self.cost_model.compute_statistics(
            physical_expr, children_statistics
        )
        operation_cost = self.cost_model.compute_operaton_cost(physical_expr)
        cost = operation_cost + sum(children_costs)

        self.memo.update_physical_expr_cost_and_statistics(
            physical_expr.id, cost, statistics
        )
