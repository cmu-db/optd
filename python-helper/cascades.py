from memo import Memo
from itertools import product

rules = []
cost_model = {}
memo = Memo()


# Finds and stores the best physical expression for a given *relational* group, satisfying the required
# physical properties. Stores the group winner in the memo table.
def optimize_relational_group(group_id, required_phy_props, cost_budget):
    if memo.get_group_explore_status(group_id) == "Unexplored":
        explore_relational_group(group_id)

    # The group has been explored at this point.
    # We start optimizing the required goal (i.e. group/physical properties pair).
    memo.set_goal_optimize_status(group_id, required_phy_props, "Pending")

    # We optimize the group by optimizing all the *logical* expressions in the group.
    # Note: scalar expressions are *not* optimized here.
    for logical_expr in memo.get_all_logical_exprs_in_group(group_id):
        optimize_logical_expr(logical_expr, required_phy_props, cost_budget)

    # We go through all the optimized physical expressions in the group that satisfy the required physical
    # properties (a.k.a. the "goal"), and take the one with the lowest cost to be the group winner.
    expr, cost, stats = memo.get_goal_optimize_winner(
        group_id, required_phy_props)
    memo.set_goal_optimize_winner(
        group_id, required_phy_props, expr, cost, stats)


# Explore a relational group by adding all the logical expressions that can be derived from it.
def explore_relational_group(group_id):
    memo.set_group_explore_status(group_id, "Exploring")

    # We start exploring the current logical expressions in the group.
    # Can be a subset & ordered (promise & guidance).
    for logical_expr in memo.get_all_logical_exprs_in_group(group_id):
        explore_logical_expr(logical_expr)

    memo.set_group_explore_status(group_id, "Explored")


# Generates all partial logical plans that can be derived from a given logical expression & transformation rule.
# This function triggers group exploration on demand (if needed by the rule), and recursively generates partial
# logical plans for all the children of the logical expression.
def match_transformation_rule(logical_expr, matcher):
    # Match current operator.
    operator_match, children_matches = matcher
    if not logical_expr.operator == operator_match:
        return set()

    # Now recursively match the children.
    # TODO: Only logical group children for now. Scalar is analogous.
    matching_children_expr = []
    for child_group_id, child_match in zip(logical_expr.children(), children_matches):
        current_matches = []

        if child_match == "GroupId":
            current_matches.append(child_group_id)
        else:
            if memo.get_group_explore_status(child_group_id) == "Unexplored":
                explore_relational_group(child_group_id)

            # Recursively generate partial plans for the child.
            for child_expr in memo.get_all_logical_exprs_in_group(child_group_id):
                child_plans = match_transformation_rule(
                    child_expr, child_match)
                if not child_plans:  # If no matches found for this child, entire match fails.
                    return set()
                current_matches.extend(child_plans)

        matching_children_expr.append(current_matches)

    result = set()  # Generate all possible combinations.
    for children_plan in product(*matching_children_expr):
        # TODO: How to make this codegen friendly? (or have some trait?)
        new_expr = (logical_expr.operator, children_plan)
        result.add(new_expr)

    return result


# Applies the rule transformation to the partial logical plan, and returns a new partial logical plan.
def apply_transformation_rule(partial_logical_plan, applier):
    # TODO: Figure out this part... No clue how we can do this yet, as I don't want to hardcode rule
    # applications.
    pass


# Merges g2 into g1. Replaces (all?) references of g2 with g1 in the memo table.
def merge_groups(g1, g2):
    # TODO: Figure out how to do this with CRUD operations.
    # TODO: We also want to add this action into the memo table.
    pass


# Extracts all children expressions within the partial logical plan, inserts them into their group,
# or creates a new group if necessary. Has to explore the DAG in reverse topological order.
# Returns the root expression inserted.
# NOTE: The partial_logical_plan cannot be a group_id here (TODO: tradeoff in Alexis'
# type system alternative).
def ingest_partial_logical_plan(partial_logical_plan):
    # TODO: Lookup nice topological graph traversal algorithms.
    # When we get an expression, we lookup into the memo table with:
    # * memo.get_relational_group(logical_expr)
    #     -> if None, we create a new group and add the expression to it.
    #     -> if Some, we add the expression to the group, and replace in its parent the operator
    #        with the group_id.
    # * memo.add_logical_expr_to_group(group_id, logical_expr)
    pass


# Explore all the possible logical expressions that can be derived from a given logical expression.
def explore_logical_expr(logical_expr):
    # Apply all logical transformation rules to the current logical expression.
    # Can be a subset & ordered (promise & guidance).
    # Note: May explore new groups on demand, when needed by the matcher of a rule.
    # This is more efficient as it avoids blindly exploring all children groups recursively.
    for transformation_rule in rules:
        if not memo.has_transformation_been_applied(logical_expr, transformation_rule):
            partial_logical_plans = match_transformation_rule(
                logical_expr, transformation_rule.matcher)
            for partial_logical_plan in partial_logical_plans:
                applied_partial_logical_plan = apply_transformation_rule(
                    partial_logical_plan, transformation_rule.applier)

                # TODO: Figure out if we want the memo to support transactions?
                # Perhaps when sending to memo we attach some epoch id?
                # Then we trigger "apply all changes"?
                memo.set_transformation_applied(
                    logical_expr, transformation_rule)

                # The following scenario should be the only case when a merge might be needed.
                if applied_partial_logical_plan is "GroupId":
                    merge_groups(logical_expr.group_id,
                                 applied_partial_logical_plan.group_id)
                else:
                    root_logical_expr = ingest_partial_logical_plan(
                        applied_partial_logical_plan)
                    explore_logical_expr(root_logical_expr)


# Applies implementation rules to the logical expression and generate new physical expressions.
def optimize_logical_expr(logical_expr, required_phy_props, cost_budget):
    for implementation_rule in rules:
        if not memo.has_implementation_been_applied(logical_expr, implementation_rule):
            match_and_apply_physical_rule(
                logical_expr, implementation_rule, required_phy_props, cost_budget)


# This function adds new physical expressions to the memo table, and optimizes them with the required
# physical properties.
def match_and_apply_physical_rule(logical_expr, implementation_rule, required_phy_props, cost_budget):
    # This will not return a partial physical plan, but rather a single physical expression,
    # as per Cascades specifications.
    # TODO: This should be straightforward (not as hard as recursive logical matches).
    # Probably we apply the same paradigm for rule matching, but only on one level!
    new_physical_expr = implementation_rule.generate_expr(logical_expr)
    # TODO: Perhaps we need a new memo call for that, while working, it is a bit inefficient.
    if memo.get_relational_group(new_physical_expr) is None:
        memo.add_physical_expr_to_group(
            logical_expr.group_id, new_physical_expr)

    # This will recursively optimize the physical expression with the required physical properties.
    # Can be a subset & ordered (promise & guidance).
    optimize_physical_expr(new_physical_expr, required_phy_props, cost_budget)


# TODO: Find a better name once we reorg the code.
def optimize_physical_expr_inner(physical_expr, children_groups, required_phy_props, cost_budget, passthrough=True):
    # e.g. children_required_phy_props: [child0: [prop1, prop2], child1: [prop3, prop4]]
    children_required_phy_props, enforced_phy_props = physical_expr.derive(
        required_phy_props, passthrough)
    total_cost_so_far = 0
    for i, (child_group, child_required_phy_props) in enumerate(zip(children_groups, children_required_phy_props)):
        # TODO: We could launch in parallel, and cancel pending tasks if the current sum of costs is already above the budget.
        optimize_relational_group(
            child_group, child_required_phy_props, cost_budget)

        # TODO: There are opportunities to use statistics from only some of your children and start pricing a heuristic cost
        # of the expression (e.g. the cost model should be conservative if some children have missing statistics and assume
        # the best case scenarios for them).
        total_cost_so_far += memo.get_input_costs(child_group)

        if total_cost_so_far > cost_budget:
            return None

    children_statistics = [memo.get_goal_winner_statistics(child_group_id, children_required_phy_props) for child_group_id in children_groups]
    # Either we cost relation expressions or scalar expressions.
    total_cost_so_far += cost_model.compute_operation_cost(
        physical_expr, children_statistics)
    expr_statistics = cost_model.compute_statistics(
        physical_expr, children_statistics)
    enforce_cost, updated_expr_statistics = cost_model.compute_enforcement(
        physical_expr, expr_statistics, enforced_phy_props)
    total_cost_so_far += enforce_cost

    return total_cost_so_far, updated_expr_statistics, enforced_phy_props


# This is the physical equivalent of explore_logical_expr (NOTE: old "optimize_inputs" was a confusing name).
# --> Tries to get the lowest cost for this physical expression given the required physical properties.
# Stores the winner in the memo table (physical_expression_winner).
def optimize_physical_expr(physical_expr, required_phy_props, cost_budget):
    memo.set_physical_expr_optimize_status(
        physical_expr, required_phy_props, "Pending")

    children_groups = physical_expr.children()

    # Here we see the max physical properties we can passthrough.
    # If we can passthrough all the properties, enforced_phy_props will be empty.
    # If we can still passthrough *some* properties, enforced_phy_props will contain the properties that we must enforce.
    #
    # Note: Passthrough properties might be different for each child, and different from the original required_phy_props.
    #
    # In the optimization process we consider at most two approaches:
    # 1. Enforce all the required properties locally, and pass to the children <nil> properties.
    # 2. Passthrough as many physical properties as possible to the children, and enforce what we could not locally.
    #
    # Note: This approach is *not* exhaustive: perhaps it might be better to passthrough less and enforce more locally.
    # TODO: Probably guidance could help here, as exhaustive search would explode the search space.

    # PASS 1: With as much passthrough as possible, if feasible.
    with_passthrough = optimize_physical_expr_inner(
        physical_expr, children_groups, required_phy_props, cost_budget, passthrough=True)
    # PASS 2: Enforce all the required properties locally, and pass to the children <nil> properties.
    without_passthrough = optimize_physical_expr_inner(
        physical_expr, children_groups, required_phy_props, cost_budget, passthrough=False)

    if with_passthrough is None and without_passthrough is None:
        # We have exceeded the cost budget, and we have no results.
        return None

    if with_passthrough is None:
        cost, statistics, enforced_phy_props = without_passthrough
    elif without_passthrough is None:
        cost, statistics, enforced_phy_props = with_passthrough
    elif with_passthrough[0] < without_passthrough[0]:
        cost, statistics, enforced_phy_props = with_passthrough
    else:
        cost, statistics, enforced_phy_props = without_passthrough

    # (physical_expr.group_id, required_phy_props) is a "goal" id.
    memo.set_physical_expr_optimize_winner(
        physical_expr, required_phy_props, enforced_phy_props, cost, statistics)
