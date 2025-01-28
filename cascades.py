scheduler = ()
memo = ()
rules = []
cost_model = {}

"""
General note: we could match with all the rules at once, instead of one by one. This would allow us to explore
more alternatives at once. We chose the one by one approach to keep the code simple.
"""


# Finds and stores the best physical expression for a given group, satisfying the required
# physical properties. Stores the group winner in the memo table (group_winner).
def optimize_group(group_id, required_phy_props, cost_budget):
    if memo.get_group_status(group_id) == "Unexplored":
        scheduler.spawn(explore_group(group_id)).join()

    memo.set_group_winner_status(group_id, required_phy_props, "Pending")

    tasks = []
    # This includes Scalar & Relation expressions.
    # The group has been explored at this point.
    for logical_expr in memo.get_all_logical_exprs_in_group(group_id):
        tasks.append(scheduler.spawn(optimize_logical_expr(
            logical_expr, required_phy_props, cost_budget)))
    tasks.join_all()

    memo.find_and_store_group_winner(group_id, required_phy_props, "Done")


# Explore a group by adding all the logical expressions that can be derived from the logical
# expressions in the group.
def explore_group(group_id):
    memo.set_group_status(group_id, "Exploring")

    tasks = []
    # Can be a subset & ordered (promise & guidance).
    for logical_expr in memo.get_all_logical_exprs_in_group(group_id):
        task = scheduler.spawn(explore_logical_expr(logical_expr))
        tasks.append(task)
    tasks.join_all()

    memo.set_group_status(group_id, "Explored")


# Explore all the possible logical expressions that can be derived from a given logical expression.
def explore_logical_expr(logical_expr):
    # Recursively explore your children groups first if not already explored.
    tasks = []
    for child_group in memo.get_all_children_groups(logical_expr):
        if memo.get_group_status(child_group) == "Unexplored":
            task = scheduler.spawn(explore_group(child_group))
            tasks.append(task)
    tasks.join_all()

    # Apply all logical transformation rules to the current logical expression.
    tasks = []
    # Can be a subset & ordered (promise & guidance).
    for transformation_rule in rules:
        if not memo.rule_has_been_applied(logical_expr, transformation_rule):
            tasks.append(scheduler.spawn(
                match_and_apply_logical_rule(logical_expr, transformation_rule)))
    tasks.join_all()


# Recursively extracts all expressions within the partial logical plan, and stores them in the memo table.
# This might create new groups and even trigger group merges. The function returns (1) the new expressions added
# into the *original* group of the logical expression, and (2) all the others that were created.
def extract_store_and_return_logical_exprs(partial_logical_plan):
    # TODO: Will need from the memo table: add_logical_expr(group_id, logical_expr), find_group(logical_expr),
    # merge_groups(group1, group2).
    pass


# This function adds new logical expressions to the memo table, after application of a logical rule.
def match_and_apply_logical_rule(logical_expr, transformation_rule):
    # TODO: Will need from the memo table: get_all_logical_expr_matching_operator(group_id, operator).
    partial_logical_plans = transformation_rule.generate_all_partial_plans(
        logical_expr)
    new_logical_exprs_in_group, other_new_logical_exprs = extract_store_and_return_logical_exprs(
        partial_logical_plans)

    for new_logical_expr in new_logical_exprs_in_group.union(other_new_logical_exprs):
        memo.set_rule_applied(
            logical_expr, new_logical_expr, transformation_rule)

    tasks = []
    # Iteratively explore the newly generated logical expressions.
    # Can be a subset & ordered (promise & guidance).
    for new_logical_expr in new_logical_exprs_in_group:
        tasks.append(scheduler.spawn(explore_logical_expr(new_logical_expr)))
    tasks.join_all()


# Applies implementation rules to the logical expression and generate new physical expressions.
def optimize_logical_expr(logical_expr, required_phy_props, cost_budget):
    tasks = []
    for implementation_rule in rules:
        if not logical_expr.applied(implementation_rule):
            tasks.append(scheduler.spawn(match_and_apply_physical_rule(
                logical_expr, implementation_rule, required_phy_props, cost_budget)))
    tasks.join_all()


# This function adds new physical expressions to the memo table, and optimizes them with the required
# physical properties.
def match_and_apply_physical_rule(logical_expr, implementation_rule, required_phy_props, cost_budget):
    # This will not return a partial physical plan, but rather a single physical expression, 
    # as per Cascades specifications.
    new_physical_expr = implementation_rule.generate_expr(logical_expr)
    if memo.get_physical_expr(logical_expr.group_id, required_phy_props) is None:
        memo.add_physical_expr(logical_expr.group_id, new_physical_expr)

    # This will recursively optimize the physical expression with the required physical properties.
    # Can be a subset & ordered (promise & guidance).
    scheduler.spawn(optimize_physical_expr(
        new_physical_expr, required_phy_props, cost_budget)).join()


# TODO: Find a better name once we reorg the code.
def optimize_physical_expr_inner(physical_expr, children_groups, required_phy_props, cost_budget, passthrough=True):
    # e.g. children_required_phy_props: [child0: [prop1, prop2], child1: [prop3, prop4]]
    children_required_phy_props, enforced_phy_props = physical_expr.derive(
        required_phy_props, passthrough)
    total_cost_so_far = 0
    for i, (child_group, child_required_phy_props) in enumerate(zip(children_groups, children_required_phy_props)):
        # TODO: We could launch in parallel, and cancel pending tasks if the current sum of costs is already above the budget.
        scheduler.spawn(optimize_group(child_group,
              child_required_phy_props, cost_budget)).join()

        # TODO: There are opportunities to use statistics from only some of your children and start pricing a heuristic cost
        # of the expression (e.g. the cost model should be conservative if some children have missing statistics and assume
        # the best case scenarios for them).
        total_cost_so_far += memo.get_input_costs(child_group)

        if total_cost_so_far > cost_budget:
            return None

    children_statistics = memo.get_statistics_from_winners(
        children_groups, children_required_phy_props)
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
    memo.set_phy_expr_winner_status(physical_expr, required_phy_props, "Pending")

    children_groups = memo.get_all_children_groups(physical_expr.group_id)

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
    memo.store_phy_expr_winner(
        physical_expr, required_phy_props, enforced_phy_props, cost, statistics, "Done")
