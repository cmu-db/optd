# ---------- New code for the optimization step only -------------------
    
class TransformationRule:
    def match_and_apply(logical_expr: LogicalExpr) -> list[LogicalExpr]:
        pass

class ImplementationRule:
    def match_and_apply(logical_expr: LogicalExpr) -> list[LogicalExpr]:
        pass
    
rules = []
memo = Memo()
cost_model = {}

def spawn(task):
    pass

# Let's assume we have explored all the groups have all their discovered logical expressions.
# Logical expressions will never be added again.
# Input: root group + required phys properties.
def optimize_group(group_id, required_phys_props, cost_budget):
    logical_exprs = memo.get_all_logical_exprs_in_group(group_id) 
    for logical_expr in logical_exprs:
        spawn(optimize_logical_expr(logical_expr, required_phys_props, cost_budget))
    
    # Join all the tasks spawned.
    

def optimize_logical_expr(logical_expr, required_phy_props, cost_budget):
    for implementation_rule in rules:
        if not logical_expr.applied(implementation_rule):
            spawn(match_and_apply_physical_rule(logical_expr, implementation_rule, required_phy_props, cost_budget))
        
        # Join all the tasks spawned

def match_and_apply_physical_rule(logical_expr, implementation_rule, required_phy_props, cost_budget):
    # TODO: mark rule as applied.
    physical_exprs = implementation_rule.match_and_apply(logical_expr)
    # For each new physical expression that gets added to the memo table (no duplicates).
    new_physical_exprs = memo.update(physical_exprs) 
    for new_physical_expr in new_physical_exprs: 
        spawn(optimize_inputs(new_physical_expr, required_phy_props, cost_budget))
    
    # Join all the tasks spawned.


# TODO: Find a better name once we reorg the code.
def optimize_inputs_inner(physical_expr, children_groups, required_phy_props, cost_budget, passthrough=True):
    # e.g. children_required_phy_props: [child0: [prop1, prop2], child1: [prop3, prop4]]
    children_required_phy_props, enforced_phy_props = physical_expr.derive(required_phy_props, passthrough)    
    total_cost_so_far = 0
    for i, (child_group, child_required_phy_props) in enumerate(zip(children_groups, children_required_phy_props)):
        # TODO: We could launch in parallel, and cancel pending tasks if the current sum of costs is already above the budget.
        spawn(optimize_group(child_group, child_required_phy_props, cost_budget)).join()
        
        # TODO: There are opportunities to use statistics from only some of your children and start pricing a heuristic cost
        # of the expression (e.g. the cost model should be conservative if some children have missing statistics and assume
        # the best case scenarios for them).
        total_cost_so_far += memo.get_input_costs(child_group)
        
        if total_cost_so_far > cost_budget:
            return None
            
    children_statistics = memo.get_statistics_from_winners(children_groups, children_required_phy_props)
    # Either we cost relation expressions or scalar expressions.
    total_cost_so_far += cost_model.compute_operation_cost(physical_expr, children_statistics)
    expr_statistics = cost_model.compute_statistics(physical_expr, children_statistics)
    enforce_cost, updated_expr_statistics = cost_model.compute_enforcement(physical_expr, expr_statistics, enforced_phy_props)
    total_cost_so_far += enforce_cost 
    
    return total_cost_so_far, updated_expr_statistics, enforced_phy_props
        

# This is the physical equivalent of explore_log_expr (could be called optimize_physical_expr w/ required phys properties).
# --> Tries to get the lowest cost for this physical expression given the required physical properties.
def optimize_inputs(physical_expr, required_phy_props, cost_budget):
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
    with_passthrough = optimize_inputs_inner(physical_expr, children_groups, required_phy_props, cost_budget, passthrough=True)
    # PASS 2: Enforce all the required properties locally, and pass to the children <nil> properties.
    without_passthrough = optimize_inputs_inner(physical_expr, children_groups, required_phy_props, cost_budget, passthrough=False)

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
        
    # (physical_expr.group_id, required_phy_props) is a goal id. 
    memo.store_phy_expr_winner(physical_expr, required_phy_props, enforced_phy_props, cost, statistics)
