# Deprecated, check algo2.py for better understanding of implementation step.

GroupId = int

class LogicalExpr:
    pass
    
class TransformationRule:
    def match_and_apply(logical_expr: LogicalExpr) -> list[LogicalExpr]:
        pass

class ImplementationRule:
    def match_and_apply(logical_expr: LogicalExpr) -> list[LogicalExpr]:


class Group:
  id: GroupId
  operator: OpType
  logical_props: LogicalProps
  
   # Have we started exploring this group
  explored: bool
  logical_expr: list[LogicalExpr]
  
  physical_expr: list[PhysicalExpr]

class LogicalProps:
  statistics: int # row_count for now
  
class PhysicalProps:
    pass

# the "Shape"
class Expr:
  operator_type: OpType,
  children: list[GroupId]

class Memo:
    def get_or_create_group(expr: Expr):
        pass

    def update(exprs: list[LogicalExpr | PhysicalExpr]):
        pass
  
GroupId = int

    

memo = Memo()

class Group:
  id: GroupId
  operator: OpType
  logical_props: LogicalProps
  
   # Have we started exploring this group
  explored: bool
  logical_expr: list[LogicalExpr]
  
  physical_expr: list[PhysicalExpr]

class LogicalProps:
  statistics: int # row_count for now
  
class PhysicalProp:
    pass

class PhysicalProps:
    props: list[PhysicalProp]

# the "Shape"
class Expr:
  operator_type: OpType,
  children: list[GroupId]

class Memo:
    def get_or_create_group(expr: Expr):
        pass

    def update(exprs: list[LogicalExpr | PhysicalExpr]):
        pass
  

def join_all(tasks):
    pass

class Scheduler:
    pass

scheduler = Scheduler()

"""
optimize(expr):
    # 1. Explore the logical space: CHECK!
    
    # 2. Explore the physical space: PHYS + ENFORCER (need to figure out)

    explore(expr) # only log->log
    --> Exploreation is fully complete and all groups have been explored. DONE DONE DONE.
    
    
    --> optimize_group(group, required_phy_props, cost_budget)
"""





# Optimizes a given logical expression / group with a required phys prop
# TODO: shoudl it take a group or a expr?
def optimize_group(expr: Expr, required_phy_props: PhysicalProps, cost_budget: int):
  group = memo.get_or_create_goal(expr)
  if not group.explored:
    handle = scheduler.spawn(explore_group(group, cost_budget))
    handle.join()

    ## AT WHAT POINT WILL THIS CODE BELOW BE EXECUTED?
  tasks = []
  for expr in group.logical_expr: # Parallel launches
    task = scheduler.spawn(optimize_expr(expr, required_phy_props, cost_budget))
    tasks.append(task)

  join_all(tasks)


# Expand logical to logical rules within the group, and recursively BELOW!
def explore_group(group: Group, cost_budget: int):
    # We start exploration, other people don't need to do it
    group.explored = True

    tasks = []
    for expr in group.logical_expr: # Parallel launches
        task = scheduler.spawn(explore_log_expr(expr, cost_budget))
        tasks.append(task)

    join_all(tasks)
    

# Explore alternatives of a given logical expression
def explore_log_expr(logical_expr: LogicalExpr, cost_budget: int):
    # Before logical -> logical yourself, explore children groups first
    tasks = []
    memo.get_all_children_groups(logical_expr)
    for child_group in logical_expr.children():
        if not group.explored:
            task = scheduler.spawn(explore_group(group, limit))
            tasks.append(task)

    join_all(tasks)

    # Apply logical transformations to the current expr
    tasks = []
    for rule in rules: # can be a subset & ordered (promise & guidance)
        if not expr.has_been_applied(rule) and rule.matches(expr):
            task = scheduler.spawn(apply_logical_rule(expr, rule, limit), promise)
            tasks.append(task)

    join_all(tasks)


def apply_logical_rule(expr: LogicalExpr, rule: Rule, cost_budget: int):
    # user-defined transformations from logical expr to logical exprs (could be multiple)
    new_exprs = transform(expr, rule)
    memo.update(new_exprs)




    tasks = []
    for new_expr in new_exprs:
        task = scheduler.spawn(explore_log_expr(new_expr, limit))


def optimize_expr(logical_expr: LogicalExpr, phy_props: PhysicalProps , cost_budget: int):
    # Before logical -> physical yourself, explore children groups first (memo: double guidance change mind)
    tasks = []
    for child_group in logical_expr.children():
        if not group.explored:
            task = scheduler.spawn(explore_group(child_group, cost_budget)) # Will this ever be reached?
            tasks.append(task)

    join_all(tasks)

    tasks = []
    for rule in rules:
        if not logical_expr.applied(rule) and rule.matches(logical_expr):
            task = scheduler.spawn(apply_physical_rule(logical_expr, rule, phy_props, cost_budget))

            tasks.append(task)
    join_all(tasks)

def apply_physical_rule(expr: LogicalExpr, rule: Rule, phy_props: PhysicalProps, cost_budget: int):
    new_exprs: list[PhysicalExprs] = transform(expr, rule)
    memo.update(new_exprs)

    tasks = []
    for new_expr in new_exprs:
        cost_budget = update_cost_budget(new_expr, cost_budget) # Can fail if goes below : plan is bad, stop here
        task = scheduler.spawn(optimize_input(new_expr, cost_budget, phy_props))

# We need to think about how to split the `budget`.
# We could for example launch all of them in //, then cancel remaining children if the optimed childfren have exceeded the budget.
def optimize_inputs(expr: PhysicalExpr, budget: int):
    for child in expr.children(): # TODO: Musn't be done sequentially, but simplify for now.
       
        # randomized 
        # child_budget = budget - (cost of every child before this one)
        budget = update_cost_budget(expr, budget) # Can fail if goes below : plan is bad, stop here
        

        # TODO: derive physicalProp of children like in Volcano
        childPhsyicalProp = derive(expr, physicalProp)
        task = scheduler.spawn(optimize_group(memo.get_group(child), childPhsyicalProp, budget))
        task.join()  
    memo.update_best_plan(expr)

def join_all(tasks):
    pass


