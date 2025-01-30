# The memo table API.
class Memo:
    def get_group_explore_status(group_id):
        pass

    def set_group_explore_status(group_id, status):
        pass

    def set_goal_optimize_status(group_id, required_phy_props, status):
        pass

    def get_goal_optimize_winner(group_id, required_phy_props):
        pass

    def set_goal_optimize_winner(group_id, required_phy_props, winner_expr, cost, winner_stats):
        pass

    def has_transformation_been_applied(logical_expr, transformation_rule):
        pass

    def set_transformation_applied(logical_expr, new_logical_expr, transformation_rule):
        pass

    def get_logical_group_children(group_id):
        pass

    def get_all_logical_exprs_in_group(group_id):
        pass

    def has_implementation_been_applied(logical_expr, implementation_rule):
        pass

    def get_relational_group(expression):
        pass

    def add_logical_expr_to_group(group_id, logical_expr):
        pass

    def add_physical_expr_to_group(group_id, physical_expr):
        pass

    def set_physical_expr_optimize_status(group_id, physical_expr, status):
        pass

    def set_physical_expr_optimize_winner(group_id, physical_expr, winner_expr, cost, winner_stats):
        pass

    def get_goal_winner_statistics(group_id, required_phy_props):
        pass
