use super::storage_types::*;
use std::sync::Arc;
use tokio::task::JoinSet;
pub struct OptimizerState {
    memo: MemoTable,
}

#[derive(Clone, Debug)]
pub struct MemoTable;

#[derive(Clone, Debug, Copy)]
pub struct Rule;
impl Rule {
    pub fn match_and_apply(&self, expr: &Expr) -> Vec<Expr> {
        // TODO: Need to convert into partial here, while potentially expanding.
        // Vec<Partial> --> Vec<Expr>
        // If the lower layers of the partial expression creates new expressions or groups (join associativity), 
        // then this function updates the memo table accordingly.
        // We should take partials here.
        todo!()
    }
}

impl MemoTable {
    pub async fn new() -> Self {
        todo!()
    }

    pub async fn add_logical_expr(&self, logical_expr: LogicalExpr) -> (LogicalExprId, RelGroupId) {
        todo!()
    }

    pub async fn add_logical_expr_to_group(
        &self,
        logical_expr: LogicalExpr,
        rel_group: RelGroupId,
    ) {
        todo!()
    }

    pub async fn get_logical_expr_identifiers(
        &self,
        logical_expr: LogicalExpr,
    ) -> Option<(LogicalExprId, PhysicalExprId)> {
        todo!()
    }

    pub async fn get_all_children_groups(&self, expr: &Expr) -> Vec<Arc<Group>> {
        todo!()
    }

    pub fn get_all_exprs_in_group(&self, rel_group: &Group) -> Vec<Arc<Expr>> {
        todo!()
    }
    
    pub fn get_all_rules(&self) -> Vec<Rule> {
        return vec![];
    }
}

#[derive(Debug, Clone)]
pub struct Optimizer {
    memo: MemoTable,
}

impl Optimizer {
    pub async fn optimize_group(
        self: Arc<Self>,
        group: Arc<Group>,
        required_props: PhysicalProperties,
        cost_budget: Cost,
    ) {
        if group.explore_status() == ExploreStatus::Unexplored {
            // TODO: mark exploring
            let opt = self.clone();
            let group = group.clone();
            tokio::spawn(async move { opt.explore_group(group).await }).await;
            // TODO: mark explored
        }

        let exprs = self.memo.get_all_exprs_in_group(group.as_ref());

        let mut set = JoinSet::new();

        for expr in exprs {
            let opt = self.clone();
            let required_props = required_props.clone();
            set.spawn(async move { opt.optimize_expr(expr, required_props, cost_budget).await });
        }
        set.join_all().await;
    }

    pub async fn explore_group(self: Arc<Self>, group: Arc<Group>) {
        let exprs = self.memo.get_all_exprs_in_group(group.as_ref());
        let mut set = JoinSet::new();
        for expr in exprs {
            let opt = self.clone();
            set.spawn(async move { opt.explore_expr(expr).await });
        }
        set.join_all().await;
    }

    pub async fn explore_expr(self: Arc<Self>, expr: Arc<Expr>) {
        let children = self.memo.get_all_children_groups(&expr).await;

        let mut set = JoinSet::new();
        for child_group in children {
            if child_group.explore_status() == ExploreStatus::Unexplored {
                // TODO: mark exploring
                let opt = self.clone();
               set.spawn(async move { opt.explore_group(child_group).await });
                // TODO: mark explored
            }
        }

        set.join_all().await;
        
        for rule in self.memo.get_all_rules() {
            let opt = self.clone();
            s// et.spawn(async move { opt.apply_rule(expr).await });
        }
        
    }

    pub async fn optimize_expr(
        self: Arc<Self>,
        expr: Arc<Expr>,
        req_physical_props: PhysicalProperties,
        cost_budget: Cost,
    ) {
        let mut set = JoinSet::new();
        for child in self.memo.get_all_children_groups(expr.as_ref()).await {
            if child.explore_status() == ExploreStatus::Unexplored {
                // TODO: Mark exploring.
                let opt = self.clone();
                set.spawn(async move { opt.explore_group(child).await });
                // TODO: Mark explored.
            }   
        }
        set.join_all();

        for rule in self.memo.get_all_rules() {
            let opt = self.clone();
            // Exprs belongs to the same group?
            let exprs = rule.match_and_apply(&expr);
            

        }
        
    }
    
}
