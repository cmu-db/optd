use crate::expression::LogicalExpression as Expr;
use crate::{ExprId, GroupId};

pub struct Memo;

impl Memo {
    pub async fn add_expr(&mut self, logical_expr: Expr) -> (ExprId, GroupId) {
        todo!()
    }

    pub async fn add_expr_to_group(&mut self, logical_expr: Expr, group_id: GroupId) -> ExprId {
        todo!()
    }

    pub async fn get_group_exprs(&mut self, group_id: GroupId) -> Vec<(ExprId, Expr)> {
        todo!()
    }

    pub async fn get_expr_group(&mut self, logical_expr_id: ExprId) -> GroupId {
        todo!()
    }

    pub async fn create_new_group(&mut self) -> GroupId {
        todo!()
    }
}