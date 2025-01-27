use super::*;

impl Memo {
    pub async fn add_logical_expr(
        &mut self,
        logical_expr: LogicalExpr,
    ) -> (LogicalExprId, GroupId) {
        todo!()
    }

    pub async fn add_logical_expr_to_group(
        &mut self,
        logical_expr: LogicalExpr,
        group_id: GroupId,
    ) -> LogicalExprId {
        todo!()
    }

    pub async fn get_group_logical_exprs(
        &mut self,
        group_id: GroupId,
    ) -> Vec<(LogicalExprId, LogicalExpr)> {
        todo!()
    }

    pub async fn get_group_of_logical_expr(&mut self, logical_expr_id: LogicalExprId) -> GroupId {
        todo!()
    }

    pub async fn create_new_group(&mut self) -> GroupId {
        todo!()
    }
}
