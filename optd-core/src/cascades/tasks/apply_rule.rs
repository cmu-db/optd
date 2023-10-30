use anyhow::Result;
use tracing::trace;

use crate::{
    cascades::{
        optimizer::{CascadesOptimizer, ExprId, RuleId},
        tasks::{OptimizeExpressionTask, OptimizeInputsTask},
    },
    rel_node::RelNodeTyp,
};

use super::Task;

pub struct ApplyRuleTask {
    rule_id: RuleId,
    expr_id: ExprId,
    exploring: bool,
}

impl ApplyRuleTask {
    pub fn new(rule_id: RuleId, expr_id: ExprId, exploring: bool) -> Self {
        Self {
            rule_id,
            expr_id,
            exploring,
        }
    }
}

impl<T: RelNodeTyp> Task<T> for ApplyRuleTask {
    fn execute(&self, optimizer: &mut CascadesOptimizer<T>) -> Result<Vec<Box<dyn Task<T>>>> {
        if optimizer.is_rule_fired(self.expr_id, self.rule_id) {
            return Ok(vec![]);
        }
        let rule = optimizer.rules()[self.rule_id].clone();
        let group_id = optimizer.get_group_id(self.expr_id);
        let binding_exprs = optimizer.get_all_expr_bindings(self.expr_id);
        let mut tasks = vec![];
        for expr in binding_exprs {
            let applied = rule.apply(expr);
            for expr in applied {
                let expr_typ = expr.typ;
                let (_, expr_id) = optimizer.add_group_expr(expr, Some(group_id));
                trace!(event = "apply_rule", expr_id = %self.expr_id, rule_id = %self.rule_id, new_expr_id = %expr_id);
                if expr_typ.is_logical() {
                    tasks.push(
                        Box::new(OptimizeExpressionTask::new(expr_id, self.exploring))
                            as Box<dyn Task<T>>,
                    );
                } else {
                    tasks.push(Box::new(OptimizeInputsTask::new(expr_id)) as Box<dyn Task<T>>);
                }
            }
        }
        optimizer.mark_rule_fired(self.expr_id, self.rule_id);

        trace!(event = "task_end", task = "apply_rule", expr_id = %self.expr_id, rule_id = %self.rule_id);
        Ok(tasks)
    }
}
