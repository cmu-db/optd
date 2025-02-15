use optd_core::cascades::{expressions::LogicalExpression, types::PartialLogicalPlan};

use crate::irs::lir::{Expr, Identifier, Program, Value};
use std::collections::{HashMap, VecDeque};

pub struct Context(VecDeque<HashMap<String, Value>>);

pub fn transform(program: &Program, rule_id: Identifier, expr: LogicalExpression) -> Vec<PartialLogicalPlan> {
    // implement Program::get_ctx();
    let mut context = Context(VecDeque::new());
    let rule = program.functions.get(&rule_id).unwrap();

    todo!()
}


#[allow(unused)]
fn eval_expr(expr: Expr, context: &Context) -> Value {
    match expr {
        Expr::IfThenElse { cond, then, otherwise } => todo!(),
        Expr::PatternMatch { on, arms } => {
            let on_value = eval_expr(*on, context);
            for arm in arms {
                // add back to the context if a pattern is a binding.
            }
            match on_value {
                Value::Array(value) => todo!(),
                Value::Map(value, value1) => todo!(),
                Value::Tuple(values) => todo!(),
                Value::Function(function) => todo!(),
                Value::Terminal(terminal) => {
                    match terminal {
                        crate::irs::lir::Terminal::PartialLogicalPlan(partial_logical_plan) => todo!(),
                        crate::irs::lir::Terminal::PartialScalarPlan(partial_logical_plan) => todo!(),
                        crate::irs::lir::Terminal::OptdValue(optd_value) => todo!(),
                    }
                },
            }
        }, // Member call expr.fun(), expr.access
        Expr::Val { identifier, value, next } => todo!(),
        Expr::Binary { left, op, right } => todo!(),
        Expr::Unary { op, expr } => todo!(),
        // whitelist of member calls
        Expr::MemberCall { expr, member, args } => todo!(),
        Expr::MemberAccess { expr, member } => todo!(),
        Expr::Call { expr, args } => todo!(),
        Expr::Fail(_) => todo!(),
        Expr::Ref(_) => todo!(),
        Expr::Array(exprs) => todo!(),
        Expr::Map(items) => todo!(),
        Expr::Tuple(exprs) => todo!(),
        Expr::Value(value) => todo!(),
    }
}
