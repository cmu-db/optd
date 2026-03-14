use std::sync::Arc;

use crate::ir::convert::{IntoOperator, IntoScalar};
use crate::ir::operator::{Join, Operator, Select, join::JoinType};
use crate::ir::rule::{OperatorPattern, Rule};
use crate::ir::scalar::{NaryOp, NaryOpKind};
use crate::ir::{IRContext, OperatorKind};

pub struct LogicalSelectSimplifyRule {
    pattern: OperatorPattern,
}

impl Default for LogicalSelectSimplifyRule {
    fn default() -> Self {
        Self::new()
    }
}

impl LogicalSelectSimplifyRule {
    pub fn new() -> Self {
        let is_logical_select = |kind: &OperatorKind| matches!(kind, OperatorKind::Select(_));
        let pattern = OperatorPattern::with_top_matches(is_logical_select);
        Self { pattern }
    }
}

impl Rule for LogicalSelectSimplifyRule {
    fn name(&self) -> &'static str {
        "logical_select_simplify"
    }

    fn pattern(&self) -> &OperatorPattern {
        &self.pattern
    }

    fn transform(
        &self,
        operator: &Operator,
        _ctx: &IRContext,
    ) -> crate::error::Result<Vec<Arc<Operator>>> {
        let select = operator.try_borrow::<Select>().unwrap();
        let predicate = select.predicate().clone();
        if predicate.is_true_scalar() {
            return Ok(vec![select.input().clone()]);
        }
        if let OperatorKind::Join(join_meta) = &select.input().kind {
            if join_meta.implementation.is_some() {
                return Ok(vec![]);
            }
            let join = Join::borrow_raw_parts(join_meta, &select.input().common);
            if !matches!(join.join_type(), &JoinType::Inner) {
                return Ok(vec![]);
            }
            let join_cond = join.join_cond().clone();
            let merged_cond = if join_cond.is_true_scalar() {
                predicate
            } else {
                NaryOp::new(NaryOpKind::And, vec![join_cond, predicate].into()).into_scalar()
            };
            Ok(vec![
                Join::logical(
                    JoinType::Inner,
                    join.outer().clone(),
                    join.inner().clone(),
                    merged_cond,
                )
                .into_operator(),
            ])
        } else {
            Ok(vec![])
        }
    }
}
