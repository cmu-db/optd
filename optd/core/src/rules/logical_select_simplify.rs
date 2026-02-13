use std::sync::Arc;

use crate::ir::convert::{IntoOperator, IntoScalar};
use crate::ir::operator::{LogicalJoin, LogicalSelect, Operator, join::JoinType};
use crate::ir::rule::{OperatorPattern, Rule};
use crate::ir::scalar::{NaryOp, NaryOpKind};
use crate::ir::{IRContext, OperatorKind, Scalar, ScalarKind, ScalarValue};

fn is_true_scalar(scalar: &Arc<Scalar>) -> bool {
    matches!(
        &scalar.kind,
        ScalarKind::Literal(meta) if matches!(meta.value, ScalarValue::Boolean(Some(true)))
    )
}

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
        let is_logical_select = |kind: &OperatorKind| matches!(kind, OperatorKind::LogicalSelect(_));
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
        let select = operator.try_borrow::<LogicalSelect>().unwrap();
        let predicate = select.predicate().clone();
        if is_true_scalar(&predicate) {
            return Ok(vec![select.input().clone()])
        }
        if let OperatorKind::LogicalJoin(join_meta) = &select.input().kind {
            let join = LogicalJoin::borrow_raw_parts(join_meta, &select.input().common);
            if !matches!(join.join_type(), &JoinType::Inner) {
                return Ok(vec![])
            }
            let join_cond = join.join_cond().clone();
            let merged_cond = if is_true_scalar(&join_cond) {
                predicate
            } else {
                NaryOp::new(NaryOpKind::And, vec![join_cond, predicate].into()).into_scalar()
            };
            return Ok(vec![
                LogicalJoin::new(
                    JoinType::Inner,
                    join.outer().clone(),
                    join.inner().clone(),
                    merged_cond,
                ).into_operator()
            ])
        } else {
            return Ok(vec![])
        };
    }
}
