use std::{collections::HashMap, sync::Arc};

use snafu::whatever;

use crate::ir::{
    IRContext, OperatorKind,
    operator::LogicalSelect,
    rule::{OperatorPattern, Rule},
    scalar::NaryOp,
};

pub struct SelectionPushdownRule {
    pattern: OperatorPattern,
}

impl Default for SelectionPushdownRule {
    fn default() -> Self {
        Self::new()
    }
}

impl SelectionPushdownRule {
    pub fn new() -> Self {
        const INPUT: usize = 0;
        let mut pattern = OperatorPattern::with_top_matches(|kind| {
            matches!(kind, OperatorKind::LogicalSelect(_))
        });
        pattern.add_input_operator_pattern(INPUT, OperatorPattern::with_top_matches(|_| true));
        Self { pattern }
    }

    fn extract_conjuncts(predicate: &crate::ir::Scalar, acc: &mut Vec<Arc<crate::ir::Scalar>>) {
        if let Ok(nary_op) = predicate.try_borrow::<NaryOp>()
            && nary_op.is_and()
        {
            nary_op
                .terms()
                .iter()
                .for_each(|term| Self::extract_conjuncts(term, acc));
        } else {
            acc.push(predicate.clone().into());
        }
    }

    fn transform_inner(
        &self,
        operator: &crate::ir::Operator,
        ctx: &IRContext,
        mut acc: Vec<Arc<crate::ir::Scalar>>,
    ) -> crate::error::Result<Arc<crate::ir::Operator>> {
        if let Ok(select) = operator.try_borrow::<LogicalSelect>() {
            Self::extract_conjuncts(&select.predicate(), &mut acc);
            return self.transform_inner(&select.input(), ctx, acc);
        }
        let mut ht = HashMap::new();
        let mut remains = Vec::new();
        for conjunct in acc.iter() {
            let mut pushed_down = false;
            for (i, input_op) in operator.input_operators().iter().enumerate() {
                let output_columns = input_op.output_columns(ctx);
                if conjunct.used_columns().is_subset(&output_columns) {
                    ht.entry(i).or_insert_with(Vec::new).push(conjunct.clone());
                    pushed_down = true;
                    break;
                }
            }
            if !pushed_down {
                remains.push(conjunct.clone());
            }
        }

        let input_ops = operator
            .input_operators()
            .iter()
            .enumerate()
            .map(|(i, op)| {
                let acc = ht.remove(&i).unwrap_or_else(|| Vec::new());
                self.transform_inner(&op, ctx, acc)
            })
            .collect::<Result<Vec<_>, _>>()?;

        let new_op = Arc::new(operator.clone_with_inputs(Some(input_ops.into()), None));
        Ok(remains
            .into_iter()
            .reduce(|x, y| x.and(y))
            .map(|pred| new_op.clone().logical_select(pred))
            .unwrap_or_else(|| new_op))
    }
}

impl Rule for SelectionPushdownRule {
    fn name(&self) -> &'static str {
        "selection_pushdown"
    }

    fn pattern(&self) -> &OperatorPattern {
        &self.pattern
    }

    fn transform(
        &self,
        operator: &crate::ir::Operator,
        ctx: &IRContext,
    ) -> crate::error::Result<Vec<std::sync::Arc<crate::ir::Operator>>> {
        let result = self.transform_inner(operator, ctx, Vec::new())?;
        Ok(vec![result])
    }
}

#[cfg(test)]
mod tests {
    use crate::ir::{
        Column, IRContext,
        builder::{boolean, column_ref, integer},
        explain::quick_explain,
        operator::join::JoinType,
    };

    use super::*;

    #[test]
    fn selection_pushdown_behavior() {
        let ctx = IRContext::with_empty_magic();
        let t1 = ctx.mock_scan(1, vec![0, 1, 2], 100.);
        let t2 = ctx.mock_scan(2, vec![3, 4, 5], 100.);
        let plan = t1
            .logical_join(t2, boolean(true), JoinType::Inner)
            .logical_select(
                column_ref(Column(0))
                    .eq(column_ref(Column(3)))
                    .and(column_ref(Column(1)).eq(integer(3)))
                    .and(column_ref(Column(4)).ge(integer(4))),
            );

        let _output_columns = plan.output_columns(&ctx);
        let rule = SelectionPushdownRule::new();
        println!("{}", quick_explain(&plan, &ctx));

        let transformed = rule.transform(&plan, &ctx).unwrap();
        assert_eq!(transformed.len(), 1);
        let _output_columns = transformed[0].output_columns(&ctx);

        println!("{}", quick_explain(&transformed[0], &ctx));
    }
}
