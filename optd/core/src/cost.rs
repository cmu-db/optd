//! Cost model interfaces.

use crate::optimize::{OptimizeError, OptimizeResult};
use crate::{AnalysisContext, Operator, QueryContext, Relation};

/// Estimates operator costs for an optd query tree.
pub trait CostModel: Send + Sync + 'static {
    type Cost: Clone + PartialOrd;

    fn zero(&self) -> Self::Cost;

    fn add(&self, left: Self::Cost, right: Self::Cost) -> Self::Cost;

    /// Computes this operator's local cost, excluding input costs.
    fn operator_cost(
        &self,
        op: Operator,
        ctx: &QueryContext,
        analyses: &mut AnalysisContext,
    ) -> OptimizeResult<Self::Cost>;

    /// Recursively computes total costs for the operator's inputs.
    fn input_costs(
        &self,
        op: Operator,
        ctx: &QueryContext,
        analyses: &mut AnalysisContext,
    ) -> OptimizeResult<Vec<Self::Cost>> {
        ctx.operator(op)
            .inputs()
            .into_iter()
            .map(|input| self.total_cost(input, ctx, analyses))
            .collect()
    }

    /// Computes total cost from this operator's local cost and precomputed input costs.
    fn total_cost_with_inputs(
        &self,
        op: Operator,
        input_costs: &[Self::Cost],
        ctx: &QueryContext,
        analyses: &mut AnalysisContext,
    ) -> OptimizeResult<Self::Cost> {
        let input_count = ctx.operator(op).inputs().len();
        if input_count != input_costs.len() {
            return Err(OptimizeError::PassError {
                pass: "CostModel",
                message: format!(
                    "operator {op} has {input_count} inputs but received {} input costs",
                    input_costs.len()
                ),
            });
        }

        let operator_cost = self.operator_cost(op, ctx, analyses)?;
        Ok(input_costs
            .iter()
            .cloned()
            .fold(operator_cost, |acc, cost| self.add(acc, cost)))
    }

    /// Recursively computes total cost for the operator subtree.
    fn total_cost(
        &self,
        op: Operator,
        ctx: &QueryContext,
        analyses: &mut AnalysisContext,
    ) -> OptimizeResult<Self::Cost> {
        let input_costs = self.input_costs(op, ctx, analyses)?;
        self.total_cost_with_inputs(op, &input_costs, ctx, analyses)
    }
}

/// Default scalar cost model used by optimizer heuristics.
#[derive(Debug, Clone, Copy, Default)]
pub struct DefaultCostModel;

impl CostModel for DefaultCostModel {
    type Cost = f64;

    fn zero(&self) -> Self::Cost {
        0.0
    }

    fn add(&self, left: Self::Cost, right: Self::Cost) -> Self::Cost {
        left + right
    }

    fn operator_cost(
        &self,
        _op: Operator,
        _ctx: &QueryContext,
        _analyses: &mut AnalysisContext,
    ) -> OptimizeResult<Self::Cost> {
        Ok(0.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ColumnData, ExprData, OperatorData, Output, Scan, Selection, TableRef};
    use arrow_schema::DataType;

    #[test]
    fn total_cost_sums_local_operator_costs() {
        let mut ctx = QueryContext::new();
        let column = ColumnData::new("a", DataType::Int64).add(&mut ctx);
        let scan = OperatorData::Scan(Scan {
            table: TableRef::bare("A"),
            columns: vec![column],
        })
        .add(&mut ctx);
        let predicate = ExprData::ColumnRef(column).add(&mut ctx);
        let selection = OperatorData::Selection(Selection {
            predicate,
            input: scan,
        })
        .add(&mut ctx);
        let output = OperatorData::Output(Output { input: selection }).add(&mut ctx);
        let mut analyses = AnalysisContext::new();
        let model = DefaultCostModel;

        let total = model.total_cost(output, &ctx, &mut analyses).unwrap();

        assert_eq!(total, 0.0);
    }

    #[test]
    fn total_cost_with_inputs_rejects_wrong_input_count() {
        let mut ctx = QueryContext::new();
        let column = ColumnData::new("a", DataType::Int64).add(&mut ctx);
        let scan = OperatorData::Scan(Scan {
            table: TableRef::bare("A"),
            columns: vec![column],
        })
        .add(&mut ctx);
        let output = OperatorData::Output(Output { input: scan }).add(&mut ctx);
        let mut analyses = AnalysisContext::new();
        let model = DefaultCostModel;

        let err = model
            .total_cost_with_inputs(output, &[], &ctx, &mut analyses)
            .unwrap_err();

        assert!(err.to_string().contains("has 1 inputs but received 0"));
    }
}
