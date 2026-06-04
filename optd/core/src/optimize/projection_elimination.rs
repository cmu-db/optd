//! Projection elimination pass.
//!
//! Two rewrites:
//! 1. Identity projection: `Projection(cols, input)` where `cols` equals the
//!    available columns of `input` in the same order → replaced by `input`.
//! 2. Consecutive projections: `Projection(cols, Projection(_, input))` →
//!    `Projection(cols, input)` (inner projection is redundant).

use crate::{
    AvailableColumns, Operator, OperatorData, OptimizerContext, Projection,
    optimize::{OperatorRewrite, OptimizeResult, Pass, Rewrite},
};

pub struct ProjectionElimination;

impl Pass for ProjectionElimination {
    fn name(&self) -> &'static str {
        "ProjectionElimination"
    }
}

impl OperatorRewrite for ProjectionElimination {
    fn rewrite(&mut self, op: Operator, ctx: &mut OptimizerContext) -> OptimizeResult<Rewrite> {
        let OperatorData::Projection(proj) = ctx.query.operator(op).clone() else {
            return Ok(Rewrite::Keep);
        };

        // Collapse consecutive projections.
        if let OperatorData::Projection(inner_proj) = ctx.query.operator(proj.input).clone() {
            let new_op = OperatorData::Projection(Projection {
                columns: proj.columns,
                input: inner_proj.input,
            })
            .add(&mut ctx.query);
            return Ok(Rewrite::Replace(new_op));
        }

        // Remove identity projection.
        let available = ctx
            .analyses
            .get::<AvailableColumns>(&ctx.query, proj.input)
            .unwrap_or_default();
        if proj.columns == available {
            return Ok(Rewrite::Replace(proj.input));
        }

        Ok(Rewrite::Keep)
    }
}
