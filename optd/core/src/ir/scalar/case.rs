//! Represents a SQL CASE scalar expression.

use std::sync::Arc;

use itertools::Itertools;
use pretty_xmlish::Pretty;

use crate::ir::{
    IRCommon, Scalar,
    explain::Explain,
    macros::{define_node, impl_scalar_conversion},
    properties::ScalarProperties,
};

define_node!(
    /// Metadata:
    /// - has_expr: Whether the CASE expression has a base expression.
    /// - when_then_count: Number of WHEN/THEN pairs.
    /// - has_else: Whether the CASE expression has an ELSE branch.
    /// Scalars:
    /// - scalars: Flattened scalar inputs in the order:
    ///   `[expr?][when_0, then_0, when_1, then_1, ...][else?]`.
    Case, CaseBorrowed {
        properties: ScalarProperties,
        metadata: CaseMetadata {
            has_expr: bool,
            when_then_count: usize,
            has_else: bool,
        },
        inputs: {
            operators: [],
            scalars: scalars[],
        }
    }
);
impl_scalar_conversion!(Case, CaseBorrowed);

impl Case {
    pub fn new(
        expr: Option<Arc<Scalar>>,
        when_then_expr: Arc<[(Arc<Scalar>, Arc<Scalar>)]>,
        else_expr: Option<Arc<Scalar>>,
    ) -> Self {
        let mut scalars = Vec::with_capacity(
            usize::from(expr.is_some())
                + when_then_expr.len() * 2
                + usize::from(else_expr.is_some()),
        );

        if let Some(expr) = expr.clone() {
            scalars.push(expr);
        }

        for (when, then) in when_then_expr.iter() {
            scalars.push(when.clone());
            scalars.push(then.clone());
        }

        if let Some(else_expr) = else_expr.clone() {
            scalars.push(else_expr);
        }

        Self {
            meta: CaseMetadata {
                has_expr: expr.is_some(),
                when_then_count: when_then_expr.len(),
                has_else: else_expr.is_some(),
            },
            common: IRCommon::with_input_scalars_only(scalars.into()),
        }
    }

    fn when_then_start(&self) -> usize {
        usize::from(self.meta.has_expr)
    }

    fn when_then_end(&self) -> usize {
        self.when_then_start() + self.meta.when_then_count * 2
    }

    pub fn expr(&self) -> Option<&Arc<Scalar>> {
        self.meta.has_expr.then(|| &self.common.input_scalars[0])
    }

    pub fn when_then_expr(&self) -> impl ExactSizeIterator<Item = (&Arc<Scalar>, &Arc<Scalar>)> {
        self.common.input_scalars[self.when_then_start()..self.when_then_end()]
            .chunks_exact(2)
            .map(|chunk| (&chunk[0], &chunk[1]))
    }

    pub fn else_expr(&self) -> Option<&Arc<Scalar>> {
        self.meta
            .has_else
            .then(|| &self.common.input_scalars[self.when_then_end()])
    }
}

impl<'ir> CaseBorrowed<'ir> {
    fn when_then_start(&self) -> usize {
        usize::from(self.meta.has_expr)
    }

    fn when_then_end(&self) -> usize {
        self.when_then_start() + self.meta.when_then_count * 2
    }

    pub fn expr(&self) -> Option<&'ir Arc<Scalar>> {
        self.meta.has_expr.then(|| &self.common.input_scalars[0])
    }

    pub fn when_then_expr(
        &self,
    ) -> impl ExactSizeIterator<Item = (&'ir Arc<Scalar>, &'ir Arc<Scalar>)> {
        self.common.input_scalars[self.when_then_start()..self.when_then_end()]
            .chunks_exact(2)
            .map(|chunk| (&chunk[0], &chunk[1]))
    }

    pub fn else_expr(&self) -> Option<&'ir Arc<Scalar>> {
        self.meta
            .has_else
            .then(|| &self.common.input_scalars[self.when_then_end()])
    }
}

impl Explain for CaseBorrowed<'_> {
    fn explain<'a>(
        &self,
        ctx: &crate::ir::IRContext,
        option: &crate::ir::explain::ExplainOption,
    ) -> Pretty<'a> {
        let mut parts = vec!["CASE".to_string()];

        if let Some(expr) = self.expr() {
            parts.push(expr.explain(ctx, option).to_one_line_string(true));
        }

        parts.extend(self.when_then_expr().map(|(when, then)| {
            format!(
                "WHEN {} THEN {}",
                when.explain(ctx, option).to_one_line_string(true),
                then.explain(ctx, option).to_one_line_string(true),
            )
        }));

        if let Some(else_expr) = self.else_expr() {
            parts.push(format!(
                "ELSE {}",
                else_expr.explain(ctx, option).to_one_line_string(true)
            ));
        }

        parts.push("END".to_string());

        Pretty::display(&parts.into_iter().join(" "))
    }
}
