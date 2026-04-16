//! Scalar node for `IS NOT NULL`.

use crate::ir::{
    IRCommon, Scalar,
    explain::Explain,
    macros::{define_node, impl_scalar_conversion},
    properties::ScalarProperties,
};
use pretty_xmlish::Pretty;
use std::sync::Arc;

define_node!(
    /// Metadata: (none)
    /// Scalars:
    /// - expr: The scalar expression to test for non-NULL.
    IsNotNull, IsNotNullBorrowed {
        properties: ScalarProperties,
        metadata: IsNotNullMetadata {},
        inputs: {
            operators: [],
            scalars: [expr],
        }
    }
);
impl_scalar_conversion!(IsNotNull, IsNotNullBorrowed);

impl IsNotNull {
    pub fn new(expr: Arc<Scalar>) -> Self {
        Self {
            meta: IsNotNullMetadata {},
            common: IRCommon::with_input_scalars_only(Arc::new([expr])),
        }
    }
}

impl Explain for IsNotNullBorrowed<'_> {
    fn explain<'a>(
        &self,
        ctx: &crate::ir::IRContext,
        option: &crate::ir::explain::ExplainOption,
    ) -> pretty_xmlish::Pretty<'a> {
        Pretty::display(&format!(
            "{} IS NOT NULL",
            self.expr().explain(ctx, option).to_one_line_string(true)
        ))
    }
}
