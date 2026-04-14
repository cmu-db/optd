//! Scalar node for `IS NULL`.

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
    /// - expr: The scalar expression to test for NULL.
    IsNull, IsNullBorrowed {
        properties: ScalarProperties,
        metadata: IsNullMetadata {},
        inputs: {
            operators: [],
            scalars: [expr],
        }
    }
);
impl_scalar_conversion!(IsNull, IsNullBorrowed);

impl IsNull {
    pub fn new(expr: Arc<Scalar>) -> Self {
        Self {
            meta: IsNullMetadata {},
            common: IRCommon::with_input_scalars_only(Arc::new([expr])),
        }
    }
}

impl Explain for IsNullBorrowed<'_> {
    fn explain<'a>(
        &self,
        ctx: &crate::ir::IRContext,
        option: &crate::ir::explain::ExplainOption,
    ) -> pretty_xmlish::Pretty<'a> {
        Pretty::display(&format!(
            "{} IS NULL",
            self.expr().explain(ctx, option).to_one_line_string(true)
        ))
    }
}
