//! IR node for casting scalar expressions to different data types.

use std::sync::Arc;
use pretty_xmlish::Pretty;
use crate::ir::{
    DataType, IRCommon, Scalar,
    explain::Explain,
    macros::{define_node, impl_scalar_conversion},
    properties::ScalarProperties,
};

define_node!(
    Cast, CastBorrowed {
        properties: ScalarProperties,
        metadata: CastMetadata {
            data_type: DataType,
        },
        inputs: {
            operators: [],
            scalars: [expr],
        }
    }
);
impl_scalar_conversion!(Cast, CastBorrowed);

/// Metadata:
/// - data_type: The target data type to cast to.
/// Scalars:
/// - expr: The scalar expression to be cast.
impl Cast {
    pub fn new(data_type: DataType, expr: Arc<Scalar>) -> Self {
        Self {
            meta: CastMetadata { data_type },
            common: IRCommon::with_input_scalars_only(Arc::new([expr])),
        }
    }
}

impl Explain for CastBorrowed<'_> {
    fn explain<'a>(
        &self,
        ctx: &crate::ir::IRContext,
        option: &crate::ir::explain::ExplainOption,
    ) -> pretty_xmlish::Pretty<'a> {
        let fmt = format!(
            "CAST ({} AS {:?})",
            self.expr().explain(ctx, option).to_one_line_string(true),
            self.data_type(),
        );
        Pretty::display(&fmt)
    }
}
