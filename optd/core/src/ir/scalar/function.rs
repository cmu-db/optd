//! Scalar functions are used to represent scalar function calls in the IR.

use crate::ir::{
    DataType, IRCommon, Scalar,
    explain::Explain,
    macros::{define_node, impl_scalar_conversion},
    properties::ScalarProperties,
};
use itertools::Itertools;
use pretty_xmlish::Pretty;
use std::sync::Arc;

// TODO: Full type signature in optd, right now just use id.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FunctionKind {
    Scalar,
    Aggregate,
    Window,
}

define_node!(
    /// Metadata:
    /// - id: The identifier of the function.
    /// - kind: The kind of the function (scalar, aggregate, window).
    /// - return_type: The return data type of the function.
    /// Scalars:
    /// - params: The parameters of the function.
    Function, FunctionBorrowed {
        properties: ScalarProperties,
        metadata: FunctionMetadata {
            id: Arc<str>,
            kind: FunctionKind,
            return_type: DataType,
        },
        inputs: {
            operators: [],
            scalars: params[],
        },
    }
);
impl_scalar_conversion!(Function, FunctionBorrowed);

impl Function {
    pub fn new_aggregate(
        id: impl Into<Arc<str>>,
        params: Arc<[Arc<Scalar>]>,
        return_type: DataType,
    ) -> Self {
        Self {
            meta: FunctionMetadata {
                id: id.into(),
                kind: FunctionKind::Aggregate,
                return_type,
            },
            common: IRCommon::with_input_scalars_only(params),
        }
    }
}

impl Explain for FunctionBorrowed<'_> {
    fn explain<'a>(
        &self,
        ctx: &crate::ir::IRContext,
        option: &crate::ir::explain::ExplainOption,
    ) -> pretty_xmlish::Pretty<'a> {
        let params = self
            .params()
            .iter()
            .map(|t| t.explain(ctx, option).to_one_line_string(true))
            .join(", ");

        Pretty::Text(format!("{}({params})", self.id()).into())
    }
}
