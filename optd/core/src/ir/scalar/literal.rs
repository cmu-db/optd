use pretty_xmlish::Pretty;

use crate::ir::{
    IRCommon, ScalarValue,
    explain::Explain,
    macros::{define_node, impl_scalar_conversion},
    properties::ScalarProperties,
};

define_node!(
    /// A literal that holds an [`ScalarValue`].
    Literal, LiteralBorrowed {
        properties: ScalarProperties,
        metadata: LiteralMetadata {
            value: ScalarValue,
        },
        inputs: {
            operators: [],
            scalars: [],
        }
    }
);
impl_scalar_conversion!(Literal, LiteralBorrowed);

impl Literal {
    pub fn new(value: ScalarValue) -> Self {
        Self {
            meta: LiteralMetadata { value },
            common: IRCommon::empty(),
        }
    }
}

impl Literal {
    pub fn boolean(v: impl Into<Option<bool>>) -> Self {
        Self::new(ScalarValue::Boolean(v.into()))
    }

    pub fn int32(v: impl Into<Option<i32>>) -> Self {
        Self::new(ScalarValue::Int32(v.into()))
    }
}

impl Explain for LiteralBorrowed<'_> {
    fn explain<'a>(
        &self,
        _ctx: &crate::ir::IRContext,
        _option: &crate::ir::explain::ExplainOption,
    ) -> pretty_xmlish::Pretty<'a> {
        Pretty::display(self.value())
    }
}
