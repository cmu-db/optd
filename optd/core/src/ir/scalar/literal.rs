use crate::ir::{
    IRCommon, ScalarValue,
    macros::{define_node, impl_scalar_conversion},
    properties::ScalarProperties,
};

define_node!(
    Literal {
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
impl_scalar_conversion!(Literal);

impl Literal {
    pub fn new(value: ScalarValue) -> Self {
        Self {
            meta: LiteralMetadata { value },
            common: IRCommon::empty(),
        }
    }
}
