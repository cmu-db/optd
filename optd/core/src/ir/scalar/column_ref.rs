use crate::ir::{
    Column, IRCommon,
    macros::{define_node, impl_scalar_conversion},
    properties::ScalarProperties,
};

define_node!(
    /// A reference to a [`Column`].
    ColumnRef, ColumnRefBorrowed {
        properties: ScalarProperties,
        metadata: ColumnRefMetadata {
            column: Column,
        },
        inputs: {
            operators: [],
            scalars: [],
        }
    }
);
impl_scalar_conversion!(ColumnRef, ColumnRefBorrowed);

impl ColumnRef {
    pub fn new(column: Column) -> Self {
        Self {
            meta: ColumnRefMetadata { column },
            common: IRCommon::empty(),
        }
    }
}
