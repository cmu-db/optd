use crate::ir::{
    Column, IRCommon,
    macros::{define_node, impl_scalar_conversion},
    properties::ScalarProperties,
};

define_node!(
    ColumnRef {
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
impl_scalar_conversion!(ColumnRef);

impl ColumnRef {
    pub fn new(column: Column) -> Self {
        Self {
            meta: ColumnRefMetadata { column },
            common: IRCommon::empty(),
        }
    }
}
