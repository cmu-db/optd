use std::sync::Arc;

use crate::ir::{
    IRCommon, Scalar,
    catalog::DataSourceId,
    macros::{define_node, impl_operator_conversion},
    properties::{Derive, OperatorProperties, OutputColumns},
    scalar::ProjectionList,
};

define_node!(
    PhysicalTableScan, PhysicalTableScanBorrowed {
        properties: OperatorProperties,
        metadata: PhysicalTableScanMetadata {
            table_id: DataSourceId,
        },
        inputs: {
            operators: [],
            scalars: [projection_list],
        }
    }
);
impl_operator_conversion!(PhysicalTableScan, PhysicalTableScanBorrowed);

impl PhysicalTableScan {
    pub fn new(table_id: DataSourceId, projection_list: Arc<Scalar>) -> Self {
        Self {
            meta: PhysicalTableScanMetadata { table_id },
            common: IRCommon::with_input_scalars_only(Arc::new([projection_list])),
        }
    }
}

impl Derive<OutputColumns> for PhysicalTableScan {
    fn derive_by_compute(&self, _ctx: &crate::ir::context::IRContext) -> OutputColumns {
        let projections = self
            .projection_list()
            .try_borrow::<ProjectionList>()
            .unwrap();
        OutputColumns::from_column_set(projections.get_all_assignees().collect())
    }

    fn derive(&self, ctx: &crate::ir::context::IRContext) -> OutputColumns {
        self.common
            .properties
            .output_columns
            .get_or_init(|| self.derive_by_compute(ctx))
            .clone()
    }
}

#[cfg(test)]
#[allow(unused)]
impl PhysicalTableScan {
    pub(crate) fn mock(columns: Vec<i64>) -> Self {
        use crate::ir::{Column, convert::IntoScalar, scalar::*};

        let projections = columns
            .into_iter()
            .map(|i| Assign::new(Column(i), ColumnRef::new(Column(i)).into_scalar()).into_scalar())
            .collect::<Vec<_>>();

        Self::new(
            DataSourceId(1),
            ProjectionList::new(projections.into()).into_scalar(),
        )
    }
}
