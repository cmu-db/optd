use std::{collections::HashSet, sync::Arc};

use crate::ir::{
    ColumnSet, OperatorKind,
    operator::{LogicalGet, PhysicalTableScan},
    properties::{Derive, GetProperty, PropertyMarker},
};

#[derive(Debug, Clone, PartialEq)]
pub struct OutputColumns(Arc<ColumnSet>);

impl OutputColumns {
    pub fn from_column_set(set: ColumnSet) -> Self {
        Self(Arc::new(set))
    }

    pub fn set(&self) -> &ColumnSet {
        &self.0
    }
}

impl PropertyMarker for OutputColumns {}

impl Derive<OutputColumns> for crate::ir::Operator {
    fn derive_by_compute(&self, ctx: &crate::ir::context::IRContext) -> OutputColumns {
        match &self.kind {
            OperatorKind::Group(_) => {
                // Always derive a placeholder using the normalized expression.
                panic!("Right now group's properties should always be set.")
            }
            OperatorKind::LogicalGet(meta) => {
                LogicalGet::from_raw_parts(meta.clone(), self.common.clone()).derive_by_compute(ctx)
            }
            OperatorKind::PhysicalTableScan(meta) => {
                PhysicalTableScan::from_raw_parts(meta.clone(), self.common.clone())
                    .derive_by_compute(ctx)
            }
            OperatorKind::LogicalJoin(_)
            | OperatorKind::PhysicalNLJoin(_)
            | OperatorKind::LogicalSelect(_)
            | OperatorKind::PhysicalFilter(_)
            | OperatorKind::EnforcerSort(_) => {
                let set = self
                    .input_operators()
                    .iter()
                    .fold(HashSet::new(), |mut set, op| {
                        let output_from_child: OutputColumns = op.derive(ctx);
                        set.extend(output_from_child.set());
                        set
                    });
                OutputColumns::from_column_set(set)
            }
            #[cfg(test)]
            OperatorKind::MockScan(meta) => meta.spec.mocked_output_columns.clone(),
        }
    }

    fn derive(&self, ctx: &crate::ir::context::IRContext) -> OutputColumns {
        self.common
            .properties
            .output_columns
            .get_or_init(|| self.derive_by_compute(ctx))
            .clone()
    }
}

impl GetProperty for crate::ir::Operator {}
