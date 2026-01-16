//! The mock scan operator is a scan on some mock data - as one implementation
//! of the logical get operator.

use std::sync::Arc;
use pretty_xmlish::Pretty;
use crate::ir::{
    Column, ColumnSet, IRCommon,
    cost::Cost,
    explain::Explain,
    macros::{define_node, impl_operator_conversion},
    properties::{Cardinality, OperatorProperties, TupleOrdering},
};

#[derive(Clone)]
pub struct MockSpec {
    pub mocked_output_columns: Arc<ColumnSet>,
    pub mocked_card: Cardinality,
    pub mocked_operator_cost: Option<Cost>,
    pub mocked_provided_ordering: TupleOrdering,
}

impl Default for MockSpec {
    fn default() -> Self {
        Self {
            mocked_output_columns: Arc::default(),
            mocked_card: Cardinality::ZERO,
            mocked_operator_cost: Some(Cost::new(0.)),
            mocked_provided_ordering: Default::default(),
        }
    }
}

impl std::fmt::Debug for MockSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "<private>")
    }
}

impl Explain for MockScanBorrowed<'_> {
    fn explain<'a>(
        &self,
        ctx: &crate::ir::IRContext,
        option: &crate::ir::explain::ExplainOption,
    ) -> pretty_xmlish::Pretty<'a> {
        let mut fields = Vec::with_capacity(2);
        fields.push((".mock_id", Pretty::display(self.mock_id())));
        fields.extend(self.common.explain_operator_properties(ctx, option));
        Pretty::childless_record("MockScan", fields)
    }
}

impl MockSpec {
    pub fn new_test_only(ids: Vec<Column>, card: f64) -> Self {
        let mocked_output_columns = Arc::new(ids.iter().copied().collect());
        let mocked_card = Cardinality::new(card);
        Self {
            mocked_output_columns,
            mocked_card,
            mocked_operator_cost: Some(Cost::UNIT * card * 1.1),
            ..Default::default()
        }
    }

    pub fn set_operator_cost(&mut self, c: Option<Cost>) -> &mut Self {
        self.mocked_operator_cost = c;
        self
    }

    pub fn set_provided_ordering(&mut self, ordering: TupleOrdering) -> &mut Self {
        self.mocked_provided_ordering = ordering;
        self
    }
}

impl PartialEq for MockSpec {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}
impl Eq for MockSpec {}
impl std::hash::Hash for MockSpec {
    fn hash<H: std::hash::Hasher>(&self, _state: &mut H) {}
}

define_node!(
    MockScan, MockScanBorrowed {
        properties: OperatorProperties,
        metadata: MockScanMetadata {
            mock_id: usize,
            spec: Arc<MockSpec>,
        },
        inputs: {
            operators: [],
            scalars: [],
        }
    }
);
impl_operator_conversion!(MockScan, MockScanBorrowed);

/// Metadata: 
/// - mock_id: The mock data source to scan.
/// - spec: The mocked schema for this mock data source
/// Scalars: (none)
impl MockScan {
    pub fn with_mock_spec(id: usize, spec: MockSpec) -> Self {
        Self {
            meta: MockScanMetadata {
                mock_id: id,
                spec: Arc::new(spec),
            },
            common: IRCommon::empty(),
        }
    }
}
