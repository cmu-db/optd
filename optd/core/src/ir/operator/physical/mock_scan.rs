use crate::ir::{
    Column, ColumnSet, IRCommon,
    cost::Cost,
    macros::{define_node, impl_operator_conversion},
    properties::{Cardinality, OperatorProperties, OutputColumns, TupleOrdering},
};

#[derive(Debug, Clone, PartialEq)]
pub struct MockSpec {
    pub mocked_output_columns: OutputColumns,
    pub mocked_card: Cardinality,
    pub mocked_operator_cost: Option<Cost>,
    pub mocked_provided_ordering: TupleOrdering,
}

impl Default for MockSpec {
    fn default() -> Self {
        Self {
            mocked_output_columns: OutputColumns::from_column_set(ColumnSet::new()),
            mocked_card: Cardinality::ZERO,
            mocked_operator_cost: Some(Cost::new(0.)),
            mocked_provided_ordering: Default::default(),
        }
    }
}

impl MockSpec {
    pub fn new_test_only(ids: Vec<i64>, card: f64) -> Self {
        let mocked_output_columns =
            OutputColumns::from_column_set(ids.iter().map(|id| Column(*id)).collect());
        let mocked_card = Cardinality::new(card);
        Self {
            mocked_output_columns,
            mocked_card,
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

impl Eq for MockSpec {}
impl std::hash::Hash for MockSpec {
    fn hash<H: std::hash::Hasher>(&self, _state: &mut H) {}
}

define_node!(
    MockScan {
        properties: OperatorProperties,
        metadata: MockScanMetadata {
            spec: MockSpec,
        },
        inputs: {
            operators: [],
            scalars: [],
        }
    }
);
impl_operator_conversion!(MockScan);

impl MockScan {
    pub fn with_mock_spec(spec: MockSpec) -> Self {
        Self {
            meta: MockScanMetadata { spec },
            common: IRCommon::empty(),
        }
    }
}
