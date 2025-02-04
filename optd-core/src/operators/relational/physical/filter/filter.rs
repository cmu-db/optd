use serde::Deserialize;

/// Physical filter operator that applies a boolean predicate to filter input rows.
///
/// Takes a child operator (`Relation`) providing input rows and a predicate expression
/// (`Scalar`) that evaluates to true/false. Only rows where predicate is true
/// are emitted.
#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct PhysicalFilter<Relation, Scalar> {
    pub child: Relation,
    pub predicate: Scalar,
}

impl<Relation, Scalar> PhysicalFilter<Relation, Scalar> {
    /// Create a new filter operator.
    pub fn new(child: Relation, predicate: Scalar) -> Self {
        Self { child, predicate }
    }
}
