/// Physical filter operator that applies a boolean predicate to filter input rows.
///
/// Takes a child operator (`Relation`) providing input rows and a predicate expression
/// (`Scalar`) that evaluates to true/false. Only rows where predicate is true
/// are emitted.
#[derive(Clone)]
pub struct Filter<Relation, Scalar> {
    pub child: Relation,
    pub predicate: Scalar,
}
