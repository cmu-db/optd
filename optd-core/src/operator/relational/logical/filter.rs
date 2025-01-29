/// Logical filter operator that selects rows matching a condition.
///
/// Takes input relation (`Relation`) and filters rows using a boolean predicate (`Scalar`).
#[derive(Clone)]
pub struct Filter<Relation, Scalar> {
    pub child: Relation,
    pub predicate: Scalar,
}
