/// Logical join operator that combines rows from two relations.
///
/// Takes left and right relations (`Relation`) and joins their rows using a join condition
/// (`Scalar`).
#[derive(Clone)]
pub struct Join<Relation, Scalar> {
    pub join_type: String,
    pub left: Relation,
    pub right: Relation,
    pub condition: Scalar,
}
