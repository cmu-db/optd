/// Logical join operator that combines rows from two relations.
///
/// Takes left and right relations (`Relation`) and joins their rows using a join condition
/// (`Scalar`).
#[derive(Clone)]
pub struct Join<Metadata, Relation, Scalar> {
    pub join_type: Metadata,
    pub left: Relation,
    pub right: Relation,
    pub condition: Scalar,
}
