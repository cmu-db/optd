/// Column projection operator that transforms input rows.
///
/// Takes input relation (`Relation`) and projects columns/expressions (`Scalar`)
/// to produce output rows with selected/computed fields.
#[derive(Clone)]
pub struct Project<Relation, Scalar> {
    pub child: Relation,
    pub fields: Vec<Scalar>,
}
