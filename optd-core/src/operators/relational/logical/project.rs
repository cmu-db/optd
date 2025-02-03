/// Logical project operator that specifies output columns.
///
/// Takes input relation (`Relation`) and defines output columns/expressions
/// (`Scalar`).
#[derive(Clone)]
pub struct Project<Relation, Scalar> {
    pub child: Relation,
    pub fields: Vec<Scalar>,
}
