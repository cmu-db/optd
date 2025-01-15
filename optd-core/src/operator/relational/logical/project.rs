/// Logical project operator that specifies output columns.
///
/// Takes input relation (`RelLink`) and defines output columns/expressions
/// (`ScalarLink`).
#[derive(Clone)]
pub struct Project<RelLink, ScalarLink> {
    pub child: RelLink,
    pub fields: Vec<ScalarLink>,
}
