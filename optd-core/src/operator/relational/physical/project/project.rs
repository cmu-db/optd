/// Column projection operator that transforms input rows.
///
/// Takes input relation (`RelLink`) and projects columns/expressions (`ScalarLink`)
/// to produce output rows with selected/computed fields.
#[derive(Clone)]
pub struct Project<RelLink, ScalarLink> {
    pub child: RelLink,
    pub fields: Vec<ScalarLink>,
}
