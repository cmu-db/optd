/// Physical filter operator that applies a boolean predicate to filter input rows.
///
/// Takes a child operator (`RelLink`) providing input rows and a predicate expression
/// (`ScalarLink`) that evaluates to true/false. Only rows where predicate is true
/// are emitted.
pub struct FilterOperator<RelLink, ScalarLink> {
    pub child: RelLink,
    pub predicate: ScalarLink,
}
