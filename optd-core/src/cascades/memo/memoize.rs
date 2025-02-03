/// A trait for memoizing expressions.
#[trait_variant::make(Send)]
pub trait Memoize: Send + Sync + 'static {
    /// Gets the exploration status of a group.
    async fn get_group_exploration_status(
        &self,
        group_id: GroupId,
    ) -> anyhow::Result<ExplorationStatus>;

    /// Sets the exploration status of a group.
    async fn set_group_exploration_status(
        &self,
        group_id: GroupId,
        status: ExplorationStatus,
    ) -> anyhow::Result<()>;

    /// Gets all logical expressions in a group.
    async fn get_all_logical_exprs_in_group(
        &self,
        group_id: GroupId,
    ) -> anyhow::Result<Vec<LogicalExpression>>;

    /// Adds a logical expression to a group.
    async fn add_logical_expr_to_group(
        &self,
        logical_expr: &LogicalExpression,
        group_id: GroupId,
    ) -> anyhow::Result<GroupId>;

    /// Adds a logical expression.
    async fn add_logical_expr(&self, logical_expr: &LogicalExpression) -> anyhow::Result<GroupId>;

    /// Gets the exploration status of a scalar group.
    async fn get_scalar_group_exploration_status(
        &self,
        group_id: ScalarGroupId,
    ) -> anyhow::Result<ExplorationStatus>;

    /// Sets the exploration status of a scalar group.
    async fn set_scalar_group_exploration_status(
        &self,
        group_id: ScalarGroupId,
        status: ExplorationStatus,
    ) -> anyhow::Result<()>;

    /// Gets all scalar expressions in a group.
    async fn get_all_scalar_exprs_in_group(
        &self,
        group_id: ScalarGroupId,
    ) -> anyhow::Result<Vec<ScalarExpression>>;

    /// Adds a scalar expression to a group.
    async fn add_scalar_expr_to_group(
        &self,
        scalar_expr: &ScalarExpression,
        group_id: ScalarGroupId,
    ) -> anyhow::Result<ScalarGroupId>;

    /// Adds a scalar expression.
    async fn add_scalar_expr(
        &self,
        scalar_expr: &ScalarExpression,
    ) -> anyhow::Result<ScalarGroupId>;
}
