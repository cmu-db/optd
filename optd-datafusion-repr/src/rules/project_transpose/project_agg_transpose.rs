// projects away aggregate calls that are not used
// TODO
define_rule!(
    ProjectAggregatePushDown,
    apply_projection_agg_pushdown,
    (
        Projection, 
        (Agg, child, [agg_exprs], [agg_groups]), 
        [exprs]
    )
);

fn apply_projection_agg_pushdown(
    _optimizer: &impl Optimizer<OptRelNodeTyp>,
    ProjectAggregatePushDownPicks { child, agg_exprs, agg_groups, exprs }: ProjectAggregatePushDownPicks,
) -> Vec<RelNode<OptRelNodeTyp>> {
    vec![]
}