use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use itertools::Itertools;

use optd_core::{
    cascades::Cascades,
    ir::{
        Column, IRContext, Operator,
        builder::*,
        explain::quick_explain,
        operator::join::JoinType,
        properties::{Required, TupleOrdering, TupleOrderingDirection},
        rule::RuleSet,
        table_ref::TableRef,
    },
    rules,
};

async fn optimize_plan(
    opt: Arc<Cascades>,
    initial_plan: &Arc<Operator>,
    required: Arc<Required>,
) -> Option<Arc<Operator>> {
    println!("available rules:");
    for rule in opt.rule_set.iter() {
        println!("- {}", rule.name());
    }
    {
        println!("\n MEMO BEFORE OPT");
        opt.memo.read().await.dump();
    }
    let optimized = opt.optimize(initial_plan, required.clone()).await;
    {
        println!("\nMEMO AFTER OPT");
        opt.memo.read().await.dump();
    }
    let initial_explained = quick_explain(initial_plan, &opt.ctx);
    println!("{initial_explained}");
    let optimized = optimized.unwrap();
    let optimized_explained = quick_explain(&optimized, &opt.ctx);

    let initial_explained = initial_explained.split('\n').collect::<Vec<&str>>();
    let optimized_explained = optimized_explained.split('\n').collect::<Vec<&str>>();
    let initial_len = initial_explained[0].len();

    println!("\nEXPLAIN (root_requirement: {required}):");
    std::iter::once(format!("{:<initial_len$}", "initial plan:").as_str())
        .chain(initial_explained)
        .zip_longest(std::iter::once("final plan:").chain(optimized_explained))
        .for_each(|res| match res {
            itertools::EitherOrBoth::Both(l, r) => println!("{l}       {r}"),
            itertools::EitherOrBoth::Left(l) => println!("{l}"),
            itertools::EitherOrBoth::Right(r) => {
                println!("{}       {r}", " ".repeat(initial_len))
            }
        });
    Some(optimized)
}

#[tokio::test]
async fn integration() -> Result<(), Box<dyn std::error::Error>> {
    // console_subscriber::init();
    tracing_subscriber::fmt()
        .without_time()
        .with_max_level(tracing::Level::INFO)
        // .with_target(false) // Optional: also remove target
        .compact() // Optional: use compact format
        .init();

    let ctx = Arc::new(IRContext::with_empty_magic());
    let schema = Arc::new(Schema::new(vec![
        Field::new("v1", DataType::Int64, true),
        Field::new("v2", DataType::Int64, false),
        Field::new("v3", DataType::Int64, false),
    ]));
    let m1_table_index = ctx.add_binding(Some(TableRef::bare("m1")), schema.clone())?;
    let m1 = ctx.logical_get(m1_table_index, &schema, None);
    let schema = Arc::new(Schema::new(vec![
        Field::new("v4", DataType::Int64, true),
        Field::new("v5", DataType::Int64, false),
    ]));
    let m2_table_index = ctx.add_binding(Some(TableRef::bare("m2")), schema.clone())?;
    let m2 = ctx.logical_get(m2_table_index, &schema, None);

    let schema = Arc::new(Schema::new(vec![
        Field::new("v6", DataType::Int64, true),
        Field::new("v7", DataType::Int64, false),
    ]));
    let m3_table_index = ctx.add_binding(Some(TableRef::bare("m3")), schema.clone())?;
    let m3 = ctx.logical_get(m3_table_index, &schema, None);

    // CREATE TABLE m1(v1 int, v2 int, v3 int);
    // CREATE TABLE m2(v4 int, v5 int);
    // CREATE TABLE m3(v6 int, v7 int);
    // INSERT INTO m1 VALUES ... ;
    // 10
    // INSERT INTO m2 VALUES ... ;
    // 1000
    // INSERT INTO m3 VALUES ... ;
    // 20
    // SELECT * FROM m1
    // INNER JOIN m2 ON m1.v1 = m2.v4
    // INNER JOIN m3 ON m1.v2 = m3.v6
    // WHERE m3.v7 = 445 AND m1.v3 = 799 ORDER BY v4;

    let required = Arc::new(Required {
        tuple_ordering: TupleOrdering::from_iter([(
            Column(m2_table_index, 1),
            TupleOrderingDirection::Asc,
        )]),
    });
    let join_m1_m2_and_m3 = m1
        .logical_join(
            m2,
            column_ref(Column(m1_table_index, 0)).eq(column_ref(Column(m2_table_index, 0))),
            JoinType::Inner,
        )
        .logical_join(
            m3,
            column_ref(Column(m1_table_index, 1)).eq(column_ref(Column(m2_table_index, 0))),
            JoinType::Inner,
        )
        .select(column_ref(Column(m1_table_index, 2)).eq(int32(799)))
        .select(column_ref(Column(m3_table_index, 1)).eq(int32(445)))
        .project(
            4,
            [
                column_ref(Column(m1_table_index, 2)),
                column_ref(Column(m1_table_index, 0)).plus(int32(1)),
            ],
        );

    let rule_set = RuleSet::builder()
        .add_rule(rules::LogicalGetAsPhysicalTableScanRule::new())
        .add_rule(rules::LogicalJoinAsPhysicalHashJoinRule::new())
        .add_rule(rules::LogicalJoinAsPhysicalNLJoinRule::new())
        .add_rule(rules::LogicalSelectSimplifyRule::new())
        .add_rule(rules::LogicalSelectJoinTransposeRule::new())
        .add_rule(rules::LogicalJoinInnerCommuteRule::new())
        .add_rule(rules::LogicalJoinInnerAssocRule::new())
        .build();
    let opt = Arc::new(Cascades::new(ctx, rule_set));

    optimize_plan(opt, &join_m1_m2_and_m3, required)
        .await
        .unwrap();
    Ok(())
}
