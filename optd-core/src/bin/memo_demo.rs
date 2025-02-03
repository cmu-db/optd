use optd_core::memo::persistent_memo::PersistentMemo;

// async fn ingest_partial_plan(
//     memo: &PersistentMemo,
//     partial_logical_plan: &PartialLogicalPlan,
// ) -> anyhow::Result<()> {
//     todo!()
// }

// async fn ingest_partial_plan_inner(
//     memo: &PersistentMemo,
//     partial_logical_plan: &PartialLogicalPlan,
// ) -> anyhow::Result<()> {
//     todo!()
// }

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _memo = PersistentMemo::new_in_memory().await?;

    // let partial_logical_plan = PartialLogicalPlan {
    // };

    // ingest_partial_plan(&memo, &partial_logical_plan).await?;

    Ok(())
}
