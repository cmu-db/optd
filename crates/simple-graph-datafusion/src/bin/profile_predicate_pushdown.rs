use simple_graph_datafusion::profiling::{
    predicate_pushdown_queries, profile_predicate_pushdown_sql, synthetic_session,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runs = std::env::args()
        .nth(1)
        .map(|arg| arg.parse::<usize>())
        .transpose()?
        .unwrap_or(100);

    let session = synthetic_session()?;
    let queries = predicate_pushdown_queries();

    println!("query\trun\titeration\tpass_index\tpass\tresult\tduration_ms");
    for query in queries {
        // Warm up SQL planning and conversion once per shape.
        let _ = profile_predicate_pushdown_sql(&session, &query.sql).await?;

        for run in 0..runs {
            for profile in profile_predicate_pushdown_sql(&session, &query.sql).await? {
                println!(
                    "{}\t{}\t{}\t{}\t{}\t{:?}\t{:.6}",
                    query.name,
                    run + 1,
                    profile.iteration,
                    profile.pass_index,
                    profile.pass,
                    profile.result,
                    profile.duration_ms
                );
            }
        }
    }

    Ok(())
}
