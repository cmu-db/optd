#[cfg(feature = "serde")]
use std::sync::Arc;

#[cfg(feature = "serde")]
use optd_core::{AnalysisContext, Catalog, MemoryCatalog, TableRef};
use optd_core::{ColumnData, OperatorData, Output, QueryContext, Rename, Scan};

fn main() {
    let mut ctx = QueryContext::new();

    // original columns and scan
    let id = ColumnData::new("id", arrow_schema::DataType::Int64).add(&mut ctx);
    let scan = OperatorData::Scan(Scan {
        table: optd_core::TableRef::bare("users"),
        columns: vec![id],
    })
    .add(&mut ctx);

    // renamed column with qualifier
    let renamed_id = ColumnData::with_qualifier("id", arrow_schema::DataType::Int64, "alias_users")
        .add(&mut ctx);

    // defs: (renamed, original)
    let defs = vec![(renamed_id, id)];

    let rename = OperatorData::Rename(Rename {
        alias: "alias_users".to_string(),
        defs,
        input: scan,
    })
    .add(&mut ctx);

    let output = OperatorData::Output(Output { input: rename }).add(&mut ctx);
    ctx.set_root(output);

    // Print optimizer visualizer JSON
    #[cfg(feature = "serde")]
    {
        let catalog = Arc::new(MemoryCatalog::new("memory", "public"));
        catalog
            .create_table(
                TableRef::bare("users"),
                Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                    "id",
                    arrow_schema::DataType::Int64,
                    true,
                )])),
                None,
            )
            .unwrap();
        println!(
            "{}",
            ctx.optimizer_visualizer_json("rename-example", AnalysisContext::new(catalog))
        );
    }

    #[cfg(not(feature = "serde"))]
    {
        eprintln!("enable serde feature to print JSON");
    }
}
