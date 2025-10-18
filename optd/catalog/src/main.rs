mod optd_catalog;
mod optd_table;

use duckdb::{
    Connection, Error, Result,
    arrow::{
        array::{Int32Array, RecordBatch, StringArray},
        datatypes::{DataType, Field, SchemaBuilder},
    },
    params,
};

fn main() -> Result<(), Error> {
    let conn = Connection::open_in_memory()?;

    conn.execute_batch(
        r#"
        INSTALL ducklake;
        LOAD ducklake;

        ATTACH 'ducklake:metadata.ducklake' AS meta_lake (DATA_PATH 'data_files');
        USE meta_lake;
        "#,
    )?;

    Ok(())
}
