use datafusion::{
    arrow::{
        datatypes::DataType,
        util::display::{ArrayFormatter, FormatOptions},
    },
    error::DataFusionError,
};
use optd_datafusion::DataFusionDB;
use sqllogictest::{DBOutput, DefaultColumnType};

pub struct DBWrapper(pub DataFusionDB);

#[async_trait::async_trait]
impl sqllogictest::AsyncDB for DBWrapper {
    type Error = DataFusionError;
    type ColumnType = DefaultColumnType;

    async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        let batches = self.0.execute_one(sql).await?;
        let options = FormatOptions::default().with_null("NULL");

        let mut rows = Vec::new();
        let mut types = Vec::new();
        for batch in batches {
            if types.is_empty() {
                types = batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| match f.data_type() {
                        DataType::Utf8 => DefaultColumnType::Text,
                        DataType::Int32 | DataType::Int64 => DefaultColumnType::Integer,
                        DataType::Float32 | DataType::Float64 => DefaultColumnType::FloatingPoint,
                        _ => DefaultColumnType::Any,
                    })
                    .collect();
            }
            let converters = batch
                .columns()
                .iter()
                .map(|a| ArrayFormatter::try_new(a.as_ref(), &options))
                .collect::<Result<Vec<_>, _>>()?;
            for row_idx in 0..batch.num_rows() {
                let mut row = Vec::with_capacity(batch.num_columns());
                for converter in converters.iter() {
                    let mut buffer = String::with_capacity(8);
                    converter.value(row_idx).write(&mut buffer)?;
                    row.push(buffer);
                }
                rows.push(row);
            }
        }

        Ok(DBOutput::Rows { types, rows })
    }

    async fn shutdown(&mut self) {}
}
