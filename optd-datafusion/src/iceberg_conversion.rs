use crate::NAMESPACE;
use datafusion::common::arrow::datatypes::{DataType as DFType, Schema as DFSchema};
use datafusion::execution::SessionState;
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::{Catalog, NamespaceIdent, Result, TableCreation, TableIdent};
use std::sync::Arc;

// Given a map of table names to [`TableProvider`]s, ingest them into an Iceberg [`Catalog`].
pub(crate) async fn ingest_providers<C>(
    iceberg_catalog: &C,
    datafusion_session: &SessionState,
) -> Result<()>
where
    C: Catalog,
{
    let mut catalog_names = datafusion_session.catalog_list().catalog_names();
    assert_eq!(
        catalog_names.len(),
        1,
        "TODO(connor): There should only be 1 catalog by our usage"
    );

    let catalog_name = catalog_names.pop().expect("We checked non-empty above");

    let datafusion_catalog = datafusion_session
        .catalog_list()
        .catalog(&catalog_name)
        .expect("This catalog must exist if it was just listed");

    // Ignore the method name, it's just DataFusion naming.
    let mut table_collection_names = datafusion_catalog.schema_names();
    assert_eq!(
        table_collection_names.len(),
        1,
        "TODO(connor): There should only be 1 catalog by our usage"
    );

    let table_collection_name = table_collection_names
        .pop()
        .expect("We checked non-empty above");

    let table_collection = datafusion_catalog
        .schema(&table_collection_name)
        .expect("This collection must exist if it was just listed");

    let namespace_ident = NamespaceIdent::from_vec(vec![NAMESPACE.to_string()]).unwrap();

    for name in table_collection.table_names() {
        let provider = table_collection
            .table(&name)
            .await
            .expect("TODO(connor): Error handle")
            .expect("This table must exist if it was just listed");

        // Create the table identifier.
        let table_ident = TableIdent::new(namespace_ident.clone(), name.clone());

        if iceberg_catalog.table_exists(&table_ident).await? {
            eprintln!("TODO(connor): Table update is unimplemented, doing nothing for now");
        } else {
            let df_schema = provider.schema();
            let iceberg_schema = df_to_iceberg_schema(&df_schema);

            let create_table = TableCreation {
                name: name.clone(),
                schema: iceberg_schema,
                location: None,
                properties: df_schema.metadata.clone(),
                partition_spec: None,
                sort_order: None,
            };

            iceberg_catalog
                .create_table(&namespace_ident, create_table)
                .await?;
        }
    }

    Ok(())
}

/// Converts a DataFusion [`DFSchema`] to an Iceberg [`Schema`].
fn df_to_iceberg_schema(df_schema: &DFSchema) -> Schema {
    let fields = &df_schema.fields;

    let fields = fields.iter().enumerate().map(|(i, field)| {
        let field_name = field.name();
        let iceberg_type = df_to_iceberg_type(field.data_type());

        Arc::new(NestedField {
            id: i as i32,
            name: field_name.clone(),
            required: true,
            field_type: Box::new(iceberg_type),
            doc: None,
            initial_default: None,
            write_default: None,
        })
    });

    Schema::builder()
        .with_fields(fields)
        .build()
        .expect("Failed to convert DataFusion schema to Iceberg schema")
}

/// Converts a DataFusion [`DFType`] to an Iceberg [`Type`].
///
/// TODO(connor): Some of these are probably wrong.
///
/// See:
/// - https://docs.rs/datafusion/latest/datafusion/common/arrow/datatypes/enum.DataType.html
/// - https://docs.rs/iceberg/latest/iceberg/spec/enum.Type.html
fn df_to_iceberg_type(df_datatype: &DFType) -> Type {
    match df_datatype {
        DFType::Null => unimplemented!("TODO: All Iceberg types are (seem to be) nullable"),
        DFType::Boolean => Type::Primitive(PrimitiveType::Boolean),
        DFType::Int8 => Type::Primitive(PrimitiveType::Int),
        DFType::Int16 => Type::Primitive(PrimitiveType::Int),
        DFType::Int32 => Type::Primitive(PrimitiveType::Int),
        DFType::Int64 => Type::Primitive(PrimitiveType::Long),
        DFType::UInt8 => Type::Primitive(PrimitiveType::Int),
        DFType::UInt16 => Type::Primitive(PrimitiveType::Int),
        DFType::UInt32 => Type::Primitive(PrimitiveType::Int),
        DFType::UInt64 => Type::Primitive(PrimitiveType::Long),
        DFType::Float16 => Type::Primitive(PrimitiveType::Float),
        DFType::Float32 => Type::Primitive(PrimitiveType::Float),
        DFType::Float64 => Type::Primitive(PrimitiveType::Double),
        DFType::Timestamp(_, _) => Type::Primitive(PrimitiveType::Timestamp),
        DFType::Date32 => Type::Primitive(PrimitiveType::Date),
        DFType::Date64 => Type::Primitive(PrimitiveType::Date),
        DFType::Time32(_) => Type::Primitive(PrimitiveType::Timestamp),
        DFType::Time64(_) => Type::Primitive(PrimitiveType::Timestamp),
        DFType::Binary => Type::Primitive(PrimitiveType::Binary),
        DFType::FixedSizeBinary(bytes) => Type::Primitive(PrimitiveType::Fixed(*bytes as u64)),
        DFType::LargeBinary => Type::Primitive(PrimitiveType::Binary),
        DFType::BinaryView => Type::Primitive(PrimitiveType::Binary),
        DFType::Utf8 => Type::Primitive(PrimitiveType::String),
        DFType::LargeUtf8 => Type::Primitive(PrimitiveType::String),
        DFType::Utf8View => Type::Primitive(PrimitiveType::String),
        DFType::Decimal128(precision, scale) => Type::Primitive(PrimitiveType::Decimal {
            precision: *precision as u32,
            scale: *scale as u32,
        }),
        DFType::Decimal256(precision, scale) => Type::Primitive(PrimitiveType::Decimal {
            precision: *precision as u32,
            scale: *scale as u32,
        }),
        dt => unimplemented!("Unsupported data type: {:?}", dt),
    }
}
