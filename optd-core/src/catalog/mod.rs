use std::sync::Arc;

use sqlx::prelude::FromRow;

use crate::storage::memo::SqliteMemo;

#[trait_variant::make(Send)]
pub trait Catalog {
    async fn create_database(&self, db_name: &str) -> anyhow::Result<Arc<DatabaseMetadata>>;

    async fn get_database(&self, db_name: &str) -> anyhow::Result<Arc<DatabaseMetadata>>;

    async fn create_namespace(
        &self,
        database_id: DatabaseId,
        namespace_name: &str,
    ) -> anyhow::Result<Arc<NamespaceMetadata>>;

    async fn get_namespace(
        &self,
        db_name: &str,
        namespace_name: &str,
    ) -> anyhow::Result<Arc<NamespaceMetadata>>;

    async fn create_table(
        &self,
        namespace_id: NamespaceId,
        table_name: &str,
        schema: &Schema,
    ) -> anyhow::Result<Arc<TableMetadata>>;

    async fn get_table(
        &self,
        db_name: &str,
        namespace_name: &str,
        table_name: &str,
    ) -> anyhow::Result<Arc<TableMetadata>>;

    async fn get_schema(&self, table_id: TableId) -> anyhow::Result<Schema>;
}

pub struct OptdCatalog {
    storage: Arc<SqliteMemo>,
}

impl Catalog for OptdCatalog {
    async fn create_database(&self, db_name: &str) -> anyhow::Result<Arc<DatabaseMetadata>> {
        let mut txn = self.storage.begin().await?;
        let db: DatabaseMetadata =
            sqlx::query_as("INSERT INTO database_metadata (name) VALUES (?) RETURNING id, name")
                .bind(db_name)
                .fetch_one(&mut *txn)
                .await
                .map_err(|e| {
                    anyhow::anyhow!("Failed to create database metadata for {}: {}", db_name, e)
                })?;
        txn.commit().await?;
        Ok(Arc::new(db))
    }

    async fn get_database(&self, db_name: &str) -> anyhow::Result<Arc<DatabaseMetadata>> {
        let mut txn = self.storage.begin().await?;
        let db: DatabaseMetadata =
            sqlx::query_as("SELECT id, name FROM database_metadata WHERE name = ?")
                .bind(db_name)
                .fetch_one(&mut *txn)
                .await
                .map_err(|e| {
                    anyhow::anyhow!("Failed to get database metadata for {}: {}", db_name, e)
                })?;
        txn.commit().await?;
        Ok(Arc::new(db))
    }

    async fn create_namespace(
        &self,
        database_id: DatabaseId,
        namespace_name: &str,
    ) -> anyhow::Result<Arc<NamespaceMetadata>> {
        let mut txn = self.storage.begin().await?;
        let namespace: NamespaceMetadata = sqlx::query_as(
            "INSERT INTO namespace_metadata (name, database_id) VALUES (?, ?) RETURNING id, name",
        )
        .bind(namespace_name)
        .bind(database_id)
        .fetch_one(&mut *txn)
        .await
        .map_err(|e| {
            anyhow::anyhow!(
                "Failed to create namespace metadata for {}: {}",
                namespace_name,
                e
            )
        })?;
        txn.commit().await?;
        Ok(Arc::new(namespace))
    }

    async fn get_namespace(
        &self,
        db_name: &str,
        namespace_name: &str,
    ) -> anyhow::Result<Arc<NamespaceMetadata>> {
        let mut txn = self.storage.begin().await?;
        let namespace: NamespaceMetadata = sqlx::query_as(
            "SELECT namespace_metadata.id, namespace_metadata.name FROM namespace_metadata, database_metadata WHERE database_metadata.name = ? and namspace_metadata.name = ? and namespace_metadata.database_id = database_metadata.id",
        )
        .bind(db_name)
        .bind(namespace_name)
        .fetch_one(&mut *txn)
        .await
        .map_err(|e| {
            anyhow::anyhow!(
                "Failed to get namespace metadata for {}.{}: {}",
                db_name,
                namespace_name,
                e
            )
        })?;
        txn.commit().await?;
        Ok(Arc::new(namespace))
    }

    async fn create_table(
        &self,
        namespace_id: NamespaceId,
        table_name: &str,
        schema: &Schema,
    ) -> anyhow::Result<Arc<TableMetadata>> {
        let mut txn = self.storage.begin().await?;
        let table: TableMetadata = sqlx::query_as(
            "INSERT INTO table_metadata (name, namespace_id) VALUES (?, ?) RETURNING id, name",
        )
        .bind(table_name)
        .bind(namespace_id)
        .fetch_one(&mut *txn)
        .await
        .map_err(|e| {
            anyhow::anyhow!("Failed to create table metadata for {}: {}", table_name, e)
        })?;
        for (i, attribute) in schema.attributes.iter().enumerate() {
            sqlx::query("INSERT INTO attributes (name, kind, table_id, base_attribute_number) VALUES (?, ?, ?, ?)")
                .bind(&attribute.name)
                .bind(&attribute.kind)
                .bind(table.id)
                .bind(i as i64)
                .execute(&mut *txn)
                .await
                .map_err(|e| {
                    anyhow::anyhow!(
                        "Failed to create attribute metadata for {}.{}: {}",
                        table_name,
                        attribute.name,
                        e
                    )
                })?;
        }
        txn.commit().await?;
        Ok(Arc::new(table))
    }

    async fn get_table(
        &self,
        db_name: &str,
        namespace_name: &str,
        table_name: &str,
    ) -> anyhow::Result<Arc<TableMetadata>> {
        let mut txn = self.storage.begin().await?;
        let table: TableMetadata = sqlx::query_as(
            "SELECT table_metadata.id, table_metadata.name FROM table_metadata, namespace_metadata, database_metadata WHERE database_metadata.name = ? and namspace_metadata.name = ? and namespace_metadata.database_id = database_metadata.id and table_metadata.namespace_id = namespace_metadata.id and table_metadata.name = ?",
        )
        .bind(db_name)
        .bind(namespace_name)
        .bind(table_name)
        .fetch_one(&mut *txn)
        .await
        .map_err(|e| {
            anyhow::anyhow!(
                "Failed to get table metadata for {}.{}.{}: {}",
                db_name,
                namespace_name,
                table_name,
                e
            )
        })?;
        txn.commit().await?;
        Ok(Arc::new(table))
    }

    async fn get_schema(&self, table_id: TableId) -> anyhow::Result<Schema> {
        let mut txn = self.storage.begin().await?;
        let attributes: Vec<Attribute> = sqlx::query_as(
            "SELECT attributes.id, attributes.name, attributes.kind FROM attributes WHERE attributes.table_id = ?",
        )
        .bind(table_id)
        .fetch_all(&mut *txn)
        .await
        .map_err(|e| {
            anyhow::anyhow!(
                "Failed to get schema metadata for table {:?}: {}",
                table_id,
                e
            )
        })?;

        Ok(Schema { attributes })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, sqlx::Type)]
#[repr(transparent)]
#[sqlx(transparent)]
pub struct DatabaseId(i64);

#[derive(Debug, Clone, PartialEq, Eq, FromRow, sqlx::Type)]
pub struct DatabaseMetadata {
    pub id: DatabaseId,
    pub name: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, sqlx::Type)]
#[repr(transparent)]
#[sqlx(transparent)]
pub struct NamespaceId(i64);

#[derive(Debug, Clone, PartialEq, Eq, FromRow, sqlx::Type)]
pub struct NamespaceMetadata {
    pub id: NamespaceId,
    pub name: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, sqlx::Type)]
#[repr(transparent)]
#[sqlx(transparent)]
pub struct TableId(i64);

#[derive(Debug, Clone, PartialEq, Eq, FromRow, sqlx::Type)]
pub struct TableMetadata {
    pub id: TableId,
    pub name: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, sqlx::Type)]
#[repr(transparent)]
#[sqlx(transparent)]
pub struct AttributeId(i64);

#[derive(Debug, Clone, PartialEq, Eq, FromRow, sqlx::Type)]
pub struct Attribute {
    /// The unique identifier for the attribute.
    pub id: AttributeId,
    /// The name of the attribute.
    pub name: String,
    /// The kind (data type) of the attribute.
    pub kind: String,
}

pub struct Schema {
    /// The attributes in the schema.
    pub attributes: Vec<Attribute>,
}

#[cfg(test)]
mod tests {

    use super::*;

    #[tokio::test]
    async fn test_schema() -> anyhow::Result<()> {
        let storage = Arc::new(SqliteMemo::new_in_memory().await?);
        let catalog = OptdCatalog { storage };

        let db = catalog.create_database("test_db").await?;
        let namespace = catalog.create_namespace(db.id, "test_namespace").await?;
        let schema = Schema {
            attributes: vec![
                Attribute {
                    id: AttributeId(1),
                    name: "id".to_string(),
                    kind: "INTEGER".to_string(),
                },
                Attribute {
                    id: AttributeId(2),
                    name: "name".to_string(),
                    kind: "TEXT".to_string(),
                },
            ],
        };
        let table = catalog
            .create_table(namespace.id, "test_table", &schema)
            .await?;

        assert_eq!(table.name, "test_table");
        assert_eq!(table.id, TableId(1));

        let schema = catalog.get_schema(table.id).await?;
        assert_eq!(schema.attributes.len(), 2);
        assert_eq!(schema.attributes[0].name, "id");
        assert_eq!(schema.attributes[0].kind, "INTEGER");
        assert_eq!(schema.attributes[1].name, "name");
        assert_eq!(schema.attributes[1].kind, "TEXT");

        Ok(())
    }
}
