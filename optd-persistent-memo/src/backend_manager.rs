use optd_core::{
    cascades::{ExprId, GroupId},
    nodes::NodeType,
};
use optd_persistent::{
    entities::{
        cascades_group, logical_expression, physical_expression,
        prelude::{LogicalExpression, PhysicalExpression},
    },
    StorageResult,
};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, Database, DatabaseConnection, EntityTrait, QueryFilter, Set,
};

// TODO: Use the junction table for children instead of data field!
// TODO: Physical version of everything :/
pub struct MemoBackendManager {
    db: DatabaseConnection,
}

impl MemoBackendManager {
    pub async fn new(database_url: Option<&str>) -> StorageResult<Self> {
        Ok(Self {
            db: Database::connect(database_url.unwrap()).await?,
        })
    }

    pub async fn get_expr_by_id(
        &self,
        expr_id: i32,
    ) -> StorageResult<Option<(i32, i16, Vec<i32>)>> {
        let expr = logical_expression::Entity::find()
            .filter(logical_expression::Column::Id.eq(expr_id))
            .one(&self.db)
            .await?;

        if let Some(expr) = expr {
            let children: Vec<i32> = serde_json::from_value(expr.data).unwrap();
            Ok(Some((expr.group_id, expr.variant_tag, children)))
        } else {
            Ok(None)
        }
    }

    pub async fn lookup_expr(
        &self,
        typ_id: i16,
        children: &Vec<i32>,
    ) -> StorageResult<Option<(i32, i32)>> {
        let children_json = serde_json::to_value(children).unwrap();
        let matches = logical_expression::Entity::find()
            .filter(logical_expression::Column::VariantTag.eq(typ_id))
            .filter(logical_expression::Column::Data.eq(children_json))
            .all(&self.db)
            .await?;

        if matches.is_empty() {
            return Ok(None);
        }

        // Only 1 should be identical, else something has gone wrong
        assert!(
            matches.len() <= 1,
            "Found duplicate expressions in the logical_expression table!"
        );

        let existing_expression = matches
            .last()
            .expect("we just checked that an element exists");

        Ok(Some((existing_expression.group_id, existing_expression.id)))
    }

    pub async fn insert_expr(
        &self,
        typ_id: i16,
        children: &Vec<i32>,
        add_to_group_id: Option<i32>,
    ) -> StorageResult<(i32, i32)> {
        let children_json = serde_json::to_value(children).unwrap();

        // assert no identical expressions exist
        debug_assert!(
            self.lookup_expr(typ_id, children).await?.is_none(),
            "Identical expression already exists in the logical_expression table!"
        );

        let group_id = if let Some(group_id) = add_to_group_id {
            group_id
        } else {
            // Insert a new group.
            let group = cascades_group::ActiveModel {
                latest_winner: Set(None),
                in_progress: Set(false),
                is_optimized: Set(false),
                ..Default::default()
            };
            group.insert(&self.db).await?.id
        };

        // Insert the input expression with the correct `group_id`.
        let new_expr = logical_expression::ActiveModel {
            group_id: Set(group_id),
            variant_tag: Set(typ_id.try_into().unwrap()),
            data: Set(children_json),
            ..Default::default()
        };
        let new_expr = new_expr.insert(&self.db).await?;

        Ok((new_expr.group_id, new_expr.id))
    }
}
