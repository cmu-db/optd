use optd_persistent::{
    entities::{
        cascades_group, group_winner, logical_children, logical_expression, physical_expression,
    },
    StorageResult,
};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, Database, DatabaseConnection, EntityTrait, PaginatorTrait,
    QueryFilter, Set,
};

// Ben:
// Support logical/physical expressions
// Figure out how to store and use winners

// Yuchen:
// Support predicates
// Figure out how to support group merging

pub struct BackendWinnerInfo {
    pub expr_id: i32,
    pub cost: f64,
}

pub struct BackendGroupInfo {
    pub group_exprs: Vec<i32>,
    pub winner: Option<BackendWinnerInfo>,
}

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

    async fn get_winner(&self, winner_id: i32) -> StorageResult<Option<group_winner::Model>> {
        let winner = group_winner::Entity::find()
            .filter(group_winner::Column::Id.eq(winner_id))
            .one(&self.db)
            .await?;

        Ok(winner)
    }

    pub async fn get_group(&self, group_id: i32) -> StorageResult<BackendGroupInfo> {
        let group = cascades_group::Entity::find()
            .filter(cascades_group::Column::Id.eq(group_id))
            .one(&self.db)
            .await?;

        let group = group.expect("Group not found!");

        // todo no physical here either
        let children: Vec<i32> = logical_children::Entity::find()
            .filter(logical_children::Column::GroupId.eq(group_id))
            .all(&self.db)
            .await?
            .iter()
            .map(|child| child.logical_expression_id)
            .collect();

        let winner = self.get_winner(group.latest_winner.unwrap()).await?;

        Ok(BackendGroupInfo {
            group_exprs: children,
            winner: winner.map(|winner| BackendWinnerInfo {
                expr_id: winner.physical_expression_id,
                cost: 100.0, // TODO
            }),
        })
    }

    pub async fn update_winner(&self) {
        todo!()
    }

    pub async fn get_expr_count(&self) -> StorageResult<u64> {
        let count = logical_expression::Entity::find().count(&self.db).await?;
        Ok(count)
    }

    pub async fn get_all_group_ids(&self) -> StorageResult<Vec<i32>> {
        let groups = cascades_group::Entity::find().all(&self.db).await?;
        Ok(groups.iter().map(|g| g.id).collect())
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
            return Ok(Some((expr.group_id, expr.variant_tag, children)));
        }

        let expr = physical_expression::Entity::find()
            .filter(physical_expression::Column::Id.eq(expr_id))
            .one(&self.db)
            .await?;

        if let Some(expr) = expr {
            let children: Vec<i32> = serde_json::from_value(expr.data).unwrap();
            return Ok(Some((expr.group_id, expr.variant_tag, children)));
        }

        Ok(None)
    }

    async fn lookup_logical_expr(
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

    async fn lookup_physical_expr(
        &self,
        typ_id: i16,
        children: &Vec<i32>,
    ) -> StorageResult<Option<(i32, i32)>> {
        let children_json = serde_json::to_value(children).unwrap();
        let matches = physical_expression::Entity::find()
            .filter(physical_expression::Column::VariantTag.eq(typ_id))
            .filter(physical_expression::Column::Data.eq(children_json))
            .all(&self.db)
            .await?;

        if matches.is_empty() {
            return Ok(None);
        }

        // Only 1 should be identical, else something has gone wrong
        assert!(
            matches.len() <= 1,
            "Found duplicate expressions in the physical_expression table!"
        );

        let existing_expression = matches
            .last()
            .expect("we just checked that an element exists");

        Ok(Some((existing_expression.group_id, existing_expression.id)))
    }

    pub async fn lookup_expr(
        &self,
        typ_id: i16,
        is_logical: bool,
        children: &Vec<i32>,
    ) -> StorageResult<Option<(i32, i32)>> {
        if is_logical {
            self.lookup_logical_expr(typ_id, children).await
        } else {
            self.lookup_physical_expr(typ_id, children).await
        }
    }

    pub async fn insert_expr(
        &self,
        typ_id: i16,
        is_logical: bool,
        children: &Vec<i32>,
        add_to_group_id: Option<i32>,
    ) -> StorageResult<(i32, i32)> {
        let children_json = serde_json::to_value(children).unwrap();

        // assert no identical expressions exist
        debug_assert!(
            self.lookup_expr(typ_id, is_logical, children)
                .await?
                .is_none(),
            "Identical expression already exists!"
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
        if is_logical {
            let new_expr = logical_expression::ActiveModel {
                group_id: Set(group_id),
                variant_tag: Set(typ_id.try_into().unwrap()),
                data: Set(children_json),
                ..Default::default()
            };
            let new_expr = new_expr.insert(&self.db).await?;

            Ok((new_expr.group_id, new_expr.id))
        } else {
            let new_expr = physical_expression::ActiveModel {
                group_id: Set(group_id),
                variant_tag: Set(typ_id.try_into().unwrap()),
                data: Set(children_json),
                ..Default::default()
            };
            let new_expr = new_expr.insert(&self.db).await?;

            Ok((new_expr.group_id, new_expr.id))
        }
    }
}
