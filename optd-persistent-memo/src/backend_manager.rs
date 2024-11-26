use optd_core::{cascades::WinnerInfo, cost::Cost};
use optd_persistent::{
    entities::{
        cascades_group, group_winner, logical_children, logical_expression, physical_expression,
        plan_cost, predicate, predicate_children, predicate_logical_expression_junction,
        predicate_physical_expression_junction,
    },
    StorageResult,
};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, Database, DatabaseConnection, EntityTrait, PaginatorTrait,
    QueryFilter, Set,
};
use serde::{Deserialize, Serialize};

pub struct BackendGroupInfo {
    pub group_exprs: Vec<i32>,
    pub winner: Option<BackendWinnerInfo>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct BackendWinnerInfo {
    pub physical_expr_id: i32,
    pub total_weighted_cost: f64,
    pub operation_weighted_cost: f64,
    pub total_cost: Cost,
    pub operation_cost: Cost,
    // TODO: Statistics
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct PredicateData {
    pub children_ids: Vec<i32>,
    pub data: Option<optd_core::nodes::Value>,
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

    async fn get_winner(
        &self,
        winner_id: Option<i32>,
    ) -> StorageResult<Option<group_winner::Model>> {
        if let Some(winner_id) = winner_id {
            let winner = group_winner::Entity::find()
                .filter(group_winner::Column::Id.eq(winner_id))
                .one(&self.db)
                .await?;

            Ok(Some(winner.unwrap()))
        } else {
            Ok(None)
        }
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

        let winner = self.get_winner(group.latest_winner).await?;

        Ok(BackendGroupInfo {
            group_exprs: children,
            winner: winner.map(|winner| BackendWinnerInfo {
                physical_expr_id: winner.physical_expression_id,
                total_weighted_cost: 0.0, // TODO: Cost is not saved
                operation_weighted_cost: 0.0,
                total_cost: Cost(vec![]),
                operation_cost: Cost(vec![]),
            }),
        })
    }

    pub async fn update_winner(
        &self,
        group_id: i32,
        winner: Option<BackendWinnerInfo>,
    ) -> StorageResult<()> {
        // TODO: Do we need garbage collection?
        let new_winner = if let Some(winner) = winner {
            let cost = plan_cost::ActiveModel {
                physical_expression_id: Set(winner.physical_expr_id),
                ..Default::default()
            }; // TODO: how can we store cost??

            let cost_res = cost.insert(&self.db).await?;

            let winner = group_winner::ActiveModel {
                group_id: Set(group_id),
                physical_expression_id: Set(winner.physical_expr_id),
                cost_id: Set(cost_res.id),
                ..Default::default()
            };
            let winner_res = winner.insert(&self.db).await?;

            Some(winner_res.id)
        } else {
            None
        };

        // update
        let mut group_res: cascades_group::ActiveModel = cascades_group::Entity::find()
            .filter(cascades_group::Column::Id.eq(group_id))
            .one(&self.db)
            .await?
            .unwrap()
            .into();

        group_res.latest_winner = Set(new_winner);
        group_res.update(&self.db).await?;

        return Ok(());
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
        is_logical: bool,
    ) -> StorageResult<Option<(i32, i16, Vec<i32>)>> {
        if is_logical {
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
        } else {
            let expr = physical_expression::Entity::find()
                .filter(physical_expression::Column::Id.eq(expr_id))
                .one(&self.db)
                .await?;

            if let Some(expr) = expr {
                let children: Vec<i32> = serde_json::from_value(expr.data).unwrap();
                Ok(Some((expr.group_id, expr.variant_tag, children)))
            } else {
                Ok(None)
            }
        }
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

    /// Returns the predicate id.
    pub async fn insert_pred(&self, typ_id: i32, data: serde_json::Value) -> StorageResult<i32> {
        debug_assert!(
            self.lookup_pred(typ_id, data.clone()).await?.is_none(),
            "Identical predicate already exists!"
        );

        let new_pred = predicate::ActiveModel {
            data: Set(data),
            variant: Set(typ_id),
            ..Default::default()
        };
        Ok(new_pred.insert(&self.db).await?.id)
    }

    /// Returns the predicate id if the predicate already exist. Otherwise returns `None`.
    pub async fn lookup_pred(
        &self,
        typ_id: i32,
        data: serde_json::Value,
    ) -> StorageResult<Option<i32>> {
        let matches = predicate::Entity::find()
            .filter(predicate::Column::Variant.eq(typ_id))
            .filter(predicate::Column::Data.eq(data))
            .all(&self.db)
            .await?;

        if matches.is_empty() {
            return Ok(None);
        }

        assert_eq!(
            matches.len(),
            1,
            "Found duplicate entries stored in the predicate table"
        );

        let existing_pred = matches.last().expect("pred exist");
        Ok(Some(existing_pred.id))
    }

    /// Returns `data` and `typ_id`.
    pub async fn get_pred(&self, id: i32) -> StorageResult<Option<predicate::Model>> {
        let pred = predicate::Entity::find()
            .filter(predicate::Column::Id.eq(id))
            .one(&self.db)
            .await?;
        Ok(pred)
    }

    pub async fn insert_pred_children(&self, id: i32, children: &Vec<i32>) -> StorageResult<()> {
        let iter = children
            .iter()
            .map(|&child_id| predicate_children::ActiveModel {
                parent_id: Set(id),
                child_id: Set(child_id),
            });
        predicate_children::Entity::insert_many(iter)
            .exec(&self.db)
            .await?;
        Ok(())
    }

    pub async fn get_pred_children(&self, id: i32) -> StorageResult<Vec<Option<predicate::Model>>> {
        let ids = predicate_children::Entity::find()
            .filter(predicate_children::Column::ParentId.eq(id))
            .all(&self.db)
            .await?
            .into_iter()
            .map(|x| x.child_id)
            .collect::<Vec<_>>();

        let mut children = Vec::with_capacity(ids.len());
        for id in ids {
            let pred = self.get_pred(id).await?;
            children.push(pred);
        }
        Ok(children)
    }

    pub async fn link_logical_expr_to_predicates(
        &self,
        id: i32,
        predicates: &Vec<i32>,
    ) -> StorageResult<()> {
        let iter =
            predicates.iter().map(
                |&pred_id| predicate_logical_expression_junction::ActiveModel {
                    logical_expr_id: Set(id),
                    predicate_id: Set(pred_id),
                },
            );
        predicate_logical_expression_junction::Entity::insert_many(iter)
            .exec(&self.db)
            .await?;
        Ok(())
    }

    pub async fn link_physical_expr_to_predicates(
        &self,
        id: i32,
        predicates: &Vec<i32>,
    ) -> StorageResult<()> {
        let iter =
            predicates.iter().map(
                |&pred_id| predicate_physical_expression_junction::ActiveModel {
                    physical_expr_id: Set(id),
                    predicate_id: Set(pred_id),
                },
            );
        predicate_physical_expression_junction::Entity::insert_many(iter)
            .exec(&self.db)
            .await?;
        Ok(())
    }

    pub async fn get_pred_ids_in_expr(
        &self,
        expr_id: i32,
        is_logical: bool,
    ) -> StorageResult<Vec<i32>> {
        if is_logical {
            let preds = predicate_logical_expression_junction::Entity::find()
                .filter(predicate_logical_expression_junction::Column::LogicalExprId.eq(expr_id))
                .all(&self.db)
                .await?
                .iter()
                .map(|x| x.predicate_id)
                .collect();
            Ok(preds)
        } else {
            // The order of pred is troublesome (add index to each junction entry?).
            let preds = predicate_physical_expression_junction::Entity::find()
                .filter(predicate_physical_expression_junction::Column::PhysicalExprId.eq(expr_id))
                .all(&self.db)
                .await?
                .iter()
                .map(|x| x.predicate_id)
                .collect();
            Ok(preds)
        }
    }
}
