use diesel::prelude::*;

#[derive(Queryable, Selectable)]
#[diesel(table_name = super::schema::logical_exprs)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct LogicalExpr {
    pub id: i64,
    pub typ_desc: i64,
    pub group_id: i64,
    pub created_at: chrono::NaiveDateTime,
}

#[derive(Queryable, Selectable)]
#[diesel(table_name = super::schema::logical_props)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct LogicalProp {
    pub id: i64,
    pub group_id: i64,
    pub card_est: i64,
}

#[derive(Queryable, Selectable)]
#[diesel(table_name = super::schema::logical_typ_descs)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct LogicalTypDesc {
    pub id: i64,
    pub name: String,
}

#[derive(Queryable, Selectable)]
#[diesel(table_name = super::schema::physical_exprs)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct PhysicalExpr {
    pub id: i64,
    pub typ_desc: i64,
    pub group_id: i64,
    pub derived_phys_prop_id: i64,
    pub cost: f64,
    pub created_at: chrono::NaiveDateTime,
}

#[derive(Queryable, Selectable)]
#[diesel(table_name = super::schema::physical_props)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct PhysicalProp {
    pub id: i64,
    pub payload: Vec<u8>,
}

#[derive(Queryable, Selectable)]
#[diesel(table_name = super::schema::physical_typ_descs)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct PhysicalTypDesc {
    pub id: i64,
    pub name: String,
}

#[derive(Queryable, Selectable)]
#[diesel(table_name = super::schema::rel_groups)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct RelGroup {
    pub id: i64,
    pub status: i32,
    pub created_at: chrono::NaiveDateTime,
    pub rep_id: Option<i64>,
}

#[derive(Queryable, Selectable)]
#[diesel(table_name = super::schema::rel_group_winners)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct RelGroupWinner {
    pub group_id: i64,
    pub required_phys_prop_id: i64,
    pub physical_expr_id: i64,
}

#[derive(Queryable, Selectable)]
#[diesel(table_name = super::schema::scalar_exprs)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct ScalarExpr {
    pub id: i64,
    pub typ_desc: i64,
    pub group_id: i64,
    pub created_at: chrono::NaiveDateTime,
    pub cost: Option<f64>,
}

#[derive(Queryable, Selectable)]
#[diesel(table_name = super::schema::scalar_groups)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct ScalarGroup {
    pub id: i64,
    pub status: i32,
    pub created_at: chrono::NaiveDateTime,
    pub rep_id: Option<i64>,
}

#[derive(Queryable, Selectable)]
#[diesel(table_name = super::schema::scalar_group_winners)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct ScalarGroupWinner {
    pub group_id: i64,
    pub scalar_expr_id: i64,
}

#[derive(Queryable, Selectable)]
#[diesel(table_name = super::schema::scalar_props)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct ScalarProp {
    pub id: i64,
    pub payload: Vec<u8>,
}

#[derive(Queryable, Selectable)]
#[diesel(table_name = super::schema::scalar_typ_descs)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct ScalarTyeDesc {
    pub id: i64,
    pub name: String,
}
