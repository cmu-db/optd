use anyhow::bail;
use diesel::{
    backend::Backend,
    deserialize::{FromSql, FromSqlRow},
    expression::AsExpression,
    prelude::*,
    serialize::{IsNull, ToSql},
    sql_types::{BigInt, Integer},
};

#[derive(Queryable, Selectable)]
#[diesel(table_name = super::schema::rel_groups)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct RelGroup {
    /// The relational group identifier.
    pub id: RelGroupId,
    /// Optimization status of the group.
    pub status: RelGroupStatus,
    /// Timestamp at which the group was created.
    pub created_at: chrono::NaiveDateTime,
    /// The group identifier of the representative.
    pub rep_id: Option<RelGroupId>,
}

#[derive(Queryable, Selectable)]
#[diesel(table_name = super::schema::rel_group_winners)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct RelGroupWinner {
    /// The group we are interested in.
    pub group_id: RelGroupId,
    /// The required physical property.
    pub required_phys_prop_id: PhysicalPropId,
    /// The winner of the group with `group_id` and required physical property.
    pub physical_expr_id: PhysicalExprId,
}

#[derive(Queryable, Selectable)]
#[diesel(table_name = super::schema::logical_exprs)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct LogicalExpr {
    /// The logical expression identifier.
    pub id: LogicalExprId,
    /// The type descriptor of the logical expression.
    pub typ_desc: LogicalTypDescId,
    /// The relational group that this logical expression belongs to.
    pub group_id: RelGroupId,
    /// The time at which this logical expression was created.
    pub created_at: chrono::NaiveDateTime,
}

#[derive(Queryable, Selectable)]
#[diesel(table_name = super::schema::logical_props)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct LogicalProp {
    /// The logical property identifier.
    pub id: LogicalPropId,
    /// The relational group that shares this property.
    pub group_id: RelGroupId,
    /// The number of rows produced by this relation.
    pub card_est: i64,
}

#[derive(Queryable, Selectable)]
#[diesel(table_name = super::schema::logical_typ_descs)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct LogicalTypDesc {
    /// The logical type descriptor identifier.
    pub id: LogicalTypDescId,
    /// The name of the logical type.
    pub name: String,
}

#[derive(Queryable, Selectable)]
#[diesel(table_name = super::schema::physical_exprs)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct PhysicalExpr {
    /// The physical expression id.
    pub id: PhysicalExprId,
    /// The type descriptor of the physical expression.
    pub typ_desc: PhysicalTypDescId,
    /// The relational group that this physical expression belongs to.
    pub group_id: RelGroupId,
    /// The physical property dervied based on the properties of the children expressions.
    pub derived_phys_prop_id: PhysicalPropId,
    /// The cost associated with this physical expression.
    pub cost: f64,
    /// The time at which this physical expression was created.
    pub created_at: chrono::NaiveDateTime,
}

#[derive(Queryable, Selectable)]
#[diesel(table_name = super::schema::physical_props)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct PhysicalProp {
    /// The physical property id.
    pub id: PhysicalPropId,
    /// The opaquely stored payload.
    pub payload: Vec<u8>,
}

#[derive(Queryable, Selectable)]
#[diesel(table_name = super::schema::physical_typ_descs)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct PhysicalTypDesc {
    /// The physical type descriptor id.
    pub id: PhysicalTypDescId,
    /// The name of the physical type.
    pub name: String,
}

#[derive(Queryable, Selectable)]
#[diesel(table_name = super::schema::scalar_exprs)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct ScalarExpr {
    /// The scalar expression id.
    pub id: ScalarExprId,
    /// The type descriptor of the scalar expression.
    pub typ_desc: ScalarTypDescId,
    /// The scalar group that this scalar expression belongs to.
    pub group_id: ScalarGroupId,
    pub created_at: chrono::NaiveDateTime,
    pub cost: Option<f64>,
}

#[derive(Queryable, Selectable)]
#[diesel(table_name = super::schema::scalar_groups)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct ScalarGroup {
    pub id: ScalarGroupId,
    pub status: i32,
    pub created_at: chrono::NaiveDateTime,
    pub rep_id: Option<i64>,
}

#[derive(Queryable, Selectable)]
#[diesel(table_name = super::schema::scalar_group_winners)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct ScalarGroupWinner {
    pub group_id: ScalarGroupId,
    pub scalar_expr_id: ScalarExprId,
}

#[derive(Queryable, Selectable)]
#[diesel(table_name = super::schema::scalar_props)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct ScalarProp {
    /// The scalar property id.
    pub id: ScalarPropId,
    /// The opaquely stored payload.
    pub payload: Vec<u8>,
}

#[derive(Queryable, Selectable)]
#[diesel(table_name = super::schema::scalar_typ_descs)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct ScalarTyeDesc {
    /// The scalar type descriptor id.
    pub id: ScalarTypDescId,
    /// The name of the scalar type.
    pub name: String,
}

/// Defines a new ID type with the given name, inner type, and SQL type.
#[macro_export]
macro_rules! impl_diesel_new_type_from_to_sql {
    ($type_name:ident, $inner_type:ty, $sql_type:ty) => {
        #[derive(
            Clone,
            Copy,
            PartialEq,
            Eq,
            PartialOrd,
            Ord,
            Debug,
            Default,
            Hash,
            AsExpression,
            FromSqlRow,
        )]
        #[diesel(sql_type = $sql_type)]
        pub struct $type_name(pub $inner_type);

        impl<DB> FromSql<$sql_type, DB> for $type_name
        where
            DB: Backend,
            $inner_type: FromSql<$sql_type, DB>,
        {
            fn from_sql(bytes: <DB as Backend>::RawValue<'_>) -> diesel::deserialize::Result<Self> {
                <$inner_type>::from_sql(bytes).map($type_name)
            }
        }

        impl ToSql<$sql_type, diesel::sqlite::Sqlite> for $type_name
        where
            $inner_type: ToSql<$sql_type, diesel::sqlite::Sqlite>,
        {
            fn to_sql<'b>(
                &'b self,
                out: &mut diesel::serialize::Output<'b, '_, diesel::sqlite::Sqlite>,
            ) -> diesel::serialize::Result {
                out.set_value(self.0);
                Ok(IsNull::No)
            }
        }
    };
}

impl_diesel_new_type_from_to_sql!(RelGroupId, i64, BigInt);
impl_diesel_new_type_from_to_sql!(LogicalExprId, i64, BigInt);
impl_diesel_new_type_from_to_sql!(PhysicalExprId, i64, BigInt);
impl_diesel_new_type_from_to_sql!(LogicalPropId, i64, BigInt);
impl_diesel_new_type_from_to_sql!(PhysicalPropId, i64, BigInt);
impl_diesel_new_type_from_to_sql!(LogicalTypDescId, i64, BigInt);
impl_diesel_new_type_from_to_sql!(PhysicalTypDescId, i64, BigInt);
impl_diesel_new_type_from_to_sql!(ScalarGroupId, i64, BigInt);
impl_diesel_new_type_from_to_sql!(ScalarExprId, i64, BigInt);
impl_diesel_new_type_from_to_sql!(ScalarPropId, i64, BigInt);
impl_diesel_new_type_from_to_sql!(ScalarTypDescId, i64, BigInt);

#[repr(i32)]
#[derive(Debug, Clone, Copy, AsExpression, FromSqlRow)]
#[diesel(sql_type = Integer)]
pub enum RelGroupStatus {
    Unexplored = 1,
    Exploring,
    Explored,
    Optimizing,
    Optimized,
}

impl TryFrom<i32> for RelGroupStatus {
    type Error = anyhow::Error;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        use RelGroupStatus::*;
        match value {
            x if x == Unexplored as i32 => Ok(Unexplored),
            x if x == Exploring as i32 => Ok(Exploring),
            x if x == Explored as i32 => Ok(Explored),
            x if x == Optimizing as i32 => Ok(Optimizing),
            x if x == Optimized as i32 => Ok(Optimized),
            _ => bail!("Invalid integer value for RelGroupStatus: {}", value),
        }
    }
}

impl<DB> FromSql<Integer, DB> for RelGroupStatus
where
    DB: Backend,
    i32: FromSql<Integer, DB>,
{
    fn from_sql(bytes: <DB as Backend>::RawValue<'_>) -> diesel::deserialize::Result<Self> {
        let status = i32::from_sql(bytes)?.try_into()?;
        Ok(status)
    }
}

impl ToSql<Integer, diesel::sqlite::Sqlite> for RelGroupStatus
where
    i32: ToSql<Integer, diesel::sqlite::Sqlite>,
{
    fn to_sql<'b>(
        &'b self,
        out: &mut diesel::serialize::Output<'b, '_, diesel::sqlite::Sqlite>,
    ) -> diesel::serialize::Result {
        out.set_value(*self as i32);
        Ok(IsNull::No)
    }
}

#[repr(i32)]
#[derive(Debug, Clone, Copy, AsExpression, FromSqlRow)]
#[diesel(sql_type = Integer)]
pub enum ScalarGroupStatus {
    Unexplored = 1,
    Exploring,
    Explored,
    Optimizing,
    Optimized,
}

impl TryFrom<i32> for ScalarGroupStatus {
    type Error = anyhow::Error;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        use ScalarGroupStatus::*;
        match value {
            x if x == Unexplored as i32 => Ok(Unexplored),
            x if x == Exploring as i32 => Ok(Exploring),
            x if x == Explored as i32 => Ok(Explored),
            x if x == Optimizing as i32 => Ok(Optimizing),
            x if x == Optimized as i32 => Ok(Optimized),
            _ => bail!("Invalid integer value for ScalarGroupStatus: {}", value),
        }
    }
}

impl<DB> FromSql<Integer, DB> for ScalarGroupStatus
where
    DB: Backend,
    i32: FromSql<Integer, DB>,
{
    fn from_sql(bytes: <DB as Backend>::RawValue<'_>) -> diesel::deserialize::Result<Self> {
        let status = i32::from_sql(bytes)?.try_into()?;
        Ok(status)
    }
}

impl ToSql<Integer, diesel::sqlite::Sqlite> for ScalarGroupStatus
where
    i32: ToSql<Integer, diesel::sqlite::Sqlite>,
{
    fn to_sql<'b>(
        &'b self,
        out: &mut diesel::serialize::Output<'b, '_, diesel::sqlite::Sqlite>,
    ) -> diesel::serialize::Result {
        out.set_value(*self as i32);
        Ok(IsNull::No)
    }
}
