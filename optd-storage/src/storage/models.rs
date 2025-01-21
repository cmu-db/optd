use anyhow::bail;
use diesel::{
    backend::Backend,
    deserialize::{FromSql, FromSqlRow},
    expression::AsExpression,
    prelude::*,
    serialize::{IsNull, ToSql},
    sql_types::{BigInt, Integer},
};

/// A relational group contains one or more equivalent logical expressions
/// and zero or more physical expressions.
#[derive(Queryable, Selectable, Identifiable, AsChangeset)]
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

/// A relational subgroup contains a subset of physical expressions in a relational group that
/// can satisfies the same required physical properties.
#[derive(Queryable, Selectable, Identifiable, AsChangeset)]
#[diesel(table_name = super::schema::rel_subgroups)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct RelSubGroup {
    pub id: RelSubGroupId,
    /// The group the subgroup belongs to.
    pub group_id: RelGroupId,
    /// The required physical property of the subgroup.
    pub required_phys_prop_id: PhysicalPropId,
}

/// A subgroup winner is a physical expression that is the winner of a group with a required physical property.
#[derive(Queryable, Selectable, Identifiable, AsChangeset)]
#[diesel(table_name = super::schema::rel_subgroup_winners)]
#[diesel(belongs_to(RelSubGroup))]
#[diesel(belongs_to(PhysicalExpr))]
#[diesel(primary_key(subgroup_id))]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct RelSubgroupWinner {
    /// The subgroup id of the winner, i.e. the winner of the group with `group_id` and some required physical property.
    pub subgroup_id: RelSubGroupId,
    /// The physical expression id of the winner.
    pub physical_expr_id: PhysicalExprId,
}

/// A logical expression is a relational expression that consists of a tree of logical operators.
#[derive(Queryable, Selectable, Identifiable, AsChangeset)]
#[diesel(table_name = super::schema::logical_exprs)]
#[diesel(belongs_to(RelGroup))]
#[diesel(belongs_to(LogicalTypDesc))]
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

/// Logicial properties are shared by a relational group.
#[derive(Queryable, Selectable, Identifiable, AsChangeset)]
#[diesel(table_name = super::schema::logical_props)]
#[diesel(belongs_to(RelGroup))]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct LogicalProp {
    /// The logical property identifier.
    pub id: LogicalPropId,
    /// The relational group that shares this property.
    pub group_id: RelGroupId,
    /// The number of rows produced by this relation.
    pub card_est: i64,
}

/// Descriptor for a logical relational operator type.
#[derive(Queryable, Selectable, Identifiable, AsChangeset)]
#[diesel(table_name = super::schema::logical_typ_descs)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct LogicalTypDesc {
    /// The logical type descriptor identifier.
    pub id: LogicalTypDescId,
    /// The name of the logical type.
    pub name: String,
}

#[derive(Queryable, Selectable, Identifiable, AsChangeset)]
#[diesel(table_name = super::schema::physical_exprs)]
#[diesel(belongs_to(RelGroup))]
#[diesel(belongs_to(PhysicalTypDesc))]
#[diesel(belongs_to(PhysicalProp))]
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

// TODO(yuchen): Do we need a junction table for (logical_expr, required_phys_prop) <=> subgroup? TBD.
/// A relational subgroup expression entry specifies if a physical expression belongs to a subgroup.
/// It is a m:n relationship since a subgroup can have multiple physical expressions,
/// and a physical expression can belong to multiple subgroups.
#[derive(Queryable, Selectable, Identifiable, Associations)]
#[diesel(table_name = super::schema::rel_subgroup_physical_exprs)]
#[diesel(primary_key(subgroup_id, physical_expr_id))]
#[diesel(belongs_to(RelSubGroup, foreign_key = subgroup_id))]
#[diesel(belongs_to(PhysicalExpr))]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct RelSubGroupPhysicalExpr {
    /// The subgroup the physical expression belongs to.
    pub subgroup_id: RelSubGroupId,
    /// TThe physical expression id.
    pub physical_expr_id: PhysicalExprId,
}

/// A physical property is a characteristic of an expression that impacts its layout,
/// presentation, or location, but not its logical content.
/// They could be either required by a subgroup or derived on a physical expression.
#[derive(Queryable, Selectable, Identifiable, AsChangeset)]
#[diesel(table_name = super::schema::physical_props)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct PhysicalProp {
    /// The physical property id.
    pub id: PhysicalPropId,
    /// The opaquely stored payload.
    // TODO(yuchen): Instead, probably could do something simliar to
    // the relational and scalar expression "inheritance" pattern.
    pub payload: Vec<u8>,
}

/// Descriptor for a physical relational operator type.
#[derive(Queryable, Selectable, Identifiable, AsChangeset)]
#[diesel(table_name = super::schema::physical_typ_descs)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct PhysicalTypDesc {
    /// The physical type descriptor id.
    pub id: PhysicalTypDescId,
    /// The name of the physical type.
    pub name: String,
}

// TODO: ideally you want scalar to mimic the relational expressions. We don't have a definition of a physical scalar expression yet.
/// A scalar expression consists of a tree of scalar operators.
#[derive(Queryable, Selectable, Identifiable, AsChangeset)]
#[diesel(table_name = super::schema::scalar_exprs)]
#[diesel(belongs_to(ScalarGroup))]
#[diesel(belongs_to(ScalarTyeDesc))]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct ScalarExpr {
    /// The scalar expression id.
    pub id: ScalarExprId,
    /// The type descriptor of the scalar expression.
    pub typ_desc: ScalarTypDescId,
    /// The scalar group that this scalar expression belongs to.
    pub group_id: ScalarGroupId,
    /// The time at which this scalar expression was created.
    pub created_at: chrono::NaiveDateTime,
    /// The cost associated with this scalar expression. None if the cost has not been computed.
    pub cost: Option<f64>,
}

/// A scalar group contains one or more equivalent scalar expressions.
#[derive(Queryable, Selectable, Identifiable, AsChangeset)]
#[diesel(table_name = super::schema::scalar_groups)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct ScalarGroup {
    pub id: ScalarGroupId,
    pub status: i32,
    pub created_at: chrono::NaiveDateTime,
    pub rep_id: Option<ScalarGroupId>,
}

/// A scalar group winner is a scalar expression with the lowest cost in a scalar group.
#[derive(Queryable, Selectable, Identifiable, AsChangeset)]
#[diesel(table_name = super::schema::scalar_group_winners)]
#[diesel(primary_key(group_id))]
#[diesel(belongs_to(ScalarGroup))]
#[diesel(belongs_to(ScalarExpr))]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct ScalarGroupWinner {
    pub group_id: ScalarGroupId,
    pub scalar_expr_id: ScalarExprId,
}

/// A scalar property is a property shared by a scalar group.
#[derive(Queryable, Selectable, Identifiable, AsChangeset)]
#[diesel(table_name = super::schema::scalar_props)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct ScalarProp {
    /// The scalar property id.
    pub id: ScalarPropId,
    /// The opaquely stored payload.
    // TODO(yuchen): Instead, probably could do something simliar to
    // the relational and scalar expression "inheritance" pattern.
    pub payload: Vec<u8>,
}

/// Descriptor for a scalar type.
#[derive(Queryable, Selectable, Identifiable, AsChangeset)]
#[diesel(table_name = super::schema::scalar_typ_descs)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct ScalarTyeDesc {
    /// The scalar type descriptor id.
    pub id: ScalarTypDescId,
    /// The name of the scalar type.
    pub name: String,
}

/// Defines a new ID type with the given name, inner type, and SQL type.
/// Also deriving some common traits for the new type.
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
impl_diesel_new_type_from_to_sql!(RelSubGroupId, i64, BigInt);
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
