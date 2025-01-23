//! Common utilities for the storage models.

/// A declarative macro that defines a new ID type with the given name,
/// inner type, and SQL type. Also deriving some common traits
/// for the new type.
#[macro_export]
macro_rules! define_diesel_new_id_type_from_to_sql {
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
            diesel::AsExpression,
            diesel::FromSqlRow,
        )]
        #[diesel(sql_type = $sql_type)]
        pub struct $type_name(pub $inner_type);

        impl<DB> diesel::deserialize::FromSql<$sql_type, DB> for $type_name
        where
            DB: diesel::backend::Backend,
            $inner_type: diesel::deserialize::FromSql<$sql_type, DB>,
        {
            fn from_sql(
                bytes: <DB as diesel::backend::Backend>::RawValue<'_>,
            ) -> diesel::deserialize::Result<Self> {
                <$inner_type>::from_sql(bytes).map($type_name)
            }
        }

        impl diesel::serialize::ToSql<$sql_type, diesel::sqlite::Sqlite> for $type_name
        where
            $inner_type: diesel::serialize::ToSql<$sql_type, diesel::sqlite::Sqlite>,
        {
            fn to_sql<'b>(
                &'b self,
                out: &mut diesel::serialize::Output<'b, '_, diesel::sqlite::Sqlite>,
            ) -> diesel::serialize::Result {
                out.set_value(self.0);
                Ok(diesel::serialize::IsNull::No)
            }
        }
    };
}

/// The type of join to perform.
#[repr(i32)]
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Hash,
    diesel::AsExpression,
    diesel::deserialize::FromSqlRow,
)]
#[diesel(sql_type = diesel::sql_types::Integer)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    LeftSemi,
    RightSemi,
    LeftAnti,
    RightAnti,
    LeftMark,
}

impl TryFrom<i32> for JoinType {
    type Error = anyhow::Error;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        use JoinType::*;
        match value {
            x if x == Inner as i32 => Ok(Inner),
            x if x == Left as i32 => Ok(Left),
            x if x == Right as i32 => Ok(Right),
            x if x == Full as i32 => Ok(Full),
            x if x == LeftSemi as i32 => Ok(LeftSemi),
            x if x == RightSemi as i32 => Ok(LeftAnti),
            x if x == LeftAnti as i32 => Ok(LeftAnti),
            x if x == RightAnti as i32 => Ok(RightAnti),
            x if x == LeftMark as i32 => Ok(LeftMark),
            _ => anyhow::bail!("Invalid integer value for JoinType: {}", value),
        }
    }
}

impl<DB> diesel::deserialize::FromSql<diesel::sql_types::Integer, DB> for JoinType
where
    DB: diesel::backend::Backend,
    i32: diesel::deserialize::FromSql<diesel::sql_types::Integer, DB>,
{
    fn from_sql(
        bytes: <DB as diesel::backend::Backend>::RawValue<'_>,
    ) -> diesel::deserialize::Result<Self> {
        let status = i32::from_sql(bytes)?.try_into()?;
        Ok(status)
    }
}

impl diesel::serialize::ToSql<diesel::sql_types::Integer, diesel::sqlite::Sqlite> for JoinType
where
    i32: diesel::serialize::ToSql<diesel::sql_types::Integer, diesel::sqlite::Sqlite>,
{
    fn to_sql<'b>(
        &'b self,
        out: &mut diesel::serialize::Output<'b, '_, diesel::sqlite::Sqlite>,
    ) -> diesel::serialize::Result {
        out.set_value(*self as i32);
        Ok(diesel::serialize::IsNull::No)
    }
}
