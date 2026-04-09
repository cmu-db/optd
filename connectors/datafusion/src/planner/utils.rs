use datafusion::{
    common::{Column as DFColumn, JoinType as DFJoinType},
    scalar::ScalarValue as DFScalarValue,
    sql::TableReference,
};
use optd_core::ir::{
    Column, ScalarValue as OptdScalarValue, operator::join::JoinType as OptdJoinType,
    table_ref::TableRef,
};
use snafu::{ResultExt, whatever};

use crate::planner::{OptdQueryPlannerContext, OptdSnafu, Result};

impl OptdQueryPlannerContext<'_> {
    pub fn into_optd_table_ref(table_ref: &TableReference) -> TableRef {
        match table_ref {
            TableReference::Bare { table } => TableRef::bare(table.clone()),
            TableReference::Partial { schema, table } => {
                TableRef::partial(schema.clone(), table.clone())
            }
            TableReference::Full {
                catalog,
                schema,
                table,
            } => TableRef::full(catalog.clone(), schema.clone(), table.clone()),
        }
    }

    pub fn from_optd_table_ref(table_ref: &TableRef) -> TableReference {
        match table_ref {
            TableRef::Bare { table } => TableReference::bare(table.clone()),
            TableRef::Partial { schema, table } => {
                TableReference::partial(schema.clone(), table.clone())
            }
            TableRef::Full {
                catalog,
                schema,
                table,
            } => TableReference::full(catalog.clone(), schema.clone(), table.clone()),
        }
    }

    pub fn try_get_optd_column(
        &self,
        table_ref: Option<&TableReference>,
        column_name: &str,
    ) -> Result<Column> {
        let table_ref = table_ref.map(Self::into_optd_table_ref);
        let column = self
            .inner
            .col(table_ref.as_ref(), column_name)
            .context(OptdSnafu)?;
        Ok(column)
    }

    pub fn try_from_optd_column(&self, column: &Column) -> Result<DFColumn> {
        let (table_ref, field) = self.inner.get_column_name(column).context(OptdSnafu)?;
        let table_reference = Self::from_optd_table_ref(&table_ref);
        let column = DFColumn::new(Some(table_reference), field.name());
        Ok(column)
    }

    pub fn try_into_optd_scalar_value(value: DFScalarValue) -> Result<OptdScalarValue> {
        match value {
            DFScalarValue::Null => Ok(OptdScalarValue::Null),
            DFScalarValue::Boolean(v) => Ok(OptdScalarValue::Boolean(v)),
            DFScalarValue::Float16(v) => Ok(OptdScalarValue::Float16(v)),
            DFScalarValue::Float32(v) => Ok(OptdScalarValue::Float32(v)),
            DFScalarValue::Float64(v) => Ok(OptdScalarValue::Float64(v)),
            DFScalarValue::Int8(v) => Ok(OptdScalarValue::Int8(v)),
            DFScalarValue::Int16(v) => Ok(OptdScalarValue::Int16(v)),
            DFScalarValue::Int32(v) => Ok(OptdScalarValue::Int32(v)),
            DFScalarValue::Int64(v) => Ok(OptdScalarValue::Int64(v)),
            DFScalarValue::UInt8(v) => Ok(OptdScalarValue::UInt8(v)),
            DFScalarValue::UInt16(v) => Ok(OptdScalarValue::UInt16(v)),
            DFScalarValue::UInt32(v) => Ok(OptdScalarValue::UInt32(v)),
            DFScalarValue::UInt64(v) => Ok(OptdScalarValue::UInt64(v)),
            DFScalarValue::Utf8(v) => Ok(OptdScalarValue::Utf8(v)),
            DFScalarValue::Utf8View(v) => Ok(OptdScalarValue::Utf8View(v)),
            DFScalarValue::LargeUtf8(v) => Ok(OptdScalarValue::LargeUtf8(v)),
            DFScalarValue::Binary(v) => Ok(OptdScalarValue::Binary(v)),
            DFScalarValue::BinaryView(v) => Ok(OptdScalarValue::BinaryView(v)),
            DFScalarValue::FixedSizeBinary(size, v) => Ok(OptdScalarValue::FixedSizeBinary(size, v)),
            DFScalarValue::LargeBinary(v) => Ok(OptdScalarValue::LargeBinary(v)),
            DFScalarValue::List(v) => Ok(OptdScalarValue::List(v)),
            DFScalarValue::Struct(v) => Ok(OptdScalarValue::Struct(v)),
            DFScalarValue::Map(v) => Ok(OptdScalarValue::Map(v)),
            DFScalarValue::Decimal32(v, p, s) => Ok(OptdScalarValue::Decimal32(v, p, s)),
            DFScalarValue::Decimal64(v, p, s) => Ok(OptdScalarValue::Decimal64(v, p, s)),
            DFScalarValue::Decimal128(v, p, s) => Ok(OptdScalarValue::Decimal128(v, p, s)),
            DFScalarValue::Decimal256(v, p, s) => Ok(OptdScalarValue::Decimal256(v, p, s)),
            DFScalarValue::Date32(v) => Ok(OptdScalarValue::Date32(v)),
            DFScalarValue::Date64(v) => Ok(OptdScalarValue::Date64(v)),
            DFScalarValue::Time32Second(v) => Ok(OptdScalarValue::Time32Second(v)),
            DFScalarValue::Time32Millisecond(v) => Ok(OptdScalarValue::Time32Millisecond(v)),
            DFScalarValue::Time64Microsecond(v) => Ok(OptdScalarValue::Time64Microsecond(v)),
            DFScalarValue::Time64Nanosecond(v) => Ok(OptdScalarValue::Time64Nanosecond(v)),
            DFScalarValue::TimestampSecond(v, tz) => Ok(OptdScalarValue::TimestampSecond(v, tz)),
            DFScalarValue::TimestampMillisecond(v, tz) => {
                Ok(OptdScalarValue::TimestampMillisecond(v, tz))
            }
            DFScalarValue::TimestampMicrosecond(v, tz) => {
                Ok(OptdScalarValue::TimestampMicrosecond(v, tz))
            }
            DFScalarValue::TimestampNanosecond(v, tz) => {
                Ok(OptdScalarValue::TimestampNanosecond(v, tz))
            }
            DFScalarValue::IntervalYearMonth(v) => Ok(OptdScalarValue::IntervalYearMonth(v)),
            DFScalarValue::IntervalDayTime(v) => Ok(OptdScalarValue::IntervalDayTime(v)),
            DFScalarValue::IntervalMonthDayNano(v) => {
                Ok(OptdScalarValue::IntervalMonthDayNano(v))
            }
            DFScalarValue::DurationSecond(v) => Ok(OptdScalarValue::DurationSecond(v)),
            DFScalarValue::DurationMillisecond(v) => {
                Ok(OptdScalarValue::DurationMillisecond(v))
            }
            DFScalarValue::DurationMicrosecond(v) => {
                Ok(OptdScalarValue::DurationMicrosecond(v))
            }
            DFScalarValue::DurationNanosecond(v) => Ok(OptdScalarValue::DurationNanosecond(v)),
            value => whatever!(
                "Conversion from DataFusion ScalarValue {:?} is not implemented",
                value
            ),
        }
    }

    /// Converts an optd `ScalarValue` to a DataFusion `ScalarValue``.
    pub fn from_optd_value(value: OptdScalarValue) -> DFScalarValue {
        match value {
            OptdScalarValue::Null => DFScalarValue::Null,
            OptdScalarValue::Boolean(v) => DFScalarValue::Boolean(v),
            OptdScalarValue::Float16(v) => DFScalarValue::Float16(v),
            OptdScalarValue::Float32(v) => DFScalarValue::Float32(v),
            OptdScalarValue::Float64(v) => DFScalarValue::Float64(v),
            OptdScalarValue::Int8(v) => DFScalarValue::Int8(v),
            OptdScalarValue::Int16(v) => DFScalarValue::Int16(v),
            OptdScalarValue::Int32(v) => DFScalarValue::Int32(v),
            OptdScalarValue::Int64(v) => DFScalarValue::Int64(v),
            OptdScalarValue::UInt8(v) => DFScalarValue::UInt8(v),
            OptdScalarValue::UInt16(v) => DFScalarValue::UInt16(v),
            OptdScalarValue::UInt32(v) => DFScalarValue::UInt32(v),
            OptdScalarValue::UInt64(v) => DFScalarValue::UInt64(v),
            OptdScalarValue::Utf8(v) => DFScalarValue::Utf8(v),
            OptdScalarValue::Utf8View(v) => DFScalarValue::Utf8View(v),
            OptdScalarValue::LargeUtf8(v) => DFScalarValue::LargeUtf8(v),
            OptdScalarValue::Binary(v) => DFScalarValue::Binary(v),
            OptdScalarValue::BinaryView(v) => DFScalarValue::BinaryView(v),
            OptdScalarValue::FixedSizeBinary(size, v) => DFScalarValue::FixedSizeBinary(size, v),
            OptdScalarValue::LargeBinary(v) => DFScalarValue::LargeBinary(v),
            OptdScalarValue::List(v) => DFScalarValue::List(v),
            OptdScalarValue::Struct(v) => DFScalarValue::Struct(v),
            OptdScalarValue::Map(v) => DFScalarValue::Map(v),
            OptdScalarValue::Decimal32(v, p, s) => DFScalarValue::Decimal32(v, p, s),
            OptdScalarValue::Decimal64(v, p, s) => DFScalarValue::Decimal64(v, p, s),
            OptdScalarValue::Decimal128(v, p, s) => DFScalarValue::Decimal128(v, p, s),
            OptdScalarValue::Decimal256(v, p, s) => DFScalarValue::Decimal256(v, p, s),
            OptdScalarValue::Date32(v) => DFScalarValue::Date32(v),
            OptdScalarValue::Date64(v) => DFScalarValue::Date64(v),
            OptdScalarValue::Time32Second(v) => DFScalarValue::Time32Second(v),
            OptdScalarValue::Time32Millisecond(v) => DFScalarValue::Time32Millisecond(v),
            OptdScalarValue::Time64Microsecond(v) => DFScalarValue::Time64Microsecond(v),
            OptdScalarValue::Time64Nanosecond(v) => DFScalarValue::Time64Nanosecond(v),
            OptdScalarValue::TimestampSecond(v, tz) => DFScalarValue::TimestampSecond(v, tz),
            OptdScalarValue::TimestampMillisecond(v, tz) => {
                DFScalarValue::TimestampMillisecond(v, tz)
            }
            OptdScalarValue::TimestampMicrosecond(v, tz) => {
                DFScalarValue::TimestampMicrosecond(v, tz)
            }
            OptdScalarValue::TimestampNanosecond(v, tz) => {
                DFScalarValue::TimestampNanosecond(v, tz)
            }
            OptdScalarValue::IntervalYearMonth(v) => DFScalarValue::IntervalYearMonth(v),
            OptdScalarValue::IntervalDayTime(v) => DFScalarValue::IntervalDayTime(v),
            OptdScalarValue::IntervalMonthDayNano(v) => DFScalarValue::IntervalMonthDayNano(v),
            OptdScalarValue::DurationSecond(v) => DFScalarValue::DurationSecond(v),
            OptdScalarValue::DurationMillisecond(v) => DFScalarValue::DurationMillisecond(v),
            OptdScalarValue::DurationMicrosecond(v) => DFScalarValue::DurationMicrosecond(v),
            OptdScalarValue::DurationNanosecond(v) => DFScalarValue::DurationNanosecond(v),
        }
    }

    pub fn try_into_optd_join_type(join_type: DFJoinType) -> Result<OptdJoinType> {
        match join_type {
            DFJoinType::Inner => Ok(OptdJoinType::Inner),
            DFJoinType::Left => Ok(OptdJoinType::LeftOuter),
            DFJoinType::LeftSemi => Ok(OptdJoinType::LeftSemi),
            DFJoinType::LeftAnti => Ok(OptdJoinType::LeftAnti),
            v => whatever!("Unsupported join type: {}", v),
        }
    }

    pub fn try_from_optd_join_type(join_type: &OptdJoinType) -> Result<DFJoinType> {
        match join_type {
            OptdJoinType::Inner => Ok(DFJoinType::Inner),
            OptdJoinType::LeftOuter => Ok(DFJoinType::Left),
            OptdJoinType::LeftSemi => Ok(DFJoinType::LeftSemi),
            OptdJoinType::LeftAnti => Ok(DFJoinType::LeftAnti),
            // TODO: add single and mark join.
            v => whatever!("Unsupported join type: {:?}", v),
        }
    }
}
