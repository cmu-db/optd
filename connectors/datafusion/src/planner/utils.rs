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
            .get_column_by_name(table_ref.as_ref(), column_name)
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
            DFScalarValue::Boolean(v) => Ok(OptdScalarValue::Boolean(v)),
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
            DFScalarValue::Decimal32(v, p, s) => Ok(OptdScalarValue::Decimal32(v, p, s)),
            DFScalarValue::Decimal64(v, p, s) => Ok(OptdScalarValue::Decimal64(v, p, s)),
            DFScalarValue::Decimal128(v, p, s) => Ok(OptdScalarValue::Decimal128(v, p, s)),
            DFScalarValue::Date32(v) => Ok(OptdScalarValue::Date32(v)),
            DFScalarValue::Date64(v) => Ok(OptdScalarValue::Date64(v)),
            value => whatever!(
                "Conversion from DataFusion ScalarValue {:?} is not implemented",
                value
            ),
        }
    }

    /// Converts an optd `ScalarValue` to a DataFusion `ScalarValue``.
    pub fn from_optd_value(value: OptdScalarValue) -> DFScalarValue {
        match value {
            OptdScalarValue::Boolean(v) => DFScalarValue::Boolean(v),
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
            OptdScalarValue::Decimal32(v, p, s) => DFScalarValue::Decimal32(v, p, s),
            OptdScalarValue::Decimal64(v, p, s) => DFScalarValue::Decimal64(v, p, s),
            OptdScalarValue::Decimal128(v, p, s) => DFScalarValue::Decimal128(v, p, s),
            OptdScalarValue::Date32(v) => DFScalarValue::Date32(v),
            OptdScalarValue::Date64(v) => DFScalarValue::Date64(v),
        }
    }

    pub fn try_into_optd_join_type(join_type: DFJoinType) -> Result<OptdJoinType> {
        match join_type {
            DFJoinType::Inner => Ok(OptdJoinType::Inner),
            DFJoinType::Left => Ok(OptdJoinType::Left),
            v => whatever!("Unsupported join type: {}", v),
        }
    }

    pub fn try_from_optd_join_type(join_type: &OptdJoinType) -> Result<DFJoinType> {
        match join_type {
            OptdJoinType::Inner => Ok(DFJoinType::Inner),
            OptdJoinType::Left => Ok(DFJoinType::Left),
            // TODO: add single and mark join.
            v => whatever!("Unsupported join type: {:?}", v),
        }
    }
}
