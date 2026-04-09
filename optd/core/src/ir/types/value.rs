//! A module for representing nullable scalar values in the IR.

use half::f16;
use snafu::ResultExt;
use std::{
    convert::Infallible,
    hash::{Hash, Hasher},
    str::FromStr,
    sync::Arc,
};

use arrow::{
    array::{
        new_null_array, Array, ArrayRef, BinaryArray, BinaryViewArray, BinaryViewBuilder,
        BooleanArray, Date32Array, Date64Array, Decimal32Array, Decimal64Array,
        Decimal128Array, Decimal256Array, DurationMicrosecondArray,
        DurationMillisecondArray, DurationNanosecondArray, DurationSecondArray,
        FixedSizeBinaryArray, FixedSizeBinaryBuilder, Float16Array, Float32Array, Float64Array,
        Int8Array, Int16Array, Int32Array, Int64Array, IntervalDayTimeArray,
        IntervalMonthDayNanoArray, IntervalYearMonthArray, LargeBinaryArray, LargeStringArray,
        ListArray, MapArray, StringArray, StringViewArray, StringViewBuilder, StructArray,
        Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray,
        Time64NanosecondArray, TimestampMicrosecondArray, TimestampMillisecondArray,
        TimestampNanosecondArray, TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array,
        UInt64Array,
    },
    compute::kernels::cast::{cast_with_options, CastOptions},
    datatypes::{i256, IntervalDayTime, IntervalMonthDayNano, IntervalUnit, TimeUnit},
    util::display::{array_value_to_string, DurationFormat, FormatOptions},
};

use crate::{
    error::{whatever, Result},
    ir::DataType,
};

const DEFAULT_FORMAT_OPTIONS: FormatOptions<'static> =
    FormatOptions::new().with_duration_format(DurationFormat::Pretty);

const DEFAULT_CAST_OPTIONS: CastOptions<'static> = CastOptions {
    safe: false,
    format_options: DEFAULT_FORMAT_OPTIONS,
};

#[derive(Debug, Clone)]
pub enum ScalarValue {
    /// Represents `DataType::Null`.
    Null,
    /// True or false.
    Boolean(Option<bool>),
    /// 16-bit float.
    Float16(Option<f16>),
    /// 32-bit float.
    Float32(Option<f32>),
    /// 64-bit float.
    Float64(Option<f64>),
    /// Signed 8-bit integer.
    Int8(Option<i8>),
    /// Signed 16-bit integer.
    Int16(Option<i16>),
    /// Signed 32-bit integer.
    Int32(Option<i32>),
    /// Signed 64-bit integer.
    Int64(Option<i64>),
    /// Unsigned 8-bit integer.
    UInt8(Option<u8>),
    /// Unsigned 16-bit integer.
    UInt16(Option<u16>),
    /// Unsigned 32-bit integer.
    UInt32(Option<u32>),
    /// Unsigned 64-bit integer.
    UInt64(Option<u64>),
    /// UTF-8 encoded string.
    Utf8(Option<String>),
    /// UTF-8 encoded string view.
    Utf8View(Option<String>),
    /// Large UTF-8 encoded string.
    LargeUtf8(Option<String>),
    /// Variable-width binary.
    Binary(Option<Vec<u8>>),
    /// Variable-width binary view.
    BinaryView(Option<Vec<u8>>),
    /// Fixed-width binary.
    FixedSizeBinary(i32, Option<Vec<u8>>),
    /// Large variable-width binary.
    LargeBinary(Option<Vec<u8>>),
    /// List scalar represented as a single-row [`ListArray`].
    List(Arc<ListArray>),
    /// Struct scalar represented as a single-row [`StructArray`].
    Struct(Arc<StructArray>),
    /// Map scalar represented as a single-row [`MapArray`].
    Map(Arc<MapArray>),
    /// Date stored as a signed 32-bit int days since UNIX epoch 1970-01-01.
    Date32(Option<i32>),
    /// Date stored as a signed 64-bit int milliseconds since UNIX epoch 1970-01-01.
    Date64(Option<i64>),
    /// Time stored as signed 32-bit int seconds since midnight.
    Time32Second(Option<i32>),
    /// Time stored as signed 32-bit int milliseconds since midnight.
    Time32Millisecond(Option<i32>),
    /// Time stored as signed 64-bit int microseconds since midnight.
    Time64Microsecond(Option<i64>),
    /// Time stored as signed 64-bit int nanoseconds since midnight.
    Time64Nanosecond(Option<i64>),
    /// Timestamp stored as seconds since UNIX epoch 1970-01-01, with optional timezone.
    TimestampSecond(Option<i64>, Option<Arc<str>>),
    /// Timestamp stored as milliseconds since UNIX epoch 1970-01-01, with optional timezone.
    TimestampMillisecond(Option<i64>, Option<Arc<str>>),
    /// Timestamp stored as microseconds since UNIX epoch 1970-01-01, with optional timezone.
    TimestampMicrosecond(Option<i64>, Option<Arc<str>>),
    /// Timestamp stored as nanoseconds since UNIX epoch 1970-01-01, with optional timezone.
    TimestampNanosecond(Option<i64>, Option<Arc<str>>),
    /// Interval stored as year-month count.
    IntervalYearMonth(Option<i32>),
    /// Interval stored as day-time pair.
    IntervalDayTime(Option<IntervalDayTime>),
    /// Interval stored as month-day-nanosecond triple.
    IntervalMonthDayNano(Option<IntervalMonthDayNano>),
    /// Duration in seconds.
    DurationSecond(Option<i64>),
    /// Duration in milliseconds.
    DurationMillisecond(Option<i64>),
    /// Duration in microseconds.
    DurationMicrosecond(Option<i64>),
    /// Duration in nanoseconds.
    DurationNanosecond(Option<i64>),
    /// 32-bit decimal, using the i32 to represent the decimal, precision, scale.
    Decimal32(Option<i32>, u8, i8),
    /// 64-bit decimal, using the i64 to represent the decimal, precision, scale.
    Decimal64(Option<i64>, u8, i8),
    /// 128-bit decimal, using the i128 to represent the decimal, precision, scale.
    Decimal128(Option<i128>, u8, i8),
    /// 256-bit decimal, using the i256 to represent the decimal, precision, scale.
    Decimal256(Option<i256>, u8, i8),
}

impl ScalarValue {
    pub fn is_null(&self) -> bool {
        match self {
            ScalarValue::Null => true,
            ScalarValue::Boolean(v) => v.is_none(),
            ScalarValue::Float16(v) => v.is_none(),
            ScalarValue::Float32(v) => v.is_none(),
            ScalarValue::Float64(v) => v.is_none(),
            ScalarValue::Int8(v) => v.is_none(),
            ScalarValue::Int16(v) => v.is_none(),
            ScalarValue::Int32(v) => v.is_none(),
            ScalarValue::Int64(v) => v.is_none(),
            ScalarValue::UInt8(v) => v.is_none(),
            ScalarValue::UInt16(v) => v.is_none(),
            ScalarValue::UInt32(v) => v.is_none(),
            ScalarValue::UInt64(v) => v.is_none(),
            ScalarValue::Utf8(v) => v.is_none(),
            ScalarValue::Utf8View(v) => v.is_none(),
            ScalarValue::LargeUtf8(v) => v.is_none(),
            ScalarValue::Binary(v) => v.is_none(),
            ScalarValue::BinaryView(v) => v.is_none(),
            ScalarValue::FixedSizeBinary(_, v) => v.is_none(),
            ScalarValue::LargeBinary(v) => v.is_none(),
            ScalarValue::List(v) => v.is_null(0),
            ScalarValue::Struct(v) => v.is_null(0),
            ScalarValue::Map(v) => v.is_null(0),
            ScalarValue::Date32(v) => v.is_none(),
            ScalarValue::Date64(v) => v.is_none(),
            ScalarValue::Time32Second(v) => v.is_none(),
            ScalarValue::Time32Millisecond(v) => v.is_none(),
            ScalarValue::Time64Microsecond(v) => v.is_none(),
            ScalarValue::Time64Nanosecond(v) => v.is_none(),
            ScalarValue::TimestampSecond(v, _) => v.is_none(),
            ScalarValue::TimestampMillisecond(v, _) => v.is_none(),
            ScalarValue::TimestampMicrosecond(v, _) => v.is_none(),
            ScalarValue::TimestampNanosecond(v, _) => v.is_none(),
            ScalarValue::IntervalYearMonth(v) => v.is_none(),
            ScalarValue::IntervalDayTime(v) => v.is_none(),
            ScalarValue::IntervalMonthDayNano(v) => v.is_none(),
            ScalarValue::DurationSecond(v) => v.is_none(),
            ScalarValue::DurationMillisecond(v) => v.is_none(),
            ScalarValue::DurationMicrosecond(v) => v.is_none(),
            ScalarValue::DurationNanosecond(v) => v.is_none(),
            ScalarValue::Decimal32(v, _, _) => v.is_none(),
            ScalarValue::Decimal64(v, _, _) => v.is_none(),
            ScalarValue::Decimal128(v, _, _) => v.is_none(),
            ScalarValue::Decimal256(v, _, _) => v.is_none(),
        }
    }

    pub fn data_type(&self) -> DataType {
        match self {
            ScalarValue::Null => DataType::Null,
            ScalarValue::Boolean(_) => DataType::Boolean,
            ScalarValue::Float16(_) => DataType::Float16,
            ScalarValue::Float32(_) => DataType::Float32,
            ScalarValue::Float64(_) => DataType::Float64,
            ScalarValue::Int8(_) => DataType::Int8,
            ScalarValue::Int16(_) => DataType::Int16,
            ScalarValue::Int32(_) => DataType::Int32,
            ScalarValue::Int64(_) => DataType::Int64,
            ScalarValue::UInt8(_) => DataType::UInt8,
            ScalarValue::UInt16(_) => DataType::UInt16,
            ScalarValue::UInt32(_) => DataType::UInt32,
            ScalarValue::UInt64(_) => DataType::UInt64,
            ScalarValue::Utf8(_) => DataType::Utf8,
            ScalarValue::Utf8View(_) => DataType::Utf8View,
            ScalarValue::LargeUtf8(_) => DataType::LargeUtf8,
            ScalarValue::Binary(_) => DataType::Binary,
            ScalarValue::BinaryView(_) => DataType::BinaryView,
            ScalarValue::FixedSizeBinary(size, _) => DataType::FixedSizeBinary(*size),
            ScalarValue::LargeBinary(_) => DataType::LargeBinary,
            ScalarValue::List(array) => array.data_type().clone(),
            ScalarValue::Struct(array) => array.data_type().clone(),
            ScalarValue::Map(array) => array.data_type().clone(),
            ScalarValue::Date32(_) => DataType::Date32,
            ScalarValue::Date64(_) => DataType::Date64,
            ScalarValue::Time32Second(_) => DataType::Time32(TimeUnit::Second),
            ScalarValue::Time32Millisecond(_) => DataType::Time32(TimeUnit::Millisecond),
            ScalarValue::Time64Microsecond(_) => DataType::Time64(TimeUnit::Microsecond),
            ScalarValue::Time64Nanosecond(_) => DataType::Time64(TimeUnit::Nanosecond),
            ScalarValue::TimestampSecond(_, tz) => DataType::Timestamp(TimeUnit::Second, tz.clone()),
            ScalarValue::TimestampMillisecond(_, tz) => {
                DataType::Timestamp(TimeUnit::Millisecond, tz.clone())
            }
            ScalarValue::TimestampMicrosecond(_, tz) => {
                DataType::Timestamp(TimeUnit::Microsecond, tz.clone())
            }
            ScalarValue::TimestampNanosecond(_, tz) => {
                DataType::Timestamp(TimeUnit::Nanosecond, tz.clone())
            }
            ScalarValue::IntervalYearMonth(_) => DataType::Interval(IntervalUnit::YearMonth),
            ScalarValue::IntervalDayTime(_) => DataType::Interval(IntervalUnit::DayTime),
            ScalarValue::IntervalMonthDayNano(_) => {
                DataType::Interval(IntervalUnit::MonthDayNano)
            }
            ScalarValue::DurationSecond(_) => DataType::Duration(TimeUnit::Second),
            ScalarValue::DurationMillisecond(_) => DataType::Duration(TimeUnit::Millisecond),
            ScalarValue::DurationMicrosecond(_) => DataType::Duration(TimeUnit::Microsecond),
            ScalarValue::DurationNanosecond(_) => DataType::Duration(TimeUnit::Nanosecond),
            ScalarValue::Decimal32(_, precision, scale) => DataType::Decimal32(*precision, *scale),
            ScalarValue::Decimal64(_, precision, scale) => DataType::Decimal64(*precision, *scale),
            ScalarValue::Decimal128(_, precision, scale) => {
                DataType::Decimal128(*precision, *scale)
            }
            ScalarValue::Decimal256(_, precision, scale) => {
                DataType::Decimal256(*precision, *scale)
            }
        }
    }

    fn to_array(&self) -> Result<ArrayRef> {
        let array: ArrayRef = match self {
            ScalarValue::Null => new_null_array(&DataType::Null, 1),
            ScalarValue::Boolean(value) => Arc::new(BooleanArray::from(vec![*value])),
            ScalarValue::Float16(value) => Arc::new(Float16Array::from(vec![*value])),
            ScalarValue::Float32(value) => Arc::new(Float32Array::from(vec![*value])),
            ScalarValue::Float64(value) => Arc::new(Float64Array::from(vec![*value])),
            ScalarValue::Int8(value) => Arc::new(Int8Array::from(vec![*value])),
            ScalarValue::Int16(value) => Arc::new(Int16Array::from(vec![*value])),
            ScalarValue::Int32(value) => Arc::new(Int32Array::from(vec![*value])),
            ScalarValue::Int64(value) => Arc::new(Int64Array::from(vec![*value])),
            ScalarValue::UInt8(value) => Arc::new(UInt8Array::from(vec![*value])),
            ScalarValue::UInt16(value) => Arc::new(UInt16Array::from(vec![*value])),
            ScalarValue::UInt32(value) => Arc::new(UInt32Array::from(vec![*value])),
            ScalarValue::UInt64(value) => Arc::new(UInt64Array::from(vec![*value])),
            ScalarValue::Utf8(value) => match value {
                Some(value) => Arc::new(StringArray::from_iter_values([value.as_str()])),
                None => new_null_array(&DataType::Utf8, 1),
            },
            ScalarValue::Utf8View(value) => {
                let mut builder = StringViewBuilder::with_capacity(1);
                match value {
                    Some(value) => builder.append_value(value),
                    None => builder.append_null(),
                }
                Arc::new(builder.finish())
            }
            ScalarValue::LargeUtf8(value) => match value {
                Some(value) => Arc::new(LargeStringArray::from_iter_values([value.as_str()])),
                None => new_null_array(&DataType::LargeUtf8, 1),
            },
            ScalarValue::Binary(value) => match value {
                Some(value) => Arc::new(BinaryArray::from_iter_values([value.as_slice()])),
                None => new_null_array(&DataType::Binary, 1),
            },
            ScalarValue::BinaryView(value) => {
                let mut builder = BinaryViewBuilder::with_capacity(1);
                match value {
                    Some(value) => builder.append_value(value),
                    None => builder.append_null(),
                }
                Arc::new(builder.finish())
            }
            ScalarValue::FixedSizeBinary(size, value) => {
                let mut builder = FixedSizeBinaryBuilder::new(*size);
                match value {
                    Some(value) => whatever!(
                        builder.append_value(value),
                        "invalid fixed-size binary scalar size={size}"
                    ),
                    None => builder.append_null(),
                }
                Arc::new(builder.finish())
            }
            ScalarValue::LargeBinary(value) => match value {
                Some(value) => Arc::new(LargeBinaryArray::from_iter_values([value.as_slice()])),
                None => new_null_array(&DataType::LargeBinary, 1),
            },
            ScalarValue::List(array) => array.clone() as ArrayRef,
            ScalarValue::Struct(array) => array.clone() as ArrayRef,
            ScalarValue::Map(array) => array.clone() as ArrayRef,
            ScalarValue::Date32(value) => Arc::new(Date32Array::from(vec![*value])),
            ScalarValue::Date64(value) => Arc::new(Date64Array::from(vec![*value])),
            ScalarValue::Time32Second(value) => Arc::new(Time32SecondArray::from(vec![*value])),
            ScalarValue::Time32Millisecond(value) => {
                Arc::new(Time32MillisecondArray::from(vec![*value]))
            }
            ScalarValue::Time64Microsecond(value) => {
                Arc::new(Time64MicrosecondArray::from(vec![*value]))
            }
            ScalarValue::Time64Nanosecond(value) => {
                Arc::new(Time64NanosecondArray::from(vec![*value]))
            }
            ScalarValue::TimestampSecond(value, tz) => {
                Arc::new(TimestampSecondArray::from(vec![*value]).with_timezone_opt(tz.clone()))
            }
            ScalarValue::TimestampMillisecond(value, tz) => Arc::new(
                TimestampMillisecondArray::from(vec![*value]).with_timezone_opt(tz.clone()),
            ),
            ScalarValue::TimestampMicrosecond(value, tz) => Arc::new(
                TimestampMicrosecondArray::from(vec![*value]).with_timezone_opt(tz.clone()),
            ),
            ScalarValue::TimestampNanosecond(value, tz) => Arc::new(
                TimestampNanosecondArray::from(vec![*value]).with_timezone_opt(tz.clone()),
            ),
            ScalarValue::IntervalYearMonth(value) => {
                Arc::new(IntervalYearMonthArray::from(vec![*value]))
            }
            ScalarValue::IntervalDayTime(value) => {
                Arc::new(IntervalDayTimeArray::from(vec![*value]))
            }
            ScalarValue::IntervalMonthDayNano(value) => {
                Arc::new(IntervalMonthDayNanoArray::from(vec![*value]))
            }
            ScalarValue::DurationSecond(value) => Arc::new(DurationSecondArray::from(vec![*value])),
            ScalarValue::DurationMillisecond(value) => {
                Arc::new(DurationMillisecondArray::from(vec![*value]))
            }
            ScalarValue::DurationMicrosecond(value) => {
                Arc::new(DurationMicrosecondArray::from(vec![*value]))
            }
            ScalarValue::DurationNanosecond(value) => {
                Arc::new(DurationNanosecondArray::from(vec![*value]))
            }
            ScalarValue::Decimal32(value, precision, scale) => {
                let array = whatever!(
                    Decimal32Array::from(vec![*value]).with_precision_and_scale(*precision, *scale),
                    "invalid decimal32 scalar metadata precision={precision}, scale={scale}"
                );
                Arc::new(array)
            }
            ScalarValue::Decimal64(value, precision, scale) => {
                let array = whatever!(
                    Decimal64Array::from(vec![*value]).with_precision_and_scale(*precision, *scale),
                    "invalid decimal64 scalar metadata precision={precision}, scale={scale}"
                );
                Arc::new(array)
            }
            ScalarValue::Decimal128(value, precision, scale) => {
                let array = whatever!(
                    Decimal128Array::from(vec![*value])
                        .with_precision_and_scale(*precision, *scale),
                    "invalid decimal128 scalar metadata precision={precision}, scale={scale}"
                );
                Arc::new(array)
            }
            ScalarValue::Decimal256(value, precision, scale) => {
                let array = whatever!(
                    Decimal256Array::from(vec![*value])
                        .with_precision_and_scale(*precision, *scale),
                    "invalid decimal256 scalar metadata precision={precision}, scale={scale}"
                );
                Arc::new(array)
            }
        };

        Ok(array)
    }

    fn try_from_array(array: &dyn Array, index: usize) -> Result<Self> {
        if index >= array.len() {
            whatever!(
                "array index {index} out of bounds for scalar extraction from len={}",
                array.len()
            );
        }

        macro_rules! extract_primitive {
            ($array_ty:ty, $variant:ident) => {{
                let array = array.as_any().downcast_ref::<$array_ty>().unwrap();
                ScalarValue::$variant((!array.is_null(index)).then(|| array.value(index)))
            }};
        }

        Ok(match array.data_type() {
            DataType::Null => ScalarValue::Null,
            DataType::Boolean => extract_primitive!(BooleanArray, Boolean),
            DataType::Float16 => extract_primitive!(Float16Array, Float16),
            DataType::Float32 => extract_primitive!(Float32Array, Float32),
            DataType::Float64 => extract_primitive!(Float64Array, Float64),
            DataType::Int8 => extract_primitive!(Int8Array, Int8),
            DataType::Int16 => extract_primitive!(Int16Array, Int16),
            DataType::Int32 => extract_primitive!(Int32Array, Int32),
            DataType::Int64 => extract_primitive!(Int64Array, Int64),
            DataType::UInt8 => extract_primitive!(UInt8Array, UInt8),
            DataType::UInt16 => extract_primitive!(UInt16Array, UInt16),
            DataType::UInt32 => extract_primitive!(UInt32Array, UInt32),
            DataType::UInt64 => extract_primitive!(UInt64Array, UInt64),
            DataType::Utf8 => {
                let array = array.as_any().downcast_ref::<StringArray>().unwrap();
                ScalarValue::Utf8((!array.is_null(index)).then(|| array.value(index).to_string()))
            }
            DataType::Utf8View => {
                let array = array.as_any().downcast_ref::<StringViewArray>().unwrap();
                ScalarValue::Utf8View(
                    (!array.is_null(index)).then(|| array.value(index).to_string()),
                )
            }
            DataType::LargeUtf8 => {
                let array = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
                ScalarValue::LargeUtf8(
                    (!array.is_null(index)).then(|| array.value(index).to_string()),
                )
            }
            DataType::Binary => {
                let array = array.as_any().downcast_ref::<BinaryArray>().unwrap();
                ScalarValue::Binary((!array.is_null(index)).then(|| array.value(index).to_vec()))
            }
            DataType::BinaryView => {
                let array = array.as_any().downcast_ref::<BinaryViewArray>().unwrap();
                ScalarValue::BinaryView(
                    (!array.is_null(index)).then(|| array.value(index).to_vec()),
                )
            }
            DataType::FixedSizeBinary(size) => {
                let array = array.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap();
                ScalarValue::FixedSizeBinary(
                    *size,
                    (!array.is_null(index)).then(|| array.value(index).to_vec()),
                )
            }
            DataType::LargeBinary => {
                let array = array.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
                ScalarValue::LargeBinary(
                    (!array.is_null(index)).then(|| array.value(index).to_vec()),
                )
            }
            DataType::List(_) => {
                let array = array.slice(index, 1);
                let array = array.as_any().downcast_ref::<ListArray>().unwrap().to_owned();
                ScalarValue::List(Arc::new(array))
            }
            DataType::Struct(_) => {
                let array = array.slice(index, 1);
                let array = array
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .unwrap()
                    .to_owned();
                ScalarValue::Struct(Arc::new(array))
            }
            DataType::Map(_, _) => {
                let array = array.slice(index, 1);
                let array = array.as_any().downcast_ref::<MapArray>().unwrap().to_owned();
                ScalarValue::Map(Arc::new(array))
            }
            DataType::Date32 => extract_primitive!(Date32Array, Date32),
            DataType::Date64 => extract_primitive!(Date64Array, Date64),
            DataType::Time32(TimeUnit::Second) => {
                extract_primitive!(Time32SecondArray, Time32Second)
            }
            DataType::Time32(TimeUnit::Millisecond) => {
                extract_primitive!(Time32MillisecondArray, Time32Millisecond)
            }
            DataType::Time64(TimeUnit::Microsecond) => {
                extract_primitive!(Time64MicrosecondArray, Time64Microsecond)
            }
            DataType::Time64(TimeUnit::Nanosecond) => {
                extract_primitive!(Time64NanosecondArray, Time64Nanosecond)
            }
            DataType::Timestamp(TimeUnit::Second, tz) => {
                let array = array.as_any().downcast_ref::<TimestampSecondArray>().unwrap();
                ScalarValue::TimestampSecond(
                    (!array.is_null(index)).then(|| array.value(index)),
                    tz.clone(),
                )
            }
            DataType::Timestamp(TimeUnit::Millisecond, tz) => {
                let array = array
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .unwrap();
                ScalarValue::TimestampMillisecond(
                    (!array.is_null(index)).then(|| array.value(index)),
                    tz.clone(),
                )
            }
            DataType::Timestamp(TimeUnit::Microsecond, tz) => {
                let array = array
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .unwrap();
                ScalarValue::TimestampMicrosecond(
                    (!array.is_null(index)).then(|| array.value(index)),
                    tz.clone(),
                )
            }
            DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
                let array = array
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap();
                ScalarValue::TimestampNanosecond(
                    (!array.is_null(index)).then(|| array.value(index)),
                    tz.clone(),
                )
            }
            DataType::Interval(IntervalUnit::YearMonth) => {
                extract_primitive!(IntervalYearMonthArray, IntervalYearMonth)
            }
            DataType::Interval(IntervalUnit::DayTime) => {
                extract_primitive!(IntervalDayTimeArray, IntervalDayTime)
            }
            DataType::Interval(IntervalUnit::MonthDayNano) => {
                extract_primitive!(IntervalMonthDayNanoArray, IntervalMonthDayNano)
            }
            DataType::Duration(TimeUnit::Second) => {
                extract_primitive!(DurationSecondArray, DurationSecond)
            }
            DataType::Duration(TimeUnit::Millisecond) => {
                extract_primitive!(DurationMillisecondArray, DurationMillisecond)
            }
            DataType::Duration(TimeUnit::Microsecond) => {
                extract_primitive!(DurationMicrosecondArray, DurationMicrosecond)
            }
            DataType::Duration(TimeUnit::Nanosecond) => {
                extract_primitive!(DurationNanosecondArray, DurationNanosecond)
            }
            DataType::Decimal32(precision, scale) => {
                let array = array.as_any().downcast_ref::<Decimal32Array>().unwrap();
                ScalarValue::Decimal32(
                    (!array.is_null(index)).then(|| array.value(index)),
                    *precision,
                    *scale,
                )
            }
            DataType::Decimal64(precision, scale) => {
                let array = array.as_any().downcast_ref::<Decimal64Array>().unwrap();
                ScalarValue::Decimal64(
                    (!array.is_null(index)).then(|| array.value(index)),
                    *precision,
                    *scale,
                )
            }
            DataType::Decimal128(precision, scale) => {
                let array = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
                ScalarValue::Decimal128(
                    (!array.is_null(index)).then(|| array.value(index)),
                    *precision,
                    *scale,
                )
            }
            DataType::Decimal256(precision, scale) => {
                let array = array.as_any().downcast_ref::<Decimal256Array>().unwrap();
                ScalarValue::Decimal256(
                    (!array.is_null(index)).then(|| array.value(index)),
                    *precision,
                    *scale,
                )
            }
            other => whatever!("unsupported scalar cast target type {other}"),
        })
    }
}

impl PartialEq for ScalarValue {
    fn eq(&self, other: &Self) -> bool {
        use ScalarValue::*;

        match (self, other) {
            (Null, Null) => true,
            (Null, _) => false,
            (Boolean(v1), Boolean(v2)) => v1 == v2,
            (Boolean(_), _) => false,
            (Float16(v1), Float16(v2)) => match (v1, v2) {
                (Some(f1), Some(f2)) => f1.to_bits() == f2.to_bits(),
                _ => v1 == v2,
            },
            (Float16(_), _) => false,
            (Float32(v1), Float32(v2)) => match (v1, v2) {
                (Some(f1), Some(f2)) => f1.to_bits() == f2.to_bits(),
                _ => v1 == v2,
            },
            (Float32(_), _) => false,
            (Float64(v1), Float64(v2)) => match (v1, v2) {
                (Some(f1), Some(f2)) => f1.to_bits() == f2.to_bits(),
                _ => v1 == v2,
            },
            (Float64(_), _) => false,
            (Int8(v1), Int8(v2)) => v1 == v2,
            (Int8(_), _) => false,
            (Int16(v1), Int16(v2)) => v1 == v2,
            (Int16(_), _) => false,
            (Int32(v1), Int32(v2)) => v1 == v2,
            (Int32(_), _) => false,
            (Int64(v1), Int64(v2)) => v1 == v2,
            (Int64(_), _) => false,
            (UInt8(v1), UInt8(v2)) => v1 == v2,
            (UInt8(_), _) => false,
            (UInt16(v1), UInt16(v2)) => v1 == v2,
            (UInt16(_), _) => false,
            (UInt32(v1), UInt32(v2)) => v1 == v2,
            (UInt32(_), _) => false,
            (UInt64(v1), UInt64(v2)) => v1 == v2,
            (UInt64(_), _) => false,
            (Utf8(v1), Utf8(v2)) => v1 == v2,
            (Utf8(_), _) => false,
            (Utf8View(v1), Utf8View(v2)) => v1 == v2,
            (Utf8View(_), _) => false,
            (LargeUtf8(v1), LargeUtf8(v2)) => v1 == v2,
            (LargeUtf8(_), _) => false,
            (Binary(v1), Binary(v2)) => v1 == v2,
            (Binary(_), _) => false,
            (BinaryView(v1), BinaryView(v2)) => v1 == v2,
            (BinaryView(_), _) => false,
            (FixedSizeBinary(size1, v1), FixedSizeBinary(size2, v2)) => {
                size1 == size2 && v1 == v2
            }
            (FixedSizeBinary(_, _), _) => false,
            (LargeBinary(v1), LargeBinary(v2)) => v1 == v2,
            (LargeBinary(_), _) => false,
            (List(v1), List(v2)) => v1 == v2,
            (List(_), _) => false,
            (Struct(v1), Struct(v2)) => v1 == v2,
            (Struct(_), _) => false,
            (Map(v1), Map(v2)) => v1 == v2,
            (Map(_), _) => false,
            (Date32(v1), Date32(v2)) => v1 == v2,
            (Date32(_), _) => false,
            (Date64(v1), Date64(v2)) => v1 == v2,
            (Date64(_), _) => false,
            (Time32Second(v1), Time32Second(v2)) => v1 == v2,
            (Time32Second(_), _) => false,
            (Time32Millisecond(v1), Time32Millisecond(v2)) => v1 == v2,
            (Time32Millisecond(_), _) => false,
            (Time64Microsecond(v1), Time64Microsecond(v2)) => v1 == v2,
            (Time64Microsecond(_), _) => false,
            (Time64Nanosecond(v1), Time64Nanosecond(v2)) => v1 == v2,
            (Time64Nanosecond(_), _) => false,
            (TimestampSecond(v1, tz1), TimestampSecond(v2, tz2)) => v1 == v2 && tz1 == tz2,
            (TimestampSecond(_, _), _) => false,
            (TimestampMillisecond(v1, tz1), TimestampMillisecond(v2, tz2)) => {
                v1 == v2 && tz1 == tz2
            }
            (TimestampMillisecond(_, _), _) => false,
            (TimestampMicrosecond(v1, tz1), TimestampMicrosecond(v2, tz2)) => {
                v1 == v2 && tz1 == tz2
            }
            (TimestampMicrosecond(_, _), _) => false,
            (TimestampNanosecond(v1, tz1), TimestampNanosecond(v2, tz2)) => {
                v1 == v2 && tz1 == tz2
            }
            (TimestampNanosecond(_, _), _) => false,
            (IntervalYearMonth(v1), IntervalYearMonth(v2)) => v1 == v2,
            (IntervalYearMonth(_), _) => false,
            (IntervalDayTime(v1), IntervalDayTime(v2)) => v1 == v2,
            (IntervalDayTime(_), _) => false,
            (IntervalMonthDayNano(v1), IntervalMonthDayNano(v2)) => v1 == v2,
            (IntervalMonthDayNano(_), _) => false,
            (DurationSecond(v1), DurationSecond(v2)) => v1 == v2,
            (DurationSecond(_), _) => false,
            (DurationMillisecond(v1), DurationMillisecond(v2)) => v1 == v2,
            (DurationMillisecond(_), _) => false,
            (DurationMicrosecond(v1), DurationMicrosecond(v2)) => v1 == v2,
            (DurationMicrosecond(_), _) => false,
            (DurationNanosecond(v1), DurationNanosecond(v2)) => v1 == v2,
            (DurationNanosecond(_), _) => false,
            (Decimal32(v1, p1, s1), Decimal32(v2, p2, s2)) => {
                v1 == v2 && p1 == p2 && s1 == s2
            }
            (Decimal32(_, _, _), _) => false,
            (Decimal64(v1, p1, s1), Decimal64(v2, p2, s2)) => {
                v1 == v2 && p1 == p2 && s1 == s2
            }
            (Decimal64(_, _, _), _) => false,
            (Decimal128(v1, p1, s1), Decimal128(v2, p2, s2)) => {
                v1 == v2 && p1 == p2 && s1 == s2
            }
            (Decimal128(_, _, _), _) => false,
            (Decimal256(v1, p1, s1), Decimal256(v2, p2, s2)) => {
                v1 == v2 && p1 == p2 && s1 == s2
            }
            (Decimal256(_, _, _), _) => false,
        }
    }
}

impl Eq for ScalarValue {}

struct Fl<T>(T);

macro_rules! hash_float_value {
    ($(($t:ty, $i:ty)),+ $(,)?) => {
        $(
            impl Hash for Fl<$t> {
                fn hash<H: Hasher>(&self, state: &mut H) {
                    state.write(&<$i>::from_ne_bytes(self.0.to_ne_bytes()).to_ne_bytes())
                }
            }
        )+
    };
}

hash_float_value!((f16, u16), (f32, u32), (f64, u64));

fn hash_nested_scalar<H: Hasher>(array: &dyn Array, state: &mut H) {
    array.data_type().hash(state);
    array.len().hash(state);
    array.null_count().hash(state);
    if array.len() > 0 {
        array_value_to_string(array, 0)
            .expect("nested scalar should be displayable")
            .hash(state);
    }
}

impl Hash for ScalarValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        use ScalarValue::*;

        match self {
            Null => 0_u8.hash(state),
            Boolean(v) => v.hash(state),
            Float16(v) => v.map(Fl).hash(state),
            Float32(v) => v.map(Fl).hash(state),
            Float64(v) => v.map(Fl).hash(state),
            Int8(v) => v.hash(state),
            Int16(v) => v.hash(state),
            Int32(v) => v.hash(state),
            Int64(v) => v.hash(state),
            UInt8(v) => v.hash(state),
            UInt16(v) => v.hash(state),
            UInt32(v) => v.hash(state),
            UInt64(v) => v.hash(state),
            Utf8(v) | Utf8View(v) | LargeUtf8(v) => v.hash(state),
            Binary(v) | BinaryView(v) | LargeBinary(v) => v.hash(state),
            FixedSizeBinary(size, v) => {
                size.hash(state);
                v.hash(state);
            }
            List(v) => hash_nested_scalar(v.as_ref(), state),
            Struct(v) => hash_nested_scalar(v.as_ref(), state),
            Map(v) => hash_nested_scalar(v.as_ref(), state),
            Date32(v) => v.hash(state),
            Date64(v) => v.hash(state),
            Time32Second(v) => v.hash(state),
            Time32Millisecond(v) => v.hash(state),
            Time64Microsecond(v) => v.hash(state),
            Time64Nanosecond(v) => v.hash(state),
            TimestampSecond(v, tz) => {
                v.hash(state);
                tz.hash(state);
            }
            TimestampMillisecond(v, tz) => {
                v.hash(state);
                tz.hash(state);
            }
            TimestampMicrosecond(v, tz) => {
                v.hash(state);
                tz.hash(state);
            }
            TimestampNanosecond(v, tz) => {
                v.hash(state);
                tz.hash(state);
            }
            IntervalYearMonth(v) => v.hash(state),
            IntervalDayTime(v) => v.hash(state),
            IntervalMonthDayNano(v) => v.hash(state),
            DurationSecond(v) => v.hash(state),
            DurationMillisecond(v) => v.hash(state),
            DurationMicrosecond(v) => v.hash(state),
            DurationNanosecond(v) => v.hash(state),
            Decimal32(v, p, s) => {
                v.hash(state);
                p.hash(state);
                s.hash(state);
            }
            Decimal64(v, p, s) => {
                v.hash(state);
                p.hash(state);
                s.hash(state);
            }
            Decimal128(v, p, s) => {
                v.hash(state);
                p.hash(state);
                s.hash(state);
            }
            Decimal256(v, p, s) => {
                v.hash(state);
                p.hash(state);
                s.hash(state);
            }
        }
    }
}

macro_rules! impl_scalar {
    ($ty:ty, $scalar:tt) => {
        impl From<$ty> for ScalarValue {
            fn from(value: $ty) -> Self {
                ScalarValue::$scalar(Some(value))
            }
        }

        impl From<Option<$ty>> for ScalarValue {
            fn from(value: Option<$ty>) -> Self {
                ScalarValue::$scalar(value)
            }
        }
    };
}

impl_scalar!(f16, Float16);
impl_scalar!(f32, Float32);
impl_scalar!(f64, Float64);
impl_scalar!(i8, Int8);
impl_scalar!(i16, Int16);
impl_scalar!(i32, Int32);
impl_scalar!(i64, Int64);
impl_scalar!(bool, Boolean);
impl_scalar!(u8, UInt8);
impl_scalar!(u16, UInt16);
impl_scalar!(u32, UInt32);
impl_scalar!(u64, UInt64);

impl From<&str> for ScalarValue {
    fn from(value: &str) -> Self {
        Some(value).into()
    }
}

impl From<Option<&str>> for ScalarValue {
    fn from(value: Option<&str>) -> Self {
        let value = value.map(|s| s.to_string());
        value.into()
    }
}

impl FromStr for ScalarValue {
    type Err = Infallible;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        Ok(s.into())
    }
}

impl From<String> for ScalarValue {
    fn from(value: String) -> Self {
        Some(value).into()
    }
}

impl From<Option<String>> for ScalarValue {
    fn from(value: Option<String>) -> Self {
        ScalarValue::Utf8(value)
    }
}

impl std::fmt::Display for ScalarValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fn fmt_optional<T: std::fmt::Display>(
            f: &mut std::fmt::Formatter<'_>,
            optional: &Option<T>,
            type_name: &str,
        ) -> std::fmt::Result {
            match optional {
                Some(v) => write!(f, "{v}::{type_name}"),
                None => write!(f, "null::{type_name}"),
            }
        }

        fn fmt_optional_debug<T: std::fmt::Debug>(
            f: &mut std::fmt::Formatter<'_>,
            optional: &Option<T>,
            type_name: &str,
        ) -> std::fmt::Result {
            match optional {
                Some(v) => write!(f, "{v:?}::{type_name}"),
                None => write!(f, "null::{type_name}"),
            }
        }

        fn fmt_quoted<T: std::fmt::Display>(
            f: &mut std::fmt::Formatter<'_>,
            optional: &Option<T>,
            type_name: &str,
        ) -> std::fmt::Result {
            match optional {
                Some(v) => write!(f, "'{v}'::{type_name}"),
                None => write!(f, "null::{type_name}"),
            }
        }

        fn fmt_bytes(
            f: &mut std::fmt::Formatter<'_>,
            optional: &Option<Vec<u8>>,
            type_name: &str,
        ) -> std::fmt::Result {
            match optional {
                Some(bytes) => {
                    write!(f, "'")?;
                    for byte in bytes {
                        write!(f, "{byte:02X}")?;
                    }
                    write!(f, "'::{type_name}")
                }
                None => write!(f, "null::{type_name}"),
            }
        }

        fn fmt_timestamp_type(base: &str, tz: &Option<Arc<str>>) -> String {
            match tz {
                Some(tz) => format!("{base}[{tz}]"),
                None => base.to_string(),
            }
        }

        fn fmt_nested(
            f: &mut std::fmt::Formatter<'_>,
            array: &dyn Array,
            type_name: &str,
        ) -> std::fmt::Result {
            if array.is_null(0) {
                write!(f, "null::{type_name}")
            } else {
                let value = array_value_to_string(array, 0).map_err(|_| std::fmt::Error)?;
                write!(f, "{value}::{type_name}")
            }
        }

        match self {
            ScalarValue::Null => write!(f, "null"),
            ScalarValue::Int32(v) => fmt_optional(f, v, "integer"),
            ScalarValue::Int64(v) => fmt_optional(f, v, "bigint"),
            ScalarValue::Boolean(v) => fmt_optional(f, v, "boolean"),
            ScalarValue::Float16(v) => fmt_optional(f, v, "float16"),
            ScalarValue::Float32(v) => fmt_optional(f, v, "float32"),
            ScalarValue::Float64(v) => fmt_optional(f, v, "float64"),
            ScalarValue::Utf8(v) => fmt_quoted(f, v, "utf8"),
            ScalarValue::Utf8View(v) => fmt_quoted(f, v, "utf8_view"),
            ScalarValue::LargeUtf8(v) => fmt_quoted(f, v, "large_utf8"),
            ScalarValue::Int8(v) => fmt_optional(f, v, "int8"),
            ScalarValue::Int16(v) => fmt_optional(f, v, "int16"),
            ScalarValue::UInt8(v) => fmt_optional(f, v, "uint8"),
            ScalarValue::UInt16(v) => fmt_optional(f, v, "uint16"),
            ScalarValue::UInt32(v) => fmt_optional(f, v, "uint32"),
            ScalarValue::UInt64(v) => fmt_optional(f, v, "uint64"),
            ScalarValue::Binary(v) => fmt_bytes(f, v, "binary"),
            ScalarValue::BinaryView(v) => fmt_bytes(f, v, "binary_view"),
            ScalarValue::FixedSizeBinary(size, v) => {
                fmt_bytes(f, v, &format!("fixed_size_binary({size})"))
            }
            ScalarValue::LargeBinary(v) => fmt_bytes(f, v, "large_binary"),
            ScalarValue::List(v) => fmt_nested(f, v.as_ref(), "list"),
            ScalarValue::Struct(v) => fmt_nested(f, v.as_ref(), "struct"),
            ScalarValue::Map(v) => fmt_nested(f, v.as_ref(), "map"),
            ScalarValue::Date32(v) => fmt_optional(
                f,
                &v.map(|v| {
                    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                    match epoch.checked_add_signed(chrono::Duration::try_days(v as i64).unwrap()) {
                        Some(date) => date.to_string(),
                        None => "".to_string(),
                    }
                }),
                "date32",
            ),
            ScalarValue::Date64(v) => fmt_optional(
                f,
                &v.map(|v| {
                    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                    match epoch.checked_add_signed(chrono::Duration::try_milliseconds(v).unwrap()) {
                        Some(date) => date.to_string(),
                        None => "".to_string(),
                    }
                }),
                "date64",
            ),
            ScalarValue::Time32Second(v) => fmt_optional(f, v, "time32_second"),
            ScalarValue::Time32Millisecond(v) => fmt_optional(f, v, "time32_millisecond"),
            ScalarValue::Time64Microsecond(v) => fmt_optional(f, v, "time64_microsecond"),
            ScalarValue::Time64Nanosecond(v) => fmt_optional(f, v, "time64_nanosecond"),
            ScalarValue::TimestampSecond(v, tz) => {
                fmt_optional(f, v, &fmt_timestamp_type("timestamp_second", tz))
            }
            ScalarValue::TimestampMillisecond(v, tz) => {
                fmt_optional(f, v, &fmt_timestamp_type("timestamp_millisecond", tz))
            }
            ScalarValue::TimestampMicrosecond(v, tz) => {
                fmt_optional(f, v, &fmt_timestamp_type("timestamp_microsecond", tz))
            }
            ScalarValue::TimestampNanosecond(v, tz) => {
                fmt_optional(f, v, &fmt_timestamp_type("timestamp_nanosecond", tz))
            }
            ScalarValue::IntervalYearMonth(v) => fmt_optional(f, v, "interval_year_month"),
            ScalarValue::IntervalDayTime(v) => fmt_optional_debug(f, v, "interval_day_time"),
            ScalarValue::IntervalMonthDayNano(v) => {
                fmt_optional_debug(f, v, "interval_month_day_nano")
            }
            ScalarValue::DurationSecond(v) => fmt_optional(f, v, "duration_second"),
            ScalarValue::DurationMillisecond(v) => fmt_optional(f, v, "duration_millisecond"),
            ScalarValue::DurationMicrosecond(v) => fmt_optional(f, v, "duration_microsecond"),
            ScalarValue::DurationNanosecond(v) => fmt_optional(f, v, "duration_nanosecond"),
            ScalarValue::Decimal32(v, precision, scale) => match v {
                Some(val) => write!(f, "{}::decimal32({}, {})", val, precision, scale),
                None => write!(f, "null::decimal32({}, {})", precision, scale),
            },
            ScalarValue::Decimal64(v, precision, scale) => match v {
                Some(val) => write!(f, "{}::decimal64({}, {})", val, precision, scale),
                None => write!(f, "null::decimal64({}, {})", precision, scale),
            },
            ScalarValue::Decimal128(v, precision, scale) => match v {
                Some(val) => write!(f, "{}::decimal128({}, {})", val, precision, scale),
                None => write!(f, "null::decimal128({}, {})", precision, scale),
            },
            ScalarValue::Decimal256(v, precision, scale) => match v {
                Some(val) => write!(f, "{val:?}::decimal256({}, {})", precision, scale),
                None => write!(f, "null::decimal256({}, {})", precision, scale),
            },
        }
    }
}

impl ScalarValue {
    pub fn try_from_string(value: String, target_type: &DataType) -> Result<Self> {
        ScalarValue::Utf8(Some(value)).cast_to(target_type)
    }

    pub fn try_into_nullable_string(&self) -> Result<Option<String>> {
        match self.cast_to(&DataType::Utf8)? {
            ScalarValue::Utf8(value) => Ok(value),
            other => whatever!(
                "expected utf8 scalar after casting {} to utf8, got {}",
                self.data_type(),
                other.data_type()
            ),
        }
    }

    pub fn cast_to(&self, target_type: &DataType) -> Result<Self> {
        self.cast_to_with_options(target_type, &DEFAULT_CAST_OPTIONS)
    }

    pub fn cast_to_with_options(
        &self,
        target_type: &DataType,
        options: &CastOptions<'_>,
    ) -> Result<Self> {
        if self.data_type() == *target_type {
            return Ok(self.clone());
        }

        let scalar_array = self.to_array()?;
        let cast_arr = cast_with_options(&scalar_array, target_type, options)
            .with_whatever_context(|_| {
                format!(
                    "failed to cast scalar value from {} to {}",
                    self.data_type(),
                    target_type
                )
            })?;

        Self::try_from_array(cast_arr.as_ref(), 0)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use half::f16;

    use super::{DEFAULT_FORMAT_OPTIONS, ScalarValue};
    use crate::ir::DataType;
    use arrow::{
        array::{
            ArrayRef, Int32Array, Int32Builder, ListArray, MapBuilder, StringArray,
            StringBuilder, StructArray,
        },
        compute::kernels::cast::CastOptions,
        datatypes::{i256, Field, Int32Type, IntervalDayTimeType},
    };

    #[test]
    fn float_values_report_types_and_nullability() {
        assert_eq!(
            ScalarValue::Float32(Some(1.0)).data_type(),
            DataType::Float32
        );
        assert_eq!(
            ScalarValue::Float64(Some(1.0)).data_type(),
            DataType::Float64
        );
        assert!(ScalarValue::Float32(None).is_null());
        assert!(ScalarValue::Float64(None).is_null());
    }

    #[test]
    fn float_values_compare_and_hash_by_bits() {
        let nan16_a = ScalarValue::Float16(Some(f16::from_bits(0x7e01)));
        let nan16_b = ScalarValue::Float16(Some(f16::from_bits(0x7e01)));
        let nan16_c = ScalarValue::Float16(Some(f16::from_bits(0x7e02)));
        assert_eq!(nan16_a, nan16_b);
        assert_ne!(nan16_a, nan16_c);

        let nan32_a = ScalarValue::Float32(Some(f32::from_bits(0x7fc0_0001)));
        let nan32_b = ScalarValue::Float32(Some(f32::from_bits(0x7fc0_0001)));
        let nan32_c = ScalarValue::Float32(Some(f32::from_bits(0x7fc0_0002)));

        assert_eq!(nan32_a, nan32_b);
        assert_ne!(nan32_a, nan32_c);

        let neg_zero = ScalarValue::Float64(Some(-0.0));
        let pos_zero = ScalarValue::Float64(Some(0.0));
        assert_ne!(neg_zero, pos_zero);

        let mut set = HashSet::new();
        set.insert(nan32_a.clone());
        assert!(set.contains(&nan32_b));
        assert!(!set.contains(&nan32_c));
    }

    #[test]
    fn scalar_values_cast_via_arrow_kernels() {
        assert_eq!(
            ScalarValue::Utf8(Some("42".to_string()))
                .cast_to(&DataType::Int32)
                .unwrap(),
            ScalarValue::Int32(Some(42))
        );
        assert_eq!(
            ScalarValue::Int32(Some(42))
                .cast_to(&DataType::Utf8)
                .unwrap(),
            ScalarValue::Utf8(Some("42".to_string()))
        );
        assert_eq!(
            ScalarValue::Utf8(Some("view".to_string()))
                .cast_to(&DataType::Utf8View)
                .unwrap(),
            ScalarValue::Utf8View(Some("view".to_string()))
        );
        assert_eq!(
            ScalarValue::Utf8(Some("hello".to_string()))
                .cast_to(&DataType::LargeUtf8)
                .unwrap(),
            ScalarValue::LargeUtf8(Some("hello".to_string()))
        );
    }

    #[test]
    fn scalar_values_safe_casts_return_null() {
        let options = CastOptions {
            safe: true,
            format_options: DEFAULT_FORMAT_OPTIONS,
        };

        assert_eq!(
            ScalarValue::Utf8(Some("abc".to_string()))
                .cast_to_with_options(&DataType::Int32, &options)
                .unwrap(),
            ScalarValue::Int32(None)
        );
    }

    fn assert_string_round_trip(value: ScalarValue) {
        let data_type = value.data_type();
        match value.try_into_nullable_string().unwrap() {
            Some(string_value) => {
                assert_eq!(
                    ScalarValue::try_from_string(string_value, &data_type).unwrap(),
                    value
                );
            }
            None => {
                assert_eq!(ScalarValue::Utf8(None).cast_to(&data_type).unwrap(), value);
            }
        }
    }

    #[test]
    fn scalar_values_round_trip_through_utf8_casts() {
        for value in [
            ScalarValue::Boolean(Some(true)),
            ScalarValue::Boolean(None),
            ScalarValue::Float32(Some(1.5)),
            ScalarValue::Float32(None),
            ScalarValue::Float64(Some(-2.25)),
            ScalarValue::Float64(None),
            ScalarValue::Int8(Some(-8)),
            ScalarValue::Int8(None),
            ScalarValue::Int16(Some(-16)),
            ScalarValue::Int16(None),
            ScalarValue::Int32(Some(-32)),
            ScalarValue::Int32(None),
            ScalarValue::Int64(Some(-64)),
            ScalarValue::Int64(None),
            ScalarValue::UInt8(Some(8)),
            ScalarValue::UInt8(None),
            ScalarValue::UInt16(Some(16)),
            ScalarValue::UInt16(None),
            ScalarValue::UInt32(Some(32)),
            ScalarValue::UInt32(None),
            ScalarValue::UInt64(Some(64)),
            ScalarValue::UInt64(None),
            ScalarValue::Utf8(Some("hello".to_string())),
            ScalarValue::Utf8(None),
            ScalarValue::Utf8View(Some("view".to_string())),
            ScalarValue::Utf8View(None),
            ScalarValue::LargeUtf8(Some("large".to_string())),
            ScalarValue::LargeUtf8(None),
            ScalarValue::Date32(Some(1)),
            ScalarValue::Date32(None),
            ScalarValue::Date64(Some(86_400_000)),
            ScalarValue::Date64(None),
            ScalarValue::Decimal32(Some(1234), 6, 2),
            ScalarValue::Decimal32(None, 6, 2),
            ScalarValue::Decimal64(Some(5678), 8, 2),
            ScalarValue::Decimal64(None, 8, 2),
            ScalarValue::Decimal128(Some(9012), 10, 2),
            ScalarValue::Decimal128(None, 10, 2),
            ScalarValue::Decimal256(Some(i256::from_i128(3456)), 12, 2),
            ScalarValue::Decimal256(None, 12, 2),
        ] {
            assert_string_round_trip(value);
        }
    }

    #[test]
    fn scalar_values_try_from_string_matches_utf8_round_trip_targets() {
        assert_eq!(
            ScalarValue::try_from_string("42".to_string(), &DataType::Int32).unwrap(),
            ScalarValue::Int32(Some(42))
        );
        assert_eq!(
            ScalarValue::try_from_string("1970-01-02".to_string(), &DataType::Date32).unwrap(),
            ScalarValue::Date32(Some(1))
        );
        assert_eq!(
            ScalarValue::try_from_string("12.34".to_string(), &DataType::Decimal64(8, 2)).unwrap(),
            ScalarValue::Decimal64(Some(1234), 8, 2)
        );
        assert_eq!(
            ScalarValue::try_from_string("view".to_string(), &DataType::Utf8View).unwrap(),
            ScalarValue::Utf8View(Some("view".to_string()))
        );
    }

    #[test]
    fn scalar_values_try_to_string_matches_utf8_casts() {
        assert_eq!(
            ScalarValue::Int32(Some(42))
                .try_into_nullable_string()
                .unwrap(),
            Some("42".to_string())
        );
        assert_eq!(
            ScalarValue::Date32(Some(1))
                .try_into_nullable_string()
                .unwrap(),
            Some("1970-01-02".to_string())
        );
        assert_eq!(
            ScalarValue::Decimal64(Some(1234), 8, 2)
                .try_into_nullable_string()
                .unwrap(),
            Some("12.34".to_string())
        );
        assert_eq!(
            ScalarValue::Int32(None).try_into_nullable_string().unwrap(),
            None
        );
    }

    #[test]
    fn scalar_value_round_trips_through_arrow_arrays_for_new_variants() {
        let day_time = IntervalDayTimeType::make_value(2, 3);
        let list = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![
            Some(1),
            None,
            Some(3),
        ])]);
        let struct_array = StructArray::from(vec![
            (
                Arc::new(Field::new("a", DataType::Int32, true)),
                Arc::new(Int32Array::from(vec![Some(1)])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("b", DataType::Utf8, true)),
                Arc::new(StringArray::from(vec![Some("x")])) as ArrayRef,
            ),
        ]);
        let mut map_builder = MapBuilder::new(
            None,
            StringBuilder::new(),
            Int32Builder::new(),
        );
        map_builder.keys().append_value("k");
        map_builder.values().append_value(7);
        map_builder.append(true).unwrap();
        let map = map_builder.finish();
        for value in [
            ScalarValue::Null,
            ScalarValue::Float16(Some(f16::from_f32(2.5))),
            ScalarValue::LargeUtf8(Some("hello".to_string())),
            ScalarValue::Binary(Some(vec![0xde, 0xad, 0xbe, 0xef])),
            ScalarValue::BinaryView(Some(vec![1, 2, 3])),
            ScalarValue::FixedSizeBinary(3, Some(vec![1, 2, 3])),
            ScalarValue::LargeBinary(Some(vec![4, 5, 6])),
            ScalarValue::List(Arc::new(list)),
            ScalarValue::Struct(Arc::new(struct_array)),
            ScalarValue::Map(Arc::new(map)),
            ScalarValue::Time32Second(Some(10)),
            ScalarValue::Time32Millisecond(Some(10)),
            ScalarValue::Time64Microsecond(Some(10)),
            ScalarValue::Time64Nanosecond(Some(10)),
            ScalarValue::TimestampMillisecond(Some(42), Some(Arc::<str>::from("UTC"))),
            ScalarValue::TimestampMicrosecond(Some(42), None),
            ScalarValue::TimestampNanosecond(Some(42), None),
            ScalarValue::IntervalYearMonth(Some(7)),
            ScalarValue::IntervalDayTime(Some(day_time)),
            ScalarValue::DurationMillisecond(Some(9)),
            ScalarValue::DurationMicrosecond(Some(9)),
            ScalarValue::DurationNanosecond(Some(9)),
            ScalarValue::Decimal256(Some(i256::from_i128(999)), 10, 2),
        ] {
            let array = value.to_array().unwrap();
            let round_trip = ScalarValue::try_from_array(array.as_ref(), 0).unwrap();
            assert_eq!(round_trip, value);
        }
    }

    #[test]
    fn scalar_value_timestamp_timezone_participates_in_equality() {
        let utc = ScalarValue::TimestampSecond(Some(1), Some(Arc::<str>::from("UTC")));
        let est = ScalarValue::TimestampSecond(Some(1), Some(Arc::<str>::from("US/Eastern")));
        assert_ne!(utc, est);
    }
}
