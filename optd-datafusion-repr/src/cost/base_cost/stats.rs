use std::{collections::HashMap, sync::Arc};

use arrow_schema::{ArrowError, DataType};
use datafusion::arrow::array::{
    Array, BooleanArray, Date32Array, Float32Array, Int16Array, Int32Array, Int8Array, RecordBatch,
    RecordBatchIterator, RecordBatchReader, StringArray, UInt16Array, UInt32Array, UInt8Array,
};
use itertools::Itertools;
use optd_core::rel_node::{SerializableOrderedF64, Value};
use optd_gungnir::{
    stats::{
        counter::Counter,
        hyperloglog::{self, HyperLogLog},
        misragries::{self, MisraGries},
        tdigest::{self, TDigest},
    },
    utils::arith_encoder,
};
use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};

// The "standard" concrete types that optd currently uses.
// All of optd (except unit tests) must use the same types.
pub type DataFusionMostCommonValues = Counter<Vec<Option<Value>>>;
pub type DataFusionDistribution = TDigest;

pub type DataFusionBaseTableStats =
    BaseTableStats<DataFusionMostCommonValues, DataFusionDistribution>;
pub type DataFusionPerTableStats =
    PerTableStats<DataFusionMostCommonValues, DataFusionDistribution>;

/// A more general interface meant to perform the task of a histogram.
//.
/// This more general interface is still compatible with histograms but allows
/// more powerful statistics like TDigest.
pub trait Distribution: 'static + Send + Sync {
    // Give the probability of a random value sampled from the distribution being <= `value`
    fn cdf(&self, value: &Value) -> f64;
}

impl Distribution for TDigest {
    fn cdf(&self, value: &Value) -> f64 {
        match value {
            Value::Int8(i) => self.cdf(*i as f64),
            Value::Int16(i) => self.cdf(*i as f64),
            Value::Int32(i) => self.cdf(*i as f64),
            Value::Int64(i) => self.cdf(*i as f64),
            Value::Int128(i) => self.cdf(*i as f64),
            Value::Float(i) => self.cdf(*i.0),
            _ => panic!("Value is not a number"),
        }
    }
}

pub trait MostCommonValues: 'static + Send + Sync {
    // it is true that we could just expose freq_over_pred() and use that for freq() and total_freq()
    // however, freq() and total_freq() each have potential optimizations (freq() is O(1) instead of
    //     O(n) and total_freq() can be cached)
    // additionally, it makes sense to return an Option<f64> for freq() instead of just 0 if value doesn't exist
    // thus, I expose three different functions
    fn freq(&self, value: &[Option<Value>]) -> Option<f64>;
    fn total_freq(&self) -> f64;
    fn freq_over_pred(&self, pred: Box<dyn Fn(&[Option<Value>]) -> bool>) -> f64;

    // returns the # of entries (i.e. value + freq) in the most common values structure
    fn cnt(&self) -> usize;
}

impl MostCommonValues for Counter<Vec<Option<Value>>> {
    fn freq(&self, value: &[Option<Value>]) -> Option<f64> {
        self.frequencies().get(value).copied()
    }

    fn total_freq(&self) -> f64 {
        self.frequencies().values().sum()
    }

    fn freq_over_pred(&self, pred: Box<dyn Fn(&[Option<Value>]) -> bool>) -> f64 {
        self.frequencies()
            .iter()
            .filter(|(val, _)| pred(val))
            .map(|(_, freq)| freq)
            .sum()
    }

    fn cnt(&self) -> usize {
        self.frequencies().len()
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct PerColumnStats<M: MostCommonValues, D: Distribution> {
    // even if nulls are the most common, they cannot appear in mcvs
    pub mcvs: M,

    // ndistinct _does_ include the values in mcvs
    // ndistinct _does not_ include nulls
    pub ndistinct: u64,

    // postgres uses null_frac instead of something like "num_nulls" so we'll follow suit
    // my guess for why they use null_frac is because we only ever use the fraction of nulls, not the #
    pub null_frac: f64,

    // distribution _does not_ include the values in mcvs
    // distribution _does not_ include nulls
    pub distr: Option<D>,
}

impl<M: MostCommonValues, D: Distribution> PerColumnStats<M, D> {
    pub fn new(mcvs: M, ndistinct: u64, null_frac: f64, distr: D) -> Self {
        Self {
            mcvs,
            ndistinct,
            null_frac,
            distr: Some(distr),
        }
    }

    pub fn new_comb(mcvs: M, ndistinct: u64, null_frac: f64) -> Self {
        Self {
            mcvs,
            ndistinct,
            null_frac,
            distr: None,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct PerTableStats<M: MostCommonValues, D: Distribution> {
    pub row_cnt: usize,
    // This is a Vec of Options instead of just a Vec because some columns may not have stats
    //   due to their type being non-comparable.
    // Further, I chose to represent it as a Vec of Options instead of a HashMap because a Vec
    //   of Options clearly differentiates between two different failure modes: "out-of-bounds
    //   access" and "column has no stats".
    pub per_column_stats_vec: Vec<Option<PerColumnStats<M, D>>>,

    pub column_combi_stats: HashMap<Vec<usize>, PerColumnStats<M, D>>,
}

impl<M: MostCommonValues, D: Distribution> PerTableStats<M, D> {
    pub fn new(row_cnt: usize, per_column_stats_vec: Vec<Option<PerColumnStats<M, D>>>) -> Self {
        Self {
            row_cnt,
            per_column_stats_vec,
            column_combi_stats: HashMap::new(),
        }
    }
}

pub type BaseTableStats<M, D> = HashMap<String, PerTableStats<M, D>>;

impl PerTableStats<Counter<Vec<Option<Value>>>, TDigest> {
    pub fn from_record_batches<I: IntoIterator<Item = Result<RecordBatch, ArrowError>>>(
        batch_iter_builder: impl Fn() -> anyhow::Result<RecordBatchIterator<I>>,
    ) -> anyhow::Result<Self> {
        let batch_iter1 = batch_iter_builder()?;
        let batch_iter2 = batch_iter_builder()?;

        let schema = batch_iter1.schema();
        let col_types = schema
            .fields()
            .iter()
            .map(|f| f.data_type().clone())
            .collect_vec();
        let col_cnt = col_types.len();

        let mut row_cnt = 0;

        let mut hlls = vec![HyperLogLog::new(hyperloglog::DEFAULT_PRECISION); col_cnt];
        let mut mgs: Vec<MisraGries<Value>> =
            vec![MisraGries::new(misragries::DEFAULT_K_TO_TRACK); col_cnt];
        let mut null_cnt = vec![0; col_cnt];

        // 1. First pass: HLL + MG + null_cnt + row_cnt.
        for batch in batch_iter1 {
            let batch = batch?;
            row_cnt += batch.num_rows();

            for (i, col) in batch.columns().iter().enumerate() {
                let col_type = &col_types[i];
                if Self::is_type_supported(col_type) {
                    null_cnt[i] += col.null_count();
                    Self::generate_partial_stats_for_column(
                        col,
                        col_type,
                        &mut mgs[i],
                        &mut hlls[i],
                    );
                }
            }
        }

        let mut distr = col_types
            .iter()
            .map(|col_type| {
                if Self::is_type_supported(col_type) {
                    Some(TDigest::new(tdigest::DEFAULT_COMPRESSION))
                } else {
                    None
                }
            })
            .collect_vec();
        let mut cnts: Vec<Counter<Vec<Option<Value>>>> = mgs
            .iter()
            .map(|mg| {
                let mfk: Vec<Option<Value>> = mg
                    .most_frequent_keys()
                    .into_iter()
                    .cloned()
                    .map(|v| Some(v))
                    .collect();
                Counter::new(&vec![mfk])
            })
            .collect();

        // 2. Second pass: MCV + TDigest.
        // TODO(Alexis): Remove MCV from TDigest.
        for batch in batch_iter2 {
            let batch = batch?;

            for (i, col) in batch.columns().iter().enumerate() {
                let col_type = &col_types[i];
                if Self::is_type_supported(col_type) {
                    Self::generate_stats_for_column(col, col_type, &mut distr[i], &mut cnts[i]);
                }
            }
        }

        // 3. Assemble stats.
        let mut per_column_stats_vec = Vec::with_capacity(col_cnt);
        for i in 0..col_cnt {
            let counter = cnts.remove(0);
            per_column_stats_vec.push(if Self::is_type_supported(&col_types[i]) {
                Some(PerColumnStats::new(
                    counter,
                    hlls[i].n_distinct(),
                    null_cnt[i] as f64 / row_cnt as f64,
                    distr[i].take().unwrap(),
                ))
            } else {
                None
            });
        }

        Ok(Self {
            row_cnt,
            per_column_stats_vec,
            column_combi_stats: HashMap::new(),
        })
    }

    fn get_col_projection(batch: RecordBatch, comb: &Vec<usize>) -> Vec<Arc<dyn Array>> {
        let mut cols = Vec::<Arc<dyn Array>>::new();
        let mut col_types = Vec::<DataType>::new();
        for idx in comb.iter() {
            cols.push(batch.column(*idx).clone());
            col_types.push(col_types[*idx].clone());
        }

        cols
    }

    pub fn extend_with_multi_column_stats<
        I: IntoIterator<Item = Result<RecordBatch, ArrowError>>,
    >(
        &mut self,
        batch_iter_builder: impl Fn() -> anyhow::Result<RecordBatchIterator<I>>,
        combinations: Vec<Vec<usize>>,
    ) -> anyhow::Result<()> {
        for comb in combinations.iter() {
            let batch_iter1 = batch_iter_builder()?;
            let batch_iter2 = batch_iter_builder()?;

            let schema = batch_iter1.schema();
            let col_types = schema
                .fields()
                .iter()
                .map(|f| f.data_type().clone())
                .collect_vec();

            if comb
                .iter()
                .all(|&idx| !Self::is_type_supported(&col_types[idx]))
            {
                let mut row_cnt = 0;

                let mut hll = HyperLogLog::new(hyperloglog::DEFAULT_PRECISION);
                let mut mg: MisraGries<Vec<Option<Value>>> =
                    MisraGries::new(misragries::DEFAULT_K_TO_TRACK);
                let mut null_cnt = 0;

                // 1. First pass: HLL + MG + null_cnt + row_cnt.
                for batch in batch_iter1 {
                    let batch = batch?;
                    row_cnt += batch.num_rows();

                    Self::generate_partial_stats_for_comb(
                        Self::get_col_projection(batch, comb),
                        &col_types,
                        &mut mg,
                        &mut hll,
                        &mut null_cnt,
                    );
                }

                let mfk: Vec<Vec<Option<Value>>> =
                    mg.most_frequent_keys().into_iter().cloned().collect();
                let mut cnt = Counter::new(&mfk);

                // 2. Second pass: MCV.
                for batch in batch_iter2 {
                    let batch = batch?;

                    Self::generate_stats_for_comb(
                        Self::get_col_projection(batch, comb),
                        &col_types,
                        &mut cnt,
                    );
                }

                // 3. Assemble stats.
                // Any distribution type (TDigest) to silence type checker...
                let stats = PerColumnStats::<Counter<Vec<Option<Value>>>, TDigest>::new_comb(
                    cnt,
                    hll.n_distinct(),
                    null_cnt as f64 / row_cnt as f64,
                );

                self.column_combi_stats.insert(comb.to_vec(), stats);
            }
        }

        Ok(())
    }

    fn is_type_supported(data_type: &DataType) -> bool {
        matches!(
            data_type,
            DataType::Boolean
                | DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::Float32
                | DataType::Float64
                | DataType::Utf8
        )
    }

    /// Matches to the typed column.
    fn to_typed_column(col: &Arc<dyn Array>, col_type: &DataType) -> Vec<Option<Value>> {
        macro_rules! simple_col_cast {
            ({ $col:expr, $array_type:path, $value_type:path}) => {
                $col.as_any()
                    .downcast_ref::<$array_type>()
                    .unwrap()
                    .iter()
                    .map(|x| x.map($value_type))
                    .collect::<Vec<_>>()
            };
        }

        macro_rules! float_col_cast {
            ({ $col:expr}) => {
                $col.as_any()
                    .downcast_ref::<Float32Array>()
                    .unwrap()
                    .iter()
                    .map(|x| {
                        x.map(|y| {
                            Value::Float(SerializableOrderedF64(OrderedFloat::from(y as f64)))
                        })
                    })
                    .collect::<Vec<_>>()
            };
        }

        macro_rules! utf8_col_cast {
            ({ $col:expr}) => {
                col.as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .iter()
                    .map(|x| x.map(|y| Value::String(y.to_string().into())))
                    .collect::<Vec<_>>()
            };
        }

        match col_type {
            DataType::Boolean => simple_col_cast!({col, BooleanArray, Value::Bool}),
            DataType::Int8 => simple_col_cast!({col, Int8Array, Value::Int8}),
            DataType::Int16 => simple_col_cast!({col, Int16Array, Value::Int16}),
            DataType::Int32 => simple_col_cast!({col, Int32Array, Value::Int32}),
            DataType::UInt8 => simple_col_cast!({col, UInt8Array, Value::UInt8}),
            DataType::UInt16 => simple_col_cast!({col, UInt16Array, Value::UInt16}),
            DataType::UInt32 => simple_col_cast!({col, UInt32Array, Value::UInt32}),
            DataType::Float32 => float_col_cast!({ col }),
            DataType::Float64 => float_col_cast!({ col }),
            DataType::Date32 => simple_col_cast!({col, Date32Array, Value::Date32}),
            DataType::Utf8 => utf8_col_cast!({ col }),
            _ => unreachable!(),
        }
    }

    fn get_typed_rows(
        cols: Vec<Arc<dyn Array>>,
        col_types: &Vec<DataType>,
    ) -> Vec<Vec<Option<Value>>> {
        let mut rows = Vec::<Vec<Option<Value>>>::new();
        for (col, col_type) in cols.iter().zip(col_types.iter()) {
            for (idx, val) in Self::to_typed_column(col, col_type).iter().enumerate() {
                rows[idx].push(val.clone());
            }
        }

        rows
    }

    /// Generate partial statistics for a combination of columns.
    fn generate_partial_stats_for_comb(
        cols: Vec<Arc<dyn Array>>,
        col_types: &Vec<DataType>,
        mg: &mut MisraGries<Vec<Option<Value>>>,
        hll: &mut HyperLogLog,
        null_count: &mut i32,
    ) {
        let rows = Self::get_typed_rows(cols, col_types);

        let (fully_null_rows, non_fully_null_rows): (Vec<_>, Vec<_>) = rows
            .into_iter()
            .partition(|row| row.iter().all(|v| v.is_none()));

        *null_count += fully_null_rows.len() as i32;

        mg.aggregate(&non_fully_null_rows);
        hll.aggregate(&non_fully_null_rows);
    }

    fn generate_stats_for_comb(
        cols: Vec<Arc<dyn Array>>,
        col_types: &Vec<DataType>,
        cnt: &mut Counter<Vec<Option<Value>>>,
    ) {
        let rows = Self::get_typed_rows(cols, col_types);

        cnt.aggregate(&rows);
    }

    /// Generate partial statistics for a column.
    fn generate_partial_stats_for_column(
        col: &Arc<dyn Array>,
        col_type: &DataType,
        mg: &mut MisraGries<Value>,
        hll: &mut HyperLogLog,
    ) {
        macro_rules! generate_partial_stats_for_col {
            ({ $col:expr, $mg:expr, $hll:expr, $array_type:path, $value_type:path}) => {{
                let array = $col.as_any().downcast_ref::<$array_type>().unwrap();

                // Filter out `None` values.
                let mg_values = array
                    .iter()
                    .flatten()
                    .map(|x| $value_type(x))
                    .collect::<Vec<_>>();
                let hll_values = array.iter().flatten().collect::<Vec<_>>();

                $mg.aggregate(&mg_values);
                $hll.aggregate(&hll_values);
            }};
        }

        match col_type {
            DataType::Boolean => {
                generate_partial_stats_for_col!({ col, mg, hll, BooleanArray, Value::Bool })
            }
            DataType::Int8 => {
                generate_partial_stats_for_col!({ col, mg, hll, Int8Array, Value::Int8 })
            }
            DataType::Int16 => {
                generate_partial_stats_for_col!({ col, mg, hll, Int16Array, Value::Int16 })
            }
            DataType::Int32 => {
                generate_partial_stats_for_col!({ col, mg, hll, Int32Array, Value::Int32 })
            }
            DataType::UInt8 => {
                generate_partial_stats_for_col!({ col, mg, hll, UInt8Array, Value::UInt8 })
            }
            DataType::UInt16 => {
                generate_partial_stats_for_col!({ col, mg, hll, UInt16Array, Value::UInt16 })
            }
            DataType::UInt32 => {
                generate_partial_stats_for_col!({ col, mg, hll, UInt32Array, Value::UInt32 })
            }
            DataType::Float32 => {
                let array = col.as_any().downcast_ref::<Float32Array>().unwrap();

                let mg_values = array
                    .iter()
                    .flatten()
                    .map(|x| Value::Float(SerializableOrderedF64(OrderedFloat::from(x as f64))))
                    .collect::<Vec<_>>();
                let hll_values = array.iter().flatten().collect::<Vec<_>>();

                mg.aggregate(&mg_values);
                hll.aggregate(&hll_values);
            }
            DataType::Float64 => {
                let array = col.as_any().downcast_ref::<Float32Array>().unwrap();

                let mg_values = array
                    .iter()
                    .flatten()
                    .map(|x| Value::Float(SerializableOrderedF64(OrderedFloat::from(x as f64))))
                    .collect::<Vec<_>>();
                let hll_values = array.iter().flatten().collect::<Vec<_>>();

                mg.aggregate(&mg_values);
                hll.aggregate(&hll_values);
            }
            DataType::Date32 => {
                generate_partial_stats_for_col!({ col, mg, hll, Date32Array, Value::Date32 })
            }
            DataType::Utf8 => {
                let array = col.as_any().downcast_ref::<StringArray>().unwrap();

                let mg_values = array
                    .iter()
                    .flatten()
                    .map(|x| x.to_string())
                    .map(|x| Value::String(x.into()))
                    .collect::<Vec<_>>();
                let hll_values = array
                    .iter()
                    .flatten()
                    .map(|x| x.to_string())
                    .collect::<Vec<_>>();

                mg.aggregate(&mg_values);
                hll.aggregate(&hll_values);
            }

            _ => unreachable!(),
        }
    }

    /// Generate statistics for a column.
    fn generate_stats_for_column(
        col: &Arc<dyn Array>,
        col_type: &DataType,
        distr: &mut Option<TDigest>,
        cnt: &mut Counter<Vec<Option<Value>>>,
    ) {
        macro_rules! generate_stats_for_col {
            ({ $col:expr, $distr:expr, $array_type:path, $to_f64:ident, $value_type:path }) => {{
                let array = $col.as_any().downcast_ref::<$array_type>().unwrap();
                // Filter out `None` values.
                let distr_values = array.iter().flatten().collect::<Vec<_>>();
                let cnt_values = array
                    .iter()
                    .filter(|e| !e.is_none())
                    .map(|x| Some($value_type(x.unwrap())))
                    .collect::<Vec<_>>();

                *$distr = {
                    let mut f64_values =
                        distr_values.iter().map(|x| $to_f64(*x)).collect::<Vec<_>>();
                    Some($distr.take().unwrap().merge_values(&mut f64_values))
                };
                cnt.aggregate(&[cnt_values]);
            }};
        }

        /// Convert a value to f64 with no out of range or precision loss.
        fn to_f64_safe<T: Into<f64>>(val: T) -> f64 {
            val.into()
        }

        fn str_to_f64(string: &str) -> f64 {
            arith_encoder::encode(string)
        }

        match col_type {
            DataType::Boolean => {
                generate_stats_for_col!({ col, distr, BooleanArray, to_f64_safe, Value::Bool })
            }
            DataType::Int8 => {
                generate_stats_for_col!({ col, distr, Int8Array, to_f64_safe, Value::Int8 })
            }
            DataType::Int16 => {
                generate_stats_for_col!({ col, distr, Int16Array, to_f64_safe, Value::Int16 })
            }
            DataType::Int32 => {
                generate_stats_for_col!({ col, distr, Int32Array, to_f64_safe, Value::Int32 })
            }
            DataType::UInt8 => {
                generate_stats_for_col!({ col, distr, UInt8Array, to_f64_safe, Value::UInt8 })
            }
            DataType::UInt16 => {
                generate_stats_for_col!({ col, distr, UInt16Array, to_f64_safe, Value::UInt16 })
            }
            DataType::UInt32 => {
                generate_stats_for_col!({ col, distr, UInt32Array, to_f64_safe, Value::UInt32 })
            }
            DataType::Float32 => {
                let array = col.as_any().downcast_ref::<Float32Array>().unwrap();

                let distr_values = array.iter().flatten().collect::<Vec<_>>();
                let cnt_values = array
                    .iter()
                    .filter(|e| !e.is_none())
                    .map(|x| {
                        Some(Value::Float(SerializableOrderedF64(OrderedFloat::from(
                            x.unwrap() as f64,
                        ))))
                    })
                    .collect::<Vec<_>>();

                *distr = {
                    let mut f64_values = distr_values
                        .iter()
                        .map(|x| to_f64_safe(*x))
                        .collect::<Vec<_>>();
                    Some(distr.take().unwrap().merge_values(&mut f64_values))
                };
                cnt.aggregate(&[cnt_values]);
            }
            DataType::Float64 => {
                let array = col.as_any().downcast_ref::<Float32Array>().unwrap();

                let distr_values = array.iter().flatten().collect::<Vec<_>>();
                let cnt_values = array
                    .iter()
                    .filter(|e| !e.is_none())
                    .map(|x| {
                        Some(Value::Float(SerializableOrderedF64(OrderedFloat::from(
                            x.unwrap() as f64,
                        ))))
                    })
                    .collect::<Vec<_>>();

                *distr = {
                    let mut f64_values = distr_values
                        .iter()
                        .map(|x| to_f64_safe(*x))
                        .collect::<Vec<_>>();
                    Some(distr.take().unwrap().merge_values(&mut f64_values))
                };
                cnt.aggregate(&[cnt_values]);
            }
            DataType::Date32 => {
                generate_stats_for_col!({ col, distr, Date32Array, to_f64_safe, Value::Date32 })
            }
            DataType::Utf8 => {
                let array = col.as_any().downcast_ref::<StringArray>().unwrap();

                let distr_values = array
                    .iter()
                    .flatten()
                    .map(|x| x.to_string())
                    .collect::<Vec<_>>();
                let cnt_values = array
                    .iter()
                    .filter(|e| !e.is_none())
                    .map(|x| Some(Value::String(x.unwrap().to_string().into())))
                    .collect::<Vec<_>>();

                *distr = {
                    let mut f64_values = distr_values
                        .iter()
                        .map(|x| str_to_f64(x))
                        .collect::<Vec<_>>();
                    Some(distr.take().unwrap().merge_values(&mut f64_values))
                };
                cnt.aggregate(&[cnt_values]);
            }
            _ => unreachable!(),
        }
    }
}
