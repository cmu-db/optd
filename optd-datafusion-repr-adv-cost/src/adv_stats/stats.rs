use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::{DataType, Schema, SchemaRef};
use datafusion::arrow::array::{
    Array, BooleanArray, Date32Array, Float32Array, Float64Array, Int16Array, Int32Array,
    Int8Array, RecordBatch, StringArray, UInt16Array, UInt32Array, UInt8Array,
};
use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReader;
use itertools::Itertools;
use optd_core::nodes::{SerializableOrderedF64, Value};
use optd_gungnir::stats::counter::Counter;
use optd_gungnir::stats::hyperloglog::{self, HyperLogLog};
use optd_gungnir::stats::misragries::{self, MisraGries};
use optd_gungnir::stats::tdigest::{self, TDigest};
use ordered_float::OrderedFloat;
use rayon::prelude::*;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

// The "standard" concrete types that optd currently uses.
// All of optd (except unit tests) must use the same types.
pub type DataFusionMostCommonValues = Counter<Vec<Option<Value>>>;
pub type DataFusionDistribution = TDigest<Value>;

pub type DataFusionBaseTableStats =
    BaseTableStats<DataFusionMostCommonValues, DataFusionDistribution>;
pub type DataFusionPerTableStats = TableStats<DataFusionMostCommonValues, DataFusionDistribution>;

/// A more general interface meant to perform the task of a histogram.
//.
/// This more general interface is still compatible with histograms but allows
/// more powerful statistics like TDigest.
/// Ideally, MostCommonValues would have trait bounds for Serialize and Deserialize. However, I have
/// not figured
//    out how to both have Deserialize as a trait bound and utilize the Deserialize macro, because
// the Deserialize    trait involves lifetimes.
pub trait Distribution: 'static + Send + Sync {
    // Give the probability of a random value sampled from the distribution being <= `value`
    fn cdf(&self, value: &Value) -> f64;
}

impl Distribution for TDigest<Value> {
    fn cdf(&self, value: &Value) -> f64 {
        let nb_rows = self.norm_weight;
        if nb_rows == 0 {
            self.cdf(value)
        } else {
            self.centroids.len() as f64 * self.cdf(value) / nb_rows as f64
        }
    }
}

// Some values in a column combination can be null.
pub type ColumnsIdx = Vec<usize>;
pub type ColumnsType = Vec<DataType>;
pub type ColumnCombValue = Vec<Option<Value>>;

/// Ideally, MostCommonValues would have trait bounds for Serialize and Deserialize. However, I have
/// not figured   out how to both have Deserialize as a trait bound and utilize the Deserialize
/// macro, because the Deserialize   trait involves lifetimes.
pub trait MostCommonValues: 'static + Send + Sync {
    // it is true that we could just expose freq_over_pred() and use that for freq() and
    // total_freq() however, freq() and total_freq() each have potential optimizations (freq()
    // is O(1) instead of     O(n) and total_freq() can be cached)
    // additionally, it makes sense to return an Option<f64> for freq() instead of just 0 if value
    // doesn't exist thus, I expose three different functions
    fn freq(&self, value: &ColumnCombValue) -> Option<f64>;
    fn total_freq(&self) -> f64;
    fn freq_over_pred(&self, pred: Box<dyn Fn(&ColumnCombValue) -> bool>) -> f64;

    // returns the # of entries (i.e. value + freq) in the most common values structure
    fn cnt(&self) -> usize;
}

impl MostCommonValues for Counter<ColumnCombValue> {
    fn freq(&self, value: &ColumnCombValue) -> Option<f64> {
        self.frequencies().get(value).copied()
    }

    fn total_freq(&self) -> f64 {
        self.frequencies().values().sum()
    }

    fn freq_over_pred(&self, pred: Box<dyn Fn(&ColumnCombValue) -> bool>) -> f64 {
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

enum StatType {
    Full,    // Mcvs, distr, n_distinct, null_frac.
    Partial, // Only mcvs, n_distinct, null_frac.
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ColumnCombValueStats<M: MostCommonValues, D: Distribution> {
    pub mcvs: M,          // Does NOT contain full nulls.
    pub distr: Option<D>, // Does NOT contain mcvs; optional.
    pub ndistinct: u64,   // Does NOT contain full nulls.
    pub null_frac: f64,   // % of full nulls.
}

impl<M: MostCommonValues, D: Distribution> ColumnCombValueStats<M, D> {
    pub fn new(mcvs: M, ndistinct: u64, null_frac: f64, distr: Option<D>) -> Self {
        Self {
            mcvs,
            ndistinct,
            null_frac,
            distr,
        }
    }
}

#[serde_with::serde_as]
#[derive(Serialize, Deserialize, Debug)]
pub struct TableStats<
    M: MostCommonValues + Serialize + DeserializeOwned,
    D: Distribution + Serialize + DeserializeOwned,
> {
    pub row_cnt: usize,
    #[serde_as(as = "HashMap<serde_with::json::JsonString, _>")]
    pub column_comb_stats: HashMap<ColumnsIdx, ColumnCombValueStats<M, D>>,
}

impl<
        M: MostCommonValues + Serialize + DeserializeOwned,
        D: Distribution + Serialize + DeserializeOwned,
    > TableStats<M, D>
{
    pub fn new(
        row_cnt: usize,
        column_comb_stats: HashMap<ColumnsIdx, ColumnCombValueStats<M, D>>,
    ) -> Self {
        Self {
            row_cnt,
            column_comb_stats,
        }
    }
}

pub type BaseTableStats<M, D> = HashMap<String, TableStats<M, D>>;

type FirstPassState = (
    Vec<HyperLogLog<ColumnCombValue>>,
    Vec<MisraGries<ColumnCombValue>>,
    Vec<i32>,
);

type SecondPassState = (
    Vec<Option<TDigest<Value>>>,
    Vec<Counter<ColumnCombValue>>,
    Vec<i32>,
);

impl TableStats<Counter<ColumnCombValue>, TDigest<Value>> {
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

    fn first_pass_stats_id(nb_stats: usize) -> anyhow::Result<FirstPassState> {
        Ok((
            vec![HyperLogLog::<ColumnCombValue>::new(hyperloglog::DEFAULT_PRECISION); nb_stats],
            vec![MisraGries::<ColumnCombValue>::new(misragries::DEFAULT_K_TO_TRACK); nb_stats],
            vec![0; nb_stats],
        ))
    }

    fn second_pass_stats_id(
        comb_stat_types: &[(Vec<usize>, Vec<DataType>, StatType)],
        mgs: &[MisraGries<ColumnCombValue>],
        nb_stats: usize,
    ) -> anyhow::Result<SecondPassState> {
        Ok((
            comb_stat_types
                .iter()
                .map(|(_, _, stat_type)| match stat_type {
                    StatType::Full => Some(TDigest::new(tdigest::DEFAULT_COMPRESSION)),
                    StatType::Partial => None,
                })
                .collect(),
            mgs.iter()
                .map(|mg| {
                    let mfk = mg.most_frequent_keys().into_iter().cloned().collect_vec();
                    Counter::new(&mfk)
                })
                .collect(),
            vec![0; nb_stats],
        ))
    }

    fn get_stats_types(
        combinations: &[ColumnsIdx],
        schema: &SchemaRef,
    ) -> Vec<(ColumnsIdx, ColumnsType, StatType)> {
        let col_types: Vec<DataType> = schema
            .fields()
            .iter()
            .map(|f| f.data_type().clone())
            .collect();

        combinations
            .iter()
            .map(|cols_idx| {
                let cols_type: Vec<DataType> =
                    cols_idx.iter().map(|&col| col_types[col].clone()).collect();
                let stat_type = if cols_idx.len() == 1 {
                    StatType::Full
                } else {
                    StatType::Partial
                };

                (cols_idx.clone(), cols_type, stat_type)
            })
            .filter(|(_, cols_type, _)| cols_type.iter().all(Self::is_type_supported))
            .collect()
    }

    fn to_typed_column(col: &Arc<dyn Array>, col_type: &DataType) -> Vec<Option<Value>> {
        macro_rules! simple_col_cast {
            ({ $col:expr, $array_type:path, $value_type:path }) => {
                $col.as_any()
                    .downcast_ref::<$array_type>()
                    .unwrap()
                    .iter()
                    .map(|x| x.map($value_type))
                    .collect_vec()
            };
        }

        macro_rules! float_col_cast {
            ({ $col:expr, $array_type:path }) => {
                $col.as_any()
                    .downcast_ref::<$array_type>()
                    .unwrap()
                    .iter()
                    .map(|x| {
                        x.map(|y| {
                            Value::Float(SerializableOrderedF64(OrderedFloat::from(y as f64)))
                        })
                    })
                    .collect_vec()
            };
        }

        macro_rules! utf8_col_cast {
            ({ $col:expr }) => {
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
            DataType::Float32 => float_col_cast!({ col, Float32Array }),
            DataType::Float64 => float_col_cast!({ col, Float64Array }),
            DataType::Date32 => simple_col_cast!({col, Date32Array, Value::Date32}),
            DataType::Utf8 => utf8_col_cast!({ col }),
            _ => unreachable!(),
        }
    }

    fn get_column_combs(
        batch: &RecordBatch,
        comb_stat_types: &[(ColumnsIdx, ColumnsType, StatType)],
    ) -> Vec<Vec<ColumnCombValue>> {
        comb_stat_types
            .iter()
            .map(|(comb, types, _)| {
                let mut column_comb_values =
                    vec![ColumnCombValue::with_capacity(comb.len()); batch.num_rows()];

                for (&col_idx, typ) in comb.iter().zip(types.iter()) {
                    let column_values = Self::to_typed_column(batch.column(col_idx), typ);

                    for (row_values, value) in
                        column_comb_values.iter_mut().zip(column_values.iter())
                    {
                        // This redundant copy is faster than making to_typed_column return an
                        // iterator!
                        row_values.push(value.clone());
                    }
                }

                column_comb_values
            })
            .collect()
    }

    fn generate_partial_stats(
        column_combs: &[Vec<ColumnCombValue>],
        mgs: &mut [MisraGries<ColumnCombValue>],
        hlls: &mut [HyperLogLog<ColumnCombValue>],
        null_counts: &mut [i32],
    ) {
        column_combs
            .iter()
            .zip(mgs)
            .zip(hlls)
            .zip(null_counts)
            .for_each(|(((column_comb, mg), hll), count)| {
                let filtered_nulls = column_comb
                    .iter()
                    .filter(|row| row.iter().any(|val| val.is_some()));

                *count += column_comb.len() as i32;

                filtered_nulls.for_each(|e| {
                    mg.insert_element(e, 1);
                    hll.process(e);
                    *count -= 1;
                });
            });
    }

    fn generate_full_stats(
        column_combs: &[Vec<ColumnCombValue>],
        cnts: &mut [Counter<ColumnCombValue>],
        distrs: &mut [Option<TDigest<Value>>],
        row_counts: &mut [i32],
    ) {
        column_combs
            .iter()
            .zip(cnts)
            .zip(distrs)
            .zip(row_counts)
            .for_each(|(((column_comb, cnt), distr), count)| {
                let nb_rows = column_comb.len() as i32;
                *count += nb_rows;
                cnt.aggregate(column_comb);

                if let Some(d) = distr.as_mut() {
                    let filtered_values: Vec<_> = column_comb
                        .iter()
                        .filter(|row| !cnt.is_tracking(row))
                        .filter_map(|row| row.first().and_then(|v| v.as_ref()))
                        .cloned()
                        .collect();

                    d.norm_weight += nb_rows as usize;
                    d.merge_values(&filtered_values);
                }
            });
    }

    pub fn from_record_batches(
        first_batch_reader: impl FnOnce() -> Vec<ParquetRecordBatchReader>,
        second_batch_reader: impl FnOnce() -> Vec<ParquetRecordBatchReader>,
        combinations: Vec<ColumnsIdx>,
        schema: Arc<Schema>,
    ) -> anyhow::Result<Self> {
        let comb_stat_types = Self::get_stats_types(&combinations, &schema);
        let nb_stats = comb_stat_types.len();

        // 1. FIRST PASS: hlls + mgs + null_cnts.
        let local_partial_stats: Vec<_> = first_batch_reader()
            .into_par_iter()
            .map(|group| {
                group.fold(Self::first_pass_stats_id(nb_stats), |local_stats, batch| {
                    let mut local_stats = local_stats?;

                    match batch {
                        Ok(batch) => {
                            let (hlls, mgs, null_cnts) = &mut local_stats;
                            let comb = Self::get_column_combs(&batch, &comb_stat_types);
                            Self::generate_partial_stats(&comb, mgs, hlls, null_cnts);
                            Ok(local_stats)
                        }
                        Err(e) => Err(e.into()),
                    }
                })
            })
            .collect();

        let (hlls, mgs, null_cnts) = local_partial_stats.into_iter().fold(
            Self::first_pass_stats_id(nb_stats),
            |final_stats, local_stats| {
                let mut final_stats = final_stats?;
                let local_stats = local_stats?;

                let (final_hlls, final_mgs, final_counts) = &mut final_stats;
                let (local_hlls, local_mgs, local_counts) = local_stats;

                for i in 0..nb_stats {
                    final_hlls[i].merge(&local_hlls[i]);
                    final_mgs[i].merge(&local_mgs[i]);
                    final_counts[i] += local_counts[i];
                }

                Ok(final_stats)
            },
        )?;

        // 2. SECOND PASS: mcv + tdigest + row_cnts.
        let local_final_stats: Vec<_> = second_batch_reader()
            .into_par_iter()
            .map(|group| {
                group.fold(
                    Self::second_pass_stats_id(&comb_stat_types, &mgs, nb_stats),
                    |local_stats, batch| {
                        let mut local_stats = local_stats?;

                        match batch {
                            Ok(batch) => {
                                let (distrs, cnts, row_cnts) = &mut local_stats;
                                let comb = Self::get_column_combs(&batch, &comb_stat_types);
                                Self::generate_full_stats(&comb, cnts, distrs, row_cnts);
                                Ok(local_stats)
                            }
                            Err(e) => Err(e.into()),
                        }
                    },
                )
            })
            .collect();

        let (distrs, cnts, row_cnts) = local_final_stats.into_iter().fold(
            Self::second_pass_stats_id(&comb_stat_types, &mgs, nb_stats),
            |final_stats, local_stats| {
                let mut final_stats = final_stats?;
                let local_stats = local_stats?;

                let (final_distrs, final_cnts, final_counts) = &mut final_stats;
                let (local_distrs, local_cnts, local_counts) = local_stats;

                for i in 0..nb_stats {
                    final_cnts[i].merge(&local_cnts[i]);
                    if let (Some(final_distr), Some(local_distr)) =
                        (&mut final_distrs[i], &local_distrs[i])
                    {
                        final_distr.merge(local_distr);
                        final_distr.norm_weight += local_distr.norm_weight;
                    }

                    final_counts[i] += local_counts[i];
                }

                Ok(final_stats)
            },
        )?;

        // 3. ASSEMBLE STATS.
        let row_cnt = row_cnts[0];
        let mut column_comb_stats = HashMap::new();

        let iter_comb = comb_stat_types
            .into_iter()
            .map(|(comb, _, _)| comb)
            .zip(cnts)
            .zip(distrs)
            .zip(hlls)
            .zip(null_cnts.iter())
            .map(|((((comb, cnt), distr), hll), null_cnt)| {
                (comb, cnt, distr, hll, *null_cnt as f64)
            });

        for (comb, cnt, distr, hll, null_cnt) in iter_comb {
            let column_stats = ColumnCombValueStats::new(
                cnt,
                hll.n_distinct(),
                null_cnt / (row_cnt as f64),
                distr,
            );
            column_comb_stats.insert(comb, column_stats);
        }

        Ok(Self {
            row_cnt: row_cnt as usize,
            column_comb_stats,
        })
    }
}
