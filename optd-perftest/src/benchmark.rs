use crate::tpch::TpchConfig;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub enum Benchmark {
    #[allow(dead_code)]
    Test,
    Tpch(TpchConfig),
}

impl Benchmark {
    /// Get a unique string that deterministically describes the "data" of this benchmark.
    /// Note that benchmarks consist of "data" and "queries". This name is only for the data
    /// For instance, if you have two TPC-H benchmarks with the same scale factor and seed
    ///   but different queries, they could both share the same database and would thus
    ///   have the same dbname.
    /// This name must be compatible with the rules all databases have for their names, which
    ///   are described below:
    ///
    /// Postgres' rules:
    ///   - The name can only contain A-Z a-z 0-9 _ and cannot start with 0-9.
    ///   - There is a weird behavior where if you use CREATE DATABASE to create a database,
    ///     Postgres will convert uppercase letters to lowercase. However, if you use psql to
    ///     then connect to the database, Postgres will *not* convert capital letters to
    ///     lowercase. To resolve the inconsistency, the names output by this function will
    ///     *not* contain uppercase letters.
    pub fn get_dbname(&self) -> String {
        let dbname = match self {
            Self::Test => String::from("test"),
            Self::Tpch(tpch_config) => {
                format!("tpch_sf{}", tpch_config.scale_factor)
            }
        };
        // since Postgres names cannot contain periods
        let dbname = dbname.replace('.', "point");
        // due to the weird inconsistency with Postgres (see function comment)
        dbname.to_lowercase()
    }

    /// An ID is just a unique string identifying the benchmark
    /// It's not always used in the same situations as get_dbname(), so it's a separate function
    pub fn get_id(&self) -> String {
        // the fact that it happens to return dbname is an implementation detail
        self.get_dbname()
    }

    pub fn is_readonly(&self) -> bool {
        match self {
            Self::Test => true,
            Self::Tpch(_) => true,
        }
    }
}
