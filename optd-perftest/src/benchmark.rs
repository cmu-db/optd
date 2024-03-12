use crate::tpch::TpchConfig;

pub enum Benchmark {
    #[allow(dead_code)]
    Test,
    Tpch(TpchConfig),
}

impl Benchmark {
    pub fn get_stringid(&self) -> String {
        match self {
            Self::Test => String::from("test"),
            Self::Tpch(tpch_config) => format!("tpch_{}", tpch_config.get_stringid()),
        }
    }

    pub fn is_readonly(&self) -> bool {
        match self {
            Self::Test => true,
            Self::Tpch(_) => true,
        }
    }
}
