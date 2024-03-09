use crate::tpch::TpchConfig;

pub enum Benchmark {
    Test,
    Tpch(TpchConfig),
}

impl Benchmark {
    pub fn get_strid(&self) -> String {
        match self {
            Self::Test => String::from("test"),
            Self::Tpch(tpch_cfg) => format!("tpch_{}", tpch_cfg.get_strid()),
        }
    }

    pub fn is_readonly(&self) -> bool {
        match self {
            Self::Test => true,
            Self::Tpch(_) => true,
        }
    }
}
