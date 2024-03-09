use crate::tpch::TpchConfig;

pub enum Benchmark {
    Test,
    Tpch(TpchConfig),
}