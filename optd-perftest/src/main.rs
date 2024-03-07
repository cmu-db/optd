/// Get the Q-error of a query when using optd-datafusion-repr's cost model
/// Q-error is defined in [Leis 2015](https://15721.courses.cs.cmu.edu/spring2024/papers/16-costmodels/p204-leis.pdf)
/// One detail not specified in the paper is that Q-error is based on the ratio of true and estimated cardinality
///   of the entire query, not of a subtree of the query. This detail is specified in Section 7.1 of
///   [Yang 2020](https://arxiv.org/pdf/2006.08109.pdf)
fn get_optd_datafusion_qerror() {
    let datafusion_db = DatafusionDb::new();
    println!("hi");
}

fn main() {
    println!("Hello, world!");
    get_optd_datafusion_qerror();
}
