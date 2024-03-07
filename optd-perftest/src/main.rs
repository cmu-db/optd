use futures::executor::block_on;
use optd_sqlplannertest::DatafusionDb;

fn main() {
    let datafusion_db = block_on(DatafusionDb::new()).unwrap();
    let q_error = block_on(datafusion_db.get_qerror("")).unwrap();
    println!("q_error: {}", q_error);
}
