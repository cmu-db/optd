use cardtest::{CardtestRunner, CardtestRunnerDBHelper};
use futures::executor::block_on;
use optd_sqlplannertest::DatafusionDb;
use postgres::PostgresDb;

mod cardtest;
mod postgres;

fn main() {
    let databases: Vec<Box<dyn CardtestRunnerDBHelper>> = vec![
        Box::new(block_on(PostgresDb::new()).unwrap()),
        Box::new(block_on(DatafusionDb::new()).unwrap()),
    ];
    let cardtest_runner = block_on(CardtestRunner::new(databases)).unwrap();
    let qerrors = block_on(cardtest_runner.eval_qerrors("")).unwrap();
    println!("qerrors: {:?}", qerrors);
}
