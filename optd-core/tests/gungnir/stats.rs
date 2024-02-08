//----------------------------------------------------//
//   This free (MIT) Software is provided to you by   //
//       _____                         _              //
//      / ____|                       (_)             //
//      | |  __ _   _ _ __   __ _ _ __  _ _ __        //
//      | | |_ | | | | '_ \ / _` | '_ \| | '__|       //
//      | |__| | |_| | | | | (_| | | | | | |          //
//       \_____|\__,_|_| |_|\__, |_| |_|_|_|          //
//                           __/ |                    //
//                          |___/                     //
//                                                    //
// Author: Alexis Schlomer <aschlome@andrew.cmu.edu>  //
//----------------------------------------------------//

use optd_core::gungnir::stats::compute_stats;
use optd_core::gungnir::stats::t_digest;

#[test]
fn run() {
    // TODO(Alexis) Just an access point... Generate benchmark with python script!
    compute_stats("optd-core/tests/gungnir/tpch_sf_1/customer.parquet");
    t_digest();
}
