use std::env;

use diesel::prelude::*;
use dotenvy::dotenv;
use optd::storage::models::{logical_operators::LogicalOpDesc, physical_operators::PhysicalOpDesc};
pub fn establish_connection() -> SqliteConnection {
    dotenv().ok();

    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    SqliteConnection::establish(&database_url)
        .unwrap_or_else(|_| panic!("Error connecting to {}", database_url))
}

fn main() -> anyhow::Result<()> {
    let mut conn = establish_connection();
    {
        use optd::storage::schema::logical_op_descs::dsl::*;
        let descs = logical_op_descs
            .select(LogicalOpDesc::as_select())
            .load(&mut conn)?;

        println!("logical operator support (n={})", descs.len());
        for desc in descs {
            println!("+ {}", desc.name);
        }
    }

    {
        use optd::storage::schema::physical_op_descs::dsl::*;
        let descs = physical_op_descs
            .select(PhysicalOpDesc::as_select())
            .load(&mut conn)?;

        println!("physical operator support (n={})", descs.len());
        for desc in descs {
            println!("+ {}", desc.name);
        }
    }

    Ok(())
}
