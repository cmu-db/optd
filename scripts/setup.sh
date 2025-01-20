# cargo-binstall enables directly installing Rust binaries.
cargo install cargo-binstall && 
# diesel_cli is a CLI tool for managing database schema, runnig migrations, 
# and generating code. when working with Diesel (https://diesel.rs/).
cargo binstall diesel_cli &&
# Setup a new database and runs the migrations.
echo DATABASE_URL=test_memo.db > .env &&
diesel setup