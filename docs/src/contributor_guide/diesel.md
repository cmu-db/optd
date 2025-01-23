# Working with diesel-rs

[Diesel](https://diesel.rs/) is an ORM framework we use to persist the core objects in the optd query optimizer. We chose to work with Diesel instead of other alternatives mainly for its compile-time safety guarantees, which is a good companion for our table-per-operator-kind model. 

This guide assumes that you already have the `sqlite3` binary installed.

## Setup

When working with Diesel for the first time, you could use the convenient setup scripts located at `scripts/setup.sh`. The script will install the Diesel CLI tool, generate a testing memo table database at project root, and run the Diesel setup script. 

For more details, follow the [Getting Started with Diesel](https://diesel.rs/guides/getting-started.html) guide.

## Making changes

To generate a new migration, use the following command:

```shell
diesel migration generate <migration_name>
```

Diesel CLI will create two empty files in the `optd-storgage/migrations` folder. You will see output that looks something like this:

```shell
Creating optd-storage/migrations/2025-01-20-153830_<migration_name>/up.sql
Creating optd-storage/migrations/2025-01-20-153830_<migration_name>/down.sql
```

The `up.sql` file should contain the changes you want to apply and `down.sql` should contain the command to revert the changes.

Before optd becomes stable, it is ok to directly modify the migrations themselves.

To apply the new migration, run:

```shell
diesel migration run
```

You can also check that if `down.sql` properly revert the change:

```shell
diesel migration redo [-n <REDO_NUMBER>]
```

You can also use the following command to revert changes:

```shell
diesel migration revert [-n <REVERT_NUMBER>]

## Adding a new operator

(TODO)

## Adding a new property

(TODO)
