//! This module provides utilities to construct a simple [`IRContext`].
//! In particular,
//! - a cardinality estimator [`MagicCardinalityEstimator`] primary based on magic numbers,
//! - a cost model [`MagicCostModel`] primary based on magic numbers,
//! - a mock catalog implementation [`MagicCatalog`]

mod card;
mod cat;
mod cm;

use std::sync::Arc;

use crate::ir::{DataType, catalog::*};
pub use card::MagicCardinalityEstimator;
pub use cat::MagicCatalog;
pub use cm::MagicCostModel;

use crate::ir::IRContext;

impl IRContext {
    pub fn with_empty_magic() -> Self {
        Self::new(
            Arc::new(MagicCatalog::default()),
            Arc::new(MagicCardinalityEstimator),
            Arc::new(MagicCostModel),
        )
    }

    pub fn with_magic_catalog(cat: MagicCatalog) -> Self {
        Self::new(
            Arc::new(cat),
            Arc::new(MagicCardinalityEstimator),
            Arc::new(MagicCostModel),
        )
    }
    pub fn with_course_tables() -> Self {
        let mut catalog = MagicCatalog::default();
        let course = {
            let schema = Arc::new(SchemaDescription::from_iter([
                ("course.id".to_string(), DataType::Int32),
                ("course.credit".to_string(), DataType::Int32),
            ]));

            catalog
                .try_create_table("course".to_string(), schema)
                .unwrap()
        };
        catalog.set_table_row_count(course, 10);

        let schedule = {
            let schema = Arc::new(SchemaDescription::from_iter([
                ("schedule.day_of_week".to_string(), DataType::Int32),
                ("schedule.course_id".to_string(), DataType::Int32),
                ("schedule.has_lecture".to_string(), DataType::Boolean),
            ]));

            catalog
                .try_create_table("schedule".to_string(), schema)
                .unwrap()
        };
        catalog.set_table_row_count(schedule, 25);

        let staff = {
            let schema = Arc::new(SchemaDescription::from_iter([
                ("staff.id".to_string(), DataType::Int32),
                ("staff.oh_day_of_week".to_string(), DataType::Int32),
                ("staff.course_id".to_string(), DataType::Int32),
                ("staff.oh_length".to_string(), DataType::Int32),
            ]));

            catalog
                .try_create_table("staff".to_string(), schema)
                .unwrap()
        };
        catalog.set_table_row_count(staff, 200);
        Self::with_magic_catalog(catalog)
    }
    /// Creates a context with table `t1` to `t{count}`, each has `width` number of columns.
    pub fn with_numbered_tables(row_counts: Vec<usize>, width: usize) -> Self {
        let mut catalog = MagicCatalog::default();

        let mut create_numbered_table = |table_name: String, width: usize, row_count: usize| {
            let iter = (1..=width)
                .map(|column_no| (format!("{table_name}.v{column_no}"), DataType::Int32));
            let schema = Arc::new(SchemaDescription::from_iter(iter));
            let table_id = catalog.try_create_table(table_name, schema).unwrap();
            catalog.set_table_row_count(table_id, row_count);
        };

        for (i, row_count) in row_counts.iter().enumerate() {
            create_numbered_table(format!("t{i}"), width, *row_count);
        }

        Self::with_magic_catalog(catalog)
    }
}
