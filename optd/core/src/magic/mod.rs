//! This module provides utilities to construct a simple [`IRContext`].
//! In particular,
//! - a cardinality estimator [`MagicCardinalityEstimator`] primary based on magic numbers,
//! - a cost model [`MagicCostModel`] primary based on magic numbers,
//! - a mock catalog implementation [`MagicCatalog`]

mod card;
mod cat;
mod cm;

use std::sync::Arc;

use crate::ir::{
    DataType,
    catalog::*,
    statistics::{ColumnStatistics, TableStatistics},
};
pub use card::MagicCardinalityEstimator;
pub use cat::MagicCatalog;
pub use cm::MagicCostModel;
use itertools::Itertools;

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
        let catalog = MagicCatalog::default();
        let course = {
            let schema = Arc::new(Schema::new(vec![
                // TODO: these .to_strings are not required?
                Field::new("course.id".to_string(), DataType::Int32, false),
                Field::new("course.credit".to_string(), DataType::Int32, false),
            ]));

            catalog
                .try_create_table("course".to_string(), schema)
                .unwrap()
        };

        // catalog.set_table_row_count(course, 10);
        catalog.set_table_stats(
            course,
            TableStatistics {
                row_count: 10,
                column_statistics: vec![
                    ColumnStatistics {
                        column_id: 0,
                        column_type: DataType::Int32.to_string(),
                        name: "course.id".to_string(),
                        advanced_stats: vec![],
                        min_value: Some("0".to_string()),
                        max_value: Some("9".to_string()),
                        null_count: Some(0),
                        distinct_count: Some(10),
                    },
                    ColumnStatistics {
                        column_id: 0,
                        column_type: DataType::Int32.to_string(),
                        name: "course.credit".to_string(),
                        advanced_stats: vec![],
                        min_value: Some("3".to_string()),
                        max_value: Some("15".to_string()),
                        null_count: Some(0),
                        distinct_count: Some(5),
                    },
                ],
                size_bytes: Some((4 + 4) * 10),
            },
        );

        let schedule = {
            let schema = Arc::new(Schema::new(vec![
                Field::new("schedule.day_of_week".to_string(), DataType::Int32, false),
                Field::new("schedule.course_id".to_string(), DataType::Int32, false),
                Field::new("schedule.has_lecture".to_string(), DataType::Boolean, false),
            ]));

            catalog
                .try_create_table("schedule".to_string(), schema)
                .unwrap()
        };

        // catalog.set_table_row_count(schedule, 25);
        catalog.set_table_stats(
            schedule,
            TableStatistics {
                row_count: 25,
                column_statistics: vec![
                    ColumnStatistics {
                        column_id: 0,
                        column_type: DataType::Int32.to_string(),
                        name: "schedule.day_of_week".to_string(),
                        advanced_stats: vec![],
                        min_value: Some("0".to_string()),
                        max_value: Some("6".to_string()),
                        null_count: Some(0),
                        distinct_count: Some(7),
                    },
                    ColumnStatistics {
                        column_id: 0,
                        column_type: DataType::Int32.to_string(),
                        name: "schedule.course_id".to_string(),
                        advanced_stats: vec![],
                        min_value: Some("0".to_string()),
                        max_value: Some("9".to_string()),
                        null_count: Some(0),
                        distinct_count: Some(10),
                    },
                    ColumnStatistics {
                        column_id: 0,
                        column_type: DataType::Boolean.to_string(),
                        name: "schedule.has_lecture".to_string(),
                        advanced_stats: vec![],
                        min_value: Some("0".to_string()),
                        max_value: Some("1".to_string()),
                        null_count: Some(0),
                        distinct_count: Some(2),
                    },
                ],
                size_bytes: Some((4 + 4 + 1) * 10),
            },
        );

        let staff = {
            let schema = Arc::new(Schema::new(vec![
                Field::new("staff.id".to_string(), DataType::Int32, false),
                Field::new("staff.oh_day_of_week".to_string(), DataType::Int32, false),
                Field::new("staff.course_id".to_string(), DataType::Int32, false),
                Field::new("staff.oh_length".to_string(), DataType::Int32, false),
            ]));

            catalog
                .try_create_table("staff".to_string(), schema)
                .unwrap()
        };

        // catalog.set_table_row_count(staff, 200);
        catalog.set_table_stats(
            staff,
            TableStatistics {
                row_count: 200,
                column_statistics: vec![
                    ColumnStatistics {
                        column_id: 0,
                        column_type: DataType::Int32.to_string(),
                        name: "staff.id".to_string(),
                        advanced_stats: vec![],
                        min_value: Some("0".to_string()),
                        max_value: Some("199".to_string()),
                        null_count: Some(0),
                        distinct_count: Some(200),
                    },
                    ColumnStatistics {
                        column_id: 0,
                        column_type: DataType::Int32.to_string(),
                        name: "staff.oh_day_of_week".to_string(),
                        advanced_stats: vec![],
                        min_value: Some("0".to_string()),
                        max_value: Some("6".to_string()),
                        null_count: Some(0),
                        distinct_count: Some(7),
                    },
                    ColumnStatistics {
                        column_id: 0,
                        column_type: DataType::Int32.to_string(),
                        name: "staff.course_id".to_string(),
                        advanced_stats: vec![],
                        min_value: Some("0".to_string()),
                        max_value: Some("9".to_string()),
                        null_count: Some(0),
                        distinct_count: Some(10),
                    },
                    ColumnStatistics {
                        column_id: 0,
                        column_type: DataType::Int32.to_string(),
                        name: "staff.oh_length".to_string(),
                        advanced_stats: vec![],
                        min_value: Some("0".to_string()),
                        max_value: Some("2".to_string()),
                        null_count: Some(0),
                        distinct_count: Some(3),
                    },
                ],
                size_bytes: Some((4 + 4 + 4 + 4) * 10),
            },
        );

        Self::with_magic_catalog(catalog)
    }

    /// Creates a context with table `t1` to `t{count}`, each has `width` number of columns.
    pub fn with_numbered_tables(tables_statistics: Vec<TableStatistics>, width: usize) -> Self {
        let catalog = MagicCatalog::default();

        let create_numbered_table =
            |table_name: String, width: usize, table_statistics: TableStatistics| {
                let fields = (1..=width)
                    .map(|column_no| {
                        Field::new(format!("{table_name}.v{column_no}"), DataType::Int32, false)
                    })
                    .collect_vec();
                let schema = Arc::new(Schema::new(fields));
                let table_id = catalog.try_create_table(table_name, schema).unwrap();
                catalog.set_table_stats(table_id, table_statistics);
            };

        for (i, table_statistics) in tables_statistics.into_iter().enumerate() {
            create_numbered_table(format!("t{i}"), width, table_statistics);
        }

        Self::with_magic_catalog(catalog)
    }
}
