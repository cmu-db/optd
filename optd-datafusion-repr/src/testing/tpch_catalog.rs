use core::panic;

use crate::plan_nodes::ConstantType;
use crate::properties::schema::{Catalog, Field, Schema};

pub struct TpchCatalog;

impl Catalog for TpchCatalog {
    fn get(&self, name: &str) -> Schema {
        match name {
            "region" => Schema {
                fields: vec![
                    Field {
                        name: "regionkey".to_string(),
                        typ: ConstantType::Int32,
                        nullable: false,
                    },
                    Field {
                        name: "name".to_string(),
                        typ: ConstantType::Utf8String,
                        nullable: false,
                    },
                    Field {
                        name: "comment".to_string(),
                        typ: ConstantType::Utf8String,
                        nullable: false,
                    },
                ],
            },
            "customer" => {
                // Define the schema for the "customer" table

                Schema {
                    fields: vec![
                        Field {
                            name: "custkey".to_string(),
                            typ: ConstantType::Int32,
                            nullable: false,
                        },
                        Field {
                            name: "name".to_string(),
                            typ: ConstantType::Utf8String,
                            nullable: false,
                        },
                        Field {
                            name: "address".to_string(),
                            typ: ConstantType::Utf8String,
                            nullable: false,
                        },
                        Field {
                            name: "nationkey".to_string(),
                            typ: ConstantType::Int32,
                            nullable: false,
                        },
                        Field {
                            name: "phone".to_string(),
                            typ: ConstantType::Utf8String,
                            nullable: false,
                        },
                        Field {
                            name: "acctbal".to_string(),
                            typ: ConstantType::Float64,
                            nullable: false,
                        },
                        Field {
                            name: "mktsegment".to_string(),
                            typ: ConstantType::Utf8String,
                            nullable: false,
                        },
                        Field {
                            name: "comment".to_string(),
                            typ: ConstantType::Utf8String,
                            nullable: false,
                        },
                    ],
                }
            }
            "orders" => {
                // Define the schema for the "orders" table

                Schema {
                    fields: vec![
                        Field {
                            name: "orderkey".to_string(),
                            typ: ConstantType::Int32,
                            nullable: false,
                        },
                        Field {
                            name: "custkey".to_string(),
                            typ: ConstantType::Int32,
                            nullable: false,
                        },
                        Field {
                            name: "orderstatus".to_string(),
                            typ: ConstantType::Utf8String,
                            nullable: false,
                        },
                        Field {
                            name: "totalprice".to_string(),
                            typ: ConstantType::Float64,
                            nullable: false,
                        },
                        Field {
                            name: "orderdate".to_string(),
                            typ: ConstantType::Date,
                            nullable: false,
                        },
                        Field {
                            name: "orderpriority".to_string(),
                            typ: ConstantType::Utf8String,
                            nullable: false,
                        },
                        Field {
                            name: "clerk".to_string(),
                            typ: ConstantType::Utf8String,
                            nullable: false,
                        },
                        Field {
                            name: "shippriority".to_string(),
                            typ: ConstantType::Int32,
                            nullable: false,
                        },
                        Field {
                            name: "comment".to_string(),
                            typ: ConstantType::Utf8String,
                            nullable: false,
                        },
                    ],
                }
            }
            // Add more cases for other tables as needed
            _ => {
                panic!("Unknown table: {}", name);
            }
        }
    }
}
