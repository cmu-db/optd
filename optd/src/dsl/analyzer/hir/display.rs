//! Display implementations for HIR types.
//!
//! This module provides Display trait implementations for various HIR types,
//! enabling human-readable formatting of expressions, values, and patterns.

use super::map::Map;
use super::{
    BinOp, CoreData, FunKind, Goal, GroupId, Literal, LogicalOp, Materializable, NoMetadata,
    PhysicalOp, UnaryOp, Value,
};
use std::fmt;

impl fmt::Display for Value<NoMetadata> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.data {
            CoreData::Literal(lit) => match lit {
                Literal::Int64(i) => write!(f, "{}", i),
                Literal::Float64(fl) => write!(f, "{}", fl),
                Literal::String(s) => write!(f, "\"{}\"", s),
                Literal::Bool(b) => write!(f, "{}", b),
                Literal::Unit => write!(f, "()"),
            },
            CoreData::Array(items) => {
                write!(f, "[")?;
                for (i, item) in items.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", item)?;
                }
                write!(f, "]")
            }
            CoreData::Tuple(items) => {
                write!(f, "(")?;
                for (i, item) in items.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", item)?;
                }
                if items.len() == 1 {
                    write!(f, ",")?; // Add trailing comma for 1-tuples
                }
                write!(f, ")")
            }
            CoreData::Map(map) => format_map(f, map),
            CoreData::Struct(name, fields) => {
                write!(f, "{}(", name)?;
                for (i, field) in fields.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", field)?;
                }
                write!(f, ")")
            }
            CoreData::Function(fun_kind) => match fun_kind {
                FunKind::Closure(params, _) => {
                    write!(f, "λ(")?;
                    for (i, param) in params.iter().enumerate() {
                        if i > 0 {
                            write!(f, ", ")?;
                        }
                        write!(f, "{}", param)?;
                    }
                    write!(f, ") → ‹code›")
                }
                FunKind::Udf(_) => write!(f, "udf@native"),
            },
            CoreData::Fail(value) => write!(f, "fail({})", value),
            CoreData::Logical(materializable) => match materializable {
                Materializable::Materialized(logical_op) => format_logical_op(f, logical_op),
                Materializable::UnMaterialized(group_id) => {
                    write!(f, "group({})", group_id.0)
                }
            },
            CoreData::Physical(materializable) => match materializable {
                Materializable::Materialized(physical_op) => format_physical_op(f, physical_op),
                Materializable::UnMaterialized(goal) => format_goal(f, goal),
            },
            CoreData::None => write!(f, "none"),
        }
    }
}

// Helper function to format Map contents
fn format_map(f: &mut fmt::Formatter<'_>, map: &Map) -> fmt::Result {
    write!(f, "{{")?;
    let mut first = true;

    // Sort keys for deterministic output
    let mut entries: Vec<_> = map.inner.iter().collect();
    entries.sort_by(|a, b| format!("{:?}", a.0).cmp(&format!("{:?}", b.0)));

    for (key, value) in entries {
        if !first {
            write!(f, ", ")?;
        }
        first = false;

        // Format the key and value
        write!(f, "{:?} → {}", key, value)?;
    }
    write!(f, "}}")
}

// Helper function to format logical operators
fn format_logical_op(f: &mut fmt::Formatter<'_>, op: &LogicalOp<Value<NoMetadata>>) -> fmt::Result {
    write!(f, "{}(", op.operator.tag)?;

    // Format data
    for (i, data) in op.operator.data.iter().enumerate() {
        if i > 0 {
            write!(f, ", ")?;
        }
        write!(f, "{}", data)?;
    }

    // Format children if any
    if !op.operator.children.is_empty() {
        if !op.operator.data.is_empty() {
            write!(f, "; ")?;
        }
        for (i, child) in op.operator.children.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", child)?;
        }
    }

    // Add group_id if present
    if let Some(group_id) = op.group_id {
        write!(f, ")[{}]", group_id.0)
    } else {
        write!(f, ")")
    }
}

// Helper function to format physical operators
fn format_physical_op(
    f: &mut fmt::Formatter<'_>,
    op: &PhysicalOp<Value<NoMetadata>, NoMetadata>,
) -> fmt::Result {
    write!(f, "PhysOp:{}(", op.operator.tag)?;

    // Format data
    for (i, data) in op.operator.data.iter().enumerate() {
        if i > 0 {
            write!(f, ", ")?;
        }
        write!(f, "{}", data)?;
    }

    // Format children if any
    if !op.operator.children.is_empty() {
        if !op.operator.data.is_empty() {
            write!(f, "; ")?;
        }
        for (i, child) in op.operator.children.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", child)?;
        }
    }

    write!(f, ")")?;

    // Add goal if present
    if let Some(goal) = &op.goal {
        write!(f, "[goal: {}]", goal.group_id.0)?;
    }

    // Add cost if present
    if let Some(cost) = &op.cost {
        write!(f, "[cost: {}]", cost)?;
    }

    Ok(())
}

// Helper function to format goals
fn format_goal(f: &mut fmt::Formatter<'_>, goal: &Goal) -> fmt::Result {
    write!(f, "Goal[{}]{{{}}}", goal.group_id.0, goal.properties)
}

// For completeness, let's add Display implementations for binary and unary operators

impl fmt::Display for BinOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BinOp::Add => write!(f, "+"),
            BinOp::Sub => write!(f, "-"),
            BinOp::Mul => write!(f, "*"),
            BinOp::Div => write!(f, "/"),
            BinOp::Lt => write!(f, "<"),
            BinOp::Eq => write!(f, "=="),
            BinOp::And => write!(f, "&&"),
            BinOp::Or => write!(f, "||"),
            BinOp::Range => write!(f, ".."),
            BinOp::Concat => write!(f, "++"),
        }
    }
}

impl fmt::Display for UnaryOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UnaryOp::Neg => write!(f, "-"),
            UnaryOp::Not => write!(f, "!"),
        }
    }
}

// Add Display implementation for GroupId for convenience
impl fmt::Display for GroupId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "G{}", self.0)
    }
}
