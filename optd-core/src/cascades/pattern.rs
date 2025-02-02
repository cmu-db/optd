use std::sync::Arc;

use super::user_type::UserType;

pub enum LogicalPattern {
    /// Match any subtree.
    Any,
    /// Matches anything that is not the given pattern.
    Not(Box<LogicalPattern>),
    /// Bind matched subtree to name for reuse.
    Bind(String, Box<LogicalPattern>),
    /// Match specific operator and its children recursively.
    Operator {
        op_type: String,
        content: Vec<Arc<UserTypePattern>>,
        logical_children: Vec<Box<LogicalPattern>>,
        scalar_children: Vec<Arc<ScalarPattern>>,
    },
}

pub enum ScalarPattern {
    /// Match any scalar expression.
    Any,
    /// Bind scalar subtree.
    Bind(String, Arc<ScalarPattern>),
    /// Match scalar operator and children.
    Operator {
        op_type: String,
        content: Arc<UserTypePattern>,
        scalar_children: Vec<Arc<ScalarPattern>>,
    },
}

pub enum UserTypePattern {
    /// Match any user type.
    Any,
    /// Bind user type (always a leaf node).
    Bind(String, Arc<UserType>),
}
