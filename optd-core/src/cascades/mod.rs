pub mod expressions;
pub mod memo;

#[derive(Debug, Clone, Copy)]
pub struct RelationalGroupId(pub i64);
#[derive(Debug, Clone, Copy)]
pub struct ScalarGroupId(pub i64);
