/// Constants that can appear in scalar expressions.
#[derive(Clone)]
pub enum Constant {
    /// String constant (e.g. "hello")
    String(String),
    /// Integer constant (e.g. 42)
    Integer(i64),
    /// Floating point constant (e.g. 3.14)
    Float(f64),
    /// Boolean constant (e.g. true, false)
    Boolean(bool),
}
