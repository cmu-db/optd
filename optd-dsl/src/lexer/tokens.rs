use ordered_float::OrderedFloat;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Token {
    // Keywords
    Scalar,
    Logical,
    Physical,
    LogicalProps,
    PhysicalProps,
    Type,
    TInt64,
    TFloat64,
    TString,
    TBool,
    Map,
    Val,    
    Match,
    Case,
    If,
    Then,
    Else,
    Derive,

    // Literals
    TypeIdent(String),
    TermIdent(String),
    Int64(i64),
    Float64(OrderedFloat<f64>),
    String(String),
    Bool(bool),

    // Operators
    Plus,      // +
    Minus,     // -
    Mul,       // *
    Div,       // /
    Eq,        // =
    Arrow,     // =>
    EqEq,      // ==
    NotEq,     // !=
    Greater,   // >
    Less,      // <
    GreaterEq, // >=
    LessEq,    // <=
    Not,       // !
    And,       // &&
    Or,        // ||
    Range,     // ..
    Concat,    // ++
    Unit,      // ()

    // Delimiters
    LParen,   // (
    RParen,   // )
    LBrace,   // {
    RBrace,   // }
    LBracket, // [
    RBracket, // ]
    Vertical, // |
    Comma,    // ,
    Dot,      // .
    Semi,     // ;
    Colon,    // :
    At,       // @
}

impl std::fmt::Display for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            // Keywords
            Token::Scalar => write!(f, "Scalar"),
            Token::Logical => write!(f, "Logical"),
            Token::Physical => write!(f, "Physical"),
            Token::LogicalProps => write!(f, "LogicalProps"),
            Token::PhysicalProps => write!(f, "PhysicalProps"),
            Token::Type => write!(f, "Type"),
            Token::TInt64 => write!(f, "TInt64"),
            Token::TFloat64 => write!(f, "TFloat64"),
            Token::TString => write!(f, "TString"),
            Token::TBool => write!(f, "TBool"),
            Token::Map => write!(f, "Map"),
            Token::Val => write!(f, "val"),
            Token::Match => write!(f, "match"),
            Token::Case => write!(f, "case"),
            Token::If => write!(f, "if"),
            Token::Then => write!(f, "then"),
            Token::Else => write!(f, "else"),
            Token::Derive => write!(f, "derive"),

            // Literals
            Token::TypeIdent(s) => write!(f, "TypeIdent({})", s),
            Token::TermIdent(s) => write!(f, "TermIdent({})", s),
            Token::Int64(n) => write!(f, "Int({})", n),
            Token::Float64(x) => write!(f, "Float({})", x),
            Token::String(s) => write!(f, "String(\"{}\")", s),
            Token::Bool(b) => write!(f, "Bool({})", b),

            // Operators
            Token::Plus => write!(f, "+"),
            Token::Minus => write!(f, "-"),
            Token::Mul => write!(f, "*"),
            Token::Div => write!(f, "/"),
            Token::Eq => write!(f, "="),
            Token::Arrow => write!(f, "=>"),
            Token::EqEq => write!(f, "=="),
            Token::NotEq => write!(f, "!="),
            Token::Greater => write!(f, ">"),
            Token::Less => write!(f, "<"),
            Token::GreaterEq => write!(f, ">="),
            Token::LessEq => write!(f, "<="),
            Token::Not => write!(f, "!"),
            Token::And => write!(f, "&&"),
            Token::Or => write!(f, "||"),
            Token::Range => write!(f, ".."),
            Token::Concat => write!(f, "++"),
            Token::Unit => write!(f, "()"),

            // Delimiters
            Token::LParen => write!(f, "("),
            Token::RParen => write!(f, ")"),
            Token::LBrace => write!(f, "{{"),
            Token::RBrace => write!(f, "}}"),
            Token::LBracket => write!(f, "["),
            Token::RBracket => write!(f, "]"),
            Token::Vertical => write!(f, "|"),
            Token::Comma => write!(f, ","),
            Token::Dot => write!(f, "."),
            Token::Semi => write!(f, ";"),
            Token::Colon => write!(f, ":"),
            Token::At => write!(f, "@"),
        }
    }
}
