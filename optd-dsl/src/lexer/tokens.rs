#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    // Keywords
    Scalar,
    Logical,
    Physical,
    Properties,
    Val,
    Match,
    Case,
    If,
    Then,
    Else,
    Derive,

    // Literals
    Identifier(String),
    Int64(i64),
    Float64(f64),
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

    // Delimiters
    LParen,   // (
    RParen,   // )
    LBrace,   // {
    RBrace,   // }
    LBracket, // [
    RBracket, // ]
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
            Token::Properties => write!(f, "Properties"),
            Token::Val => write!(f, "val"),
            Token::Match => write!(f, "match"),
            Token::Case => write!(f, "case"),
            Token::If => write!(f, "if"),
            Token::Then => write!(f, "then"),
            Token::Else => write!(f, "else"),
            Token::Derive => write!(f, "derive"),

            // Literals
            Token::Identifier(s) => write!(f, "Ident({})", s),
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

            // Delimiters
            Token::LParen => write!(f, "("),
            Token::RParen => write!(f, ")"),
            Token::LBrace => write!(f, "{{"),
            Token::RBrace => write!(f, "}}"),
            Token::LBracket => write!(f, "["),
            Token::RBracket => write!(f, "]"),
            Token::Comma => write!(f, ","),
            Token::Dot => write!(f, "."),
            Token::Semi => write!(f, ";"),
            Token::Colon => write!(f, ":"),
            Token::At => write!(f, "@"),
        }
    }
}
