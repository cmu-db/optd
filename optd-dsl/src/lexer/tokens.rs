use ordered_float::OrderedFloat;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Token {
    // Type keywords
    TInt64,
    TFloat64,
    TString,
    TBool,
    TUnit,

    // Other keywords
    Fn,
    Data,
    With,
    As,
    In,
    Let,
    Match,
    If,
    Then,
    Else,
    Fail,

    // Literals
    TermIdent(String),
    TypeIdent(String),
    Int64(i64),
    Float64(OrderedFloat<f64>),
    String(String),
    Bool(bool),
    Unit, // ()

    // Operators
    Plus,       // +
    Minus,      // -
    Mul,        // *
    Div,        // /
    Eq,         // =
    BigArrow,   // =>
    SmallArrow, // ->
    EqEq,       // ==
    NotEq,      // !=
    Greater,    // >
    Less,       // <
    GreaterEq,  // >=
    LessEq,     // <=
    Not,        // !
    And,        // &&
    Or,         // ||
    Range,      // ..
    Concat,     // ++

    // Delimiters
    LParen,     // (
    RParen,     // )
    LBrace,     // {
    RBrace,     // }
    LBracket,   // [
    RBracket,   // ]
    Vertical,   // |
    Backward,   // \
    Comma,      // ,
    Dot,        // .
    Colon,      // :
    UnderScore, // _
}

pub const ALL_DELIMITERS: [(Token, Token); 3] = [
    (Token::LParen, Token::RParen),
    (Token::LBracket, Token::RBracket),
    (Token::LBrace, Token::RBrace),
];

impl std::fmt::Display for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            // Type keywords
            Token::TInt64 => write!(f, "I64"),
            Token::TFloat64 => write!(f, "F64"),
            Token::TString => write!(f, "String"),
            Token::TBool => write!(f, "Bool"),
            Token::TUnit => write!(f, "Unit"),

            // Other keywords
            Token::Fn => write!(f, "fn"),
            Token::Data => write!(f, "data"),
            Token::With => write!(f, "with"),
            Token::As => write!(f, "as"),
            Token::In => write!(f, "in"),
            Token::Let => write!(f, "let"),
            Token::Match => write!(f, "match"),
            Token::If => write!(f, "if"),
            Token::Then => write!(f, "then"),
            Token::Else => write!(f, "else"),
            Token::Fail => write!(f, "fail"),

            // Literals
            Token::TermIdent(ident) => write!(f, "{}", ident),
            Token::TypeIdent(ident) => write!(f, "{}", ident),
            Token::Int64(num) => write!(f, "{}", num),
            Token::Float64(num) => write!(f, "{}", num),
            Token::String(s) => write!(f, "\"{}\"", s),
            Token::Bool(b) => write!(f, "{}", b),
            Token::Unit => write!(f, "()"),

            // Operators
            Token::Plus => write!(f, "+"),
            Token::Minus => write!(f, "-"),
            Token::Mul => write!(f, "*"),
            Token::Div => write!(f, "/"),
            Token::Eq => write!(f, "="),
            Token::BigArrow => write!(f, "=>"),
            Token::SmallArrow => write!(f, "->"),
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
            Token::Vertical => write!(f, "|"),
            Token::Backward => write!(f, "\\"),
            Token::Comma => write!(f, ","),
            Token::Dot => write!(f, "."),
            Token::Colon => write!(f, ":"),
            Token::UnderScore => write!(f, "_"),
        }
    }
}
