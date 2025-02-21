/*pub type Identifier = String;

pub enum Expr {
    IfThenElse {
        cond: Typed<Expr>,
        then: Typed<Expr>,
        otherwise: Typed<Expr>,
    },
    Ref(Identifier),
    Value(i64),
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Typed<T> {
    pub value: Box<T>,
    pub ty: Type,
}

impl<T> Typed<T> {
    pub fn new(value: T, ty: Type) -> Self {
        Self {
            value: Box::new(value),
            ty,
        }
    }
}

pub struct HIR {}
*/