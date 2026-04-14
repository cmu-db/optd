//! Scalar expressions are used to compute values in various contexts, such as
//! projections, filters, and join conditions. They can represent literals,
//! column references, and other scalar operations.
//!
//! Each scalar expression is represented by the `Scalar` struct, which contains
//! metadata specific to the type of scalar expression it represents.
//!
//! Similar to operators, scalar expressions are stored in plans using the
//! Scalar struct, which holds the specific kind of scalar expression and its
//! associated metadata. This can be "downcast" to the specific scalar type
//! when needed.

mod binary_op;
mod case;
mod cast;
mod column_ref;
mod function;
mod in_list;
mod is_not_null;
mod is_null;
mod like;
mod list;
mod literal;
mod nary_op;

use std::sync::Arc;

pub use binary_op::*;
pub use case::*;
pub use cast::*;
pub use column_ref::*;
pub use function::*;
pub use in_list::*;
pub use is_not_null::*;
pub use is_null::*;
pub use like::*;
pub use list::*;
pub use literal::*;
pub use nary_op::*;

use crate::ir::{
    ColumnSet, IRCommon, Operator, ScalarValue, convert::IntoScalar, explain::Explain,
    properties::ScalarProperties,
};

/// The scalar type and its associated metadata.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ScalarKind {
    Literal(LiteralMetadata),
    ColumnRef(ColumnRefMetadata),
    BinaryOp(BinaryOpMetadata),
    NaryOp(NaryOpMetadata),
    List(ListMetadata),
    Function(FunctionMetadata),
    Cast(CastMetadata),
    InList(InListMetadata),
    IsNull(IsNullMetadata),
    IsNotNull(IsNotNullMetadata),
    Like(LikeMetadata),
    Case(CaseMetadata),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Scalar {
    pub kind: ScalarKind,
    pub common: IRCommon<ScalarProperties>,
}

impl Scalar {
    /// Gets the slice to the input operators.
    pub fn input_operators(&self) -> &[Arc<Operator>] {
        &self.common.input_operators
    }

    /// Gets the slice to the input scalar expressions.
    pub fn input_scalars(&self) -> &[Arc<Scalar>] {
        &self.common.input_scalars
    }

    /// Clones the operator, optionally replacing the input operators and the input scalar expressions.
    pub fn clone_with_inputs(
        &self,
        scalars: Option<Arc<[Arc<Scalar>]>>,
        operators: Option<Arc<[Arc<Operator>]>>,
    ) -> Self {
        let operators = operators.unwrap_or_else(|| self.common.input_operators.clone());
        let scalars = scalars.unwrap_or_else(|| self.common.input_scalars.clone());
        Self {
            kind: self.kind.clone(),
            common: IRCommon::new(operators, scalars),
        }
    }

    /// Returns the set of columns used by this scalar expression.
    pub fn used_columns(&self) -> ColumnSet {
        match &self.kind {
            ScalarKind::Literal(_) => ColumnSet::default(),
            ScalarKind::ColumnRef(meta) => ColumnSet::from_iter(std::iter::once(meta.column)),
            ScalarKind::BinaryOp(_)
            | ScalarKind::NaryOp(_)
            | ScalarKind::List(_)
            | ScalarKind::Cast(_)
            | ScalarKind::InList(_)
            | ScalarKind::IsNull(_)
            | ScalarKind::IsNotNull(_)
            | ScalarKind::Like(_)
            | ScalarKind::Function(_)
            | ScalarKind::Case(_) => self.input_scalars().iter().fold(
                ColumnSet::default(),
                |mut used_columns, scalar| {
                    used_columns |= &scalar.used_columns();
                    used_columns
                },
            ),
        }
    }

    /// Conjoin predicates with `AND`, returning `true` for an empty list.
    pub fn combine_conjuncts(mut conds: Vec<Arc<Scalar>>) -> Arc<Scalar> {
        if conds.is_empty() {
            Literal::boolean(true).into_scalar()
        } else if conds.len() == 1 {
            conds.pop().unwrap()
        } else {
            NaryOp::new(NaryOpKind::And, conds.into()).into_scalar()
        }
    }

    // Simplifies an n-ary scalar by dropping redundant terms
    pub fn simplify_nary_scalar(self: Arc<Self>) -> Arc<Scalar> {
        match &self.kind {
            ScalarKind::BinaryOp(bin) if bin.op_kind == BinaryOpKind::IsNotDistinctFrom => {
                let lhs = self.input_scalars()[0].clone();
                let rhs = self.input_scalars()[1].clone();
                if lhs == rhs {
                    Literal::boolean(true).into_scalar()
                } else {
                    self
                }
            }
            ScalarKind::IsNull(_) => {
                let expr = self.input_scalars()[0].clone();
                if let Ok(literal) = expr.try_borrow::<Literal>() {
                    Literal::boolean(literal.value().is_null()).into_scalar()
                } else {
                    self
                }
            }
            ScalarKind::IsNotNull(_) => {
                let expr = self.input_scalars()[0].clone();
                if let Ok(literal) = expr.try_borrow::<Literal>() {
                    Literal::boolean(!literal.value().is_null()).into_scalar()
                } else {
                    self
                }
            }
            ScalarKind::NaryOp(nary) if nary.op_kind == NaryOpKind::And => {
                let mut terms = Vec::new();
                for term in self.input_scalars() {
                    if matches!(&term.kind, ScalarKind::Literal(meta) if matches!(meta.value, ScalarValue::Boolean(Some(true))))
                    {
                        continue;
                    }
                    if matches!(&term.kind, ScalarKind::Literal(meta) if matches!(meta.value, ScalarValue::Boolean(Some(false))))
                    {
                        return Literal::boolean(false).into_scalar();
                    }
                    terms.push(term.clone());
                }

                if terms.is_empty() {
                    return Literal::boolean(true).into_scalar();
                }
                if terms.len() == 1 {
                    return terms.pop().unwrap();
                }
                if terms.as_slice() == self.input_scalars() {
                    return self;
                }

                Arc::new(self.clone_with_inputs(Some(Arc::from(terms)), None))
            }
            _ => self,
        }
    }

    pub fn is_true_scalar(&self) -> bool {
        matches!(
            &self.kind,
            ScalarKind::Literal(meta) if matches!(meta.value, ScalarValue::Boolean(Some(true)))
        )
    }
}

impl Explain for Scalar {
    fn explain<'a>(
        &self,
        ctx: &super::IRContext,
        option: &super::explain::ExplainOption,
    ) -> pretty_xmlish::Pretty<'a> {
        match &self.kind {
            ScalarKind::Literal(meta) => {
                Literal::borrow_raw_parts(meta, &self.common).explain(ctx, option)
            }
            ScalarKind::ColumnRef(meta) => {
                ColumnRef::borrow_raw_parts(meta, &self.common).explain(ctx, option)
            }
            ScalarKind::BinaryOp(meta) => {
                BinaryOp::borrow_raw_parts(meta, &self.common).explain(ctx, option)
            }
            ScalarKind::NaryOp(meta) => {
                NaryOp::borrow_raw_parts(meta, &self.common).explain(ctx, option)
            }
            ScalarKind::List(meta) => {
                List::borrow_raw_parts(meta, &self.common).explain(ctx, option)
            }
            ScalarKind::Function(meta) => {
                Function::borrow_raw_parts(meta, &self.common).explain(ctx, option)
            }
            ScalarKind::Cast(meta) => {
                Cast::borrow_raw_parts(meta, &self.common).explain(ctx, option)
            }
            ScalarKind::InList(meta) => {
                InList::borrow_raw_parts(meta, &self.common).explain(ctx, option)
            }
            ScalarKind::IsNull(meta) => {
                IsNull::borrow_raw_parts(meta, &self.common).explain(ctx, option)
            }
            ScalarKind::IsNotNull(meta) => {
                IsNotNull::borrow_raw_parts(meta, &self.common).explain(ctx, option)
            }
            ScalarKind::Like(meta) => {
                Like::borrow_raw_parts(meta, &self.common).explain(ctx, option)
            }
            ScalarKind::Case(meta) => {
                Case::borrow_raw_parts(meta, &self.common).explain(ctx, option)
            }
        }
    }
}
