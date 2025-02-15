// Low lever IR of DSL, converts the AST (hir.ast) into a lower level IR that can be used by
// the Cascades optimizer.

use std::collections::HashMap;

use optd_core::{cascades::types::PartialLogicalPlan, values::OptdValue};

use super::{BinOp, UnaryOp};

// All identifiers have been validated by the analyzer
pub type Identifier = String;

#[derive(Clone, Debug)]
pub enum Expr {
    IfThenElse {
        cond: Box<Expr>,
        then: Box<Expr>,
        otherwise: Box<Expr>,
    },
    PatternMatch {
        on: Box<Expr>,
        arms: Vec<(Pattern, Box<Expr>)>,
    },
    Val {
        identifier: Identifier,
        value: Box<Expr>,
        next: Box<Expr>,
    },

    Binary {
        left: Box<Expr>,
        op: BinOp,
        right: Box<Expr>,
    },
    Unary {
        op: UnaryOp,
        expr: Box<Expr>,
    },
    MemberCall {
        expr: Box<Expr>,
        member: Identifier,
        args: Vec<Expr>,
    },
    MemberAccess {
        expr: Box<Expr>,
        member: Identifier,
    },
    Call {
        expr: Box<Expr>,
        args: Vec<Expr>,
    }, // TODO: Refactor old parser to remove unnessary nodes.
    Fail(String),

    Ref(Identifier),

    Array(Vec<Expr>),
    Map(Vec<(Expr, Expr)>),
    Tuple(Vec<Expr>),

    Value(Value),
}

/// Patterns used in match expressions
#[derive(Debug, Clone)]
pub enum Pattern {
    Bind(Identifier, Box<Pattern>),
    Logical {
        tag: Identifier,
        value_patterns: Vec<Pattern>,
        relational_children: Vec<Vec<Pattern>>,
        scalar_children: Vec<Vec<Pattern>>,
    },
    Scalar {
        tag: Identifier,
        value_patterns: Vec<Pattern>,
        scalar_children: Vec<Vec<Pattern>>,
    },
    Physical {
        tag: Identifier,
        value_patterns: Vec<Pattern>,
        relational_children: Vec<Vec<Pattern>>,
        scalar_children: Vec<Vec<Pattern>>,
    },
    Value(OptdValue), // TODO: once we add enums we need patterns here too
    Wildcard,
}

#[derive(Debug, Clone)]
pub struct Function {
    pub args: Vec<Identifier>,
    pub body: Box<Expr>,
}

/// Types supported by the language
#[derive(Debug, Clone)]
pub enum Value {
    Array(Box<Value>),
    Map(Box<Value>, Box<Value>),
    Tuple(Vec<Value>),
    Function(Function),
    Terminal(Terminal),
}

#[derive(Clone, Debug, PartialEq)]
pub enum Terminal {
    PartialLogicalPlan(PartialLogicalPlan),
    PartialScalarPlan(PartialLogicalPlan),
    OptdValue(OptdValue),
}

#[derive(Clone, Debug)]
pub struct Props(HashMap<Identifier, Value>);

/// operator tag -> (logical_prop -> derivation)
#[derive(Clone, Debug)]
pub struct LogicalDerives(HashMap<Identifier, Vec<(Identifier, Expr)>>);

#[derive(Clone, Debug)]
pub struct Program {
    pub logical_props: Props,
    pub physical_props: Props,
    pub logical_derives: LogicalDerives,
    pub transformations: Vec<Identifier>,
    pub implementations: Vec<Identifier>,
    pub enforcers: Vec<Identifier>, // Ordered from first applied to last
    pub functions: HashMap<Identifier, Function>,
}

// TODO: support structs in MVP for phy prop and that's gonna be the input type of into phys, enforce etc.
// TODO: val foo: Type = ...
// TODO: def blah(x: Type): Type?

// // Physical Operators
// Physical MergeJoin(left: Physical, right: Physical, left_keys: Scalar, right_keys: Scalar)
// Physical HashJoin(left: Physical, right: Physical, cond: Scalar)
// Physical PhyFilter(child: Physical, cond: Scalar)
//
// Physical Props(sort_order: Array[(Scalar, String)], partition: Array[Scalar])

// Logical Props(schema_len: Int64, next: Array[Int64])

// @enforce(name: 1)
// def enforce_sort_order(child: Physical, props: PhysicalProps]) -> Physical:
//  val rem = props.partition
//  if rem.len == 0 Sort(child, props.sort_order)
//  else Sort(chold, props.sort_order).enforce(rem)

// .enforce(missing_props)

// @rule(Physical)
// def into_hash_join(expr: Logical, props: PhysicalProps): Physical =
//   match expr
//     case Join("Inner", left, right, cond) =>
//       val cnf = conjunctive_normal_form(cond);
//       if format_matches(cnf, left.schema_len, right.schema_len) then
//         HashJoin(left.into_phys(Nil), right.into_phys(Nil), cond).enforce(props)
//       else
//         fail("not an equality")

// @rule(Physical)
// def remove_sort(expr: Logical): Physical =
//   match expr
//     case Sort(child, sort) => child.into_phys(into_prop(sort))

// @rule(Physical)
// def into_merge_join(expr: Logical, props: PhysicalProps): Physical =
//   match expr
//     case Join("Inner", left, right, cond) =>
//       // Check if you can do the merge
//       val props_remaining_after_merge = foo(props, cond)
//       val props_left = sorted_on(cond) // TODO(WHAT IS THIS?)
//       MergeJoin(
//         left.into_phys(extract_prop(left.schema_len, cond)),
//         right.into_phys(extract_prop(left.schema_len, cond)),
//         left_key, // TODO(WHAT IS THIS?)
//         right_keys // TODO(WHAT IS THIS?) How do we know blindly which ones to try?
//       )
//         .enforce(props_remaining_after_merge)

// @rule(Physical)
// def into_filter(expr: Logical, props: PhysicalProps): Physical =
//   match expr
//     case Filter(child, cond) =>
//       [PhyFilter(child.into_phys(props), cond), PhyFilter(child, cond).enforce(props)]
