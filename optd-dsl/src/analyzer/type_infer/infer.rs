use super::solver::Solver;
use crate::analyzer::{
    errors::AnalyzerErrorKind,
    hir::{HIR, TypedSpan},
    types::TypeRegistry,
};

pub fn infer(hir: &HIR<TypedSpan>, registry: &TypeRegistry) -> Result<(), Box<AnalyzerErrorKind>> {
    let mut solver = Solver::new(registry);
    solver.generate_constraints(hir)?;
    println!("{:#?}", solver);
    println!("{:#?}", hir);
    Ok(())
}
