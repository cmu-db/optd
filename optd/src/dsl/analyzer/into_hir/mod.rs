use super::{
    hir::{HIR, TypedSpan},
    type_checks::registry::TypeRegistry,
};
use crate::dsl::analyzer::{
    context::Context,
    hir::{CoreData, FunKind, Value},
};
use converter::convert_expr;

mod converter;

// Converts an HIR with TypedSpan metadata to an HIR without metadata.
///
/// This function performs a deep translation of all structures that may contain
/// metadata, recursively removing the metadata and producing equivalent structures
/// without it.
pub fn into_hir(hir_typedspan: HIR<TypedSpan>, registry: &TypeRegistry) -> HIR {
    use CoreData::*;
    use FunKind::*;

    let mut context: Context = Default::default();

    // Convert all function bindings from the original context.
    for (name, fun) in hir_typedspan.context.get_all_bindings() {
        let converted_fun = match &fun.data {
            Function(Closure(args, body)) => Closure(args.clone(), convert_expr(body, registry)),
            Function(Udf(udf)) => Udf(udf.clone()),
            _ => panic!("Expected a function, but got: {:?}", fun.data),
        };

        context.bind(name.clone(), Value::new(Function(converted_fun)));
    }

    // Push the context scope of the module, as we have processed all functions.
    context.push_scope();

    HIR {
        context,
        annotations: hir_typedspan.annotations,
    }
}
