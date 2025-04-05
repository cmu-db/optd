use super::expr::convert_expr;
use super::types::{convert_type, create_function_type};
use crate::analyzer::hir::{Annotation, FunKind, UdfKind};
use crate::analyzer::semantic_checker::error::SemanticError;
use crate::analyzer::types::Identifier;
use crate::analyzer::{
    context::Context,
    hir::{CoreData, HIR, TypedSpan, Value},
    types::{Type, TypeRegistry},
};
use crate::parser::ast::{Function, Item, Module};
use crate::utils::error::CompileError;
use crate::utils::span::Spanned;
use CompileError::*;
use FunKind::*;
use UdfKind::*;
use std::collections::{HashMap, HashSet};

/// Converts an AST to a High-level Intermediate Representation (HIR).
#[derive(Debug, Clone, Default)]
pub struct ASTConverter {
    /// Registry of types for type checking and subtyping.
    pub(super) type_registry: TypeRegistry,
    /// Context for variable bindings.
    pub(super) context: Context<TypedSpan>,
    /// Annotations for HIR expressions.
    pub(super) annotations: HashMap<Identifier, Vec<Annotation>>,
}

impl ASTConverter {
    /// Converts an AST module to a partially typed and spanned HIR.
    ///
    /// This method processes all items in the AST module and transforms them into the HIR representation.
    /// For functions, it handles receivers, parameters, function bodies, and return types.
    ///
    /// # Arguments
    ///
    /// * `module` - The AST module to convert
    ///
    /// # Returns
    ///
    /// The HIR representation and TypeRegistry, or a CompileError if conversion fails.
    pub fn convert(
        mut self,
        module: &Module,
    ) -> Result<(HIR<TypedSpan>, TypeRegistry), CompileError> {
        // Process all module items sequentially.
        for item in &module.items {
            match item {
                Item::Adt(spanned_adt) => {
                    self.type_registry.register_adt(&spanned_adt.value)?;
                }
                Item::Function(spanned_fn) => {
                    self.process_function(spanned_fn)?;
                }
            }
        }

        Ok((
            HIR {
                context: self.context,
                annotations: self.annotations,
            },
            self.type_registry,
        ))
    }

    /// Processes a function AST node and adds it to the context.
    ///
    /// Handles function parameters, return type, and body conversion.
    pub(super) fn process_function(
        &mut self,
        spanned_fn: &Spanned<Function>,
    ) -> Result<(), CompileError> {
        let func = &spanned_fn.value;
        let name = &*func.name.value;
        let fn_span = func.name.span.clone();

        // Reject functions without parameters.
        if func.receiver.is_none() && func.params.is_none() {
            return Err(SemanticError(SemanticError::new_incomplete_function(
                name.clone(),
                fn_span,
            )));
        }

        // Register the function in the context, while checking for duplicates.
        let generics = func
            .type_params
            .iter()
            .map(|param| (*param.value).clone())
            .collect();

        let params = Self::get_parameters(func, &generics);
        let return_type = convert_type(&func.return_type.value, &generics);
        let fn_type = create_function_type(&params, &return_type);

        match &func.body {
            Some(body_expr) => {
                // Process function with body.
                let body_hir = convert_expr(body_expr, &generics)?;
                let param_names = params.iter().map(|(name, _)| name.clone()).collect();

                let fn_value = Value::new_with(
                    CoreData::Function(Closure(param_names, body_hir.into())),
                    fn_type,
                    fn_span,
                );

                self.context.try_bind(name.clone(), fn_value)?;
            }
            None => {
                // Process external function (UDF).
                let fn_value = Value::new_with(
                    CoreData::Function(Udf(Unlinked(name.clone()))),
                    fn_type,
                    fn_span,
                );

                self.context.try_bind(name.clone(), fn_value)?;
            }
        };

        // Register function annotations if present.
        if !func.annotations.is_empty() {
            let annotations = func
                .annotations
                .iter()
                .map(|annotation| (*annotation.value).clone())
                .collect();

            self.annotations.insert(name.clone(), annotations);
        }

        Ok(())
    }

    /// Extracts parameter information from a function AST.
    ///
    /// Collects parameter names and types from both the receiver (if present)
    /// and the parameter list.
    fn get_parameters(func: &Function, generics: &HashSet<Identifier>) -> Vec<(Identifier, Type)> {
        // Start with receiver if it exists.
        let mut param_fields = match &func.receiver {
            Some(receiver) => {
                vec![(
                    (*receiver.name).clone(),
                    convert_type(&receiver.ty.value, generics),
                )]
            }
            None => vec![],
        };

        // Add regular parameters if they exist.
        if let Some(params) = &func.params {
            let regular_params = params
                .iter()
                .map(|field| {
                    (
                        (*field.name).clone(),
                        convert_type(&field.ty.value, generics),
                    )
                })
                .collect::<Vec<_>>();
            param_fields.extend(regular_params);
        }

        param_fields
    }
}
