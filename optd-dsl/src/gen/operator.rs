use crate::ast::{Field, LogicalOp, Operator, OperatorKind, ScalarOp, Type};
use proc_macro2::{Ident, TokenStream};
use quote::{format_ident, quote};
use syn::parse_quote;

/// Converts an AST type to its corresponding Rust type representation
fn type_to_tokens(ty: &Type, is_param: bool) -> TokenStream {
    match ty {
        Type::String => {
            if is_param {
                quote! { &str }
            } else {
                quote! { String }
            }
        }
        Type::Bool => quote! { bool },
        Type::Int64 => quote! { i64 },
        Type::Float64 => quote! { f64 },
        Type::Operator(OperatorKind::Logical) => quote! { Relation },
        Type::Operator(OperatorKind::Scalar) => quote! { Scalar },
        Type::Array(inner) => {
            let inner_type = type_to_tokens(inner, false);
            quote! { Vec<#inner_type> }
        }
        _ => panic!("Unexpected type: {:?}", ty),
    }
}

/// Helper struct to hold field information for code generation
struct FieldInfo {
    name: Ident,
    ty: Type,
}

impl FieldInfo {
    fn new(field: &Field) -> Self {
        Self {
            name: format_ident!("{}", field.name),
            ty: field.ty.clone(),
        }
    }

    fn struct_field(&self) -> TokenStream {
        let name = &self.name;
        let ty = type_to_tokens(&self.ty, false);
        quote! {
            pub #name: #ty
        }
    }

    fn ctor_param(&self) -> TokenStream {
        let name = &self.name;
        let ty = type_to_tokens(&self.ty, true);
        quote! { #name: #ty }
    }

    fn ctor_init(&self) -> TokenStream {
        let name = &self.name;
        match &self.ty {
            Type::String => quote! { #name: #name.into() },
            _ => quote! { #name },
        }
    }
}

fn generate_code(operators: &[Operator]) -> proc_macro2::TokenStream {
    let mut generated_code = proc_macro2::TokenStream::new();

    // Generate enums first
    let logical_enum = generate_logical_enum(&logical_ops);
    let scalar_enum = generate_scalar_enum(&scalar_ops);
    generated_code.extend(logical_enum);
    generated_code.extend(scalar_enum);

    // Then generate individual operator modules
    for op in logical_ops {
        generated_code.extend(generate_logical_operator(&op));
    }
    for op in scalar_ops {
        generated_code.extend(generate_scalar_operator(&op));
    }

    generated_code
}

fn generate_logical_operator(operator: &LogicalOp) -> TokenStream {
    let name = format_ident!("{}", &operator.name);
    let fields: Vec<FieldInfo> = operator.fields.iter().map(FieldInfo::new).collect();
    let struct_fields: Vec<_> = fields.iter().map(|f| f.struct_field()).collect();
    let ctor_params: Vec<_> = fields.iter().map(|f| f.ctor_param()).collect();
    let ctor_inits: Vec<_> = fields.iter().map(|f| f.ctor_init()).collect();
    let field_names: Vec<_> = fields.iter().map(|f| &f.name).collect();
    let fn_name = format_ident!("{}", operator.name.to_lowercase());

    quote! {
        use super::LogicalOperator;
        use crate::values::OptdValue;
        use serde::Deserialize;

        #[derive(Debug, Clone, PartialEq, Deserialize)]
        pub struct #name<Value, Relation, Scalar> {
            #(#struct_fields,)*
        }

        impl<Relation, Scalar> #name<OptdValue, Relation, Scalar> {
            pub fn new(#(#ctor_params,)*) -> Self {
                Self {
                    #(#ctor_inits,)*
                }
            }
        }

        pub fn #fn_name<Relation, Scalar>(
            #(#ctor_params,)*
        ) -> LogicalOperator<OptdValue, Relation, Scalar> {
            LogicalOperator::#name(#name::new(#(#field_names,)*))
        }
    }
}

fn generate_scalar_operator(_operator: &ScalarOp) -> proc_macro2::TokenStream {
    unimplemented!()
}

#[test]
fn test_generate_logical_operator() {
    use crate::ast::{Field, LogicalOp, OperatorKind, Type};
    use std::collections::HashMap;

    // Test with both Logical and Scalar operator types
    let filter_op = LogicalOp {
        name: "Filter".to_string(),
        fields: vec![
            Field {
                name: "child".to_string(),
                ty: Type::Operator(OperatorKind::Logical),
            },
            Field {
                name: "predicate".to_string(),
                ty: Type::Operator(OperatorKind::Scalar),
            },
        ],
        derived_props: HashMap::new(),
    };

    let generated = generate_logical_operator(&filter_op);
    let syntax_tree: syn::File = parse_quote! {
        #generated
    };
    let formatted = prettyplease::unparse(&syntax_tree);
    println!("Generated code:\n{}", formatted);

    // Basic validation
    let code = formatted.to_string();
    assert!(code.contains("pub child: Relation"));
    assert!(code.contains("pub predicate: Scalar"));
}
