/*use crate::{
    analyzer::semantic::SemanticAnalyzer,
    irs::hir::{Field, LogicalOp, Operator, OperatorKind, ScalarOp, Type},
    parser::parse_file,
};
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

fn generate_logical_enum(operators: &[LogicalOp]) -> TokenStream {
    let variants: Vec<_> = operators
        .iter()
        .map(|op| {
            let name = format_ident!("{}", &op.name);
            quote! {
                #name(#name<Value, Relation, Scalar>)
            }
        })
        .collect();

    let variant_names: Vec<_> = operators
        .iter()
        .map(|op| format_ident!("{}", &op.name))
        .collect();

    quote! {
        #[derive(Debug, Clone, PartialEq, Deserialize)]
        pub enum LogicalOperator<Value, Relation, Scalar> {
            #(#variants,)*
        }

        #[derive(Debug, Clone, PartialEq, sqlx::Type)]
        pub enum LogicalOperatorKind {
            #(#variant_names,)*
        }

        impl<Relation, Scalar> LogicalOperator<OptdValue, Relation, Scalar>
        where
            Relation: Clone,
            Scalar: Clone,
        {
            pub fn operator_kind(&self) -> LogicalOperatorKind {
                match self {
                    #(LogicalOperator::#variant_names => LogicalOperatorKind::#variant_names,)*
                }
            }
        }
    }
}

fn generate_scalar_enum(operators: &[ScalarOp]) -> TokenStream {
    let variants: Vec<_> = operators
        .iter()
        .map(|op| {
            let name = format_ident!("{}", &op.name);
            quote! {
                #name(#name<Value, Scalar>)
            }
        })
        .collect();

    let variant_names: Vec<_> = operators
        .iter()
        .map(|op| format_ident!("{}", &op.name))
        .collect();

    quote! {
        #[derive(Debug, Clone, PartialEq, Deserialize)]
        pub enum ScalarOperator<Value, Scalar> {
            #(#variants,)*
        }

        #[derive(Debug, Clone, PartialEq, sqlx::Type)]
        pub enum ScalarOperatorKind {
            #(#variant_names,)*
        }

        impl<Scalar> ScalarOperator<OptdValue, Scalar>
        where
            Scalar: Clone,
        {
            pub fn operator_kind(&self) -> ScalarOperatorKind {
                match self {
                    #(ScalarOperator::#variant_names => ScalarOperatorKind::#variant_names,)*
                }
            }
        }
    }
}

fn generate_code(operators: &[Operator]) -> TokenStream {
    let mut logical_ops = Vec::new();
    let mut scalar_ops = Vec::new();

    for operator in operators.into_iter().cloned() {
        match operator {
            Operator::Logical(op) => logical_ops.push(op),
            Operator::Scalar(op) => scalar_ops.push(op),
        }
    }

    let mut generated_code = TokenStream::new();

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

fn generate_scalar_operator(operator: &ScalarOp) -> TokenStream {
    let name = format_ident!("{}", &operator.name);
    let fields: Vec<FieldInfo> = operator.fields.iter().map(FieldInfo::new).collect();
    let struct_fields: Vec<_> = fields.iter().map(|f| f.struct_field()).collect();
    let ctor_params: Vec<_> = fields.iter().map(|f| f.ctor_param()).collect();
    let ctor_inits: Vec<_> = fields.iter().map(|f| f.ctor_init()).collect();
    let field_names: Vec<_> = fields.iter().map(|f| &f.name).collect();
    let fn_name = format_ident!("{}", operator.name.to_lowercase());

    quote! {
        use super::ScalarOperator;
        use crate::values::OptdValue;
        use serde::Deserialize;

        #[derive(Debug, Clone, PartialEq, Deserialize)]
        pub struct #name<Value, Scalar> {
            #(#struct_fields,)*
        }

        impl<Scalar> #name<OptdValue, Scalar> {
            pub fn new(#(#ctor_params,)*) -> Self {
                Self {
                    #(#ctor_inits,)*
                }
            }
        }

        pub fn #fn_name<Scalar>(
            #(#ctor_params,)*
        ) -> ScalarOperator<OptdValue, Scalar> {
            ScalarOperator::#name(#name::new(#(#field_names,)*))
        }
    }
}

#[test]
fn test_generate_logical_operator() {
    use crate::irs::hir::{Field, LogicalOp, OperatorKind, Type};
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

    // Basic validation
    let code = formatted.to_string();
    assert!(code.contains("pub child: Relation"));
    assert!(code.contains("pub predicate: Scalar"));
}

#[test]
fn test_working_file() {
    let input = include_str!("../programs/working.optd");
    let out = parse_file(input).unwrap();
    let mut analyzer = SemanticAnalyzer::new();
    analyzer.validate_file(&out).unwrap();

    let generated_code = generate_code(&out.operators);
    let syntax_tree: syn::File = parse_quote! {
        #generated_code
    };
    let formatted = prettyplease::unparse(&syntax_tree);
    println!("Generated code:\n{}", formatted);
}
*/
