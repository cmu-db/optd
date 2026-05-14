#![allow(deprecated)]

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use arrow_schema::{DataType, TimeUnit};
use substrait::proto;
use substrait::proto::{
    ComparisonJoinKey, CrossRel, Expression, FilterRel, FunctionArgument, HashJoinRel, JoinRel,
    MergeJoinRel, NamedStruct, NestedLoopJoinRel, Plan, PlanRel, ProjectRel, ReadRel, Rel,
    RelCommon, SortField, SortRel, Type, aggregate_function, aggregate_rel, comparison_join_key,
    expression, fetch_rel, function_argument, hash_join_rel, join_rel, merge_join_rel,
    nested_loop_join_rel, plan_rel, read_rel, rel, rel_common, sort_field, r#type,
};

use crate::{
    AggregateExpr, AggregateFunction, Aggregation, BinaryOp, Catalog, Column, ColumnData,
    CrossProduct, Expr, ExprData, Join, JoinType, Limit, Map, NaryOp, NullOrdering, Operator,
    OperatorData, Output, Projection, QueryContext, ScalarFunction, ScalarValue, Scan, Selection,
    Sort, SortDirection, SortKey, TableFunction, TableFunctionDef, TableRef, UnaryOp,
};

/// Converts Substrait protobuf plans into this crate's relational IR.
///
/// This importer intentionally starts with the logical subset this IR can represent:
/// reads, filters, projects/maps, joins, cross products, and simple aggregations.
pub fn from_plan(plan: &Plan) -> Result<QueryContext, SubstraitError> {
    Converter::new(plan).convert_plan(plan)
}

/// Converts a Substrait plan using `catalog` to resolve named table reads when possible.
pub fn from_plan_with_catalog(
    plan: &Plan,
    catalog: Arc<dyn Catalog>,
) -> Result<QueryContext, SubstraitError> {
    Converter::with_catalog(plan, catalog).convert_plan(plan)
}

/// Converts a single Substrait relation tree into this crate's relational IR.
pub fn from_rel(rel: &Rel) -> Result<QueryContext, SubstraitError> {
    let empty = Plan::default();
    Converter::new(&empty).convert_standalone_rel(rel)
}

/// Converts a single Substrait relation tree using `catalog` to resolve named table reads.
pub fn from_rel_with_catalog(
    rel: &Rel,
    catalog: Arc<dyn Catalog>,
) -> Result<QueryContext, SubstraitError> {
    let empty = Plan::default();
    Converter::with_catalog(&empty, catalog).convert_standalone_rel(rel)
}

/// Converts this crate's relational IR into a Substrait protobuf plan.
///
/// The exporter supports a conservative subset used by interop tests:
/// output roots, named-table scans, projections, filters, sorts, limits,
/// and simple scalar expressions.
pub fn to_plan(ctx: &QueryContext) -> Result<Plan, SubstraitError> {
    Exporter::new(ctx).export_plan()
}

/// Error produced while importing a Substrait plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubstraitError {
    EmptyPlan,
    MultipleRoots(usize),
    MissingField(&'static str),
    UnsupportedRel(&'static str),
    UnsupportedExpression(&'static str),
    UnsupportedType(&'static str),
    UnsupportedRead(&'static str),
    UnsupportedJoin(&'static str),
    UnsupportedAggregation(&'static str),
    UnsupportedSort(&'static str),
    UnsupportedFetch(&'static str),
    InvalidFetch { field: &'static str, value: i64 },
    InvalidFieldReference { index: usize, input_len: usize },
    InvalidEmit { index: i32, input_len: usize },
    InvalidGroupingReference { index: usize, input_len: usize },
}

impl fmt::Display for SubstraitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyPlan => f.write_str("Substrait plan does not contain a relation"),
            Self::MultipleRoots(count) => write!(f, "expected one Substrait root, found {count}"),
            Self::MissingField(field) => {
                write!(f, "Substrait message is missing required field {field}")
            }
            Self::UnsupportedRel(rel) => write!(f, "unsupported Substrait relation {rel}"),
            Self::UnsupportedExpression(expr) => {
                write!(f, "unsupported Substrait expression {expr}")
            }
            Self::UnsupportedType(ty) => write!(f, "unsupported Substrait type {ty}"),
            Self::UnsupportedRead(read) => write!(f, "unsupported Substrait read {read}"),
            Self::UnsupportedJoin(join) => write!(f, "unsupported Substrait join {join}"),
            Self::UnsupportedAggregation(aggr) => {
                write!(f, "unsupported Substrait aggregation {aggr}")
            }
            Self::UnsupportedSort(sort) => write!(f, "unsupported Substrait sort {sort}"),
            Self::UnsupportedFetch(fetch) => write!(f, "unsupported Substrait fetch {fetch}"),
            Self::InvalidFetch { field, value } => {
                write!(
                    f,
                    "Substrait fetch {field} must be non-negative, found {value}"
                )
            }
            Self::InvalidFieldReference { index, input_len } => {
                write!(
                    f,
                    "field reference {index} is outside input schema of {input_len} columns"
                )
            }
            Self::InvalidEmit { index, input_len } => {
                write!(
                    f,
                    "emit mapping {index} is outside input schema of {input_len} columns"
                )
            }
            Self::InvalidGroupingReference { index, input_len } => write!(
                f,
                "grouping reference {index} is outside grouping expression list of {input_len} expressions"
            ),
        }
    }
}

impl std::error::Error for SubstraitError {}

#[derive(Debug, Clone)]
struct Relation {
    operator: Operator,
    columns: Vec<Column>,
}

struct Converter {
    ctx: QueryContext,
    functions: HashMap<u32, String>,
    next_computed_column: usize,
    catalog: Option<Arc<dyn Catalog>>,
}

#[derive(Debug, Clone)]
struct ExportedRelation {
    rel: Rel,
    columns: Vec<Column>,
}

struct Exporter<'a> {
    ctx: &'a QueryContext,
    function_anchors: HashMap<String, u32>,
    next_function_anchor: u32,
}

impl<'a> Exporter<'a> {
    fn new(ctx: &'a QueryContext) -> Self {
        Self {
            ctx,
            function_anchors: HashMap::new(),
            next_function_anchor: 1,
        }
    }

    fn export_plan(mut self) -> Result<Plan, SubstraitError> {
        let root = self.ctx.root().ok_or(SubstraitError::EmptyPlan)?;
        let exported = self.export_operator(root)?;
        let names = exported
            .columns
            .iter()
            .map(|column| self.ctx.column(*column).name.clone())
            .collect();
        let extensions = self.export_function_extensions();
        let extension_urns = if extensions.is_empty() {
            Vec::new()
        } else {
            vec![proto::extensions::SimpleExtensionUrn {
                extension_urn_anchor: 1,
                urn: "extension:io.substrait:functions_simple_graph".to_string(),
            }]
        };

        Ok(Plan {
            relations: vec![PlanRel {
                rel_type: Some(plan_rel::RelType::Root(proto::RelRoot {
                    input: Some(exported.rel),
                    names,
                })),
            }],
            extension_urns,
            extensions,
            ..Plan::default()
        })
    }

    fn export_operator(&mut self, operator: Operator) -> Result<ExportedRelation, SubstraitError> {
        match self.ctx.operator(operator) {
            OperatorData::Output(output) => self.export_operator(output.input),
            OperatorData::Scan(scan) => self.export_scan(scan),
            OperatorData::Projection(projection) => self.export_projection(projection),
            OperatorData::Selection(selection) => self.export_selection(selection),
            OperatorData::Map(map) => self.export_map(map),
            OperatorData::TableFunction(table_function) => {
                self.export_table_function(table_function)
            }
            OperatorData::Join(join) => self.export_join(join),
            OperatorData::CrossProduct(cross) => self.export_cross(cross),
            OperatorData::Sort(sort) => self.export_sort(sort),
            OperatorData::Limit(limit) => self.export_limit(limit),
            OperatorData::Aggregation(aggregation) => self.export_aggregation(aggregation),
        }
    }

    fn export_scan(&mut self, scan: &Scan) -> Result<ExportedRelation, SubstraitError> {
        let columns = scan.columns.clone();
        let rel = Rel {
            rel_type: Some(rel::RelType::Read(Box::new(ReadRel {
                common: Some(direct_common()),
                base_schema: Some(self.export_named_struct(&columns)?),
                filter: None,
                best_effort_filter: None,
                projection: None,
                advanced_extension: None,
                read_type: Some(read_rel::ReadType::NamedTable(read_rel::NamedTable {
                    names: table_ref_names(&scan.table),
                    advanced_extension: None,
                })),
            }))),
        };
        Ok(ExportedRelation { rel, columns })
    }

    fn export_projection(
        &mut self,
        projection: &Projection,
    ) -> Result<ExportedRelation, SubstraitError> {
        let input = self.export_operator(projection.input)?;
        let output_mapping = projection
            .columns
            .iter()
            .map(|column| {
                input
                    .columns
                    .iter()
                    .position(|input_column| input_column == column)
                    .map(|index| index as i32)
                    .ok_or(SubstraitError::InvalidFieldReference {
                        index: column.0,
                        input_len: input.columns.len(),
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let rel = Rel {
            rel_type: Some(rel::RelType::Project(Box::new(ProjectRel {
                common: Some(emit_common(output_mapping)),
                input: Some(Box::new(input.rel)),
                expressions: Vec::new(),
                advanced_extension: None,
            }))),
        };
        Ok(ExportedRelation {
            rel,
            columns: projection.columns.clone(),
        })
    }

    fn export_selection(
        &mut self,
        selection: &Selection,
    ) -> Result<ExportedRelation, SubstraitError> {
        let input = self.export_operator(selection.input)?;
        let condition = self.export_expr(selection.predicate, &input.columns)?;
        let rel = Rel {
            rel_type: Some(rel::RelType::Filter(Box::new(FilterRel {
                common: Some(direct_common()),
                input: Some(Box::new(input.rel)),
                condition: Some(Box::new(condition)),
                advanced_extension: None,
            }))),
        };
        Ok(ExportedRelation {
            rel,
            columns: input.columns,
        })
    }

    fn export_map(&mut self, map: &Map) -> Result<ExportedRelation, SubstraitError> {
        let input = self.export_operator(map.input)?;
        let expressions = map
            .computations
            .iter()
            .map(|(column, expr)| {
                self.export_expr_with_type_hint(
                    *expr,
                    &input.columns,
                    Some(&self.ctx.column(*column).ty),
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut output_columns = input.columns.clone();
        output_columns.extend(map.computations.iter().map(|(column, _)| *column));
        let rel = Rel {
            rel_type: Some(rel::RelType::Project(Box::new(ProjectRel {
                common: Some(emit_common(identity_output_mapping(output_columns.len()))),
                input: Some(Box::new(input.rel)),
                expressions,
                advanced_extension: None,
            }))),
        };
        Ok(ExportedRelation {
            rel,
            columns: output_columns,
        })
    }

    fn export_table_function(
        &mut self,
        table_function: &TableFunction,
    ) -> Result<ExportedRelation, SubstraitError> {
        let path = table_function_path_arg(self.ctx, &table_function.args)?;
        let file_format = table_function_file_format(&table_function.function)?;
        let read = ReadRel {
            common: Some(direct_common()),
            base_schema: Some(self.export_named_struct(&table_function.columns)?),
            filter: None,
            best_effort_filter: None,
            projection: None,
            advanced_extension: None,
            read_type: Some(read_rel::ReadType::LocalFiles(read_rel::LocalFiles {
                items: vec![read_rel::local_files::FileOrFiles {
                    partition_index: 0,
                    start: 0,
                    length: 0,
                    path_type: Some(read_rel::local_files::file_or_files::PathType::UriPath(
                        path,
                    )),
                    file_format,
                }],
                advanced_extension: None,
            })),
        };
        Ok(ExportedRelation {
            rel: Rel {
                rel_type: Some(rel::RelType::Read(Box::new(read))),
            },
            columns: table_function.columns.clone(),
        })
    }

    fn export_join(&mut self, join: &Join) -> Result<ExportedRelation, SubstraitError> {
        let left = self.export_operator(join.outer)?;
        let right = self.export_operator(join.inner)?;
        let mut scope = left.columns.clone();
        scope.extend(right.columns.iter().copied());
        let condition = self.export_expr(join.on, &scope)?;
        let join_type = join_type_to_substrait(join.join_type.clone())? as i32;
        let rel = Rel {
            rel_type: Some(rel::RelType::Join(Box::new(JoinRel {
                common: Some(direct_common()),
                left: Some(Box::new(left.rel)),
                right: Some(Box::new(right.rel)),
                expression: Some(Box::new(condition)),
                post_join_filter: None,
                r#type: join_type,
                advanced_extension: None,
            }))),
        };
        Ok(ExportedRelation {
            rel,
            columns: scope,
        })
    }

    fn export_cross(&mut self, cross: &CrossProduct) -> Result<ExportedRelation, SubstraitError> {
        let left = self.export_operator(cross.outer)?;
        let right = self.export_operator(cross.inner)?;
        let mut columns = left.columns.clone();
        columns.extend(right.columns.iter().copied());
        let rel = Rel {
            rel_type: Some(rel::RelType::Cross(Box::new(CrossRel {
                common: Some(direct_common()),
                left: Some(Box::new(left.rel)),
                right: Some(Box::new(right.rel)),
                advanced_extension: None,
            }))),
        };
        Ok(ExportedRelation { rel, columns })
    }

    fn export_sort(&mut self, sort: &Sort) -> Result<ExportedRelation, SubstraitError> {
        let input = self.export_operator(sort.input)?;
        let sorts = sort
            .keys
            .iter()
            .map(|key| {
                Ok(SortField {
                    expr: Some(self.export_expr(key.expr, &input.columns)?),
                    sort_kind: Some(sort_field::SortKind::Direction(
                        sort_direction_to_substrait(key.direction, key.nulls) as i32,
                    )),
                })
            })
            .collect::<Result<Vec<_>, SubstraitError>>()?;
        let rel = Rel {
            rel_type: Some(rel::RelType::Sort(Box::new(SortRel {
                common: Some(direct_common()),
                input: Some(Box::new(input.rel)),
                sorts,
                advanced_extension: None,
            }))),
        };
        Ok(ExportedRelation {
            rel,
            columns: input.columns,
        })
    }

    fn export_limit(&mut self, limit: &Limit) -> Result<ExportedRelation, SubstraitError> {
        let input = self.export_operator(limit.input)?;
        let offset_mode = if limit.offset == 0 {
            None
        } else {
            Some(fetch_rel::OffsetMode::Offset(
                i64::try_from(limit.offset)
                    .map_err(|_| SubstraitError::UnsupportedFetch("offset is too large"))?,
            ))
        };
        let count_mode = match limit.fetch {
            Some(fetch) => Some(fetch_rel::CountMode::Count(
                i64::try_from(fetch)
                    .map_err(|_| SubstraitError::UnsupportedFetch("count is too large"))?,
            )),
            None => None,
        };
        let rel = Rel {
            rel_type: Some(rel::RelType::Fetch(Box::new(proto::FetchRel {
                common: Some(direct_common()),
                input: Some(Box::new(input.rel)),
                advanced_extension: None,
                offset_mode,
                count_mode,
            }))),
        };
        Ok(ExportedRelation {
            rel,
            columns: input.columns,
        })
    }

    fn export_aggregation(
        &mut self,
        aggregation: &Aggregation,
    ) -> Result<ExportedRelation, SubstraitError> {
        let input = self.export_operator(aggregation.input)?;
        let grouping_expressions = aggregation
            .keys
            .iter()
            .map(|key| self.export_expr(*key, &input.columns))
            .collect::<Result<Vec<_>, _>>()?;
        let measures = aggregation
            .aggregates
            .iter()
            .map(|(column, aggregate)| self.export_measure(*column, aggregate, &input.columns))
            .collect::<Result<Vec<_>, _>>()?;
        let mut output_columns = aggregation
            .keys
            .iter()
            .map(|key| self.aggregation_key_column(*key))
            .collect::<Result<Vec<_>, _>>()?;
        output_columns.extend(aggregation.aggregates.iter().map(|(column, _)| *column));

        let groupings = if !grouping_expressions.is_empty() || measures.is_empty() {
            vec![aggregate_rel::Grouping {
                grouping_expressions: Vec::new(),
                expression_references: (0..grouping_expressions.len() as u32).collect(),
            }]
        } else {
            Vec::new()
        };
        let rel = Rel {
            rel_type: Some(rel::RelType::Aggregate(Box::new(proto::AggregateRel {
                common: Some(emit_common(
                    (0..output_columns.len() as i32).collect::<Vec<_>>(),
                )),
                input: Some(Box::new(input.rel)),
                groupings,
                measures,
                grouping_expressions,
                advanced_extension: None,
            }))),
        };
        Ok(ExportedRelation {
            rel,
            columns: output_columns,
        })
    }

    fn export_measure(
        &mut self,
        output_column: Column,
        aggregate: &AggregateExpr,
        scope: &[Column],
    ) -> Result<aggregate_rel::Measure, SubstraitError> {
        let (function_name, args, invocation) = match aggregate {
            AggregateExpr::CountStar => (
                "count".to_string(),
                Vec::new(),
                aggregate_function::AggregationInvocation::All as i32,
            ),
            AggregateExpr::Func {
                func,
                arg,
                distinct,
            } => (
                func.to_string(),
                vec![self.export_expr(*arg, scope)?],
                if *distinct {
                    aggregate_function::AggregationInvocation::Distinct as i32
                } else {
                    aggregate_function::AggregationInvocation::All as i32
                },
            ),
        };
        let arguments = args
            .iter()
            .cloned()
            .map(|arg| FunctionArgument {
                arg_type: Some(function_argument::ArgType::Value(arg)),
            })
            .collect();
        let measure = proto::AggregateFunction {
            function_reference: self.register_function(&function_name),
            arguments,
            options: Vec::new(),
            output_type: Some(arrow_to_substrait_type(&self.ctx.column(output_column).ty)?),
            phase: proto::AggregationPhase::InitialToResult as i32,
            sorts: Vec::new(),
            invocation,
            args,
        };
        Ok(aggregate_rel::Measure {
            measure: Some(measure),
            filter: None,
        })
    }

    fn aggregation_key_column(&self, key: Expr) -> Result<Column, SubstraitError> {
        match self.ctx.expr(key) {
            ExprData::ColumnRef(column) => Ok(*column),
            _ => Err(SubstraitError::UnsupportedAggregation(
                "expression aggregation key export",
            )),
        }
    }

    fn export_expr(&mut self, expr: Expr, scope: &[Column]) -> Result<Expression, SubstraitError> {
        self.export_expr_with_type_hint(expr, scope, None)
    }

    fn export_expr_with_type_hint(
        &mut self,
        expr: Expr,
        scope: &[Column],
        output_type_hint: Option<&DataType>,
    ) -> Result<Expression, SubstraitError> {
        let inferred_output_type = infer_expr_data_type(self.ctx, expr)
            .as_ref()
            .and_then(|data_type| arrow_to_substrait_type(data_type).ok());
        let hinted_output_type =
            output_type_hint.and_then(|data_type| arrow_to_substrait_type(data_type).ok());
        let rex_type = match self.ctx.expr(expr) {
            ExprData::Literal(value) => expression::RexType::Literal(export_literal(value)?),
            ExprData::ColumnRef(column) => {
                let index = scope
                    .iter()
                    .position(|input_column| input_column == column)
                    .ok_or(SubstraitError::InvalidFieldReference {
                        index: column.0,
                        input_len: scope.len(),
                    })?;
                expression::RexType::Selection(Box::new(field_reference(index as i32)))
            }
            ExprData::Unary { op, expr } => {
                let argument = self.export_expr(*expr, scope)?;
                let output_type = match op {
                    UnaryOp::Not | UnaryOp::IsNull | UnaryOp::IsNotNull => {
                        Some(arrow_to_substrait_type(&DataType::Boolean)?)
                    }
                    UnaryOp::Negate => hinted_output_type
                        .clone()
                        .or_else(|| inferred_output_type.clone()),
                };
                expression::RexType::ScalarFunction(self.export_scalar_function(
                    unary_op_function_name(*op),
                    vec![argument],
                    output_type,
                ))
            }
            ExprData::Binary { op, left, right } => {
                let left = self.export_expr(*left, scope)?;
                let right = self.export_expr(*right, scope)?;
                let output_type = match op {
                    BinaryOp::Eq
                    | BinaryOp::NotEq
                    | BinaryOp::Lt
                    | BinaryOp::LtEq
                    | BinaryOp::Gt
                    | BinaryOp::GtEq => Some(arrow_to_substrait_type(&DataType::Boolean)?),
                    BinaryOp::Add | BinaryOp::Subtract | BinaryOp::Multiply | BinaryOp::Divide => {
                        hinted_output_type
                            .clone()
                            .or_else(|| inferred_output_type.clone())
                    }
                };
                expression::RexType::ScalarFunction(self.export_scalar_function(
                    binary_op_function_name(*op),
                    vec![left, right],
                    output_type,
                ))
            }
            ExprData::Nary { op, exprs } => {
                let arguments = exprs
                    .iter()
                    .map(|expr| self.export_expr(*expr, scope))
                    .collect::<Result<Vec<_>, _>>()?;
                expression::RexType::ScalarFunction(self.export_scalar_function(
                    nary_op_function_name(*op),
                    arguments,
                    Some(arrow_to_substrait_type(&DataType::Boolean)?),
                ))
            }
            ExprData::CaseWhen {
                when_then,
                else_expr,
            } => {
                let ifs = when_then
                    .iter()
                    .map(|(when, then)| {
                        Ok(expression::if_then::IfClause {
                            r#if: Some(self.export_expr(*when, scope)?),
                            then: Some(self.export_expr(*then, scope)?),
                        })
                    })
                    .collect::<Result<Vec<_>, SubstraitError>>()?;
                let r#else = else_expr
                    .map(|expr| self.export_expr(expr, scope))
                    .transpose()?
                    .map(Box::new);
                expression::RexType::IfThen(Box::new(expression::IfThen { ifs, r#else }))
            }
            ExprData::ScalarFunction { function, args } => {
                let arguments = args
                    .iter()
                    .map(|expr| self.export_expr(*expr, scope))
                    .collect::<Result<Vec<_>, _>>()?;
                let function_name = function.to_string();
                let normalized = normalize_function_name(&function_name);
                if normalized == "cast" && arguments.len() == 1 {
                    expression::RexType::Cast(Box::new(expression::Cast {
                        r#type: hinted_output_type
                            .clone()
                            .or_else(|| inferred_output_type.clone()),
                        input: Some(Box::new(arguments[0].clone())),
                        failure_behavior: expression::cast::FailureBehavior::ThrowException as i32,
                    }))
                } else {
                    expression::RexType::ScalarFunction(
                        self.export_scalar_function(
                            &function_name,
                            arguments,
                            hinted_output_type
                                .clone()
                                .or_else(|| inferred_output_type.clone()),
                        ),
                    )
                }
            }
        };
        Ok(Expression {
            rex_type: Some(rex_type),
        })
    }

    fn export_scalar_function(
        &mut self,
        name: &str,
        args: Vec<Expression>,
        output_type: Option<Type>,
    ) -> expression::ScalarFunction {
        let anchor = self.register_function(name);
        let arguments = args
            .iter()
            .cloned()
            .map(|arg| FunctionArgument {
                arg_type: Some(function_argument::ArgType::Value(arg)),
            })
            .collect();
        expression::ScalarFunction {
            function_reference: anchor,
            arguments,
            options: Vec::new(),
            output_type,
            args,
        }
    }

    fn register_function(&mut self, name: &str) -> u32 {
        if let Some(anchor) = self.function_anchors.get(name) {
            return *anchor;
        }
        let anchor = self.next_function_anchor;
        self.next_function_anchor += 1;
        self.function_anchors.insert(name.to_string(), anchor);
        anchor
    }

    fn export_function_extensions(&self) -> Vec<proto::extensions::SimpleExtensionDeclaration> {
        let mut by_anchor = self
            .function_anchors
            .iter()
            .map(|(name, anchor)| (*anchor, name.clone()))
            .collect::<Vec<_>>();
        by_anchor.sort_by_key(|(anchor, _)| *anchor);
        by_anchor
            .into_iter()
            .map(
                |(anchor, name)| {
                    proto::extensions::SimpleExtensionDeclaration {
                mapping_type: Some(
                    proto::extensions::simple_extension_declaration::MappingType::ExtensionFunction(
                        proto::extensions::simple_extension_declaration::ExtensionFunction {
                            extension_urn_reference: 1,
                            function_anchor: anchor,
                            name,
                        },
                    ),
                ),
            }
                },
            )
            .collect()
    }

    fn export_named_struct(&self, columns: &[Column]) -> Result<NamedStruct, SubstraitError> {
        Ok(NamedStruct {
            names: columns
                .iter()
                .map(|column| self.ctx.column(*column).name.clone())
                .collect(),
            r#struct: Some(r#type::Struct {
                types: columns
                    .iter()
                    .map(|column| arrow_to_substrait_type(&self.ctx.column(*column).ty))
                    .collect::<Result<Vec<_>, _>>()?,
                type_variation_reference: 0,
                nullability: r#type::Nullability::Required as i32,
            }),
        })
    }
}

fn direct_common() -> RelCommon {
    RelCommon {
        hint: None,
        advanced_extension: None,
        emit_kind: Some(rel_common::EmitKind::Direct(rel_common::Direct {})),
    }
}

fn emit_common(output_mapping: Vec<i32>) -> RelCommon {
    RelCommon {
        hint: None,
        advanced_extension: None,
        emit_kind: Some(rel_common::EmitKind::Emit(rel_common::Emit {
            output_mapping,
        })),
    }
}

fn identity_output_mapping(len: usize) -> Vec<i32> {
    (0..len as i32).collect()
}

fn field_reference(index: i32) -> expression::FieldReference {
    expression::FieldReference {
        reference_type: Some(expression::field_reference::ReferenceType::DirectReference(
            expression::ReferenceSegment {
                reference_type: Some(expression::reference_segment::ReferenceType::StructField(
                    Box::new(expression::reference_segment::StructField {
                        field: index,
                        child: None,
                    }),
                )),
            },
        )),
        root_type: Some(expression::field_reference::RootType::RootReference(
            expression::field_reference::RootReference {},
        )),
    }
}

fn export_literal(value: &ScalarValue) -> Result<expression::Literal, SubstraitError> {
    let literal_type = match value {
        ScalarValue::Null(ty) => {
            expression::literal::LiteralType::Null(arrow_to_substrait_type(ty)?)
        }
        ScalarValue::Boolean(value) => expression::literal::LiteralType::Boolean(*value),
        ScalarValue::Int32(value) => expression::literal::LiteralType::I32(*value),
        ScalarValue::Int64(value) => expression::literal::LiteralType::I64(*value),
        ScalarValue::Float64(value) => expression::literal::LiteralType::Fp64(*value),
        ScalarValue::Utf8(value) => expression::literal::LiteralType::String(value.clone()),
    };
    Ok(expression::Literal {
        nullable: matches!(value, ScalarValue::Null(_)),
        type_variation_reference: 0,
        literal_type: Some(literal_type),
    })
}

fn infer_expr_data_type(ctx: &QueryContext, expr: Expr) -> Option<DataType> {
    match ctx.expr(expr) {
        ExprData::Literal(value) => Some(value.data_type()),
        ExprData::ColumnRef(column) => Some(ctx.column(*column).ty.clone()),
        ExprData::Unary { op, expr } => match op {
            UnaryOp::Not | UnaryOp::IsNull | UnaryOp::IsNotNull => Some(DataType::Boolean),
            UnaryOp::Negate => infer_expr_data_type(ctx, *expr),
        },
        ExprData::Binary { op, left, right: _ } => match op {
            BinaryOp::Eq
            | BinaryOp::NotEq
            | BinaryOp::Lt
            | BinaryOp::LtEq
            | BinaryOp::Gt
            | BinaryOp::GtEq => Some(DataType::Boolean),
            BinaryOp::Add | BinaryOp::Subtract | BinaryOp::Multiply | BinaryOp::Divide => {
                infer_expr_data_type(ctx, *left)
            }
        },
        ExprData::Nary { op: _, exprs: _ } => Some(DataType::Boolean),
        ExprData::CaseWhen {
            when_then,
            else_expr,
        } => when_then
            .iter()
            .find_map(|(_, then)| infer_expr_data_type(ctx, *then))
            .or_else(|| else_expr.and_then(|expr| infer_expr_data_type(ctx, expr))),
        ExprData::ScalarFunction { function, args } => match function {
            ScalarFunction::Lower | ScalarFunction::Upper => Some(DataType::Utf8),
            ScalarFunction::Coalesce => args.iter().find_map(|arg| infer_expr_data_type(ctx, *arg)),
            ScalarFunction::Abs => args.first().and_then(|arg| infer_expr_data_type(ctx, *arg)),
            ScalarFunction::Extension(_) => None,
        },
    }
}

fn unary_op_function_name(op: UnaryOp) -> &'static str {
    match op {
        UnaryOp::Not => "not",
        UnaryOp::IsNull => "is_null",
        UnaryOp::IsNotNull => "is_not_null",
        UnaryOp::Negate => "negate",
    }
}

fn binary_op_function_name(op: BinaryOp) -> &'static str {
    match op {
        BinaryOp::Eq => "equal",
        BinaryOp::NotEq => "not_equal",
        BinaryOp::Lt => "lt",
        BinaryOp::LtEq => "lte",
        BinaryOp::Gt => "gt",
        BinaryOp::GtEq => "gte",
        BinaryOp::Add => "add",
        BinaryOp::Subtract => "subtract",
        BinaryOp::Multiply => "multiply",
        BinaryOp::Divide => "divide",
    }
}

fn nary_op_function_name(op: NaryOp) -> &'static str {
    match op {
        NaryOp::And => "and",
        NaryOp::Or => "or",
    }
}

fn sort_direction_to_substrait(
    direction: SortDirection,
    nulls: NullOrdering,
) -> sort_field::SortDirection {
    match (direction, nulls) {
        (SortDirection::Asc, NullOrdering::Last) => sort_field::SortDirection::AscNullsLast,
        (SortDirection::Asc, NullOrdering::First) => sort_field::SortDirection::AscNullsFirst,
        (SortDirection::Desc, NullOrdering::Last) => sort_field::SortDirection::DescNullsLast,
        (SortDirection::Desc, NullOrdering::First) => sort_field::SortDirection::DescNullsFirst,
    }
}

fn join_type_to_substrait(join_type: JoinType) -> Result<join_rel::JoinType, SubstraitError> {
    match join_type {
        JoinType::Inner => Ok(join_rel::JoinType::Inner),
        JoinType::LeftOuter => Ok(join_rel::JoinType::Left),
        JoinType::RightOuter => Ok(join_rel::JoinType::Right),
        JoinType::FullOuter => Ok(join_rel::JoinType::Outer),
        JoinType::Single => Ok(join_rel::JoinType::LeftSingle),
        JoinType::Mark(_) => Err(SubstraitError::UnsupportedJoin("mark join marker column")),
    }
}

fn table_function_path_arg(ctx: &QueryContext, args: &[Expr]) -> Result<String, SubstraitError> {
    let first = args.first().ok_or(SubstraitError::UnsupportedRel(
        "TableFunction path argument export",
    ))?;
    match ctx.expr(*first) {
        ExprData::Literal(ScalarValue::Utf8(path)) => Ok(path.clone()),
        _ => Err(SubstraitError::UnsupportedRel(
            "TableFunction non-string path export",
        )),
    }
}

fn table_function_file_format(
    function: &TableFunctionDef,
) -> Result<Option<read_rel::local_files::file_or_files::FileFormat>, SubstraitError> {
    use read_rel::local_files::file_or_files::{
        ArrowReadOptions, DelimiterSeparatedTextReadOptions, DwrfReadOptions, FileFormat,
        OrcReadOptions, ParquetReadOptions,
    };

    let file_format = match function {
        TableFunctionDef::ReadParquet => Some(FileFormat::Parquet(ParquetReadOptions {})),
        TableFunctionDef::ReadCsv => Some(FileFormat::Text(DelimiterSeparatedTextReadOptions {
            field_delimiter: ",".to_string(),
            max_line_size: 1 << 20,
            quote: "\"".to_string(),
            header_lines_to_skip: 0,
            escape: "\\".to_string(),
            value_treated_as_null: None,
        })),
        TableFunctionDef::Extension(name) if normalize_function_name(name) == "read_arrow" => {
            Some(FileFormat::Arrow(ArrowReadOptions {}))
        }
        TableFunctionDef::Extension(name) if normalize_function_name(name) == "read_orc" => {
            Some(FileFormat::Orc(OrcReadOptions {}))
        }
        TableFunctionDef::Extension(name) if normalize_function_name(name) == "read_dwrf" => {
            Some(FileFormat::Dwrf(DwrfReadOptions {}))
        }
        TableFunctionDef::Extension(name) if normalize_function_name(name) == "read_extension" => {
            return Err(SubstraitError::UnsupportedRel(
                "TableFunction read_extension format export",
            ));
        }
        TableFunctionDef::Extension(name) if normalize_function_name(name) == "read_files" => None,
        _ => return Err(SubstraitError::UnsupportedRel("TableFunction export")),
    };
    Ok(file_format)
}

fn table_ref_names(table: &TableRef) -> Vec<String> {
    match table {
        TableRef::Bare { table } => vec![table.to_string()],
        TableRef::Partial { schema, table } => vec![schema.to_string(), table.to_string()],
        TableRef::Full {
            catalog,
            schema,
            table,
        } => vec![catalog.to_string(), schema.to_string(), table.to_string()],
    }
}

fn arrow_to_substrait_type(data_type: &DataType) -> Result<Type, SubstraitError> {
    let nullability = r#type::Nullability::Required as i32;
    let kind = match data_type {
        DataType::Boolean => r#type::Kind::Bool(r#type::Boolean {
            type_variation_reference: 0,
            nullability,
        }),
        DataType::Int8 => r#type::Kind::I8(r#type::I8 {
            type_variation_reference: 0,
            nullability,
        }),
        DataType::Int16 => r#type::Kind::I16(r#type::I16 {
            type_variation_reference: 0,
            nullability,
        }),
        DataType::Int32 => r#type::Kind::I32(r#type::I32 {
            type_variation_reference: 0,
            nullability,
        }),
        DataType::Int64 => r#type::Kind::I64(r#type::I64 {
            type_variation_reference: 0,
            nullability,
        }),
        DataType::Float32 => r#type::Kind::Fp32(r#type::Fp32 {
            type_variation_reference: 0,
            nullability,
        }),
        DataType::Float64 => r#type::Kind::Fp64(r#type::Fp64 {
            type_variation_reference: 0,
            nullability,
        }),
        DataType::Utf8 => r#type::Kind::String(r#type::String {
            type_variation_reference: 0,
            nullability,
        }),
        DataType::Date32 => r#type::Kind::Date(r#type::Date {
            type_variation_reference: 0,
            nullability,
        }),
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            r#type::Kind::Timestamp(r#type::Timestamp {
                type_variation_reference: 0,
                nullability,
            })
        }
        DataType::Timestamp(unit, None) => {
            r#type::Kind::PrecisionTimestamp(r#type::PrecisionTimestamp {
                type_variation_reference: 0,
                nullability,
                precision: match unit {
                    TimeUnit::Second => 0,
                    TimeUnit::Millisecond => 3,
                    TimeUnit::Microsecond => 6,
                    TimeUnit::Nanosecond => 9,
                },
            })
        }
        DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => {
            r#type::Kind::TimestampTz(r#type::TimestampTz {
                type_variation_reference: 0,
                nullability,
            })
        }
        DataType::Timestamp(unit, Some(_)) => {
            r#type::Kind::PrecisionTimestampTz(r#type::PrecisionTimestampTz {
                type_variation_reference: 0,
                nullability,
                precision: match unit {
                    TimeUnit::Second => 0,
                    TimeUnit::Millisecond => 3,
                    TimeUnit::Microsecond => 6,
                    TimeUnit::Nanosecond => 9,
                },
            })
        }
        DataType::Decimal128(precision, scale) => r#type::Kind::Decimal(r#type::Decimal {
            type_variation_reference: 0,
            nullability,
            precision: i32::from(*precision),
            scale: i32::from(*scale),
        }),
        _ => return Err(SubstraitError::UnsupportedType("exported Arrow type")),
    };
    Ok(Type { kind: Some(kind) })
}

impl Converter {
    fn new(plan: &Plan) -> Self {
        Self {
            ctx: QueryContext::new(),
            functions: collect_function_anchors(plan),
            next_computed_column: 0,
            catalog: None,
        }
    }

    fn with_catalog(plan: &Plan, catalog: Arc<dyn Catalog>) -> Self {
        Self {
            ctx: QueryContext::new(),
            functions: collect_function_anchors(plan),
            next_computed_column: 0,
            catalog: Some(catalog),
        }
    }

    fn convert_plan(mut self, plan: &Plan) -> Result<QueryContext, SubstraitError> {
        let roots = plan
            .relations
            .iter()
            .filter(|relation| relation.rel_type.is_some())
            .collect::<Vec<_>>();

        match roots.len() {
            0 => Err(SubstraitError::EmptyPlan),
            1 => {
                let relation = self.convert_plan_rel(roots[0])?;
                let output = self.ctx.add_operator(OperatorData::Output(Output {
                    input: relation.operator,
                }));
                self.ctx.set_root(output);
                Ok(self.ctx)
            }
            count => Err(SubstraitError::MultipleRoots(count)),
        }
    }

    fn convert_standalone_rel(mut self, rel: &Rel) -> Result<QueryContext, SubstraitError> {
        let relation = self.convert_rel(rel)?;
        let output = self.ctx.add_operator(OperatorData::Output(Output {
            input: relation.operator,
        }));
        self.ctx.set_root(output);
        Ok(self.ctx)
    }

    fn convert_plan_rel(&mut self, plan_rel: &PlanRel) -> Result<Relation, SubstraitError> {
        match plan_rel
            .rel_type
            .as_ref()
            .ok_or(SubstraitError::MissingField("PlanRel.rel_type"))?
        {
            plan_rel::RelType::Rel(rel) => self.convert_rel(rel),
            plan_rel::RelType::Root(root) => {
                let input = root
                    .input
                    .as_ref()
                    .ok_or(SubstraitError::MissingField("RelRoot.input"))?;
                let mut relation = self.convert_rel(input)?;
                self.apply_root_names(&mut relation, &root.names);
                Ok(relation)
            }
        }
    }

    fn convert_rel(&mut self, rel: &Rel) -> Result<Relation, SubstraitError> {
        match rel
            .rel_type
            .as_ref()
            .ok_or(SubstraitError::MissingField("Rel.rel_type"))?
        {
            rel::RelType::Read(read) => self.convert_read(read),
            rel::RelType::Filter(filter) => {
                let input = filter
                    .input
                    .as_ref()
                    .ok_or(SubstraitError::MissingField("FilterRel.input"))?;
                let relation = self.convert_rel(input)?;
                let condition = filter
                    .condition
                    .as_ref()
                    .ok_or(SubstraitError::MissingField("FilterRel.condition"))?;
                let predicate = self.convert_expr(condition, &relation.columns)?;
                let operator = self.ctx.add_operator(OperatorData::Selection(Selection {
                    predicate,
                    input: relation.operator,
                }));
                self.apply_common(
                    filter.common.as_ref(),
                    Relation {
                        operator,
                        ..relation
                    },
                )
            }
            rel::RelType::Project(project) => self.convert_project(project),
            rel::RelType::Join(join) => self.convert_join(join),
            rel::RelType::Cross(cross) => self.convert_cross(cross),
            rel::RelType::Aggregate(aggregate) => self.convert_aggregate(aggregate),
            rel::RelType::Fetch(fetch) => self.convert_fetch(fetch),
            rel::RelType::Sort(sort) => self.convert_sort(sort),
            rel::RelType::Set(_) => Err(SubstraitError::UnsupportedRel("SetRel")),
            rel::RelType::ExtensionSingle(_) => {
                Err(SubstraitError::UnsupportedRel("ExtensionSingleRel"))
            }
            rel::RelType::ExtensionMulti(_) => {
                Err(SubstraitError::UnsupportedRel("ExtensionMultiRel"))
            }
            rel::RelType::ExtensionLeaf(_) => {
                Err(SubstraitError::UnsupportedRel("ExtensionLeafRel"))
            }
            rel::RelType::Reference(_) => Err(SubstraitError::UnsupportedRel("ReferenceRel")),
            rel::RelType::Write(_) => Err(SubstraitError::UnsupportedRel("WriteRel")),
            rel::RelType::Ddl(_) => Err(SubstraitError::UnsupportedRel("DdlRel")),
            rel::RelType::Update(_) => Err(SubstraitError::UnsupportedRel("UpdateRel")),
            rel::RelType::HashJoin(join) => self.convert_hash_join(join),
            rel::RelType::MergeJoin(join) => self.convert_merge_join(join),
            rel::RelType::NestedLoopJoin(join) => self.convert_nested_loop_join(join),
            rel::RelType::Window(_) => Err(SubstraitError::UnsupportedRel(
                "ConsistentPartitionWindowRel",
            )),
            rel::RelType::Exchange(_) => Err(SubstraitError::UnsupportedRel("ExchangeRel")),
            rel::RelType::Expand(_) => Err(SubstraitError::UnsupportedRel("ExpandRel")),
        }
    }

    fn convert_read(&mut self, read: &ReadRel) -> Result<Relation, SubstraitError> {
        let mut relation = match read
            .read_type
            .as_ref()
            .ok_or(SubstraitError::MissingField("ReadRel.read_type"))?
        {
            read_rel::ReadType::NamedTable(table) => {
                let table = Self::convert_named_table_ref(&table.names);
                self.convert_named_table_read(table, read.base_schema.as_ref())?
            }
            read_rel::ReadType::LocalFiles(files) => {
                let columns = self.convert_named_struct(read.base_schema.as_ref())?;
                let (function, args) = self.convert_local_files(files)?;
                let operator = self
                    .ctx
                    .add_operator(OperatorData::TableFunction(TableFunction {
                        function,
                        args,
                        columns: columns.clone(),
                    }));
                Relation { operator, columns }
            }
            read_rel::ReadType::VirtualTable(_) => {
                return Err(SubstraitError::UnsupportedRead("ReadRel.VirtualTable"));
            }
            read_rel::ReadType::ExtensionTable(_) => {
                return Err(SubstraitError::UnsupportedRead("ReadRel.ExtensionTable"));
            }
            read_rel::ReadType::IcebergTable(_) => {
                return Err(SubstraitError::UnsupportedRead("ReadRel.IcebergTable"));
            }
        };

        if let Some(filter) = &read.filter {
            let predicate = self.convert_expr(filter, &relation.columns)?;
            let operator = self.ctx.add_operator(OperatorData::Selection(Selection {
                predicate,
                input: relation.operator,
            }));
            relation.operator = operator;
        }

        relation = self.apply_read_projection(read.projection.as_ref(), relation)?;

        if read.best_effort_filter.is_some() {
            return Err(SubstraitError::UnsupportedRead(
                "ReadRel.best_effort_filter",
            ));
        }

        self.apply_common(read.common.as_ref(), relation)
    }

    fn apply_read_projection(
        &mut self,
        projection: Option<&expression::MaskExpression>,
        relation: Relation,
    ) -> Result<Relation, SubstraitError> {
        let Some(projection) = projection else {
            return Ok(relation);
        };
        let Some(select) = projection.select.as_ref() else {
            return Ok(relation);
        };

        let columns = select
            .struct_items
            .iter()
            .map(|item| {
                if item.child.is_some() {
                    return Err(SubstraitError::UnsupportedRead(
                        "ReadRel.projection nested field",
                    ));
                }
                if item.field < 0 || item.field as usize >= relation.columns.len() {
                    return Err(SubstraitError::InvalidEmit {
                        index: item.field,
                        input_len: relation.columns.len(),
                    });
                }
                Ok(relation.columns[item.field as usize])
            })
            .collect::<Result<Vec<_>, _>>()?;

        let operator = self.ctx.add_operator(OperatorData::Projection(Projection {
            columns: columns.clone(),
            input: relation.operator,
        }));
        Ok(Relation { operator, columns })
    }

    fn convert_named_table_read(
        &mut self,
        table: TableRef,
        base_schema: Option<&NamedStruct>,
    ) -> Result<Relation, SubstraitError> {
        if let Some(catalog) = self.catalog.clone() {
            if let Ok(operator) = self
                .ctx
                .add_scan_from_catalog(catalog.as_ref(), table.clone())
            {
                let OperatorData::Scan(scan) = operator.get(&self.ctx) else {
                    unreachable!("add_scan_from_catalog always creates a scan")
                };
                return Ok(Relation {
                    operator,
                    columns: scan.columns.clone(),
                });
            }
        }

        let columns = self.convert_named_struct(base_schema)?;
        let operator = self.ctx.add_operator(OperatorData::Scan(Scan {
            table,
            columns: columns.clone(),
        }));
        Ok(Relation { operator, columns })
    }

    fn convert_project(&mut self, project: &ProjectRel) -> Result<Relation, SubstraitError> {
        let input = project
            .input
            .as_ref()
            .ok_or(SubstraitError::MissingField("ProjectRel.input"))?;
        let relation = self.convert_rel(input)?;

        if project.expressions.is_empty() {
            return self.apply_common(project.common.as_ref(), relation);
        }

        let computations = project
            .expressions
            .iter()
            .enumerate()
            .map(|(index, expr)| {
                let value = self.convert_expr(expr, &relation.columns)?;
                let column =
                    self.add_computed_column(format!("expr_{index}"), self.expr_output_type(expr)?);
                Ok((column, value))
            })
            .collect::<Result<Vec<_>, SubstraitError>>()?;

        let mut columns = relation.columns.clone();
        columns.extend(computations.iter().map(|(column, _)| *column));
        let operator = self.ctx.add_operator(OperatorData::Map(Map {
            computations,
            input: relation.operator,
        }));
        self.apply_common(project.common.as_ref(), Relation { operator, columns })
    }

    fn convert_join(&mut self, join: &JoinRel) -> Result<Relation, SubstraitError> {
        let left = self.convert_rel(
            join.left
                .as_ref()
                .ok_or(SubstraitError::MissingField("JoinRel.left"))?,
        )?;
        let right = self.convert_rel(
            join.right
                .as_ref()
                .ok_or(SubstraitError::MissingField("JoinRel.right"))?,
        )?;

        let mut scope = left.columns.clone();
        scope.extend(right.columns.iter().copied());
        let on = if let Some(expression) = &join.expression {
            self.convert_expr(expression, &scope)?
        } else {
            self.ctx
                .add_expr(ExprData::Literal(ScalarValue::Boolean(true)))
        };

        let join_type = convert_join_type(join.r#type)?;
        let operator = self.ctx.add_operator(OperatorData::Join(Join {
            join_type,
            on,
            outer: left.operator,
            inner: right.operator,
        }));
        let operator =
            self.apply_post_join_filter(operator, &scope, join.post_join_filter.as_deref())?;
        self.apply_common(
            project_common_join(join),
            Relation {
                operator,
                columns: scope,
            },
        )
    }

    fn convert_hash_join(&mut self, join: &HashJoinRel) -> Result<Relation, SubstraitError> {
        let left = self.convert_rel(
            join.left
                .as_ref()
                .ok_or(SubstraitError::MissingField("HashJoinRel.left"))?,
        )?;
        let right = self.convert_rel(
            join.right
                .as_ref()
                .ok_or(SubstraitError::MissingField("HashJoinRel.right"))?,
        )?;
        let mut scope = left.columns.clone();
        scope.extend(right.columns.iter().copied());
        let on = self.convert_comparison_join_condition(
            &join.keys,
            &join.left_keys,
            &join.right_keys,
            &left.columns,
            &right.columns,
        )?;
        let join_type = convert_hash_join_type(join.r#type)?;
        let operator = self.ctx.add_operator(OperatorData::Join(Join {
            join_type,
            on,
            outer: left.operator,
            inner: right.operator,
        }));
        let operator =
            self.apply_post_join_filter(operator, &scope, join.post_join_filter.as_deref())?;
        self.apply_common(
            join.common.as_ref(),
            Relation {
                operator,
                columns: scope,
            },
        )
    }

    fn convert_merge_join(&mut self, join: &MergeJoinRel) -> Result<Relation, SubstraitError> {
        let left = self.convert_rel(
            join.left
                .as_ref()
                .ok_or(SubstraitError::MissingField("MergeJoinRel.left"))?,
        )?;
        let right = self.convert_rel(
            join.right
                .as_ref()
                .ok_or(SubstraitError::MissingField("MergeJoinRel.right"))?,
        )?;
        let mut scope = left.columns.clone();
        scope.extend(right.columns.iter().copied());
        let on = self.convert_comparison_join_condition(
            &join.keys,
            &join.left_keys,
            &join.right_keys,
            &left.columns,
            &right.columns,
        )?;
        let join_type = convert_merge_join_type(join.r#type)?;
        let operator = self.ctx.add_operator(OperatorData::Join(Join {
            join_type,
            on,
            outer: left.operator,
            inner: right.operator,
        }));
        let operator =
            self.apply_post_join_filter(operator, &scope, join.post_join_filter.as_deref())?;
        self.apply_common(
            join.common.as_ref(),
            Relation {
                operator,
                columns: scope,
            },
        )
    }

    fn convert_nested_loop_join(
        &mut self,
        join: &NestedLoopJoinRel,
    ) -> Result<Relation, SubstraitError> {
        let left = self.convert_rel(
            join.left
                .as_ref()
                .ok_or(SubstraitError::MissingField("NestedLoopJoinRel.left"))?,
        )?;
        let right = self.convert_rel(
            join.right
                .as_ref()
                .ok_or(SubstraitError::MissingField("NestedLoopJoinRel.right"))?,
        )?;
        let mut scope = left.columns.clone();
        scope.extend(right.columns.iter().copied());
        let on = if let Some(expression) = &join.expression {
            self.convert_expr(expression, &scope)?
        } else {
            self.ctx
                .add_expr(ExprData::Literal(ScalarValue::Boolean(true)))
        };
        let join_type = convert_nested_loop_join_type(join.r#type)?;
        let operator = self.ctx.add_operator(OperatorData::Join(Join {
            join_type,
            on,
            outer: left.operator,
            inner: right.operator,
        }));
        self.apply_common(
            join.common.as_ref(),
            Relation {
                operator,
                columns: scope,
            },
        )
    }

    fn convert_cross(&mut self, cross: &CrossRel) -> Result<Relation, SubstraitError> {
        let left = self.convert_rel(
            cross
                .left
                .as_ref()
                .ok_or(SubstraitError::MissingField("CrossRel.left"))?,
        )?;
        let right = self.convert_rel(
            cross
                .right
                .as_ref()
                .ok_or(SubstraitError::MissingField("CrossRel.right"))?,
        )?;
        let mut columns = left.columns.clone();
        columns.extend(right.columns.iter().copied());
        let operator = self
            .ctx
            .add_operator(OperatorData::CrossProduct(CrossProduct {
                outer: left.operator,
                inner: right.operator,
            }));
        self.apply_common(cross.common.as_ref(), Relation { operator, columns })
    }

    fn convert_sort(&mut self, sort: &SortRel) -> Result<Relation, SubstraitError> {
        let input = sort
            .input
            .as_ref()
            .ok_or(SubstraitError::MissingField("SortRel.input"))?;
        let relation = self.convert_rel(input)?;
        let keys = sort
            .sorts
            .iter()
            .map(|field| self.convert_sort_field(field, &relation.columns))
            .collect::<Result<Vec<_>, _>>()?;
        let operator = self.ctx.add_operator(OperatorData::Sort(Sort {
            keys,
            input: relation.operator,
        }));
        self.apply_common(
            sort.common.as_ref(),
            Relation {
                operator,
                ..relation
            },
        )
    }

    fn convert_fetch(&mut self, fetch: &proto::FetchRel) -> Result<Relation, SubstraitError> {
        let input = fetch
            .input
            .as_ref()
            .ok_or(SubstraitError::MissingField("FetchRel.input"))?;
        let relation = self.convert_rel(input)?;
        let offset = convert_fetch_offset(fetch)?;
        let fetch_count = convert_fetch_count(fetch)?;
        let operator = self.ctx.add_operator(OperatorData::Limit(Limit {
            fetch: fetch_count,
            offset,
            input: relation.operator,
        }));
        self.apply_common(
            fetch.common.as_ref(),
            Relation {
                operator,
                ..relation
            },
        )
    }

    fn convert_aggregate(
        &mut self,
        aggregate: &proto::AggregateRel,
    ) -> Result<Relation, SubstraitError> {
        let input = aggregate
            .input
            .as_ref()
            .ok_or(SubstraitError::MissingField("AggregateRel.input"))?;
        let relation = self.convert_rel(input)?;

        if aggregate.groupings.len() > 1 {
            return Err(SubstraitError::UnsupportedAggregation(
                "multiple grouping sets",
            ));
        }

        let keys = if let Some(grouping) = aggregate.groupings.first() {
            self.convert_grouping_keys(
                grouping,
                &aggregate.grouping_expressions,
                &relation.columns,
            )?
        } else {
            Vec::new()
        };

        let aggregates = aggregate
            .measures
            .iter()
            .enumerate()
            .map(|(index, measure)| self.convert_measure(index, measure, &relation.columns))
            .collect::<Result<Vec<_>, _>>()?;

        let mut columns = keys
            .iter()
            .filter_map(|expr| match self.ctx.expr(*expr) {
                ExprData::ColumnRef(column) => Some(*column),
                _ => None,
            })
            .collect::<Vec<_>>();
        columns.extend(aggregates.iter().map(|(column, _)| *column));

        let operator = self
            .ctx
            .add_operator(OperatorData::Aggregation(Aggregation {
                keys,
                aggregates,
                input: relation.operator,
            }));
        self.apply_common(aggregate.common.as_ref(), Relation { operator, columns })
    }

    fn convert_grouping_keys(
        &mut self,
        grouping: &aggregate_rel::Grouping,
        grouping_expressions: &[Expression],
        scope: &[Column],
    ) -> Result<Vec<Expr>, SubstraitError> {
        if !grouping.grouping_expressions.is_empty() {
            return grouping
                .grouping_expressions
                .iter()
                .map(|expr| self.convert_expr(expr, scope))
                .collect();
        }

        grouping
            .expression_references
            .iter()
            .map(|index| {
                let index = *index as usize;
                let expr = grouping_expressions.get(index).ok_or(
                    SubstraitError::InvalidGroupingReference {
                        index,
                        input_len: grouping_expressions.len(),
                    },
                )?;
                self.convert_expr(expr, scope)
            })
            .collect()
    }

    fn convert_measure(
        &mut self,
        index: usize,
        measure: &aggregate_rel::Measure,
        scope: &[Column],
    ) -> Result<(Column, AggregateExpr), SubstraitError> {
        if measure.filter.is_some() {
            return Err(SubstraitError::UnsupportedAggregation("measure filter"));
        }

        let function = measure
            .measure
            .as_ref()
            .ok_or(SubstraitError::MissingField("AggregateRel.Measure.measure"))?;
        let name = self.function_name(function.function_reference);
        let args = function_args(&function.arguments, &function.args)
            .iter()
            .map(|expr| self.convert_expr(expr, scope))
            .collect::<Result<Vec<_>, _>>()?;
        let distinct = matches!(
            aggregate_function::AggregationInvocation::try_from(function.invocation).ok(),
            Some(aggregate_function::AggregationInvocation::Distinct)
        );

        let aggregate = if normalize_function_name(&name) == "count" && args.is_empty() {
            AggregateExpr::CountStar
        } else {
            let arg = args
                .first()
                .copied()
                .ok_or(SubstraitError::UnsupportedAggregation(
                    "aggregate without argument",
                ))?;
            AggregateExpr::Func {
                func: aggregate_function_from_name(&name),
                arg,
                distinct,
            }
        };

        let column = self.add_computed_column(
            format!("agg_{index}"),
            function
                .output_type
                .as_ref()
                .map(substrait_type_to_arrow)
                .transpose()?,
        );
        Ok((column, aggregate))
    }

    fn convert_sort_field(
        &mut self,
        field: &SortField,
        scope: &[Column],
    ) -> Result<SortKey, SubstraitError> {
        let expr = field
            .expr
            .as_ref()
            .ok_or(SubstraitError::MissingField("SortField.expr"))?;
        let expr = self.convert_expr(expr, scope)?;
        let (direction, nulls) = convert_sort_direction(field)?;
        Ok(SortKey {
            expr,
            direction,
            nulls,
        })
    }

    fn convert_comparison_join_condition(
        &mut self,
        keys: &[ComparisonJoinKey],
        left_keys: &[expression::FieldReference],
        right_keys: &[expression::FieldReference],
        left_scope: &[Column],
        right_scope: &[Column],
    ) -> Result<Expr, SubstraitError> {
        let predicates = if keys.is_empty() {
            if left_keys.len() != right_keys.len() {
                return Err(SubstraitError::UnsupportedJoin(
                    "mismatched join key counts",
                ));
            }
            left_keys
                .iter()
                .zip(right_keys)
                .map(|(left, right)| {
                    self.convert_join_key_equality(left, right, left_scope, right_scope)
                })
                .collect::<Result<Vec<_>, _>>()?
        } else {
            keys.iter()
                .map(|key| self.convert_comparison_join_key(key, left_scope, right_scope))
                .collect::<Result<Vec<_>, _>>()?
        };

        match predicates.as_slice() {
            [] => Err(SubstraitError::UnsupportedJoin("join without keys")),
            [predicate] => Ok(*predicate),
            _ => Ok(self.ctx.add_expr(ExprData::Nary {
                op: NaryOp::And,
                exprs: predicates,
            })),
        }
    }

    fn convert_comparison_join_key(
        &mut self,
        key: &ComparisonJoinKey,
        left_scope: &[Column],
        right_scope: &[Column],
    ) -> Result<Expr, SubstraitError> {
        ensure_simple_equality_comparison(key.comparison.as_ref())?;
        let left = key
            .left
            .as_ref()
            .ok_or(SubstraitError::MissingField("ComparisonJoinKey.left"))?;
        let right = key
            .right
            .as_ref()
            .ok_or(SubstraitError::MissingField("ComparisonJoinKey.right"))?;
        self.convert_join_key_equality(left, right, left_scope, right_scope)
    }

    fn convert_join_key_equality(
        &mut self,
        left: &expression::FieldReference,
        right: &expression::FieldReference,
        left_scope: &[Column],
        right_scope: &[Column],
    ) -> Result<Expr, SubstraitError> {
        let left = self
            .ctx
            .add_expr(ExprData::ColumnRef(resolve_field_reference(
                left, left_scope,
            )?));
        let right = self
            .ctx
            .add_expr(ExprData::ColumnRef(resolve_field_reference(
                right,
                right_scope,
            )?));
        Ok(self.ctx.add_expr(ExprData::Binary {
            op: BinaryOp::Eq,
            left,
            right,
        }))
    }

    fn apply_post_join_filter(
        &mut self,
        input: Operator,
        scope: &[Column],
        filter: Option<&Expression>,
    ) -> Result<Operator, SubstraitError> {
        let Some(filter) = filter else {
            return Ok(input);
        };
        let predicate = self.convert_expr(filter, scope)?;
        Ok(self
            .ctx
            .add_operator(OperatorData::Selection(Selection { predicate, input })))
    }

    fn convert_expr(
        &mut self,
        expr: &Expression,
        scope: &[Column],
    ) -> Result<Expr, SubstraitError> {
        let expr = match expr
            .rex_type
            .as_ref()
            .ok_or(SubstraitError::MissingField("Expression.rex_type"))?
        {
            expression::RexType::Literal(literal) => ExprData::Literal(convert_literal(literal)?),
            expression::RexType::Selection(field) => {
                ExprData::ColumnRef(resolve_field_reference(field, scope)?)
            }
            expression::RexType::ScalarFunction(function) => {
                let name = self.function_name(function.function_reference);
                let args = function_args(&function.arguments, &function.args)
                    .iter()
                    .map(|arg| self.convert_expr(arg, scope))
                    .collect::<Result<Vec<_>, _>>()?;
                build_function_expr(&name, args)
            }
            expression::RexType::Cast(cast) => {
                let input = cast
                    .input
                    .as_ref()
                    .ok_or(SubstraitError::MissingField("Expression.Cast.input"))?;
                let arg = self.convert_expr(input, scope)?;
                ExprData::ScalarFunction {
                    function: ScalarFunction::Extension("cast".to_string()),
                    args: vec![arg],
                }
            }
            expression::RexType::IfThen(if_then) => {
                let mut when_then = Vec::new();
                for clause in &if_then.ifs {
                    let condition = clause.r#if.as_ref().ok_or(SubstraitError::MissingField(
                        "Expression.IfThen.IfClause.if",
                    ))?;
                    let then = clause.then.as_ref().ok_or(SubstraitError::MissingField(
                        "Expression.IfThen.IfClause.then",
                    ))?;
                    when_then.push((
                        self.convert_expr(condition, scope)?,
                        self.convert_expr(then, scope)?,
                    ));
                }
                let else_expr = if_then
                    .r#else
                    .as_ref()
                    .map(|expr| self.convert_expr(expr, scope))
                    .transpose()?;
                ExprData::CaseWhen {
                    when_then,
                    else_expr,
                }
            }
            expression::RexType::SwitchExpression(_) => {
                return Err(SubstraitError::UnsupportedExpression("SwitchExpression"));
            }
            expression::RexType::SingularOrList(_) => {
                return Err(SubstraitError::UnsupportedExpression("SingularOrList"));
            }
            expression::RexType::MultiOrList(_) => {
                return Err(SubstraitError::UnsupportedExpression("MultiOrList"));
            }
            expression::RexType::WindowFunction(_) => {
                return Err(SubstraitError::UnsupportedExpression("WindowFunction"));
            }
            expression::RexType::Subquery(_) => {
                return Err(SubstraitError::UnsupportedExpression("Subquery"));
            }
            expression::RexType::Nested(_) => {
                return Err(SubstraitError::UnsupportedExpression("Nested"));
            }
            expression::RexType::DynamicParameter(_) => {
                return Err(SubstraitError::UnsupportedExpression("DynamicParameter"));
            }
            expression::RexType::Lambda(_) => {
                return Err(SubstraitError::UnsupportedExpression("Lambda"));
            }
            expression::RexType::LambdaInvocation(_) => {
                return Err(SubstraitError::UnsupportedExpression("LambdaInvocation"));
            }
            expression::RexType::Enum(_) => {
                return Err(SubstraitError::UnsupportedExpression("Enum"));
            }
        };

        Ok(self.ctx.add_expr(expr))
    }

    fn convert_named_struct(
        &mut self,
        schema: Option<&NamedStruct>,
    ) -> Result<Vec<Column>, SubstraitError> {
        let schema = schema.ok_or(SubstraitError::MissingField("ReadRel.base_schema"))?;
        let fields = schema
            .r#struct
            .as_ref()
            .ok_or(SubstraitError::MissingField("NamedStruct.struct"))?;

        fields
            .types
            .iter()
            .enumerate()
            .map(|(index, ty)| {
                let (data_type, _nullable) = substrait_type_to_column_type(ty)?;
                let name = schema
                    .names
                    .get(index)
                    .cloned()
                    .unwrap_or_else(|| format!("field_{index}"));
                Ok(self.ctx.add_column(ColumnData::new(name, data_type)))
            })
            .collect()
    }

    fn convert_local_files(
        &mut self,
        files: &read_rel::LocalFiles,
    ) -> Result<(TableFunctionDef, Vec<Expr>), SubstraitError> {
        let Some(first) = files.items.first() else {
            return Err(SubstraitError::UnsupportedRead("LocalFiles without files"));
        };

        let path = match first.path_type.as_ref() {
            Some(read_rel::local_files::file_or_files::PathType::UriPath(path))
            | Some(read_rel::local_files::file_or_files::PathType::UriPathGlob(path))
            | Some(read_rel::local_files::file_or_files::PathType::UriFile(path))
            | Some(read_rel::local_files::file_or_files::PathType::UriFolder(path)) => path.clone(),
            None => return Err(SubstraitError::MissingField("LocalFiles.path_type")),
        };

        let function = match first.file_format.as_ref() {
            Some(read_rel::local_files::file_or_files::FileFormat::Parquet(_)) => {
                TableFunctionDef::ReadParquet
            }
            Some(read_rel::local_files::file_or_files::FileFormat::Text(_)) => {
                TableFunctionDef::ReadCsv
            }
            Some(read_rel::local_files::file_or_files::FileFormat::Arrow(_)) => {
                TableFunctionDef::Extension("read_arrow".to_string())
            }
            Some(read_rel::local_files::file_or_files::FileFormat::Orc(_)) => {
                TableFunctionDef::Extension("read_orc".to_string())
            }
            Some(read_rel::local_files::file_or_files::FileFormat::Dwrf(_)) => {
                TableFunctionDef::Extension("read_dwrf".to_string())
            }
            Some(read_rel::local_files::file_or_files::FileFormat::Extension(_)) => {
                TableFunctionDef::Extension("read_extension".to_string())
            }
            None => TableFunctionDef::Extension("read_files".to_string()),
        };

        let path = self
            .ctx
            .add_expr(ExprData::Literal(ScalarValue::Utf8(path)));
        Ok((function, vec![path]))
    }

    fn convert_named_table_ref(names: &[String]) -> TableRef {
        match names {
            [] => TableRef::bare("<unnamed>"),
            [table] => TableRef::bare(table.clone()),
            [schema, table] => TableRef::partial(schema.clone(), table.clone()),
            [catalog, schema, table] => {
                TableRef::full(catalog.clone(), schema.clone(), table.clone())
            }
            _ => TableRef::full(
                names[..names.len() - 2].join("."),
                names[names.len() - 2].clone(),
                names[names.len() - 1].clone(),
            ),
        }
    }

    fn apply_common(
        &mut self,
        common: Option<&RelCommon>,
        relation: Relation,
    ) -> Result<Relation, SubstraitError> {
        let Some(common) = common else {
            return Ok(relation);
        };

        match common.emit_kind.as_ref() {
            None | Some(proto::rel_common::EmitKind::Direct(_)) => Ok(relation),
            Some(proto::rel_common::EmitKind::Emit(emit)) => {
                let columns = emit
                    .output_mapping
                    .iter()
                    .map(|index| {
                        if *index < 0 || *index as usize >= relation.columns.len() {
                            return Err(SubstraitError::InvalidEmit {
                                index: *index,
                                input_len: relation.columns.len(),
                            });
                        }
                        Ok(relation.columns[*index as usize])
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let operator = self.ctx.add_operator(OperatorData::Projection(Projection {
                    columns: columns.clone(),
                    input: relation.operator,
                }));
                Ok(Relation { operator, columns })
            }
        }
    }

    fn apply_root_names(&mut self, relation: &mut Relation, names: &[String]) {
        for (column, name) in relation.columns.iter().zip(names) {
            self.ctx.columns[column.0].name = name.clone();
        }
    }

    fn expr_output_type(&self, expr: &Expression) -> Result<Option<DataType>, SubstraitError> {
        let Some(rex_type) = expr.rex_type.as_ref() else {
            return Ok(None);
        };

        match rex_type {
            expression::RexType::Literal(literal) => {
                Ok(Some(convert_literal(literal)?.data_type()))
            }
            expression::RexType::ScalarFunction(function) => function
                .output_type
                .as_ref()
                .map(substrait_type_to_arrow)
                .transpose(),
            expression::RexType::Cast(cast) => cast
                .r#type
                .as_ref()
                .map(substrait_type_to_arrow)
                .transpose(),
            expression::RexType::IfThen(if_then) => {
                if let Some(expr) = if_then
                    .ifs
                    .iter()
                    .find_map(|clause| clause.then.as_ref())
                    .or_else(|| if_then.r#else.as_deref())
                {
                    self.expr_output_type(expr)
                } else {
                    Ok(None)
                }
            }
            _ => Ok(None),
        }
    }

    fn add_computed_column(&mut self, name: String, ty: Option<DataType>) -> Column {
        self.next_computed_column += 1;
        self.ctx
            .add_column(ColumnData::new(name, ty.unwrap_or(DataType::Null)))
    }

    fn function_name(&self, reference: u32) -> String {
        self.functions
            .get(&reference)
            .cloned()
            .unwrap_or_else(|| format!("function#{reference}"))
    }
}

fn collect_function_anchors(plan: &Plan) -> HashMap<u32, String> {
    let mut functions = HashMap::new();
    for extension in &plan.extensions {
        if let Some(
            proto::extensions::simple_extension_declaration::MappingType::ExtensionFunction(
                function,
            ),
        ) = &extension.mapping_type
        {
            functions.insert(function.function_anchor, function.name.clone());
        }
    }
    functions
}

fn project_common_join(join: &JoinRel) -> Option<&RelCommon> {
    join.common.as_ref()
}

fn convert_join_type(join_type: i32) -> Result<JoinType, SubstraitError> {
    match join_rel::JoinType::try_from(join_type).ok() {
        Some(join_rel::JoinType::Inner) => Ok(JoinType::Inner),
        Some(join_rel::JoinType::Left) => Ok(JoinType::LeftOuter),
        Some(join_rel::JoinType::Right) => Ok(JoinType::RightOuter),
        Some(join_rel::JoinType::Outer) => Ok(JoinType::FullOuter),
        Some(join_rel::JoinType::LeftSingle) | Some(join_rel::JoinType::RightSingle) => {
            Ok(JoinType::Single)
        }
        Some(join_rel::JoinType::LeftSemi) => {
            Err(SubstraitError::UnsupportedJoin("left semi join"))
        }
        Some(join_rel::JoinType::RightSemi) => {
            Err(SubstraitError::UnsupportedJoin("right semi join"))
        }
        Some(join_rel::JoinType::LeftAnti) => {
            Err(SubstraitError::UnsupportedJoin("left anti join"))
        }
        Some(join_rel::JoinType::RightAnti) => {
            Err(SubstraitError::UnsupportedJoin("right anti join"))
        }
        Some(join_rel::JoinType::LeftMark) | Some(join_rel::JoinType::RightMark) => {
            Err(SubstraitError::UnsupportedJoin("mark join marker column"))
        }
        Some(join_rel::JoinType::Unspecified) | None => {
            Err(SubstraitError::UnsupportedJoin("unspecified join type"))
        }
    }
}

fn convert_hash_join_type(join_type: i32) -> Result<JoinType, SubstraitError> {
    match hash_join_rel::JoinType::try_from(join_type).ok() {
        Some(hash_join_rel::JoinType::Inner) => Ok(JoinType::Inner),
        Some(hash_join_rel::JoinType::Left) => Ok(JoinType::LeftOuter),
        Some(hash_join_rel::JoinType::Right) => Ok(JoinType::RightOuter),
        Some(hash_join_rel::JoinType::Outer) => Ok(JoinType::FullOuter),
        Some(hash_join_rel::JoinType::LeftSingle) | Some(hash_join_rel::JoinType::RightSingle) => {
            Ok(JoinType::Single)
        }
        Some(hash_join_rel::JoinType::LeftSemi) => {
            Err(SubstraitError::UnsupportedJoin("left semi join"))
        }
        Some(hash_join_rel::JoinType::RightSemi) => {
            Err(SubstraitError::UnsupportedJoin("right semi join"))
        }
        Some(hash_join_rel::JoinType::LeftAnti) => {
            Err(SubstraitError::UnsupportedJoin("left anti join"))
        }
        Some(hash_join_rel::JoinType::RightAnti) => {
            Err(SubstraitError::UnsupportedJoin("right anti join"))
        }
        Some(hash_join_rel::JoinType::LeftMark) | Some(hash_join_rel::JoinType::RightMark) => {
            Err(SubstraitError::UnsupportedJoin("mark join marker column"))
        }
        Some(hash_join_rel::JoinType::Unspecified) | None => {
            Err(SubstraitError::UnsupportedJoin("unspecified join type"))
        }
    }
}

fn convert_merge_join_type(join_type: i32) -> Result<JoinType, SubstraitError> {
    match merge_join_rel::JoinType::try_from(join_type).ok() {
        Some(merge_join_rel::JoinType::Inner) => Ok(JoinType::Inner),
        Some(merge_join_rel::JoinType::Left) => Ok(JoinType::LeftOuter),
        Some(merge_join_rel::JoinType::Right) => Ok(JoinType::RightOuter),
        Some(merge_join_rel::JoinType::Outer) => Ok(JoinType::FullOuter),
        Some(merge_join_rel::JoinType::LeftSingle)
        | Some(merge_join_rel::JoinType::RightSingle) => Ok(JoinType::Single),
        Some(merge_join_rel::JoinType::LeftSemi) => {
            Err(SubstraitError::UnsupportedJoin("left semi join"))
        }
        Some(merge_join_rel::JoinType::RightSemi) => {
            Err(SubstraitError::UnsupportedJoin("right semi join"))
        }
        Some(merge_join_rel::JoinType::LeftAnti) => {
            Err(SubstraitError::UnsupportedJoin("left anti join"))
        }
        Some(merge_join_rel::JoinType::RightAnti) => {
            Err(SubstraitError::UnsupportedJoin("right anti join"))
        }
        Some(merge_join_rel::JoinType::LeftMark) | Some(merge_join_rel::JoinType::RightMark) => {
            Err(SubstraitError::UnsupportedJoin("mark join marker column"))
        }
        Some(merge_join_rel::JoinType::Unspecified) | None => {
            Err(SubstraitError::UnsupportedJoin("unspecified join type"))
        }
    }
}

fn convert_nested_loop_join_type(join_type: i32) -> Result<JoinType, SubstraitError> {
    match nested_loop_join_rel::JoinType::try_from(join_type).ok() {
        Some(nested_loop_join_rel::JoinType::Inner) => Ok(JoinType::Inner),
        Some(nested_loop_join_rel::JoinType::Left) => Ok(JoinType::LeftOuter),
        Some(nested_loop_join_rel::JoinType::Right) => Ok(JoinType::RightOuter),
        Some(nested_loop_join_rel::JoinType::Outer) => Ok(JoinType::FullOuter),
        Some(nested_loop_join_rel::JoinType::LeftSingle)
        | Some(nested_loop_join_rel::JoinType::RightSingle) => Ok(JoinType::Single),
        Some(nested_loop_join_rel::JoinType::LeftSemi) => {
            Err(SubstraitError::UnsupportedJoin("left semi join"))
        }
        Some(nested_loop_join_rel::JoinType::RightSemi) => {
            Err(SubstraitError::UnsupportedJoin("right semi join"))
        }
        Some(nested_loop_join_rel::JoinType::LeftAnti) => {
            Err(SubstraitError::UnsupportedJoin("left anti join"))
        }
        Some(nested_loop_join_rel::JoinType::RightAnti) => {
            Err(SubstraitError::UnsupportedJoin("right anti join"))
        }
        Some(nested_loop_join_rel::JoinType::LeftMark)
        | Some(nested_loop_join_rel::JoinType::RightMark) => {
            Err(SubstraitError::UnsupportedJoin("mark join marker column"))
        }
        Some(nested_loop_join_rel::JoinType::Unspecified) | None => {
            Err(SubstraitError::UnsupportedJoin("unspecified join type"))
        }
    }
}

fn ensure_simple_equality_comparison(
    comparison: Option<&comparison_join_key::ComparisonType>,
) -> Result<(), SubstraitError> {
    let Some(comparison) = comparison else {
        return Ok(());
    };
    match comparison.inner_type {
        None => Ok(()),
        Some(comparison_join_key::comparison_type::InnerType::Simple(comparison)) => {
            match comparison_join_key::SimpleComparisonType::try_from(comparison).ok() {
                Some(comparison_join_key::SimpleComparisonType::Eq) => Ok(()),
                _ => Err(SubstraitError::UnsupportedJoin("non-equality join key")),
            }
        }
        Some(comparison_join_key::comparison_type::InnerType::CustomFunctionReference(_)) => Err(
            SubstraitError::UnsupportedJoin("custom comparison join key"),
        ),
    }
}

fn function_args<'a>(
    arguments: &'a [FunctionArgument],
    deprecated_args: &'a [Expression],
) -> Vec<&'a Expression> {
    if arguments.is_empty() {
        return deprecated_args.iter().collect();
    }

    arguments
        .iter()
        .filter_map(|argument| match argument.arg_type.as_ref() {
            Some(function_argument::ArgType::Value(value)) => Some(value),
            _ => None,
        })
        .collect()
}

fn build_function_expr(name: &str, args: Vec<Expr>) -> ExprData {
    let normalized = normalize_function_name(name);
    match (normalized.as_str(), args.as_slice()) {
        ("equal" | "eq", [left, right]) => ExprData::Binary {
            op: BinaryOp::Eq,
            left: *left,
            right: *right,
        },
        ("not_equal" | "neq", [left, right]) => ExprData::Binary {
            op: BinaryOp::NotEq,
            left: *left,
            right: *right,
        },
        ("lt" | "less_than", [left, right]) => ExprData::Binary {
            op: BinaryOp::Lt,
            left: *left,
            right: *right,
        },
        ("lte" | "lteq" | "less_than_or_equal", [left, right]) => ExprData::Binary {
            op: BinaryOp::LtEq,
            left: *left,
            right: *right,
        },
        ("gt" | "greater_than", [left, right]) => ExprData::Binary {
            op: BinaryOp::Gt,
            left: *left,
            right: *right,
        },
        ("gte" | "gteq" | "greater_than_or_equal", [left, right]) => ExprData::Binary {
            op: BinaryOp::GtEq,
            left: *left,
            right: *right,
        },
        ("add", [left, right]) => ExprData::Binary {
            op: BinaryOp::Add,
            left: *left,
            right: *right,
        },
        ("subtract", [left, right]) => ExprData::Binary {
            op: BinaryOp::Subtract,
            left: *left,
            right: *right,
        },
        ("multiply", [left, right]) => ExprData::Binary {
            op: BinaryOp::Multiply,
            left: *left,
            right: *right,
        },
        ("divide", [left, right]) => ExprData::Binary {
            op: BinaryOp::Divide,
            left: *left,
            right: *right,
        },
        ("and", _) => ExprData::Nary {
            op: NaryOp::And,
            exprs: args,
        },
        ("or", _) => ExprData::Nary {
            op: NaryOp::Or,
            exprs: args,
        },
        ("not", [expr]) => ExprData::Unary {
            op: UnaryOp::Not,
            expr: *expr,
        },
        ("is_null", [expr]) => ExprData::Unary {
            op: UnaryOp::IsNull,
            expr: *expr,
        },
        ("is_not_null", [expr]) => ExprData::Unary {
            op: UnaryOp::IsNotNull,
            expr: *expr,
        },
        ("negate", [expr]) => ExprData::Unary {
            op: UnaryOp::Negate,
            expr: *expr,
        },
        ("abs", _) => ExprData::ScalarFunction {
            function: ScalarFunction::Abs,
            args,
        },
        ("lower", _) => ExprData::ScalarFunction {
            function: ScalarFunction::Lower,
            args,
        },
        ("upper", _) => ExprData::ScalarFunction {
            function: ScalarFunction::Upper,
            args,
        },
        ("coalesce", _) => ExprData::ScalarFunction {
            function: ScalarFunction::Coalesce,
            args,
        },
        ("if_then" | "case_when", _) if args.len() >= 2 => {
            let mut chunks = args.chunks_exact(2);
            let when_then = chunks
                .by_ref()
                .map(|pair| (pair[0], pair[1]))
                .collect::<Vec<_>>();
            let else_expr = chunks.remainder().first().copied();
            ExprData::CaseWhen {
                when_then,
                else_expr,
            }
        }
        _ => ExprData::ScalarFunction {
            function: ScalarFunction::Extension(name.to_string()),
            args,
        },
    }
}

fn aggregate_function_from_name(name: &str) -> AggregateFunction {
    match normalize_function_name(name).as_str() {
        "count" => AggregateFunction::Count,
        "sum" => AggregateFunction::Sum,
        "avg" => AggregateFunction::Avg,
        "min" => AggregateFunction::Min,
        "max" => AggregateFunction::Max,
        _ => AggregateFunction::Extension(name.to_string()),
    }
}

fn normalize_function_name(name: &str) -> String {
    name.split(':')
        .next()
        .unwrap_or(name)
        .trim_matches(|c: char| c == '/' || c == '.')
        .to_ascii_lowercase()
}

fn convert_sort_direction(
    field: &SortField,
) -> Result<(SortDirection, NullOrdering), SubstraitError> {
    let direction = match field.sort_kind {
        Some(sort_field::SortKind::Direction(direction)) => {
            sort_field::SortDirection::try_from(direction).ok()
        }
        Some(sort_field::SortKind::ComparisonFunctionReference(_)) => {
            return Err(SubstraitError::UnsupportedSort("comparison function"));
        }
        None => Some(sort_field::SortDirection::AscNullsLast),
    };

    match direction {
        Some(sort_field::SortDirection::Unspecified)
        | Some(sort_field::SortDirection::AscNullsLast)
        | None => Ok((SortDirection::Asc, NullOrdering::Last)),
        Some(sort_field::SortDirection::AscNullsFirst) => {
            Ok((SortDirection::Asc, NullOrdering::First))
        }
        Some(sort_field::SortDirection::DescNullsFirst) => {
            Ok((SortDirection::Desc, NullOrdering::First))
        }
        Some(sort_field::SortDirection::DescNullsLast) => {
            Ok((SortDirection::Desc, NullOrdering::Last))
        }
        Some(sort_field::SortDirection::Clustered) => {
            Err(SubstraitError::UnsupportedSort("clustered sort direction"))
        }
    }
}

fn convert_fetch_offset(fetch: &proto::FetchRel) -> Result<usize, SubstraitError> {
    let offset = match &fetch.offset_mode {
        Some(fetch_rel::OffsetMode::Offset(offset)) => *offset,
        Some(fetch_rel::OffsetMode::OffsetExpr(expr)) => {
            return Ok(convert_fetch_literal_expr("offset", expr)?.unwrap_or(0));
        }
        None => 0,
    };
    usize_from_fetch_value("offset", offset)
}

fn convert_fetch_count(fetch: &proto::FetchRel) -> Result<Option<usize>, SubstraitError> {
    let count = match &fetch.count_mode {
        None | Some(fetch_rel::CountMode::Count(-1)) => return Ok(None),
        Some(fetch_rel::CountMode::Count(count)) => *count,
        Some(fetch_rel::CountMode::CountExpr(expr)) => {
            return convert_fetch_literal_expr("count", expr);
        }
    };
    usize_from_fetch_value("count", count).map(Some)
}

fn convert_fetch_literal_expr(
    field: &'static str,
    expr: &Expression,
) -> Result<Option<usize>, SubstraitError> {
    let literal = match expr.rex_type.as_ref() {
        Some(expression::RexType::Literal(literal)) => literal,
        _ => {
            return Err(SubstraitError::UnsupportedFetch(
                "non-literal fetch expression",
            ));
        }
    };
    let value = match convert_literal(literal)? {
        ScalarValue::Null(_) => return Ok(None),
        ScalarValue::Int32(value) => i64::from(value),
        ScalarValue::Int64(value) => value,
        _ => {
            return Err(SubstraitError::UnsupportedFetch(
                "non-integer fetch expression",
            ));
        }
    };
    usize_from_fetch_value(field, value).map(Some)
}

fn usize_from_fetch_value(field: &'static str, value: i64) -> Result<usize, SubstraitError> {
    if value < 0 {
        return Err(SubstraitError::InvalidFetch { field, value });
    }
    usize::try_from(value).map_err(|_| SubstraitError::InvalidFetch { field, value })
}

fn resolve_field_reference(
    field: &expression::FieldReference,
    scope: &[Column],
) -> Result<Column, SubstraitError> {
    match field.root_type.as_ref() {
        Some(expression::field_reference::RootType::RootReference(_)) => {}
        _ => {
            return Err(SubstraitError::UnsupportedExpression(
                "non-root field reference",
            ));
        }
    }

    let reference = match field.reference_type.as_ref() {
        Some(expression::field_reference::ReferenceType::DirectReference(reference)) => reference,
        Some(expression::field_reference::ReferenceType::MaskedReference(_)) => {
            return Err(SubstraitError::UnsupportedExpression(
                "masked field reference",
            ));
        }
        None => {
            return Err(SubstraitError::MissingField(
                "FieldReference.reference_type",
            ));
        }
    };

    let field = match reference.reference_type.as_ref() {
        Some(expression::reference_segment::ReferenceType::StructField(field))
            if field.child.is_none() =>
        {
            field.field
        }
        Some(expression::reference_segment::ReferenceType::StructField(_)) => {
            return Err(SubstraitError::UnsupportedExpression(
                "nested field reference",
            ));
        }
        Some(expression::reference_segment::ReferenceType::MapKey(_)) => {
            return Err(SubstraitError::UnsupportedExpression("map field reference"));
        }
        Some(expression::reference_segment::ReferenceType::ListElement(_)) => {
            return Err(SubstraitError::UnsupportedExpression(
                "list field reference",
            ));
        }
        None => {
            return Err(SubstraitError::MissingField(
                "ReferenceSegment.reference_type",
            ));
        }
    };

    if field < 0 || field as usize >= scope.len() {
        return Err(SubstraitError::InvalidFieldReference {
            index: field as usize,
            input_len: scope.len(),
        });
    }
    Ok(scope[field as usize])
}

fn convert_literal(literal: &expression::Literal) -> Result<ScalarValue, SubstraitError> {
    match literal
        .literal_type
        .as_ref()
        .ok_or(SubstraitError::MissingField("Literal.literal_type"))?
    {
        expression::literal::LiteralType::Boolean(value) => Ok(ScalarValue::Boolean(*value)),
        expression::literal::LiteralType::I32(value) => Ok(ScalarValue::Int32(*value)),
        expression::literal::LiteralType::I64(value) => Ok(ScalarValue::Int64(*value)),
        expression::literal::LiteralType::Fp64(value) => Ok(ScalarValue::Float64(*value)),
        expression::literal::LiteralType::String(value) => Ok(ScalarValue::Utf8(value.clone())),
        expression::literal::LiteralType::VarChar(value) => {
            Ok(ScalarValue::Utf8(value.value.clone()))
        }
        expression::literal::LiteralType::FixedChar(value) => Ok(ScalarValue::Utf8(value.clone())),
        expression::literal::LiteralType::Null(ty) => {
            Ok(ScalarValue::Null(substrait_type_to_arrow(ty)?))
        }
        _ => Err(SubstraitError::UnsupportedExpression("literal type")),
    }
}

fn substrait_type_to_column_type(ty: &Type) -> Result<(DataType, bool), SubstraitError> {
    Ok((substrait_type_to_arrow(ty)?, substrait_type_nullable(ty)?))
}

fn substrait_type_to_arrow(ty: &Type) -> Result<DataType, SubstraitError> {
    match ty
        .kind
        .as_ref()
        .ok_or(SubstraitError::MissingField("Type.kind"))?
    {
        r#type::Kind::Bool(_) => Ok(DataType::Boolean),
        r#type::Kind::I8(_) => Ok(DataType::Int8),
        r#type::Kind::I16(_) => Ok(DataType::Int16),
        r#type::Kind::I32(_) => Ok(DataType::Int32),
        r#type::Kind::I64(_) => Ok(DataType::Int64),
        r#type::Kind::Fp32(_) => Ok(DataType::Float32),
        r#type::Kind::Fp64(_) => Ok(DataType::Float64),
        r#type::Kind::String(_) | r#type::Kind::Varchar(_) | r#type::Kind::FixedChar(_) => {
            Ok(DataType::Utf8)
        }
        r#type::Kind::Binary(_) | r#type::Kind::FixedBinary(_) => Ok(DataType::Binary),
        r#type::Kind::Date(_) => Ok(DataType::Date32),
        r#type::Kind::Timestamp(_) | r#type::Kind::TimestampTz(_) => {
            Ok(DataType::Timestamp(TimeUnit::Microsecond, None))
        }
        r#type::Kind::PrecisionTimestamp(timestamp) => Ok(DataType::Timestamp(
            precision_to_time_unit(timestamp.precision)?,
            None,
        )),
        r#type::Kind::PrecisionTimestampTz(timestamp) => Ok(DataType::Timestamp(
            precision_to_time_unit(timestamp.precision)?,
            Some("UTC".into()),
        )),
        r#type::Kind::Time(_) => Ok(DataType::Time64(TimeUnit::Microsecond)),
        r#type::Kind::PrecisionTime(time) => {
            Ok(DataType::Time64(precision_to_time_unit(time.precision)?))
        }
        r#type::Kind::Decimal(decimal) => Ok(DataType::Decimal128(
            decimal.precision as u8,
            decimal.scale as i8,
        )),
        r#type::Kind::Struct(_)
        | r#type::Kind::List(_)
        | r#type::Kind::Map(_)
        | r#type::Kind::Func(_)
        | r#type::Kind::UserDefined(_)
        | r#type::Kind::UserDefinedTypeReference(_)
        | r#type::Kind::Alias(_)
        | r#type::Kind::IntervalYear(_)
        | r#type::Kind::IntervalDay(_)
        | r#type::Kind::IntervalCompound(_)
        | r#type::Kind::Uuid(_) => {
            Err(SubstraitError::UnsupportedType("complex or extension type"))
        }
    }
}

fn substrait_type_nullable(ty: &Type) -> Result<bool, SubstraitError> {
    let nullability = match ty
        .kind
        .as_ref()
        .ok_or(SubstraitError::MissingField("Type.kind"))?
    {
        r#type::Kind::Bool(ty) => ty.nullability,
        r#type::Kind::I8(ty) => ty.nullability,
        r#type::Kind::I16(ty) => ty.nullability,
        r#type::Kind::I32(ty) => ty.nullability,
        r#type::Kind::I64(ty) => ty.nullability,
        r#type::Kind::Fp32(ty) => ty.nullability,
        r#type::Kind::Fp64(ty) => ty.nullability,
        r#type::Kind::String(ty) => ty.nullability,
        r#type::Kind::Binary(ty) => ty.nullability,
        r#type::Kind::Timestamp(ty) => ty.nullability,
        r#type::Kind::Date(ty) => ty.nullability,
        r#type::Kind::Time(ty) => ty.nullability,
        r#type::Kind::TimestampTz(ty) => ty.nullability,
        r#type::Kind::IntervalYear(ty) => ty.nullability,
        r#type::Kind::IntervalDay(ty) => ty.nullability,
        r#type::Kind::IntervalCompound(ty) => ty.nullability,
        r#type::Kind::Uuid(ty) => ty.nullability,
        r#type::Kind::FixedChar(ty) => ty.nullability,
        r#type::Kind::Varchar(ty) => ty.nullability,
        r#type::Kind::FixedBinary(ty) => ty.nullability,
        r#type::Kind::Decimal(ty) => ty.nullability,
        r#type::Kind::PrecisionTime(ty) => ty.nullability,
        r#type::Kind::PrecisionTimestamp(ty) => ty.nullability,
        r#type::Kind::PrecisionTimestampTz(ty) => ty.nullability,
        r#type::Kind::Struct(ty) => ty.nullability,
        r#type::Kind::List(ty) => ty.nullability,
        r#type::Kind::Map(ty) => ty.nullability,
        r#type::Kind::Func(ty) => ty.nullability,
        r#type::Kind::UserDefined(ty) => ty.nullability,
        r#type::Kind::UserDefinedTypeReference(_) => return Ok(false),
        r#type::Kind::Alias(ty) => ty.nullability,
    };

    match r#type::Nullability::try_from(nullability).ok() {
        Some(r#type::Nullability::Nullable) | Some(r#type::Nullability::Unspecified) | None => {
            Ok(true)
        }
        Some(r#type::Nullability::Required) => Ok(false),
    }
}

fn precision_to_time_unit(precision: i32) -> Result<TimeUnit, SubstraitError> {
    match precision {
        0..=3 => Ok(TimeUnit::Millisecond),
        4..=6 => Ok(TimeUnit::Microsecond),
        7..=9 => Ok(TimeUnit::Nanosecond),
        _ => Err(SubstraitError::UnsupportedType("timestamp precision")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Catalog, MemoryCatalog};
    use arrow_schema::{Field, Schema};
    use std::sync::Arc;

    fn required_i64_type() -> Type {
        Type {
            kind: Some(r#type::Kind::I64(r#type::I64 {
                type_variation_reference: 0,
                nullability: r#type::Nullability::Required as i32,
            })),
        }
    }

    fn field_ref(index: i32) -> Expression {
        Expression {
            rex_type: Some(expression::RexType::Selection(Box::new(field_reference(
                index,
            )))),
        }
    }

    fn field_reference(index: i32) -> expression::FieldReference {
        super::field_reference(index)
    }

    fn comparison_key(left: i32, right: i32) -> ComparisonJoinKey {
        ComparisonJoinKey {
            left: Some(field_reference(left)),
            right: Some(field_reference(right)),
            comparison: None,
        }
    }

    fn named_read(table: &str, columns: &[&str]) -> Rel {
        Rel {
            rel_type: Some(rel::RelType::Read(Box::new(ReadRel {
                common: None,
                base_schema: Some(NamedStruct {
                    names: columns.iter().map(|column| (*column).to_string()).collect(),
                    r#struct: Some(r#type::Struct {
                        types: columns.iter().map(|_| required_i64_type()).collect(),
                        type_variation_reference: 0,
                        nullability: r#type::Nullability::Required as i32,
                    }),
                }),
                filter: None,
                best_effort_filter: None,
                projection: None,
                advanced_extension: None,
                read_type: Some(read_rel::ReadType::NamedTable(read_rel::NamedTable {
                    names: vec![table.to_string()],
                    advanced_extension: None,
                })),
            }))),
        }
    }

    #[test]
    fn exports_projection_over_named_table_read() {
        let mut ctx = QueryContext::new();
        let id = ctx.add_column(ColumnData::new("id", DataType::Int64));
        let age = ctx.add_column(ColumnData::new("age", DataType::Int32));
        let scan = ctx.add_operator(OperatorData::Scan(Scan {
            table: TableRef::bare("users"),
            columns: vec![id, age],
        }));
        let projection = ctx.add_operator(OperatorData::Projection(Projection {
            columns: vec![id],
            input: scan,
        }));
        let output = ctx.add_operator(OperatorData::Output(Output { input: projection }));
        ctx.set_root(output);

        let plan = to_plan(&ctx).expect("query should export");
        let Some(plan_rel::RelType::Root(root)) = plan.relations[0].rel_type.as_ref() else {
            panic!("plan should export a root")
        };
        assert_eq!(root.names, vec!["id"]);
        let Some(Rel {
            rel_type: Some(rel::RelType::Project(project)),
        }) = root.input.as_ref()
        else {
            panic!("root should read from project")
        };
        let Some(RelCommon {
            emit_kind: Some(rel_common::EmitKind::Emit(emit)),
            ..
        }) = project.common.as_ref()
        else {
            panic!("project should emit selected columns")
        };
        assert_eq!(emit.output_mapping, vec![0]);
    }

    #[test]
    fn exports_tpch_relevant_types_in_scan_schema() {
        let mut ctx = QueryContext::new();
        let dec = ctx.add_column(ColumnData::new(
            "extendedprice",
            DataType::Decimal128(15, 2),
        ));
        let order_date = ctx.add_column(ColumnData::new("orderdate", DataType::Date32));
        let commit_ts = ctx.add_column(ColumnData::new(
            "commit_ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
        ));
        let scan = ctx.add_operator(OperatorData::Scan(Scan {
            table: TableRef::bare("lineitem"),
            columns: vec![dec, order_date, commit_ts],
        }));
        let output = ctx.add_operator(OperatorData::Output(Output { input: scan }));
        ctx.set_root(output);

        let plan = to_plan(&ctx).expect("query should export");
        let Some(plan_rel::RelType::Root(root)) = plan.relations[0].rel_type.as_ref() else {
            panic!("plan should export a root")
        };
        let Some(Rel {
            rel_type: Some(rel::RelType::Read(read)),
        }) = root.input.as_ref()
        else {
            panic!("root should read from read")
        };
        let schema = read
            .base_schema
            .as_ref()
            .expect("read should include schema");
        let ty = &schema
            .r#struct
            .as_ref()
            .expect("schema should include struct")
            .types;
        assert!(matches!(ty[0].kind, Some(r#type::Kind::Decimal(_))));
        assert!(matches!(ty[1].kind, Some(r#type::Kind::Date(_))));
        assert!(matches!(
            ty[2].kind,
            Some(r#type::Kind::Timestamp(_)) | Some(r#type::Kind::PrecisionTimestamp(_))
        ));
    }

    #[test]
    fn exports_selection_sort_and_limit() {
        let mut ctx = QueryContext::new();
        let id = ctx.add_column(ColumnData::new("id", DataType::Int64));
        let age = ctx.add_column(ColumnData::new("age", DataType::Int64));
        let scan = ctx.add_operator(OperatorData::Scan(Scan {
            table: TableRef::bare("users"),
            columns: vec![id, age],
        }));
        let age_ref = ctx.add_expr(ExprData::ColumnRef(age));
        let eighteen = ctx.add_expr(ExprData::Literal(ScalarValue::Int64(18)));
        let predicate = ctx.add_expr(ExprData::Binary {
            op: BinaryOp::GtEq,
            left: age_ref,
            right: eighteen,
        });
        let selection = ctx.add_operator(OperatorData::Selection(Selection {
            predicate,
            input: scan,
        }));
        let id_ref = ctx.add_expr(ExprData::ColumnRef(id));
        let sort = ctx.add_operator(OperatorData::Sort(Sort {
            keys: vec![SortKey {
                expr: id_ref,
                direction: SortDirection::Desc,
                nulls: NullOrdering::Last,
            }],
            input: selection,
        }));
        let limit = ctx.add_operator(OperatorData::Limit(Limit {
            fetch: Some(5),
            offset: 2,
            input: sort,
        }));
        let output = ctx.add_operator(OperatorData::Output(Output { input: limit }));
        ctx.set_root(output);

        let plan = to_plan(&ctx).expect("query should export");
        let Some(plan_rel::RelType::Root(root)) = plan.relations[0].rel_type.as_ref() else {
            panic!("plan should export a root")
        };
        let Some(Rel {
            rel_type: Some(rel::RelType::Fetch(fetch)),
        }) = root.input.as_ref()
        else {
            panic!("root should read from fetch")
        };
        assert!(matches!(
            fetch.count_mode,
            Some(fetch_rel::CountMode::Count(5))
        ));
        assert!(matches!(
            fetch.offset_mode,
            Some(fetch_rel::OffsetMode::Offset(2))
        ));
        let Some(sort_input) = fetch.input.as_ref() else {
            panic!("fetch should read from sort")
        };
        let Rel {
            rel_type: Some(rel::RelType::Sort(sort)),
        } = sort_input.as_ref()
        else {
            panic!("fetch should read from sort")
        };
        assert_eq!(sort.sorts.len(), 1);
        assert!(matches!(
            sort.sorts[0].sort_kind,
            Some(sort_field::SortKind::Direction(x))
                if x == sort_field::SortDirection::DescNullsLast as i32
        ));
        let Some(filter_input) = sort.input.as_ref() else {
            panic!("sort should read from filter")
        };
        let Rel {
            rel_type: Some(rel::RelType::Filter(filter)),
        } = filter_input.as_ref()
        else {
            panic!("sort should read from filter")
        };
        let Some(predicate) = filter.condition.as_ref() else {
            panic!("filter should have condition")
        };
        assert!(matches!(
            predicate.rex_type.as_ref(),
            Some(expression::RexType::ScalarFunction(_))
        ));
        assert!(plan.extensions.iter().any(|extension| matches!(
            extension.mapping_type.as_ref(),
            Some(proto::extensions::simple_extension_declaration::MappingType::ExtensionFunction(
                proto::extensions::simple_extension_declaration::ExtensionFunction { name, .. }
            )) if name == "gte"
        )));
    }

    #[test]
    fn exports_logical_join_as_join_rel() {
        let mut ctx = QueryContext::new();
        let user_id = ctx.add_column(ColumnData::new("user_id", DataType::Int64));
        let order_user_id = ctx.add_column(ColumnData::new("order_user_id", DataType::Int64));
        let users = ctx.add_operator(OperatorData::Scan(Scan {
            table: TableRef::bare("users"),
            columns: vec![user_id],
        }));
        let orders = ctx.add_operator(OperatorData::Scan(Scan {
            table: TableRef::bare("orders"),
            columns: vec![order_user_id],
        }));
        let left_ref = ctx.add_expr(ExprData::ColumnRef(user_id));
        let right_ref = ctx.add_expr(ExprData::ColumnRef(order_user_id));
        let on = ctx.add_expr(ExprData::Binary {
            op: BinaryOp::Eq,
            left: left_ref,
            right: right_ref,
        });
        let join = ctx.add_operator(OperatorData::Join(Join {
            join_type: JoinType::Inner,
            on,
            outer: users,
            inner: orders,
        }));
        let output = ctx.add_operator(OperatorData::Output(Output { input: join }));
        ctx.set_root(output);

        let plan = to_plan(&ctx).expect("query should export");
        let Some(plan_rel::RelType::Root(root)) = plan.relations[0].rel_type.as_ref() else {
            panic!("plan should export a root")
        };
        let Some(Rel {
            rel_type: Some(rel::RelType::Join(join)),
        }) = root.input.as_ref()
        else {
            panic!("root should read from join")
        };
        assert_eq!(join.r#type, join_rel::JoinType::Inner as i32);
        assert!(join.left.is_some(), "join should have left input");
        assert!(join.right.is_some(), "join should have right input");
        assert!(matches!(
            join.expression
                .as_ref()
                .and_then(|expr| expr.rex_type.as_ref()),
            Some(expression::RexType::ScalarFunction(_))
        ));
        assert!(plan.extensions.iter().any(|extension| matches!(
            extension.mapping_type.as_ref(),
            Some(proto::extensions::simple_extension_declaration::MappingType::ExtensionFunction(
                proto::extensions::simple_extension_declaration::ExtensionFunction { name, .. }
            )) if name == "equal"
        )));
    }

    #[test]
    fn exports_cross_product_as_cross_rel() {
        let mut ctx = QueryContext::new();
        let user_id = ctx.add_column(ColumnData::new("user_id", DataType::Int64));
        let order_id = ctx.add_column(ColumnData::new("order_id", DataType::Int64));
        let users = ctx.add_operator(OperatorData::Scan(Scan {
            table: TableRef::bare("users"),
            columns: vec![user_id],
        }));
        let orders = ctx.add_operator(OperatorData::Scan(Scan {
            table: TableRef::bare("orders"),
            columns: vec![order_id],
        }));
        let cross = ctx.add_operator(OperatorData::CrossProduct(CrossProduct {
            outer: users,
            inner: orders,
        }));
        let output = ctx.add_operator(OperatorData::Output(Output { input: cross }));
        ctx.set_root(output);

        let plan = to_plan(&ctx).expect("query should export");
        let Some(plan_rel::RelType::Root(root)) = plan.relations[0].rel_type.as_ref() else {
            panic!("plan should export a root")
        };
        let Some(Rel {
            rel_type: Some(rel::RelType::Cross(cross)),
        }) = root.input.as_ref()
        else {
            panic!("root should read from cross")
        };
        assert!(cross.left.is_some(), "cross should have left input");
        assert!(cross.right.is_some(), "cross should have right input");
    }

    #[test]
    fn exports_map_as_project_with_expressions() {
        let mut ctx = QueryContext::new();
        let id = ctx.add_column(ColumnData::new("id", DataType::Int64));
        let id_plus_one = ctx.add_column(ColumnData::new("id_plus_one", DataType::Int64));
        let users = ctx.add_operator(OperatorData::Scan(Scan {
            table: TableRef::bare("users"),
            columns: vec![id],
        }));
        let id_ref = ctx.add_expr(ExprData::ColumnRef(id));
        let one = ctx.add_expr(ExprData::Literal(ScalarValue::Int64(1)));
        let computation = ctx.add_expr(ExprData::Binary {
            op: BinaryOp::Add,
            left: id_ref,
            right: one,
        });
        let map = ctx.add_operator(OperatorData::Map(Map {
            computations: vec![(id_plus_one, computation)],
            input: users,
        }));
        let output = ctx.add_operator(OperatorData::Output(Output { input: map }));
        ctx.set_root(output);

        let plan = to_plan(&ctx).expect("query should export");
        let Some(plan_rel::RelType::Root(root)) = plan.relations[0].rel_type.as_ref() else {
            panic!("plan should export a root")
        };
        let Some(Rel {
            rel_type: Some(rel::RelType::Project(project)),
        }) = root.input.as_ref()
        else {
            panic!("root should read from project")
        };
        assert_eq!(project.expressions.len(), 1);
        let Some(RelCommon {
            emit_kind: Some(rel_common::EmitKind::Emit(emit)),
            ..
        }) = project.common.as_ref()
        else {
            panic!("project should use emit mapping");
        };
        assert_eq!(emit.output_mapping, vec![0, 1]);
    }

    #[test]
    fn exports_aggregation_with_emit_mapping() {
        let mut ctx = QueryContext::new();
        let user_id = ctx.add_column(ColumnData::new("user_id", DataType::Int64));
        let order_id = ctx.add_column(ColumnData::new("order_id", DataType::Int64));
        let order_count = ctx.add_column(ColumnData::new("order_count", DataType::Int64));
        let orders = ctx.add_operator(OperatorData::Scan(Scan {
            table: TableRef::bare("orders"),
            columns: vec![order_id, user_id],
        }));
        let key = ctx.add_expr(ExprData::ColumnRef(user_id));
        let aggregation = ctx.add_operator(OperatorData::Aggregation(Aggregation {
            keys: vec![key],
            aggregates: vec![(order_count, AggregateExpr::CountStar)],
            input: orders,
        }));
        let output = ctx.add_operator(OperatorData::Output(Output { input: aggregation }));
        ctx.set_root(output);

        let plan = to_plan(&ctx).expect("query should export");
        let Some(plan_rel::RelType::Root(root)) = plan.relations[0].rel_type.as_ref() else {
            panic!("plan should export a root")
        };
        let Some(Rel {
            rel_type: Some(rel::RelType::Aggregate(aggregate)),
        }) = root.input.as_ref()
        else {
            panic!("root should read from aggregate")
        };

        let Some(RelCommon {
            emit_kind: Some(rel_common::EmitKind::Emit(emit)),
            ..
        }) = aggregate.common.as_ref()
        else {
            panic!("aggregate should emit output mapping");
        };
        assert_eq!(emit.output_mapping, vec![0, 1]);
        assert_eq!(aggregate.grouping_expressions.len(), 1);
        assert_eq!(aggregate.groupings.len(), 1);
        assert_eq!(aggregate.groupings[0].expression_references, vec![0]);
        assert_eq!(aggregate.measures.len(), 1);

        let measure = aggregate.measures[0]
            .measure
            .as_ref()
            .expect("measure should be populated");
        assert_eq!(
            aggregate_function::AggregationInvocation::try_from(measure.invocation).ok(),
            Some(aggregate_function::AggregationInvocation::All)
        );
        assert_eq!(
            proto::AggregationPhase::try_from(measure.phase).ok(),
            Some(proto::AggregationPhase::InitialToResult)
        );
        assert!(measure.args.is_empty());
        assert!(measure.arguments.is_empty());
    }

    #[test]
    fn exports_table_function_as_local_files_read() {
        let mut ctx = QueryContext::new();
        let col = ctx.add_column(ColumnData::new("id", DataType::Int64));
        let path = ctx.add_expr(ExprData::Literal(ScalarValue::Utf8(
            "file:///tmp/users.parquet".to_string(),
        )));
        let table_function = ctx.add_operator(OperatorData::TableFunction(TableFunction {
            function: TableFunctionDef::ReadParquet,
            args: vec![path],
            columns: vec![col],
        }));
        let output = ctx.add_operator(OperatorData::Output(Output {
            input: table_function,
        }));
        ctx.set_root(output);

        let plan = to_plan(&ctx).expect("query should export");
        let Some(plan_rel::RelType::Root(root)) = plan.relations[0].rel_type.as_ref() else {
            panic!("plan should export a root")
        };
        let Some(Rel {
            rel_type: Some(rel::RelType::Read(read)),
        }) = root.input.as_ref()
        else {
            panic!("root should read from local files");
        };
        let Some(read_rel::ReadType::LocalFiles(files)) = read.read_type.as_ref() else {
            panic!("read should be local files");
        };
        assert_eq!(files.items.len(), 1);
        assert!(matches!(
            files.items[0].file_format,
            Some(read_rel::local_files::file_or_files::FileFormat::Parquet(_))
        ));
    }

    #[test]
    fn exports_scalar_function_with_inferred_output_type() {
        let mut ctx = QueryContext::new();
        let name = ctx.add_column(ColumnData::new("name", DataType::Utf8));
        let users = ctx.add_operator(OperatorData::Scan(Scan {
            table: TableRef::bare("users"),
            columns: vec![name],
        }));
        let name_ref = ctx.add_expr(ExprData::ColumnRef(name));
        let lower = ctx.add_expr(ExprData::ScalarFunction {
            function: ScalarFunction::Lower,
            args: vec![name_ref],
        });
        let lowered = ctx.add_column(ColumnData::new("lowered", DataType::Utf8));
        let map = ctx.add_operator(OperatorData::Map(Map {
            computations: vec![(lowered, lower)],
            input: users,
        }));
        let output = ctx.add_operator(OperatorData::Output(Output { input: map }));
        ctx.set_root(output);

        let plan = to_plan(&ctx).expect("query should export");
        let Some(plan_rel::RelType::Root(root)) = plan.relations[0].rel_type.as_ref() else {
            panic!("plan should export a root")
        };
        let Some(Rel {
            rel_type: Some(rel::RelType::Project(project)),
        }) = root.input.as_ref()
        else {
            panic!("root should read from project");
        };
        let Some(expression::RexType::ScalarFunction(function)) =
            project.expressions[0].rex_type.as_ref()
        else {
            panic!("projection expression should be scalar function");
        };
        let Some(output_type) = function.output_type.as_ref() else {
            panic!("scalar function should include output type");
        };
        assert!(matches!(output_type.kind, Some(r#type::Kind::String(_))));
    }

    #[test]
    fn exports_cast_extension_as_cast_expression() {
        let mut ctx = QueryContext::new();
        let amount = ctx.add_column(ColumnData::new("amount", DataType::Int64));
        let amount_i32 = ctx.add_column(ColumnData::new("amount_i32", DataType::Int32));
        let scan = ctx.add_operator(OperatorData::Scan(Scan {
            table: TableRef::bare("orders"),
            columns: vec![amount],
        }));
        let amount_ref = ctx.add_expr(ExprData::ColumnRef(amount));
        let cast_expr = ctx.add_expr(ExprData::ScalarFunction {
            function: ScalarFunction::Extension("cast".to_string()),
            args: vec![amount_ref],
        });
        let map = ctx.add_operator(OperatorData::Map(Map {
            computations: vec![(amount_i32, cast_expr)],
            input: scan,
        }));
        let output = ctx.add_operator(OperatorData::Output(Output { input: map }));
        ctx.set_root(output);

        let plan = to_plan(&ctx).expect("query should export");
        let Some(plan_rel::RelType::Root(root)) = plan.relations[0].rel_type.as_ref() else {
            panic!("plan should export a root")
        };
        let Some(Rel {
            rel_type: Some(rel::RelType::Project(project)),
        }) = root.input.as_ref()
        else {
            panic!("root should read from project");
        };
        let Some(expression::RexType::Cast(cast)) = project.expressions[0].rex_type.as_ref() else {
            panic!("expression should export as cast");
        };
        assert!(cast.input.is_some());
        assert!(matches!(
            cast.r#type.as_ref().and_then(|ty| ty.kind.as_ref()),
            Some(r#type::Kind::I32(_))
        ));
    }

    #[test]
    fn exports_case_when_as_if_then_expression() {
        let mut ctx = QueryContext::new();
        let amount = ctx.add_column(ColumnData::new("amount", DataType::Int64));
        let bucket = ctx.add_column(ColumnData::new("bucket", DataType::Utf8));
        let scan = ctx.add_operator(OperatorData::Scan(Scan {
            table: TableRef::bare("orders"),
            columns: vec![amount],
        }));
        let amount_ref = ctx.add_expr(ExprData::ColumnRef(amount));
        let limit = ctx.add_expr(ExprData::Literal(ScalarValue::Int64(100)));
        let condition = ctx.add_expr(ExprData::Binary {
            op: BinaryOp::Gt,
            left: amount_ref,
            right: limit,
        });
        let high = ctx.add_expr(ExprData::Literal(ScalarValue::Utf8("high".to_string())));
        let low = ctx.add_expr(ExprData::Literal(ScalarValue::Utf8("low".to_string())));
        let case_when = ctx.add_expr(ExprData::CaseWhen {
            when_then: vec![(condition, high)],
            else_expr: Some(low),
        });
        let map = ctx.add_operator(OperatorData::Map(Map {
            computations: vec![(bucket, case_when)],
            input: scan,
        }));
        let output = ctx.add_operator(OperatorData::Output(Output { input: map }));
        ctx.set_root(output);

        let plan = to_plan(&ctx).expect("query should export");
        let Some(plan_rel::RelType::Root(root)) = plan.relations[0].rel_type.as_ref() else {
            panic!("plan should export a root")
        };
        let Some(Rel {
            rel_type: Some(rel::RelType::Project(project)),
        }) = root.input.as_ref()
        else {
            panic!("root should read from project");
        };
        let Some(expression::RexType::IfThen(if_then)) = project.expressions[0].rex_type.as_ref()
        else {
            panic!("expression should export as if_then");
        };
        assert_eq!(if_then.ifs.len(), 1);
        assert!(if_then.r#else.is_some());
    }

    #[test]
    fn imports_if_then_as_case_when() {
        let relation = Rel {
            rel_type: Some(rel::RelType::Project(Box::new(ProjectRel {
                common: None,
                input: Some(Box::new(named_read("orders", &["amount"]))),
                expressions: vec![Expression {
                    rex_type: Some(expression::RexType::IfThen(Box::new(expression::IfThen {
                        ifs: vec![expression::if_then::IfClause {
                            r#if: Some(Expression {
                                rex_type: Some(expression::RexType::Literal(expression::Literal {
                                    nullable: false,
                                    type_variation_reference: 0,
                                    literal_type: Some(expression::literal::LiteralType::Boolean(
                                        true,
                                    )),
                                })),
                            }),
                            then: Some(Expression {
                                rex_type: Some(expression::RexType::Literal(expression::Literal {
                                    nullable: false,
                                    type_variation_reference: 0,
                                    literal_type: Some(expression::literal::LiteralType::String(
                                        "x".to_string(),
                                    )),
                                })),
                            }),
                        }],
                        r#else: Some(Box::new(Expression {
                            rex_type: Some(expression::RexType::Literal(expression::Literal {
                                nullable: false,
                                type_variation_reference: 0,
                                literal_type: Some(expression::literal::LiteralType::String(
                                    "y".to_string(),
                                )),
                            })),
                        })),
                    }))),
                }],
                advanced_extension: None,
            }))),
        };

        let ctx = from_rel(&relation).expect("relation should import");
        let OperatorData::Output(output) = ctx.operator(ctx.root().expect("root")) else {
            panic!("root should be output");
        };
        let OperatorData::Map(map) = ctx.operator(output.input) else {
            panic!("project expression should become map");
        };
        let ExprData::CaseWhen {
            when_then,
            else_expr,
        } = ctx.expr(map.computations[0].1)
        else {
            panic!("if_then should import as case_when");
        };
        assert_eq!(when_then.len(), 1);
        assert!(else_expr.is_some());
    }

    #[test]
    fn imports_named_table_read_with_root_output() {
        let plan = Plan {
            relations: vec![PlanRel {
                rel_type: Some(plan_rel::RelType::Root(proto::RelRoot {
                    input: Some(Rel {
                        rel_type: Some(rel::RelType::Read(Box::new(ReadRel {
                            common: None,
                            base_schema: Some(NamedStruct {
                                names: vec!["id".to_string()],
                                r#struct: Some(r#type::Struct {
                                    types: vec![required_i64_type()],
                                    type_variation_reference: 0,
                                    nullability: r#type::Nullability::Required as i32,
                                }),
                            }),
                            filter: None,
                            best_effort_filter: None,
                            projection: None,
                            advanced_extension: None,
                            read_type: Some(read_rel::ReadType::NamedTable(read_rel::NamedTable {
                                names: vec!["users".to_string()],
                                advanced_extension: None,
                            })),
                        }))),
                    }),
                    names: vec!["user_id".to_string()],
                })),
            }],
            ..Plan::default()
        };

        let ctx = from_plan(&plan).expect("plan should import");
        let root = ctx.root().expect("root should be set");
        let OperatorData::Output(output) = ctx.operator(root) else {
            panic!("root should be output")
        };
        let OperatorData::Scan(scan) = ctx.operator(output.input) else {
            panic!("output should read from scan")
        };

        assert_eq!(scan.table, TableRef::bare("users"));
        assert_eq!(ctx.column(scan.columns[0]).name, "user_id");
        assert_eq!(ctx.column(scan.columns[0]).ty, DataType::Int64);
    }

    #[test]
    fn imports_named_table_read_from_catalog_without_base_schema() {
        let catalog = Arc::new(MemoryCatalog::new("memory", "public"));
        catalog
            .create_table(
                TableRef::bare("users"),
                Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Int64, false),
                    Field::new("age", DataType::Int32, true),
                ])),
                None,
            )
            .unwrap();
        let plan = Plan {
            relations: vec![PlanRel {
                rel_type: Some(plan_rel::RelType::Root(proto::RelRoot {
                    input: Some(Rel {
                        rel_type: Some(rel::RelType::Read(Box::new(ReadRel {
                            common: None,
                            base_schema: None,
                            filter: None,
                            best_effort_filter: None,
                            projection: None,
                            advanced_extension: None,
                            read_type: Some(read_rel::ReadType::NamedTable(read_rel::NamedTable {
                                names: vec!["users".to_string()],
                                advanced_extension: None,
                            })),
                        }))),
                    }),
                    names: Vec::new(),
                })),
            }],
            ..Plan::default()
        };

        let ctx = from_plan_with_catalog(&plan, catalog).expect("plan should import");
        let root = ctx.root().expect("root should be set");
        let OperatorData::Output(output) = ctx.operator(root) else {
            panic!("root should be output")
        };
        let OperatorData::Scan(scan) = ctx.operator(output.input) else {
            panic!("output should read from scan")
        };

        assert_eq!(scan.table, TableRef::full("memory", "public", "users"));
        assert_eq!(scan.columns.len(), 2);
        assert_eq!(ctx.column(scan.columns[0]).name, "id");
        assert_eq!(ctx.column(scan.columns[0]).ty, DataType::Int64);
        assert_eq!(ctx.column(scan.columns[1]).name, "age");
        assert_eq!(ctx.column(scan.columns[1]).ty, DataType::Int32);
    }

    #[test]
    fn imports_sort_and_fetch() {
        let read = Rel {
            rel_type: Some(rel::RelType::Read(Box::new(ReadRel {
                common: None,
                base_schema: Some(NamedStruct {
                    names: vec!["id".to_string()],
                    r#struct: Some(r#type::Struct {
                        types: vec![required_i64_type()],
                        type_variation_reference: 0,
                        nullability: r#type::Nullability::Required as i32,
                    }),
                }),
                filter: None,
                best_effort_filter: None,
                projection: None,
                advanced_extension: None,
                read_type: Some(read_rel::ReadType::NamedTable(read_rel::NamedTable {
                    names: vec!["users".to_string()],
                    advanced_extension: None,
                })),
            }))),
        };
        let sort = Rel {
            rel_type: Some(rel::RelType::Sort(Box::new(SortRel {
                common: None,
                input: Some(Box::new(read)),
                sorts: vec![SortField {
                    expr: Some(field_ref(0)),
                    sort_kind: Some(sort_field::SortKind::Direction(
                        sort_field::SortDirection::DescNullsLast as i32,
                    )),
                }],
                advanced_extension: None,
            }))),
        };
        let fetch = Rel {
            rel_type: Some(rel::RelType::Fetch(Box::new(proto::FetchRel {
                common: None,
                input: Some(Box::new(sort)),
                advanced_extension: None,
                offset_mode: Some(fetch_rel::OffsetMode::Offset(5)),
                count_mode: Some(fetch_rel::CountMode::Count(10)),
            }))),
        };

        let ctx = from_rel(&fetch).expect("relation should import");
        let OperatorData::Output(output) = ctx.operator(ctx.root().unwrap()) else {
            panic!("root should be output");
        };
        let OperatorData::Limit(limit) = ctx.operator(output.input) else {
            panic!("output should read from limit");
        };
        assert_eq!(limit.fetch, Some(10));
        assert_eq!(limit.offset, 5);

        let OperatorData::Sort(sort) = ctx.operator(limit.input) else {
            panic!("limit should read from sort");
        };
        assert_eq!(sort.keys.len(), 1);
        assert_eq!(sort.keys[0].direction, SortDirection::Desc);
        assert_eq!(sort.keys[0].nulls, NullOrdering::Last);
    }

    #[test]
    fn project_emit_becomes_projection() {
        let project = Rel {
            rel_type: Some(rel::RelType::Project(Box::new(ProjectRel {
                common: Some(RelCommon {
                    emit_kind: Some(proto::rel_common::EmitKind::Emit(proto::rel_common::Emit {
                        output_mapping: vec![1, 2],
                    })),
                    hint: None,
                    advanced_extension: None,
                }),
                input: Some(Box::new(named_read("users", &["id", "age"]))),
                expressions: vec![field_ref(0)],
                advanced_extension: None,
            }))),
        };

        let ctx = from_rel(&project).expect("relation should import");
        let OperatorData::Output(output) = ctx.operator(ctx.root().unwrap()) else {
            panic!("root should be output");
        };
        let OperatorData::Projection(projection) = ctx.operator(output.input) else {
            panic!("project emit should produce projection");
        };
        assert_eq!(ctx.column(projection.columns[0]).name, "age");
        assert_eq!(ctx.column(projection.columns[1]).name, "expr_0");

        let OperatorData::Map(_) = ctx.operator(projection.input) else {
            panic!("project expression should produce map");
        };
    }

    #[test]
    fn imports_hash_join_as_logical_join() {
        let hash_join = Rel {
            rel_type: Some(rel::RelType::HashJoin(Box::new(HashJoinRel {
                common: None,
                left: Some(Box::new(named_read("users", &["user_id"]))),
                right: Some(Box::new(named_read("orders", &["order_user_id"]))),
                left_keys: Vec::new(),
                right_keys: Vec::new(),
                keys: vec![comparison_key(0, 0)],
                post_join_filter: None,
                r#type: hash_join_rel::JoinType::Inner as i32,
                build_input: hash_join_rel::BuildInput::Right as i32,
                advanced_extension: None,
            }))),
        };

        let ctx = from_rel(&hash_join).expect("relation should import");
        let OperatorData::Output(output) = ctx.operator(ctx.root().unwrap()) else {
            panic!("root should be output");
        };
        let OperatorData::Join(join) = ctx.operator(output.input) else {
            panic!("hash join should lower to logical join");
        };

        assert_eq!(join.join_type, JoinType::Inner);
        assert!(matches!(
            ctx.expr(join.on),
            ExprData::Binary {
                op: BinaryOp::Eq,
                ..
            }
        ));
    }

    #[test]
    fn imports_merge_join_as_logical_join() {
        let merge_join = Rel {
            rel_type: Some(rel::RelType::MergeJoin(Box::new(MergeJoinRel {
                common: None,
                left: Some(Box::new(named_read("users", &["user_id"]))),
                right: Some(Box::new(named_read("orders", &["order_user_id"]))),
                left_keys: Vec::new(),
                right_keys: Vec::new(),
                keys: vec![comparison_key(0, 0)],
                post_join_filter: None,
                r#type: merge_join_rel::JoinType::Left as i32,
                advanced_extension: None,
            }))),
        };

        let ctx = from_rel(&merge_join).expect("relation should import");
        let OperatorData::Output(output) = ctx.operator(ctx.root().unwrap()) else {
            panic!("root should be output");
        };
        let OperatorData::Join(join) = ctx.operator(output.input) else {
            panic!("merge join should lower to logical join");
        };

        assert_eq!(join.join_type, JoinType::LeftOuter);
    }

    #[test]
    fn imports_nested_loop_join_as_logical_join() {
        let nested_loop_join = Rel {
            rel_type: Some(rel::RelType::NestedLoopJoin(Box::new(NestedLoopJoinRel {
                common: None,
                left: Some(Box::new(named_read("users", &["user_id"]))),
                right: Some(Box::new(named_read("orders", &["order_user_id"]))),
                expression: None,
                r#type: nested_loop_join_rel::JoinType::Inner as i32,
                advanced_extension: None,
            }))),
        };

        let ctx = from_rel(&nested_loop_join).expect("relation should import");
        let OperatorData::Output(output) = ctx.operator(ctx.root().unwrap()) else {
            panic!("root should be output");
        };
        let OperatorData::Join(join) = ctx.operator(output.input) else {
            panic!("nested loop join should lower to logical join");
        };

        assert_eq!(join.join_type, JoinType::Inner);
        assert_eq!(
            ctx.expr(join.on),
            &ExprData::Literal(ScalarValue::Boolean(true))
        );
    }
}
