#![allow(deprecated)]

use std::collections::HashMap;
use std::fmt;

use arrow_schema::{DataType, TimeUnit};
use substrait::proto;
use substrait::proto::{
    CrossRel, Expression, FunctionArgument, JoinRel, NamedStruct, Plan, PlanRel, ProjectRel,
    ReadRel, Rel, RelCommon, Type, aggregate_function, aggregate_rel, expression,
    function_argument, join_rel, plan_rel, read_rel, rel, r#type,
};

use crate::{
    AggregateExpr, AggregateFunction, Aggregation, BinaryOp, Column, ColumnData, CrossProduct,
    Expr, ExprData, Join, JoinType, Map, NaryOp, Operator, OperatorData, Output, Projection,
    QueryContext, ScalarFunction, ScalarValue, Scan, Selection, TableFunction, TableFunctionDef,
    UnaryOp,
};

/// Converts Substrait protobuf plans into this crate's relational IR.
///
/// This importer intentionally starts with the logical subset this IR can represent:
/// reads, filters, projects/maps, joins, cross products, and simple aggregations.
pub fn from_plan(plan: &Plan) -> Result<QueryContext, SubstraitError> {
    Converter::new(plan).convert_plan(plan)
}

/// Converts a single Substrait relation tree into this crate's relational IR.
pub fn from_rel(rel: &Rel) -> Result<QueryContext, SubstraitError> {
    let empty = Plan::default();
    Converter::new(&empty).convert_standalone_rel(rel)
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
}

impl Converter {
    fn new(plan: &Plan) -> Self {
        Self {
            ctx: QueryContext::new(),
            functions: collect_function_anchors(plan),
            next_computed_column: 0,
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
            rel::RelType::Fetch(_) => Err(SubstraitError::UnsupportedRel("FetchRel")),
            rel::RelType::Sort(_) => Err(SubstraitError::UnsupportedRel("SortRel")),
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
            rel::RelType::HashJoin(_) => Err(SubstraitError::UnsupportedRel("HashJoinRel")),
            rel::RelType::MergeJoin(_) => Err(SubstraitError::UnsupportedRel("MergeJoinRel")),
            rel::RelType::NestedLoopJoin(_) => {
                Err(SubstraitError::UnsupportedRel("NestedLoopJoinRel"))
            }
            rel::RelType::Window(_) => Err(SubstraitError::UnsupportedRel(
                "ConsistentPartitionWindowRel",
            )),
            rel::RelType::Exchange(_) => Err(SubstraitError::UnsupportedRel("ExchangeRel")),
            rel::RelType::Expand(_) => Err(SubstraitError::UnsupportedRel("ExpandRel")),
        }
    }

    fn convert_read(&mut self, read: &ReadRel) -> Result<Relation, SubstraitError> {
        if read.projection.is_some() {
            return Err(SubstraitError::UnsupportedRead("ReadRel.projection"));
        }

        let columns = self.convert_named_struct(read.base_schema.as_ref())?;
        let mut relation = match read
            .read_type
            .as_ref()
            .ok_or(SubstraitError::MissingField("ReadRel.read_type"))?
        {
            read_rel::ReadType::NamedTable(table) => {
                let table = if table.names.is_empty() {
                    "<unnamed>".to_string()
                } else {
                    table.names.join(".")
                };
                let operator = self.ctx.add_operator(OperatorData::Scan(Scan {
                    table,
                    columns: columns.clone(),
                }));
                Relation { operator, columns }
            }
            read_rel::ReadType::LocalFiles(files) => {
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

        if read.best_effort_filter.is_some() {
            return Err(SubstraitError::UnsupportedRead(
                "ReadRel.best_effort_filter",
            ));
        }

        self.apply_common(read.common.as_ref(), relation)
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

        if join.post_join_filter.is_some() {
            return Err(SubstraitError::UnsupportedJoin("JoinRel.post_join_filter"));
        }

        let join_type = convert_join_type(join.r#type)?;
        let operator = self.ctx.add_operator(OperatorData::Join(Join {
            join_type,
            on,
            outer: left.operator,
            inner: right.operator,
        }));
        self.apply_common(
            project_common_join(join),
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
            expression::RexType::IfThen(_) => {
                return Err(SubstraitError::UnsupportedExpression("IfThen"));
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
                let (data_type, nullable) = substrait_type_to_column_type(ty)?;
                let name = schema
                    .names
                    .get(index)
                    .cloned()
                    .unwrap_or_else(|| format!("field_{index}"));
                Ok(self
                    .ctx
                    .add_column(ColumnData::new(name, data_type, nullable)))
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
            _ => Ok(None),
        }
    }

    fn add_computed_column(&mut self, name: String, ty: Option<DataType>) -> Column {
        self.next_computed_column += 1;
        self.ctx
            .add_column(ColumnData::new(name, ty.unwrap_or(DataType::Null), true))
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

    fn required_i64_type() -> Type {
        Type {
            kind: Some(r#type::Kind::I64(r#type::I64 {
                type_variation_reference: 0,
                nullability: r#type::Nullability::Required as i32,
            })),
        }
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

        assert_eq!(scan.table, "users");
        assert_eq!(ctx.column(scan.columns[0]).name, "user_id");
        assert_eq!(ctx.column(scan.columns[0]).ty, DataType::Int64);
        assert!(!ctx.column(scan.columns[0]).nullable);
    }
}
