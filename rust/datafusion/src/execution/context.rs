// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! ExecutionContext contains methods for registering data sources and executing SQL
//! queries

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::string::String;
use std::sync::Arc;

use arrow::datatypes::*;

use crate::arrow::array::{ArrayRef, BooleanBuilder};
use crate::datasource::csv::CsvFile;
use crate::datasource::TableProvider;
use crate::error::{ExecutionError, Result};
use crate::execution::aggregate::AggregateRelation;
use crate::execution::expression::*;
use crate::execution::filter::FilterRelation;
use crate::execution::limit::LimitRelation;
use crate::execution::projection::ProjectRelation;
use crate::execution::relation::{DataSourceRelation, Relation};
use crate::execution::scalar_relation::ScalarRelation;
use crate::execution::table_impl::TableImpl;
use crate::logicalplan::*;
use crate::optimizer::optimizer::OptimizerRule;
use crate::optimizer::projection_push_down::ProjectionPushDown;
use crate::optimizer::type_coercion::TypeCoercionRule;
use crate::optimizer::utils;
use crate::sql::parser::FileType;
use crate::sql::parser::{DFASTNode, DFParser};
use crate::sql::planner::{SchemaProvider, SqlToRel};
use crate::table::Table;
use sqlparser::sqlast::{SQLColumnDef, SQLType};

/// Execution context for registering data sources and executing queries
pub struct ExecutionContext {
    datasources: Rc<RefCell<HashMap<String, Rc<TableProvider>>>>,
}

impl ExecutionContext {
    /// Create a new execution context for in-memory queries
    pub fn new() -> Self {
        Self {
            datasources: Rc::new(RefCell::new(HashMap::new())),
        }
    }

    /// Execute a SQL query and produce a Relation (a schema-aware iterator over a series
    /// of RecordBatch instances)
    pub fn sql(&mut self, sql: &str, batch_size: usize) -> Result<Rc<RefCell<Relation>>> {
        let plan = self.create_logical_plan(sql)?;
        let plan = self.optimize(&plan)?;
        Ok(self.execute(&plan, batch_size)?)
    }

    /// Creates a logical plan
    pub fn create_logical_plan(&mut self, sql: &str) -> Result<Arc<LogicalPlan>> {
        let ast = DFParser::parse_sql(String::from(sql))?;

        match ast {
            DFASTNode::ANSI(ansi) => {
                let schema_provider: Arc<SchemaProvider> =
                    Arc::new(ExecutionContextSchemaProvider {
                        datasources: self.datasources.clone(),
                    });

                // create a query planner
                let query_planner = SqlToRel::new(schema_provider);

                // plan the query (create a logical relational plan)
                let plan = query_planner.sql_to_rel(&ansi)?;

                Ok(plan)
            }
            DFASTNode::CreateExternalTable {
                name,
                columns,
                file_type,
                header_row,
                location,
            } => {
                let schema = Arc::new(self.build_schema(columns)?);

                Ok(Arc::new(LogicalPlan::CreateExternalTable {
                    schema,
                    name,
                    location,
                    file_type,
                    header_row,
                }))
            }
        }
    }

    fn build_schema(&self, columns: Vec<SQLColumnDef>) -> Result<Schema> {
        let mut fields = Vec::new();

        for column in columns {
            let data_type = self.make_data_type(column.data_type)?;
            fields.push(Field::new(&column.name, data_type, column.allow_null));
        }

        Ok(Schema::new(fields))
    }

    fn make_data_type(&self, sql_type: SQLType) -> Result<DataType> {
        match sql_type {
            SQLType::BigInt => Ok(DataType::Int64),
            SQLType::Int => Ok(DataType::Int32),
            SQLType::SmallInt => Ok(DataType::Int16),
            SQLType::Char(_) | SQLType::Varchar(_) | SQLType::Text => Ok(DataType::Utf8),
            SQLType::Decimal(_, _) => Ok(DataType::Float64),
            SQLType::Float(_) => Ok(DataType::Float32),
            SQLType::Real | SQLType::Double => Ok(DataType::Float64),
            SQLType::Boolean => Ok(DataType::Boolean),
            SQLType::Date => Ok(DataType::Date64(DateUnit::Day)),
            SQLType::Time => Ok(DataType::Time64(TimeUnit::Millisecond)),
            SQLType::Timestamp => Ok(DataType::Date64(DateUnit::Millisecond)),
            SQLType::Uuid
            | SQLType::Clob(_)
            | SQLType::Binary(_)
            | SQLType::Varbinary(_)
            | SQLType::Blob(_)
            | SQLType::Regclass
            | SQLType::Bytea
            | SQLType::Custom(_)
            | SQLType::Array(_) => Err(ExecutionError::General(format!(
                "Unsupported data type: {:?}.",
                sql_type
            ))),
        }
    }

    /// Register a CSV file as a table so that it can be queried from SQL
    pub fn register_csv(
        &mut self,
        name: &str,
        filename: &str,
        schema: &Schema,
        has_header: bool,
    ) {
        self.register_table(name, Rc::new(CsvFile::new(filename, schema, has_header)));
    }

    /// Register a table so that it can be queried from SQL
    pub fn register_table(&mut self, name: &str, provider: Rc<TableProvider>) {
        self.datasources
            .borrow_mut()
            .insert(name.to_string(), provider);
    }

    /// Get a table by name
    pub fn table(&mut self, table_name: &str) -> Result<Arc<Table>> {
        match (*self.datasources).borrow().get(table_name) {
            Some(provider) => {
                Ok(Arc::new(TableImpl::new(Arc::new(LogicalPlan::TableScan {
                    schema_name: "".to_string(),
                    table_name: table_name.to_string(),
                    table_schema: provider.schema().clone(),
                    projected_schema: provider.schema().clone(),
                    projection: None,
                }))))
            }
            _ => Err(ExecutionError::General(format!(
                "No table named '{}'",
                table_name
            ))),
        }
    }

    /// Optimize the logical plan by applying optimizer rules
    pub fn optimize(&self, plan: &LogicalPlan) -> Result<Arc<LogicalPlan>> {
        let rules: Vec<Box<OptimizerRule>> = vec![
            Box::new(ProjectionPushDown::new()),
            Box::new(TypeCoercionRule::new()),
        ];
        let mut plan = Arc::new(plan.clone());
        for mut rule in rules {
            plan = rule.optimize(&plan)?;
        }
        Ok(plan)
    }

    /// Execute a logical plan and produce a Relation (a schema-aware iterator over a
    /// series of RecordBatch instances)
    pub fn execute(
        &mut self,
        plan: &LogicalPlan,
        batch_size: usize,
    ) -> Result<Rc<RefCell<Relation>>> {
        match *plan {
            LogicalPlan::TableScan {
                ref table_name,
                ref projection,
                ..
            } => match (*self.datasources).borrow().get(table_name) {
                Some(provider) => {
                    let ds = provider.scan(projection, batch_size)?;
                    if ds.len() == 1 {
                        Ok(Rc::new(RefCell::new(DataSourceRelation::new(
                            ds[0].clone(),
                        ))))
                    } else {
                        Err(ExecutionError::General(
                            "Execution engine only supports single partition".to_string(),
                        ))
                    }
                }
                _ => Err(ExecutionError::General(format!(
                    "No table registered as '{}'",
                    table_name
                ))),
            },
            LogicalPlan::Selection {
                ref expr,
                ref input,
            } => {
                let input_rel = self.execute(input, batch_size)?;
                let input_schema = input_rel.as_ref().borrow().schema().clone();
                let runtime_expr = compile_expr(&self, expr, &input_schema)?;
                let rel = FilterRelation::new(input_rel, runtime_expr, input_schema);
                Ok(Rc::new(RefCell::new(rel)))
            }
            LogicalPlan::Projection {
                ref expr,
                ref input,
                ..
            } => {
                let input_rel = self.execute(input, batch_size)?;

                let input_schema = input_rel.as_ref().borrow().schema().clone();

                let project_columns: Vec<Field> =
                    utils::exprlist_to_fields(&expr, &input_schema)?;

                let project_schema = Arc::new(Schema::new(project_columns));

                let compiled_expr: Result<Vec<CompiledExpr>> = expr
                    .iter()
                    .map(|e| compile_expr(&self, e, &input_schema))
                    .collect();

                let rel = ProjectRelation::new(input_rel, compiled_expr?, project_schema);

                Ok(Rc::new(RefCell::new(rel)))
            }
            LogicalPlan::Aggregate {
                ref input,
                ref group_expr,
                ref aggr_expr,
                ..
            } => {
                let input_rel = self.execute(&input, batch_size)?;

                let input_schema = input_rel.as_ref().borrow().schema().clone();

                let compiled_group_expr_result: Result<Vec<CompiledExpr>> = group_expr
                    .iter()
                    .map(|e| compile_expr(&self, e, &input_schema))
                    .collect();
                let compiled_group_expr = compiled_group_expr_result?;

                let compiled_aggr_expr_result: Result<Vec<CompiledAggregateExpression>> =
                    aggr_expr
                        .iter()
                        .map(|e| compile_aggregate_expr(&self, e, &input_schema))
                        .collect();
                let compiled_aggr_expr = compiled_aggr_expr_result?;

                let mut output_fields: Vec<Field> = vec![];
                for expr in group_expr {
                    output_fields
                        .push(utils::expr_to_field(expr, input_schema.as_ref())?);
                }
                for expr in aggr_expr {
                    output_fields
                        .push(utils::expr_to_field(expr, input_schema.as_ref())?);
                }
                let rel = AggregateRelation::new(
                    Arc::new(Schema::new(output_fields)),
                    input_rel,
                    compiled_group_expr,
                    compiled_aggr_expr,
                );

                Ok(Rc::new(RefCell::new(rel)))
            }
            LogicalPlan::Limit {
                ref expr,
                ref input,
                ..
            } => {
                let input_rel = self.execute(input, batch_size)?;

                let input_schema = input_rel.as_ref().borrow().schema().clone();

                match expr {
                    &Expr::Literal(ref scalar_value) => {
                        let limit: usize = match scalar_value {
                            ScalarValue::Int8(x) => Ok(*x as usize),
                            ScalarValue::Int16(x) => Ok(*x as usize),
                            ScalarValue::Int32(x) => Ok(*x as usize),
                            ScalarValue::Int64(x) => Ok(*x as usize),
                            ScalarValue::UInt8(x) => Ok(*x as usize),
                            ScalarValue::UInt16(x) => Ok(*x as usize),
                            ScalarValue::UInt32(x) => Ok(*x as usize),
                            ScalarValue::UInt64(x) => Ok(*x as usize),
                            _ => Err(ExecutionError::ExecutionError(
                                "Limit only support positive integer literals"
                                    .to_string(),
                            )),
                        }?;
                        let rel = LimitRelation::new(input_rel, limit, input_schema);
                        Ok(Rc::new(RefCell::new(rel)))
                    }
                    _ => Err(ExecutionError::ExecutionError(
                        "Limit only support positive integer literals".to_string(),
                    )),
                }
            }

            LogicalPlan::CreateExternalTable {
                ref schema,
                ref name,
                ref location,
                ref file_type,
                ref header_row,
            } => {
                match file_type {
                    FileType::CSV => {
                        self.register_csv(name, location, schema, *header_row)
                    }
                    _ => {
                        return Err(ExecutionError::ExecutionError(format!(
                            "Unsupported file type {:?}.",
                            file_type
                        )));
                    }
                }
                let mut builder = BooleanBuilder::new(1);
                builder.append_value(true)?;

                let columns = vec![Arc::new(builder.finish()) as ArrayRef];
                Ok(Rc::new(RefCell::new(ScalarRelation::new(
                    Arc::new(Schema::new(vec![Field::new(
                        "result",
                        DataType::Boolean,
                        false,
                    )])),
                    columns,
                ))))
            }

            _ => Err(ExecutionError::NotImplemented(
                "Unsupported logical plan for execution".to_string(),
            )),
        }
    }
}

struct ExecutionContextSchemaProvider {
    datasources: Rc<RefCell<HashMap<String, Rc<TableProvider>>>>,
}
impl SchemaProvider for ExecutionContextSchemaProvider {
    fn get_table_meta(&self, name: &str) -> Option<Arc<Schema>> {
        match (*self.datasources).borrow().get(name) {
            Some(ds) => Some(ds.schema().clone()),
            None => None,
        }
    }

    fn get_function_meta(&self, _name: &str) -> Option<Arc<FunctionMeta>> {
        None
    }
}
