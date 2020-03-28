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

//! ExecutionContext contains methods for registering data sources and executing queries

use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::string::String;
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use arrow::csv;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;

use crate::datasource::csv::CsvFile;
use crate::datasource::parquet::ParquetTable;
use crate::datasource::TableProvider;
use crate::error::{ExecutionError, Result};
use crate::execution::physical_plan::common;
use crate::execution::physical_plan::datasource::DatasourceExec;
use crate::execution::physical_plan::expressions::{
    Alias, Avg, BinaryExpr, CastExpr, Column, Count, Literal, Max, Min, Sum,
};
use crate::execution::physical_plan::hash_aggregate::HashAggregateExec;
use crate::execution::physical_plan::limit::LimitExec;
use crate::execution::physical_plan::merge::MergeExec;
use crate::execution::physical_plan::projection::ProjectionExec;
use crate::execution::physical_plan::selection::SelectionExec;
use crate::execution::physical_plan::{AggregateExpr, ExecutionPlan, PhysicalExpr};
use crate::execution::table_impl::TableImpl;
use crate::logicalplan::*;
use crate::optimizer::optimizer::OptimizerRule;
use crate::optimizer::projection_push_down::ProjectionPushDown;
use crate::optimizer::resolve_columns::ResolveColumnsRule;
use crate::optimizer::type_coercion::TypeCoercionRule;
use crate::sql::parser::{DFASTNode, DFParser, FileType};
use crate::sql::planner::{SchemaProvider, SqlToRel};
use crate::table::Table;
use sqlparser::sqlast::{SQLColumnDef, SQLType};

/// Execution context for registering data sources and executing queries
pub struct ExecutionContext {
    datasources: HashMap<String, Box<dyn TableProvider>>,
}

impl ExecutionContext {
    /// Create a new execution context for in-memory queries
    pub fn new() -> Self {
        Self {
            datasources: HashMap::new(),
        }
    }

    /// Execute a SQL query and produce a Relation (a schema-aware iterator over a series
    /// of RecordBatch instances)
    pub fn sql(&mut self, sql: &str, batch_size: usize) -> Result<Vec<RecordBatch>> {
        let plan = self.create_logical_plan(sql)?;

        return self.collect_plan(&plan, batch_size);
    }

    /// Executes a logical plan and produce a Relation (a schema-aware iterator over a series
    /// of RecordBatch instances)
    pub fn collect_plan(
        &mut self,
        plan: &LogicalPlan,
        batch_size: usize,
    ) -> Result<Vec<RecordBatch>> {
        match plan {
            LogicalPlan::CreateExternalTable {
                ref schema,
                ref name,
                ref location,
                ref file_type,
                ref header_row,
            } => match file_type {
                FileType::CSV => {
                    self.register_csv(name, location, schema, *header_row);
                    Ok(vec![])
                }
                FileType::Parquet => {
                    self.register_parquet(name, location)?;
                    Ok(vec![])
                }
                _ => Err(ExecutionError::ExecutionError(format!(
                    "Unsupported file type {:?}.",
                    file_type
                ))),
            },

            plan => {
                let plan = self.optimize(&plan)?;
                let plan = self.create_physical_plan(&plan, batch_size)?;
                Ok(self.collect(plan.as_ref())?)
            }
        }
    }

    /// Creates a logical plan
    pub fn create_logical_plan(&mut self, sql: &str) -> Result<LogicalPlan> {
        let ast = DFParser::parse_sql(String::from(sql))?;

        match ast {
            DFASTNode::ANSI(ansi) => {
                let schema_provider = ExecutionContextSchemaProvider {
                    datasources: &self.datasources,
                };

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

                Ok(LogicalPlan::CreateExternalTable {
                    schema,
                    name,
                    location,
                    file_type,
                    header_row,
                })
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
        self.register_table(name, Box::new(CsvFile::new(filename, schema, has_header)));
    }

    /// Register a Parquet file as a table so that it can be queried from SQL
    pub fn register_parquet(&mut self, name: &str, filename: &str) -> Result<()> {
        let table = ParquetTable::try_new(&filename)?;
        self.register_table(name, Box::new(table));
        Ok(())
    }

    /// Register a table so that it can be queried from SQL
    pub fn register_table(&mut self, name: &str, provider: Box<dyn TableProvider>) {
        self.datasources.insert(name.to_string(), provider);
    }

    /// Get a table by name
    pub fn table(&mut self, table_name: &str) -> Result<Arc<dyn Table>> {
        match self.datasources.get(table_name) {
            Some(provider) => {
                let table_scan = LogicalPlan::TableScan {
                    schema_name: "".to_string(),
                    table_name: table_name.to_string(),
                    table_schema: provider.schema().clone(),
                    projected_schema: provider.schema().clone(),
                    projection: None,
                };
                Ok(Arc::new(TableImpl::new(
                    &LogicalPlanBuilder::from(&table_scan).build()?,
                )))
            }
            _ => Err(ExecutionError::General(format!(
                "No table named '{}'",
                table_name
            ))),
        }
    }

    /// Optimize the logical plan by applying optimizer rules
    pub fn optimize(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        let rules: Vec<Box<dyn OptimizerRule>> = vec![
            Box::new(ResolveColumnsRule::new()),
            Box::new(ProjectionPushDown::new()),
            Box::new(TypeCoercionRule::new()),
        ];
        let mut plan = plan.clone();
        for mut rule in rules {
            plan = rule.optimize(&plan)?;
        }
        Ok(plan)
    }

    /// Create a physical plan from a logical plan
    pub fn create_physical_plan(
        &mut self,
        logical_plan: &LogicalPlan,
        batch_size: usize,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match logical_plan {
            LogicalPlan::TableScan {
                table_name,
                projection,
                ..
            } => match self.datasources.get(table_name) {
                Some(provider) => {
                    let partitions = provider.scan(projection, batch_size)?;
                    if partitions.is_empty() {
                        Err(ExecutionError::General(
                            "Table provider returned no partitions".to_string(),
                        ))
                    } else {
                        let partition = partitions[0].lock().unwrap();
                        let schema = partition.schema();
                        let exec =
                            DatasourceExec::new(schema.clone(), partitions.clone());
                        Ok(Arc::new(exec))
                    }
                }
                _ => Err(ExecutionError::General(format!(
                    "No table named {}",
                    table_name
                ))),
            },
            LogicalPlan::Projection { input, expr, .. } => {
                let input = self.create_physical_plan(input, batch_size)?;
                let input_schema = input.as_ref().schema().clone();
                let runtime_expr = expr
                    .iter()
                    .map(|e| self.create_physical_expr(e, &input_schema))
                    .collect::<Result<Vec<_>>>()?;
                Ok(Arc::new(ProjectionExec::try_new(runtime_expr, input)?))
            }
            LogicalPlan::Aggregate {
                input,
                group_expr,
                aggr_expr,
                ..
            } => {
                // Initially need to perform the aggregate and then merge the partitions
                let input = self.create_physical_plan(input, batch_size)?;
                let input_schema = input.as_ref().schema().clone();

                let group_expr = group_expr
                    .iter()
                    .map(|e| self.create_physical_expr(e, &input_schema))
                    .collect::<Result<Vec<_>>>()?;
                let aggr_expr = aggr_expr
                    .iter()
                    .map(|e| self.create_aggregate_expr(e, &input_schema))
                    .collect::<Result<Vec<_>>>()?;

                let initial_aggr =
                    HashAggregateExec::try_new(group_expr, aggr_expr, input)?;

                let schema = initial_aggr.schema();
                let partitions = initial_aggr.partitions()?;

                if partitions.len() == 1 {
                    return Ok(Arc::new(initial_aggr));
                }

                let (final_group, final_aggr) = initial_aggr.make_final_expr();

                let merge = Arc::new(MergeExec::new(schema.clone(), partitions));

                Ok(Arc::new(HashAggregateExec::try_new(
                    final_group,
                    final_aggr,
                    merge,
                )?))
            }
            LogicalPlan::Selection { input, expr, .. } => {
                let input = self.create_physical_plan(input, batch_size)?;
                let input_schema = input.as_ref().schema().clone();
                let runtime_expr = self.create_physical_expr(expr, &input_schema)?;
                Ok(Arc::new(SelectionExec::try_new(runtime_expr, input)?))
            }
            LogicalPlan::Limit { input, expr, .. } => {
                let input = self.create_physical_plan(input, batch_size)?;
                let input_schema = input.as_ref().schema().clone();

                match expr {
                    &Expr::Literal(ref scalar_value) => {
                        let limit: usize = match scalar_value {
                            ScalarValue::Int8(limit) if *limit >= 0 => {
                                Ok(*limit as usize)
                            }
                            ScalarValue::Int16(limit) if *limit >= 0 => {
                                Ok(*limit as usize)
                            }
                            ScalarValue::Int32(limit) if *limit >= 0 => {
                                Ok(*limit as usize)
                            }
                            ScalarValue::Int64(limit) if *limit >= 0 => {
                                Ok(*limit as usize)
                            }
                            ScalarValue::UInt8(limit) => Ok(*limit as usize),
                            ScalarValue::UInt16(limit) => Ok(*limit as usize),
                            ScalarValue::UInt32(limit) => Ok(*limit as usize),
                            ScalarValue::UInt64(limit) => Ok(*limit as usize),
                            _ => Err(ExecutionError::ExecutionError(
                                "Limit only supports non-negative integer literals"
                                    .to_string(),
                            )),
                        }?;
                        Ok(Arc::new(LimitExec::new(
                            input_schema.clone(),
                            input.partitions()?,
                            limit,
                        )))
                    }
                    _ => Err(ExecutionError::ExecutionError(
                        "Limit only supports non-negative integer literals".to_string(),
                    )),
                }
            }
            _ => Err(ExecutionError::General(
                "Unsupported logical plan variant".to_string(),
            )),
        }
    }

    /// Create a physical expression from a logical expression
    pub fn create_physical_expr(
        &self,
        e: &Expr,
        input_schema: &Schema,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        match e {
            Expr::Alias(expr, name) => {
                let expr = self.create_physical_expr(expr, input_schema)?;
                Ok(Arc::new(Alias::new(expr, &name)))
            }
            Expr::Column(i) => {
                Ok(Arc::new(Column::new(*i, &input_schema.field(*i).name())))
            }
            Expr::Literal(value) => Ok(Arc::new(Literal::new(value.clone()))),
            Expr::BinaryExpr { left, op, right } => Ok(Arc::new(BinaryExpr::new(
                self.create_physical_expr(left, input_schema)?,
                op.clone(),
                self.create_physical_expr(right, input_schema)?,
            ))),
            Expr::Cast { expr, data_type } => Ok(Arc::new(CastExpr::try_new(
                self.create_physical_expr(expr, input_schema)?,
                input_schema,
                data_type.clone(),
            )?)),
            other => Err(ExecutionError::NotImplemented(format!(
                "Physical plan does not support logical expression {:?}",
                other
            ))),
        }
    }

    /// Create an aggregate expression from a logical expression
    pub fn create_aggregate_expr(
        &self,
        e: &Expr,
        input_schema: &Schema,
    ) -> Result<Arc<dyn AggregateExpr>> {
        match e {
            Expr::AggregateFunction { name, args, .. } => {
                match name.to_lowercase().as_ref() {
                    "sum" => Ok(Arc::new(Sum::new(
                        self.create_physical_expr(&args[0], input_schema)?,
                    ))),
                    "avg" => Ok(Arc::new(Avg::new(
                        self.create_physical_expr(&args[0], input_schema)?,
                    ))),
                    "max" => Ok(Arc::new(Max::new(
                        self.create_physical_expr(&args[0], input_schema)?,
                    ))),
                    "min" => Ok(Arc::new(Min::new(
                        self.create_physical_expr(&args[0], input_schema)?,
                    ))),
                    "count" => Ok(Arc::new(Count::new(
                        self.create_physical_expr(&args[0], input_schema)?,
                    ))),
                    other => Err(ExecutionError::NotImplemented(format!(
                        "Unsupported aggregate function '{}'",
                        other
                    ))),
                }
            }
            other => Err(ExecutionError::General(format!(
                "Invalid aggregate expression '{:?}'",
                other
            ))),
        }
    }

    /// Execute a physical plan and collect the results in memory
    pub fn collect(&self, plan: &dyn ExecutionPlan) -> Result<Vec<RecordBatch>> {
        let partitions = plan.partitions()?;

        match partitions.len() {
            0 => Ok(vec![]),
            1 => {
                let it = partitions[0].execute()?;
                common::collect(it)
            }
            _ => {
                // merge into a single partition
                let plan = MergeExec::new(plan.schema().clone(), partitions);
                let partitions = plan.partitions()?;
                if partitions.len() == 1 {
                    common::collect(partitions[0].execute()?)
                } else {
                    Err(ExecutionError::InternalError(format!(
                        "MergeExec returned {} partitions",
                        partitions.len()
                    )))
                }
            }
        }
    }

    /// Execute a query and write the results to a partitioned CSV file
    pub fn write_csv(&self, plan: &dyn ExecutionPlan, path: &str) -> Result<()> {
        // create directory to contain the CSV files (one per partition)
        let path = path.to_string();
        fs::create_dir(&path)?;

        let threads: Vec<JoinHandle<Result<()>>> = plan
            .partitions()?
            .iter()
            .enumerate()
            .map(|(i, p)| {
                let p = p.clone();
                let path = path.clone();
                thread::spawn(move || {
                    let filename = format!("part-{}.csv", i);
                    let path = Path::new(&path).join(&filename);
                    let file = fs::File::create(path)?;
                    let mut writer = csv::Writer::new(file);
                    let it = p.execute()?;
                    let mut it = it.lock().unwrap();
                    loop {
                        match it.next() {
                            Ok(Some(batch)) => {
                                writer.write(&batch)?;
                            }
                            Ok(None) => break,
                            Err(e) => return Err(e),
                        }
                    }
                    Ok(())
                })
            })
            .collect();

        // combine the results from each thread
        for thread in threads {
            let join = thread.join().expect("Failed to join thread");
            join?;
        }

        Ok(())
    }
}

struct ExecutionContextSchemaProvider<'a> {
    datasources: &'a HashMap<String, Box<dyn TableProvider>>,
}

impl SchemaProvider for ExecutionContextSchemaProvider<'_> {
    fn get_table_meta(&self, name: &str) -> Option<Arc<Schema>> {
        self.datasources.get(name).map(|ds| ds.schema().clone())
    }

    fn get_function_meta(&self, _name: &str) -> Option<Arc<FunctionMeta>> {
        None
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::test;
    use std::fs::File;
    use std::io::prelude::*;
    use tempdir::TempDir;

    #[test]
    fn parallel_projection() -> Result<()> {
        let partition_count = 4;
        let results = execute("SELECT c1, c2 FROM test", partition_count)?;

        // there should be one batch per partition
        assert_eq!(results.len(), partition_count);

        // each batch should contain 2 columns and 10 rows
        for batch in &results {
            assert_eq!(batch.num_columns(), 2);
            assert_eq!(batch.num_rows(), 10);
        }

        Ok(())
    }

    #[test]
    fn parallel_selection() -> Result<()> {
        let tmp_dir = TempDir::new("parallel_selection")?;
        let partition_count = 4;
        let mut ctx = create_ctx(&tmp_dir, partition_count)?;

        let logical_plan =
            ctx.create_logical_plan("SELECT c1, c2 FROM test WHERE c1 > 0 AND c1 < 3")?;
        let logical_plan = ctx.optimize(&logical_plan)?;

        let physical_plan = ctx.create_physical_plan(&logical_plan, 1024)?;

        let results = ctx.collect(physical_plan.as_ref())?;

        // there should be one batch per partition
        assert_eq!(results.len(), partition_count);

        let row_count: usize = results.iter().map(|batch| batch.num_rows()).sum();
        assert_eq!(row_count, 20);

        Ok(())
    }

    #[test]
    fn aggregate() -> Result<()> {
        let results = execute("SELECT SUM(c1), SUM(c2) FROM test", 4)?;
        assert_eq!(results.len(), 1);

        let batch = &results[0];
        let expected: Vec<&str> = vec!["60,220"];
        let mut rows = test::format_batch(&batch);
        rows.sort();
        assert_eq!(rows, expected);

        Ok(())
    }

    #[test]
    fn aggregate_avg() -> Result<()> {
        let results = execute("SELECT AVG(c1), AVG(c2) FROM test", 4)?;
        assert_eq!(results.len(), 1);

        let batch = &results[0];
        let expected: Vec<&str> = vec!["1.5,5.5"];
        let mut rows = test::format_batch(&batch);
        rows.sort();
        assert_eq!(rows, expected);

        Ok(())
    }

    #[test]
    fn aggregate_max() -> Result<()> {
        let results = execute("SELECT MAX(c1), MAX(c2) FROM test", 4)?;
        assert_eq!(results.len(), 1);

        let batch = &results[0];
        let expected: Vec<&str> = vec!["3,10"];
        let mut rows = test::format_batch(&batch);
        rows.sort();
        assert_eq!(rows, expected);

        Ok(())
    }

    #[test]
    fn aggregate_min() -> Result<()> {
        let results = execute("SELECT MIN(c1), MIN(c2) FROM test", 4)?;
        assert_eq!(results.len(), 1);

        let batch = &results[0];
        let expected: Vec<&str> = vec!["0,1"];
        let mut rows = test::format_batch(&batch);
        rows.sort();
        assert_eq!(rows, expected);

        Ok(())
    }

    #[test]
    fn aggregate_grouped() -> Result<()> {
        let results = execute("SELECT c1, SUM(c2) FROM test GROUP BY c1", 4)?;
        assert_eq!(results.len(), 1);

        let batch = &results[0];
        let expected: Vec<&str> = vec!["0,55", "1,55", "2,55", "3,55"];
        let mut rows = test::format_batch(&batch);
        rows.sort();
        assert_eq!(rows, expected);

        Ok(())
    }

    #[test]
    fn aggregate_grouped_avg() -> Result<()> {
        let results = execute("SELECT c1, AVG(c2) FROM test GROUP BY c1", 4)?;
        assert_eq!(results.len(), 1);

        let batch = &results[0];
        let expected: Vec<&str> = vec!["0,5.5", "1,5.5", "2,5.5", "3,5.5"];
        let mut rows = test::format_batch(&batch);
        rows.sort();
        assert_eq!(rows, expected);

        Ok(())
    }

    #[test]
    fn aggregate_grouped_max() -> Result<()> {
        let results = execute("SELECT c1, MAX(c2) FROM test GROUP BY c1", 4)?;
        assert_eq!(results.len(), 1);

        let batch = &results[0];
        let expected: Vec<&str> = vec!["0,10", "1,10", "2,10", "3,10"];
        let mut rows = test::format_batch(&batch);
        rows.sort();
        assert_eq!(rows, expected);

        Ok(())
    }

    #[test]
    fn aggregate_grouped_min() -> Result<()> {
        let results = execute("SELECT c1, MIN(c2) FROM test GROUP BY c1", 4)?;
        assert_eq!(results.len(), 1);

        let batch = &results[0];
        let expected: Vec<&str> = vec!["0,1", "1,1", "2,1", "3,1"];
        let mut rows = test::format_batch(&batch);
        rows.sort();
        assert_eq!(rows, expected);

        Ok(())
    }

    #[test]
    fn count_basic() -> Result<()> {
        let results = execute("SELECT COUNT(c1), COUNT(c2) FROM test", 1)?;
        assert_eq!(results.len(), 1);

        let batch = &results[0];
        let expected: Vec<&str> = vec!["10,10"];
        let mut rows = test::format_batch(&batch);
        rows.sort();
        assert_eq!(rows, expected);
        Ok(())
    }

    #[test]
    fn count_partitioned() -> Result<()> {
        let results = execute("SELECT COUNT(c1), COUNT(c2) FROM test", 4)?;
        assert_eq!(results.len(), 1);

        let batch = &results[0];
        let expected: Vec<&str> = vec!["40,40"];
        let mut rows = test::format_batch(&batch);
        rows.sort();
        assert_eq!(rows, expected);
        Ok(())
    }

    #[test]
    fn count_aggregated() -> Result<()> {
        let results = execute("SELECT c1, COUNT(c2) FROM test GROUP BY c1", 4)?;
        assert_eq!(results.len(), 1);

        let batch = &results[0];
        let expected = vec!["0,10", "1,10", "2,10", "3,10"];
        let mut rows = test::format_batch(&batch);
        rows.sort();
        assert_eq!(rows, expected);
        Ok(())
    }

    #[test]
    fn aggregate_with_alias() -> Result<()> {
        let tmp_dir = TempDir::new("execute")?;
        let mut ctx = create_ctx(&tmp_dir, 1)?;

        let schema = Arc::new(Schema::new(vec![
            Field::new("state", DataType::Utf8, false),
            Field::new("salary", DataType::UInt32, false),
        ]));

        let plan = LogicalPlanBuilder::scan("default", "test", schema.as_ref(), None)?
            .aggregate(
                vec![col("state")],
                vec![aggregate_expr("SUM", col("salary"), DataType::UInt32)],
            )?
            .project(vec![col("state"), col_index(1).alias("total_salary")])?
            .build()?;

        let plan = ctx.optimize(&plan)?;

        let physical_plan = ctx.create_physical_plan(&Arc::new(plan), 1024)?;
        assert_eq!("c1", physical_plan.schema().field(0).name().as_str());
        assert_eq!(
            "total_salary",
            physical_plan.schema().field(1).name().as_str()
        );
        Ok(())
    }

    #[test]
    fn write_csv_results() -> Result<()> {
        // create partitioned input file and context
        let tmp_dir = TempDir::new("write_csv_results_temp")?;
        let mut ctx = create_ctx(&tmp_dir, 4)?;

        // execute a simple query and write the results to CSV
        let out_dir = tmp_dir.as_ref().to_str().unwrap().to_string() + "/out";
        write_csv(&mut ctx, "SELECT c1, c2 FROM test", &out_dir)?;

        // create a new context and verify that the results were saved to a partitioned csv file
        let mut ctx = ExecutionContext::new();

        let schema = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::UInt32, false),
            Field::new("c2", DataType::UInt64, false),
        ]));

        // register each partition as well as the top level dir
        ctx.register_csv("part0", &format!("{}/part-0.csv", out_dir), &schema, true);
        ctx.register_csv("part1", &format!("{}/part-1.csv", out_dir), &schema, true);
        ctx.register_csv("part2", &format!("{}/part-2.csv", out_dir), &schema, true);
        ctx.register_csv("part3", &format!("{}/part-3.csv", out_dir), &schema, true);
        ctx.register_csv("allparts", &out_dir, &schema, true);

        let part0 = collect(&mut ctx, "SELECT c1, c2 FROM part0")?;
        let part1 = collect(&mut ctx, "SELECT c1, c2 FROM part1")?;
        let part2 = collect(&mut ctx, "SELECT c1, c2 FROM part2")?;
        let part3 = collect(&mut ctx, "SELECT c1, c2 FROM part3")?;
        let allparts = collect(&mut ctx, "SELECT c1, c2 FROM allparts")?;

        let part0_count: usize = part0.iter().map(|batch| batch.num_rows()).sum();
        let part1_count: usize = part1.iter().map(|batch| batch.num_rows()).sum();
        let part2_count: usize = part2.iter().map(|batch| batch.num_rows()).sum();
        let part3_count: usize = part3.iter().map(|batch| batch.num_rows()).sum();
        let allparts_count: usize = allparts.iter().map(|batch| batch.num_rows()).sum();

        assert_eq!(part0_count, 10);
        assert_eq!(part1_count, 10);
        assert_eq!(part2_count, 10);
        assert_eq!(part3_count, 10);
        assert_eq!(allparts_count, 40);

        Ok(())
    }

    /// Execute SQL and return results
    fn collect(ctx: &mut ExecutionContext, sql: &str) -> Result<Vec<RecordBatch>> {
        let logical_plan = ctx.create_logical_plan(sql)?;
        let logical_plan = ctx.optimize(&logical_plan)?;
        let physical_plan = ctx.create_physical_plan(&logical_plan, 1024)?;
        ctx.collect(physical_plan.as_ref())
    }

    /// Execute SQL and return results
    fn execute(sql: &str, partition_count: usize) -> Result<Vec<RecordBatch>> {
        let tmp_dir = TempDir::new("execute")?;
        let mut ctx = create_ctx(&tmp_dir, partition_count)?;
        collect(&mut ctx, sql)
    }

    /// Execute SQL and write results to partitioned csv files
    fn write_csv(ctx: &mut ExecutionContext, sql: &str, out_dir: &str) -> Result<()> {
        let logical_plan = ctx.create_logical_plan(sql)?;
        let logical_plan = ctx.optimize(&logical_plan)?;
        let physical_plan = ctx.create_physical_plan(&logical_plan, 1024)?;
        ctx.write_csv(physical_plan.as_ref(), out_dir)
    }

    /// Generate a partitioned CSV file and register it with an execution context
    fn create_ctx(tmp_dir: &TempDir, partition_count: usize) -> Result<ExecutionContext> {
        let mut ctx = ExecutionContext::new();

        // define schema for data source (csv file)
        let schema = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::UInt32, false),
            Field::new("c2", DataType::UInt64, false),
        ]));

        // generate a partitioned file
        for partition in 0..partition_count {
            let filename = format!("partition-{}.csv", partition);
            let file_path = tmp_dir.path().join(&filename);
            let mut file = File::create(file_path)?;

            // generate some data
            for i in 0..=10 {
                let data = format!("{},{}\n", partition, i);
                file.write_all(data.as_bytes())?;
            }
        }

        // register csv file with the execution context
        ctx.register_csv("test", tmp_dir.path().to_str().unwrap(), &schema, true);

        Ok(ctx)
    }
}
