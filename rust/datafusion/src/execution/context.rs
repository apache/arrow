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

use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;
use std::string::String;
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

use arrow::csv;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;

use crate::dataframe::DataFrame;
use crate::datasource::csv::CsvFile;
use crate::datasource::parquet::ParquetTable;
use crate::datasource::TableProvider;
use crate::error::{ExecutionError, Result};
use crate::execution::dataframe_impl::DataFrameImpl;
use crate::execution::physical_plan::common;
use crate::execution::physical_plan::csv::CsvReadOptions;
use crate::execution::physical_plan::merge::MergeExec;
use crate::execution::physical_plan::planner::PhysicalPlannerImpl;
use crate::execution::physical_plan::scalar_functions;
use crate::execution::physical_plan::udf::ScalarFunction;
use crate::execution::physical_plan::ExecutionPlan;
use crate::execution::physical_plan::PhysicalPlanner;
use crate::logicalplan::{FunctionMeta, FunctionType, LogicalPlan, LogicalPlanBuilder};
use crate::optimizer::optimizer::OptimizerRule;
use crate::optimizer::projection_push_down::ProjectionPushDown;
use crate::optimizer::type_coercion::TypeCoercionRule;
use crate::sql::{
    parser::{DFParser, FileType},
    planner::{SchemaProvider, SqlToRel},
};

/// ExecutionContext is the main interface for executing queries with DataFusion. The context
/// provides the following functionality:
///
/// * Create DataFrame from a CSV or Parquet data source.
/// * Register a CSV or Parquet data source as a table that can be referenced from a SQL query.
/// * Register a custom data source that can be referenced from a SQL query.
/// * Execution a SQL query
///
/// The following example demonstrates how to use the context to execute a query against a CSV
/// data source:
///
/// ```
/// use datafusion::ExecutionContext;
/// use datafusion::execution::physical_plan::csv::CsvReadOptions;
/// use datafusion::logicalplan::col;
///
/// let mut ctx = ExecutionContext::new();
/// let df = ctx.read_csv("tests/example.csv", CsvReadOptions::new()).unwrap();
/// let df = df.filter(col("a").lt_eq(col("b"))).unwrap()
///            .aggregate(vec![col("a")], vec![df.min(col("b")).unwrap()]).unwrap()
///            .limit(100).unwrap();
/// let results = df.collect();
/// ```
///
/// The following example demonstrates how to execute the same query using SQL:
///
/// ```
/// use datafusion::ExecutionContext;
/// use datafusion::execution::physical_plan::csv::CsvReadOptions;
/// use datafusion::logicalplan::col;
///
/// let mut ctx = ExecutionContext::new();
/// ctx.register_csv("example", "tests/example.csv", CsvReadOptions::new()).unwrap();
/// let results = ctx.sql("SELECT a, MIN(b) FROM example GROUP BY a LIMIT 100").unwrap();
/// ```
pub struct ExecutionContext {
    /// Internal state for the context
    pub state: Arc<Mutex<ExecutionContextState>>,
}

impl ExecutionContext {
    /// Create a new execution context using a default configuration.
    pub fn new() -> Self {
        Self::with_config(ExecutionConfig::new())
    }

    /// Create a new execution context using the provided configuration
    pub fn with_config(config: ExecutionConfig) -> Self {
        let mut ctx = Self {
            state: Arc::new(Mutex::new(ExecutionContextState {
                datasources: Arc::new(Mutex::new(HashMap::new())),
                scalar_functions: Arc::new(Mutex::new(HashMap::new())),
                config,
            })),
        };
        for udf in scalar_functions() {
            ctx.register_udf(udf);
        }
        ctx
    }

    /// Create a context from existing context state
    pub(crate) fn from(state: Arc<Mutex<ExecutionContextState>>) -> Self {
        Self { state }
    }

    /// Get the configuration of this execution context
    pub fn config(&self) -> ExecutionConfig {
        self.state
            .lock()
            .expect("failed to lock mutex")
            .config
            .clone()
    }

    /// Execute a SQL query and produce a Relation (a schema-aware iterator over a series
    /// of RecordBatch instances)
    pub fn sql(&mut self, sql: &str) -> Result<Vec<RecordBatch>> {
        let plan = self.create_logical_plan(sql)?;
        return self.collect_plan(&plan);
    }

    /// Executes a logical plan and produce a Relation (a schema-aware iterator over a series
    /// of RecordBatch instances). This function is intended for internal use and should not be
    /// called directly.
    pub fn collect_plan(&mut self, plan: &LogicalPlan) -> Result<Vec<RecordBatch>> {
        match plan {
            LogicalPlan::CreateExternalTable {
                ref schema,
                ref name,
                ref location,
                ref file_type,
                ref has_header,
            } => match file_type {
                FileType::CSV => {
                    self.register_csv(
                        name,
                        location,
                        CsvReadOptions::new()
                            .schema(&schema)
                            .has_header(*has_header),
                    )?;
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
                let plan = self.create_physical_plan(&plan)?;
                Ok(self.collect(plan.as_ref())?)
            }
        }
    }

    /// Creates a logical plan. This function is intended for internal use and should not be
    /// called directly.
    pub fn create_logical_plan(&mut self, sql: &str) -> Result<LogicalPlan> {
        let statements = DFParser::parse_sql(sql)?;

        if statements.len() != 1 {
            return Err(ExecutionError::NotImplemented(format!(
                "The context currently only supports a single SQL statement",
            )));
        }

        // create a query planner
        let state = self.state.lock().expect("failed to lock mutex");
        let query_planner = SqlToRel::new(state.clone());
        Ok(query_planner.statement_to_plan(&statements[0])?)
    }

    /// Register a scalar UDF
    pub fn register_udf(&mut self, f: ScalarFunction) {
        let state = self.state.lock().expect("failed to lock mutex");
        state
            .scalar_functions
            .lock()
            .expect("failed to lock mutex")
            .insert(f.name.clone(), Box::new(f));
    }

    /// Get a reference to the registered scalar functions
    pub fn scalar_functions(&self) -> Arc<Mutex<HashMap<String, Box<ScalarFunction>>>> {
        self.state
            .lock()
            .expect("failed to lock mutex")
            .scalar_functions
            .clone()
    }

    /// Creates a DataFrame for reading a CSV data source.
    pub fn read_csv(
        &mut self,
        filename: &str,
        options: CsvReadOptions,
    ) -> Result<Arc<dyn DataFrame>> {
        let csv = CsvFile::try_new(filename, options)?;

        let table_scan = LogicalPlan::CsvScan {
            path: filename.to_string(),
            schema: Box::new(csv.schema().as_ref().to_owned()),
            has_header: options.has_header,
            delimiter: Some(options.delimiter),
            projection: None,
            projected_schema: Box::new(csv.schema().as_ref().to_owned()),
        };

        Ok(Arc::new(DataFrameImpl::new(
            self.state.clone(),
            &LogicalPlanBuilder::from(&table_scan).build()?,
        )))
    }

    /// Creates a DataFrame for reading a Parquet data source.
    pub fn read_parquet(&mut self, filename: &str) -> Result<Arc<dyn DataFrame>> {
        let parquet = ParquetTable::try_new(filename)?;

        let table_scan = LogicalPlan::ParquetScan {
            path: filename.to_string(),
            schema: Box::new(parquet.schema().as_ref().to_owned()),
            projection: None,
            projected_schema: Box::new(parquet.schema().as_ref().to_owned()),
        };

        Ok(Arc::new(DataFrameImpl::new(
            self.state.clone(),
            &LogicalPlanBuilder::from(&table_scan).build()?,
        )))
    }

    /// Register a CSV data source so that it can be referenced from SQL statements
    /// executed against this context.
    pub fn register_csv(
        &mut self,
        name: &str,
        filename: &str,
        options: CsvReadOptions,
    ) -> Result<()> {
        self.register_table(name, Box::new(CsvFile::try_new(filename, options)?));
        Ok(())
    }

    /// Register a Parquet data source so that it can be referenced from SQL statements
    /// executed against this context.
    pub fn register_parquet(&mut self, name: &str, filename: &str) -> Result<()> {
        let table = ParquetTable::try_new(&filename)?;
        self.register_table(name, Box::new(table));
        Ok(())
    }

    /// Register a table using a custom TableProvider so that it can be referenced from SQL
    /// statements executed against this context.
    pub fn register_table(
        &mut self,
        name: &str,
        provider: Box<dyn TableProvider + Send + Sync>,
    ) {
        let state = self.state.lock().expect("failed to lock mutex");
        state
            .datasources
            .lock()
            .expect("failed to lock mutex")
            .insert(name.to_string(), provider);
    }

    /// Retrieves a DataFrame representing a table previously registered by calling the
    /// register_table function. An Err result will be returned if no table has been
    /// registered with the provided name.
    pub fn table(&mut self, table_name: &str) -> Result<Arc<dyn DataFrame>> {
        match self
            .state
            .lock()
            .expect("failed to lock mutex")
            .datasources
            .lock()
            .expect("failed to lock mutex")
            .get(table_name)
        {
            Some(provider) => {
                let schema = provider.schema().as_ref().clone();
                let table_scan = LogicalPlan::TableScan {
                    schema_name: "".to_string(),
                    table_name: table_name.to_string(),
                    table_schema: Box::new(schema.to_owned()),
                    projected_schema: Box::new(schema),
                    projection: None,
                };
                Ok(Arc::new(DataFrameImpl::new(
                    self.state.clone(),
                    &LogicalPlanBuilder::from(&table_scan).build()?,
                )))
            }
            _ => Err(ExecutionError::General(format!(
                "No table named '{}'",
                table_name
            ))),
        }
    }

    /// The set of available tables. Use `table` to get a specific table.
    pub fn tables(&self) -> HashSet<String> {
        self.state
            .lock()
            .expect("failed to lock mutex")
            .datasources
            .lock()
            .expect("failed to lock mutex")
            .keys()
            .cloned()
            .collect()
    }

    /// Optimize the logical plan by applying optimizer rules
    pub fn optimize(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        let rules: Vec<Box<dyn OptimizerRule>> = vec![
            Box::new(ProjectionPushDown::new()),
            Box::new(TypeCoercionRule::new(
                self.state
                    .lock()
                    .expect("failed to lock mutex")
                    .scalar_functions
                    .clone(),
            )),
        ];
        let mut plan = plan.clone();

        for mut rule in rules {
            plan = rule.optimize(&plan)?;
        }
        Ok(plan)
    }

    /// Create a physical plan from a logical plan
    pub fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let planner: Arc<dyn PhysicalPlanner> = match self.config().physical_planner {
            Some(planner) => planner,
            None => Arc::new(PhysicalPlannerImpl::default()),
        };
        planner.create_physical_plan(logical_plan, self.state.clone())
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
                let plan = MergeExec::new(
                    plan.schema().clone(),
                    partitions,
                    self.state
                        .lock()
                        .expect("failed to lock mutex")
                        .config
                        .concurrency,
                );
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
                    let reader = p.execute()?;
                    let mut reader = reader.lock().unwrap();
                    loop {
                        match reader.next_batch() {
                            Ok(Some(batch)) => {
                                writer.write(&batch)?;
                            }
                            Ok(None) => break,
                            Err(e) => return Err(ExecutionError::from(e)),
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

/// Configuration options for execution context
#[derive(Clone)]
pub struct ExecutionConfig {
    /// Number of concurrent threads for query execution.
    pub concurrency: usize,
    /// Default batch size when reading data sources
    pub batch_size: usize,
    /// Optional physical planner to override the default physical planner
    physical_planner: Option<Arc<dyn PhysicalPlanner>>,
}

impl ExecutionConfig {
    /// Create an execution config with default settings
    pub fn new() -> Self {
        Self {
            concurrency: num_cpus::get(),
            batch_size: 4096,
            physical_planner: None,
        }
    }

    /// Customize max_concurrency
    pub fn with_concurrency(mut self, n: usize) -> Self {
        // concurrency must be greater than zero
        assert!(n > 0);
        self.concurrency = n;
        self
    }

    /// Customize batch size
    pub fn with_batch_size(mut self, n: usize) -> Self {
        // batch size must be greater than zero
        assert!(n > 0);
        self.batch_size = n;
        self
    }

    /// Optional physical planner to override the default physical planner
    pub fn with_physical_planner(
        mut self,
        physical_planner: Arc<dyn PhysicalPlanner>,
    ) -> Self {
        self.physical_planner = Some(physical_planner);
        self
    }
}

/// Execution context for registering data sources and executing queries
#[derive(Clone)]
pub struct ExecutionContextState {
    /// Data sources that are registered with the context
    pub datasources: Arc<Mutex<HashMap<String, Box<dyn TableProvider + Send + Sync>>>>,
    /// Scalar functions that are registered with the context
    pub scalar_functions: Arc<Mutex<HashMap<String, Box<ScalarFunction>>>>,
    /// Context configuration
    pub config: ExecutionConfig,
}

impl SchemaProvider for ExecutionContextState {
    fn get_table_meta(&self, name: &str) -> Option<SchemaRef> {
        self.datasources
            .lock()
            .expect("failed to lock mutex")
            .get(name)
            .map(|ds| ds.schema().clone())
    }

    fn get_function_meta(&self, name: &str) -> Option<Arc<FunctionMeta>> {
        self.scalar_functions
            .lock()
            .expect("failed to lock mutex")
            .get(name)
            .map(|f| {
                Arc::new(FunctionMeta::new(
                    name.to_owned(),
                    f.args.clone(),
                    f.return_type.clone(),
                    FunctionType::Scalar,
                ))
            })
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::datasource::MemTable;
    use crate::execution::physical_plan::udf::ScalarUdf;
    use crate::logicalplan::{aggregate_expr, col, scalar_function};
    use crate::test;
    use arrow::array::{ArrayRef, Int32Array};
    use arrow::compute::add;
    use std::fs::File;
    use std::io::prelude::*;
    use tempdir::TempDir;
    use test::*;

    #[test]
    fn parallel_projection() -> Result<()> {
        let partition_count = 4;
        let results = execute("SELECT c1, c2 FROM test", partition_count)?;

        // there should be one batch per partition
        assert_eq!(results.len(), partition_count);

        // each batch should contain 2 columns and 10 rows with correct field names
        for batch in &results {
            assert_eq!(batch.num_columns(), 2);
            assert_eq!(batch.num_rows(), 10);

            assert_eq!(field_names(batch), vec!["c1", "c2"]);
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

        let physical_plan = ctx.create_physical_plan(&logical_plan)?;

        let results = ctx.collect(physical_plan.as_ref())?;

        // there should be one batch per partition
        assert_eq!(results.len(), partition_count);

        let row_count: usize = results.iter().map(|batch| batch.num_rows()).sum();
        assert_eq!(row_count, 20);

        Ok(())
    }

    #[test]
    fn projection_on_table_scan() -> Result<()> {
        let tmp_dir = TempDir::new("projection_on_table_scan")?;
        let partition_count = 4;
        let mut ctx = create_ctx(&tmp_dir, partition_count)?;

        let table = ctx.table("test")?;
        let logical_plan = LogicalPlanBuilder::from(&table.to_logical_plan())
            .project(vec![col("c2")])?
            .build()?;

        let optimized_plan = ctx.optimize(&logical_plan)?;
        match &optimized_plan {
            LogicalPlan::Projection { input, .. } => match &**input {
                LogicalPlan::TableScan {
                    table_schema,
                    projected_schema,
                    ..
                } => {
                    assert_eq!(table_schema.fields().len(), 2);
                    assert_eq!(projected_schema.fields().len(), 1);
                }
                _ => assert!(false, "input to projection should be TableScan"),
            },
            _ => assert!(false, "expect optimized_plan to be projection"),
        }

        let expected = "Projection: #c2\
        \n  TableScan: test projection=Some([1])";
        assert_eq!(format!("{:?}", optimized_plan), expected);

        let physical_plan = ctx.create_physical_plan(&optimized_plan)?;

        assert_eq!(1, physical_plan.schema().fields().len());
        assert_eq!("c2", physical_plan.schema().field(0).name().as_str());

        let batches = ctx.collect(physical_plan.as_ref())?;
        assert_eq!(4, batches.len());
        assert_eq!(1, batches[0].num_columns());
        assert_eq!(10, batches[0].num_rows());

        Ok(())
    }

    #[test]
    fn preserve_nullability_on_projection() -> Result<()> {
        let tmp_dir = TempDir::new("execute")?;
        let ctx = create_ctx(&tmp_dir, 1)?;

        let schema = ctx
            .state
            .lock()
            .expect("failed to lock mutex")
            .datasources
            .lock()
            .expect("failed to lock mutex")
            .get("test")
            .unwrap()
            .schema();
        assert_eq!(schema.field_with_name("c1")?.is_nullable(), false);

        let plan = LogicalPlanBuilder::scan("default", "test", schema.as_ref(), None)?
            .project(vec![col("c1")])?
            .build()?;

        let plan = ctx.optimize(&plan)?;
        let physical_plan = ctx.create_physical_plan(&Arc::new(plan))?;
        assert_eq!(
            physical_plan.schema().field_with_name("c1")?.is_nullable(),
            false
        );
        Ok(())
    }

    #[test]
    fn projection_on_memory_scan() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]);
        let plan = LogicalPlanBuilder::from(&LogicalPlan::InMemoryScan {
            data: vec![vec![RecordBatch::try_new(
                Arc::new(schema.clone()),
                vec![
                    Arc::new(Int32Array::from(vec![1, 10, 10, 100])),
                    Arc::new(Int32Array::from(vec![2, 12, 12, 120])),
                    Arc::new(Int32Array::from(vec![3, 12, 12, 120])),
                ],
            )?]],
            schema: Box::new(schema.clone()),
            projection: None,
            projected_schema: Box::new(schema.clone()),
        })
        .project(vec![col("b")])?
        .build()?;
        assert_fields_eq(&plan, vec!["b"]);

        let ctx = ExecutionContext::new();
        let optimized_plan = ctx.optimize(&plan)?;
        match &optimized_plan {
            LogicalPlan::Projection { input, .. } => match &**input {
                LogicalPlan::InMemoryScan {
                    schema,
                    projected_schema,
                    ..
                } => {
                    assert_eq!(schema.fields().len(), 3);
                    assert_eq!(projected_schema.fields().len(), 1);
                }
                _ => assert!(false, "input to projection should be InMemoryScan"),
            },
            _ => assert!(false, "expect optimized_plan to be projection"),
        }

        let expected = "Projection: #b\
        \n  InMemoryScan: projection=Some([1])";
        assert_eq!(format!("{:?}", optimized_plan), expected);

        let physical_plan = ctx.create_physical_plan(&optimized_plan)?;

        assert_eq!(1, physical_plan.schema().fields().len());
        assert_eq!("b", physical_plan.schema().field(0).name().as_str());

        let batches = ctx.collect(physical_plan.as_ref())?;
        assert_eq!(1, batches.len());
        assert_eq!(1, batches[0].num_columns());
        assert_eq!(4, batches[0].num_rows());

        Ok(())
    }

    #[test]
    fn sort() -> Result<()> {
        let results = execute("SELECT c1, c2 FROM test ORDER BY c1 DESC, c2 ASC", 4)?;
        assert_eq!(results.len(), 1);

        let batch = &results[0];
        let expected: Vec<&str> = vec![
            "3,1", "3,2", "3,3", "3,4", "3,5", "3,6", "3,7", "3,8", "3,9", "3,10", "2,1",
            "2,2", "2,3", "2,4", "2,5", "2,6", "2,7", "2,8", "2,9", "2,10", "1,1", "1,2",
            "1,3", "1,4", "1,5", "1,6", "1,7", "1,8", "1,9", "1,10", "0,1", "0,2", "0,3",
            "0,4", "0,5", "0,6", "0,7", "0,8", "0,9", "0,10",
        ];
        assert_eq!(test::format_batch(batch), expected);

        Ok(())
    }

    #[test]
    fn aggregate() -> Result<()> {
        let results = execute("SELECT SUM(c1), SUM(c2) FROM test", 4)?;
        assert_eq!(results.len(), 1);

        let batch = &results[0];

        assert_eq!(field_names(batch), vec!["SUM(c1)", "SUM(c2)"]);

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

        assert_eq!(field_names(batch), vec!["AVG(c1)", "AVG(c2)"]);

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

        assert_eq!(field_names(batch), vec!["MAX(c1)", "MAX(c2)"]);

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

        assert_eq!(field_names(batch), vec!["MIN(c1)", "MIN(c2)"]);

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

        assert_eq!(field_names(batch), vec!["c1", "SUM(c2)"]);

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

        assert_eq!(field_names(batch), vec!["c1", "AVG(c2)"]);

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

        assert_eq!(field_names(batch), vec!["c1", "MAX(c2)"]);

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

        assert_eq!(field_names(batch), vec!["c1", "MIN(c2)"]);

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

        assert_eq!(field_names(batch), vec!["COUNT(c1)", "COUNT(c2)"]);

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

        assert_eq!(field_names(batch), vec!["COUNT(c1)", "COUNT(c2)"]);

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

        assert_eq!(field_names(batch), vec!["c1", "COUNT(c2)"]);

        let expected = vec!["0,10", "1,10", "2,10", "3,10"];
        let mut rows = test::format_batch(&batch);
        rows.sort();
        assert_eq!(rows, expected);
        Ok(())
    }

    #[test]
    fn aggregate_with_alias() -> Result<()> {
        let tmp_dir = TempDir::new("execute")?;
        let ctx = create_ctx(&tmp_dir, 1)?;

        let schema = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Utf8, false),
            Field::new("c2", DataType::UInt32, false),
        ]));

        let plan = LogicalPlanBuilder::scan("default", "test", schema.as_ref(), None)?
            .aggregate(
                vec![col("c1")],
                vec![aggregate_expr("SUM", col("c2"), DataType::UInt32)],
            )?
            .project(vec![col("c1"), col("SUM(c2)").alias("total_salary")])?
            .build()?;

        let plan = ctx.optimize(&plan)?;

        let physical_plan = ctx.create_physical_plan(&Arc::new(plan))?;
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
        let csv_read_option = CsvReadOptions::new().schema(&schema);
        ctx.register_csv("part0", &format!("{}/part-0.csv", out_dir), csv_read_option)?;
        ctx.register_csv("part1", &format!("{}/part-1.csv", out_dir), csv_read_option)?;
        ctx.register_csv("part2", &format!("{}/part-2.csv", out_dir), csv_read_option)?;
        ctx.register_csv("part3", &format!("{}/part-3.csv", out_dir), csv_read_option)?;
        ctx.register_csv("allparts", &out_dir, csv_read_option)?;

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

    #[test]
    fn query_csv_with_custom_partition_extension() -> Result<()> {
        let tmp_dir = TempDir::new("query_csv_with_custom_partition_extension")?;

        // The main stipulation of this test: use a file extension that isn't .csv.
        let file_extension = ".tst";

        let mut ctx = ExecutionContext::new();
        let schema = populate_csv_partitions(&tmp_dir, 2, file_extension)?;
        ctx.register_csv(
            "test",
            tmp_dir.path().to_str().unwrap(),
            CsvReadOptions::new()
                .schema(&schema)
                .file_extension(file_extension),
        )?;
        let results = collect(&mut ctx, "SELECT SUM(c1), SUM(c2), COUNT(*) FROM test")?;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 1);
        assert_eq!(test::format_batch(&results[0]), vec!["10,110,20"]);

        Ok(())
    }

    #[test]
    fn scalar_udf() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]);

        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(Int32Array::from(vec![1, 10, 10, 100])),
                Arc::new(Int32Array::from(vec![2, 12, 12, 120])),
            ],
        )?;

        let mut ctx = ExecutionContext::new();

        let provider = MemTable::new(Arc::new(schema), vec![vec![batch]])?;
        ctx.register_table("t", Box::new(provider));

        let myfunc: ScalarUdf = Arc::new(|args: &[ArrayRef]| {
            let l = &args[0]
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("cast failed");
            let r = &args[1]
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("cast failed");
            Ok(Arc::new(add(l, r)?))
        });

        let my_add = ScalarFunction::new(
            "my_add",
            vec![
                Field::new("a", DataType::Int32, true),
                Field::new("b", DataType::Int32, true),
            ],
            DataType::Int32,
            myfunc,
        );

        ctx.register_udf(my_add);

        let t = ctx.table("t")?;

        let plan = LogicalPlanBuilder::from(&t.to_logical_plan())
            .project(vec![
                col("a"),
                col("b"),
                scalar_function("my_add", vec![col("a"), col("b")], DataType::Int32),
            ])?
            .build()?;

        assert_eq!(
            format!("{:?}", plan),
            "Projection: #a, #b, my_add(#a, #b)\n  TableScan: t projection=None"
        );

        let plan = ctx.optimize(&plan)?;
        let plan = ctx.create_physical_plan(&plan)?;
        let result = ctx.collect(plan.as_ref())?;

        let batch = &result[0];
        assert_eq!(3, batch.num_columns());
        assert_eq!(4, batch.num_rows());
        assert_eq!(field_names(batch), vec!["a", "b", "my_add(a,b)"]);

        let a = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("failed to cast a");
        let b = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("failed to cast b");
        let sum = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("failed to cast sum");

        assert_eq!(4, a.len());
        assert_eq!(4, b.len());
        assert_eq!(4, sum.len());
        for i in 0..sum.len() {
            assert_eq!(a.value(i) + b.value(i), sum.value(i));
        }

        Ok(())
    }

    #[test]
    fn custom_physical_planner() -> Result<()> {
        let mut ctx = ExecutionContext::with_config(
            ExecutionConfig::new().with_physical_planner(Arc::new(MyPhysicalPlanner {})),
        );
        ctx.sql("SELECT 1").expect_err("query not supported");
        Ok(())
    }

    struct MyPhysicalPlanner {}

    impl PhysicalPlanner for MyPhysicalPlanner {
        fn create_physical_plan(
            &self,
            _logical_plan: &LogicalPlan,
            _ctx_state: Arc<Mutex<ExecutionContextState>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            Err(ExecutionError::NotImplemented(
                "query not supported".to_string(),
            ))
        }
    }

    /// Execute SQL and return results
    fn collect(ctx: &mut ExecutionContext, sql: &str) -> Result<Vec<RecordBatch>> {
        let logical_plan = ctx.create_logical_plan(sql)?;
        let logical_plan = ctx.optimize(&logical_plan)?;
        let physical_plan = ctx.create_physical_plan(&logical_plan)?;
        ctx.collect(physical_plan.as_ref())
    }

    fn field_names(result: &RecordBatch) -> Vec<String> {
        result
            .schema()
            .fields()
            .iter()
            .map(|x| x.name().clone())
            .collect::<Vec<String>>()
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
        let physical_plan = ctx.create_physical_plan(&logical_plan)?;
        ctx.write_csv(physical_plan.as_ref(), out_dir)
    }

    /// Generate CSV partitions within the supplied directory
    fn populate_csv_partitions(
        tmp_dir: &TempDir,
        partition_count: usize,
        file_extension: &str,
    ) -> Result<SchemaRef> {
        // define schema for data source (csv file)
        let schema = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::UInt32, false),
            Field::new("c2", DataType::UInt64, false),
        ]));

        // generate a partitioned file
        for partition in 0..partition_count {
            let filename = format!("partition-{}.{}", partition, file_extension);
            let file_path = tmp_dir.path().join(&filename);
            let mut file = File::create(file_path)?;

            // generate some data
            for i in 0..=10 {
                let data = format!("{},{}\n", partition, i);
                file.write_all(data.as_bytes())?;
            }
        }

        Ok(schema)
    }

    /// Generate a partitioned CSV file and register it with an execution context
    fn create_ctx(tmp_dir: &TempDir, partition_count: usize) -> Result<ExecutionContext> {
        let mut ctx = ExecutionContext::new();

        let schema = populate_csv_partitions(tmp_dir, partition_count, ".csv")?;

        // register csv file with the execution context
        ctx.register_csv(
            "test",
            tmp_dir.path().to_str().unwrap(),
            CsvReadOptions::new().schema(&schema),
        )?;

        Ok(ctx)
    }
}
