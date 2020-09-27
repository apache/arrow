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
use std::sync::Arc;

use arrow::csv;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;

use crate::datasource::csv::CsvFile;
use crate::datasource::parquet::ParquetTable;
use crate::datasource::TableProvider;
use crate::error::{ExecutionError, Result};
use crate::execution::dataframe_impl::DataFrameImpl;
use crate::logical_plan::{FunctionRegistry, LogicalPlan, LogicalPlanBuilder};
use crate::optimizer::filter_push_down::FilterPushDown;
use crate::optimizer::optimizer::OptimizerRule;
use crate::optimizer::projection_push_down::ProjectionPushDown;
use crate::physical_plan::common;
use crate::physical_plan::csv::CsvReadOptions;
use crate::physical_plan::merge::MergeExec;
use crate::physical_plan::planner::DefaultPhysicalPlanner;
use crate::physical_plan::udf::ScalarUDF;
use crate::physical_plan::ExecutionPlan;
use crate::physical_plan::PhysicalPlanner;
use crate::sql::{
    parser::{DFParser, FileType},
    planner::{SchemaProvider, SqlToRel},
};
use crate::variable::{VarProvider, VarType};
use crate::{dataframe::DataFrame, physical_plan::udaf::AggregateUDF};

/// ExecutionContext is the main interface for executing queries with DataFusion. The context
/// provides the following functionality:
///
/// * Create DataFrame from a CSV or Parquet data source.
/// * Register a CSV or Parquet data source as a table that can be referenced from a SQL query.
/// * Register a custom data source that can be referenced from a SQL query.
/// * Execution a SQL query
///
/// The following example demonstrates how to use the context to execute a query against a CSV
/// data source using the DataFrame API:
///
/// ```
/// use datafusion::prelude::*;
/// # use datafusion::error::Result;
/// # fn main() -> Result<()> {
/// let mut ctx = ExecutionContext::new();
/// let df = ctx.read_csv("tests/example.csv", CsvReadOptions::new())?;
/// let df = df.filter(col("a").lt_eq(col("b")))?
///            .aggregate(vec![col("a")], vec![min(col("b"))])?
///            .limit(100)?;
/// let results = df.collect();
/// # Ok(())
/// # }
/// ```
///
/// The following example demonstrates how to execute the same query using SQL:
///
/// ```
/// use datafusion::prelude::*;
///
/// # use datafusion::error::Result;
/// # fn main() -> Result<()> {
/// let mut ctx = ExecutionContext::new();
/// ctx.register_csv("example", "tests/example.csv", CsvReadOptions::new())?;
/// let results = ctx.sql("SELECT a, MIN(b) FROM example GROUP BY a LIMIT 100")?;
/// # Ok(())
/// # }
/// ```
pub struct ExecutionContext {
    /// Internal state for the context
    pub state: ExecutionContextState,
}

impl ExecutionContext {
    /// Create a new execution context using a default configuration.
    pub fn new() -> Self {
        Self::with_config(ExecutionConfig::new())
    }

    /// Create a new execution context using the provided configuration
    pub fn with_config(config: ExecutionConfig) -> Self {
        let ctx = Self {
            state: ExecutionContextState {
                datasources: HashMap::new(),
                scalar_functions: HashMap::new(),
                var_provider: HashMap::new(),
                aggregate_functions: HashMap::new(),
                config,
            },
        };
        ctx
    }

    /// Create a context from existing context state
    pub(crate) fn from(state: ExecutionContextState) -> Self {
        Self { state }
    }

    /// Get the configuration of this execution context
    pub fn config(&self) -> &ExecutionConfig {
        &self.state.config
    }

    /// Execute a SQL query and produce a Relation (a schema-aware iterator over a series
    /// of RecordBatch instances)
    pub fn sql(&mut self, sql: &str) -> Result<Arc<dyn DataFrame>> {
        let plan = self.create_logical_plan(sql)?;
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
                    let plan = LogicalPlanBuilder::empty().build()?;
                    Ok(Arc::new(DataFrameImpl::new(self.state.clone(), &plan)))
                }
                FileType::Parquet => {
                    self.register_parquet(name, location)?;
                    let plan = LogicalPlanBuilder::empty().build()?;
                    Ok(Arc::new(DataFrameImpl::new(self.state.clone(), &plan)))
                }
                _ => Err(ExecutionError::ExecutionError(format!(
                    "Unsupported file type {:?}.",
                    file_type
                ))),
            },

            plan => Ok(Arc::new(DataFrameImpl::new(self.state.clone(), &plan))),
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
        let query_planner = SqlToRel::new(&self.state);
        Ok(query_planner.statement_to_plan(&statements[0])?)
    }

    /// Register variable
    pub fn register_variable(
        &mut self,
        variable_type: VarType,
        provider: Arc<dyn VarProvider + Send + Sync>,
    ) {
        self.state.var_provider.insert(variable_type, provider);
    }

    /// Register a scalar UDF
    pub fn register_udf(&mut self, f: ScalarUDF) {
        self.state
            .scalar_functions
            .insert(f.name.clone(), Arc::new(f));
    }

    /// Register a aggregate UDF
    pub fn register_udaf(&mut self, f: AggregateUDF) {
        self.state
            .aggregate_functions
            .insert(f.name.clone(), Arc::new(f));
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
            schema: csv.schema().clone(),
            has_header: options.has_header,
            delimiter: Some(options.delimiter),
            projection: None,
            projected_schema: csv.schema().clone(),
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
            schema: parquet.schema().clone(),
            projection: None,
            projected_schema: parquet.schema().clone(),
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
        self.state
            .datasources
            .insert(name.to_string(), provider.into());
    }

    /// Retrieves a DataFrame representing a table previously registered by calling the
    /// register_table function. An Err result will be returned if no table has been
    /// registered with the provided name.
    pub fn table(&mut self, table_name: &str) -> Result<Arc<dyn DataFrame>> {
        match self.state.datasources.get(table_name) {
            Some(provider) => {
                let schema = provider.schema().clone();
                let table_scan = LogicalPlan::TableScan {
                    schema_name: "".to_string(),
                    table_name: table_name.to_string(),
                    table_schema: schema.clone(),
                    projected_schema: schema,
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
        self.state.datasources.keys().cloned().collect()
    }

    /// Optimize the logical plan by applying optimizer rules
    pub fn optimize(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        // Apply standard rewrites and optimizations
        let mut plan = ProjectionPushDown::new().optimize(&plan)?;
        plan = FilterPushDown::new().optimize(&plan)?;

        self.state.config.query_planner.rewrite_logical_plan(plan)
    }

    /// Create a physical plan from a logical plan
    pub fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.state
            .config
            .query_planner
            .create_physical_plan(logical_plan, &self.state)
    }

    /// Execute a physical plan and collect the results in memory
    pub async fn collect(
        &self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Vec<RecordBatch>> {
        match plan.output_partitioning().partition_count() {
            0 => Ok(vec![]),
            1 => {
                let it = plan.execute(0).await?;
                common::collect(it)
            }
            _ => {
                // merge into a single partition
                let plan = MergeExec::new(plan.clone(), self.state.config.concurrency);
                // MergeExec must produce a single partition
                assert_eq!(1, plan.output_partitioning().partition_count());
                common::collect(plan.execute(0).await?)
            }
        }
    }

    /// Execute a query and write the results to a partitioned CSV file
    pub async fn write_csv(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        path: String,
    ) -> Result<()> {
        // create directory to contain the CSV files (one per partition)
        let path = path.to_owned();
        fs::create_dir(&path)?;

        for i in 0..plan.output_partitioning().partition_count() {
            let path = path.clone();
            let plan = plan.clone();
            let filename = format!("part-{}.csv", i);
            let path = Path::new(&path).join(&filename);
            let file = fs::File::create(path).unwrap();
            let mut writer = csv::Writer::new(file);
            let reader = plan.execute(i).await.unwrap();
            let mut reader = reader.lock().unwrap();
            loop {
                match reader.next_batch() {
                    Ok(Some(batch)) => writer.write(&batch).unwrap(),
                    Ok(None) => break,
                    Err(e) => return Err(ExecutionError::from(e)),
                }
            }
        }

        Ok(())
    }

    /// get the registry, that allows to construct logical expressions of UDFs
    pub fn registry(&self) -> &dyn FunctionRegistry {
        &self.state
    }
}

/// A planner used to add extensions to DataFusion logical and phusical plans.
pub trait QueryPlanner {
    /// Given a `LogicalPlan`, create a new, modified `LogicalPlan`
    /// plan. This method is run after built in `OptimizerRule`s. By
    /// default returns the `plan` unmodified.
    fn rewrite_logical_plan(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        Ok(plan)
    }

    /// Given a `LogicalPlan`, create an `ExecutionPlan` suitable for execution
    fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        ctx_state: &ExecutionContextState,
    ) -> Result<Arc<dyn ExecutionPlan>>;
}

/// The query planner used if no user defined planner is provided
struct DefaultQueryPlanner {}

impl QueryPlanner for DefaultQueryPlanner {
    /// Given a `LogicalPlan`, create an `ExecutionPlan` suitable for execution
    fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        ctx_state: &ExecutionContextState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let planner = DefaultPhysicalPlanner::default();
        planner.create_physical_plan(logical_plan, ctx_state)
    }
}

/// Configuration options for execution context
#[derive(Clone)]
pub struct ExecutionConfig {
    /// Number of concurrent threads for query execution.
    pub concurrency: usize,
    /// Default batch size when reading data sources
    pub batch_size: usize,
    /// Responsible for planning `LogicalPlan`s, and `ExecutionPlan`
    query_planner: Arc<dyn QueryPlanner + Send + Sync>,
}

impl ExecutionConfig {
    /// Create an execution config with default setting
    pub fn new() -> Self {
        Self {
            concurrency: num_cpus::get(),
            batch_size: 4096,
            query_planner: Arc::new(DefaultQueryPlanner {}),
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

    /// Replace the default query planner
    pub fn with_query_planner(
        mut self,
        query_planner: Arc<dyn QueryPlanner + Send + Sync>,
    ) -> Self {
        self.query_planner = query_planner;
        self
    }
}

/// Execution context for registering data sources and executing queries
#[derive(Clone)]
pub struct ExecutionContextState {
    /// Data sources that are registered with the context
    pub datasources: HashMap<String, Arc<dyn TableProvider + Send + Sync>>,
    /// Scalar functions that are registered with the context
    pub scalar_functions: HashMap<String, Arc<ScalarUDF>>,
    /// Variable provider that are registered with the context
    pub var_provider: HashMap<VarType, Arc<dyn VarProvider + Send + Sync>>,
    /// Aggregate functions registered in the context
    pub aggregate_functions: HashMap<String, Arc<AggregateUDF>>,
    /// Context configuration
    pub config: ExecutionConfig,
}

impl SchemaProvider for ExecutionContextState {
    fn get_table_meta(&self, name: &str) -> Option<SchemaRef> {
        self.datasources.get(name).map(|ds| ds.schema().clone())
    }

    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        self.scalar_functions
            .get(name)
            .and_then(|func| Some(func.clone()))
    }

    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        self.aggregate_functions
            .get(name)
            .and_then(|func| Some(func.clone()))
    }
}

impl FunctionRegistry for ExecutionContextState {
    fn udfs(&self) -> HashSet<String> {
        self.scalar_functions.keys().cloned().collect()
    }

    fn udf(&self, name: &str) -> Result<&ScalarUDF> {
        let result = self.scalar_functions.get(name);
        if result.is_none() {
            Err(ExecutionError::General(
                format!("There is no UDF named \"{}\" in the registry", name).to_string(),
            ))
        } else {
            Ok(result.unwrap())
        }
    }

    fn udaf(&self, name: &str) -> Result<&AggregateUDF> {
        let result = self.aggregate_functions.get(name);
        if result.is_none() {
            Err(ExecutionError::General(
                format!("There is no UDAF named \"{}\" in the registry", name)
                    .to_string(),
            ))
        } else {
            Ok(result.unwrap())
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::logical_plan::{col, create_udf, sum};
    use crate::physical_plan::functions::ScalarFunctionImplementation;
    use crate::test;
    use crate::variable::VarType;
    use crate::{
        datasource::MemTable, logical_plan::create_udaf,
        physical_plan::expressions::AvgAccumulator,
    };
    use arrow::array::{
        ArrayRef, Float64Array, Int32Array, PrimitiveArrayOps, StringArray,
    };
    use arrow::compute::add;
    use std::thread::{self, JoinHandle};
    use std::{cell::RefCell, fs::File, rc::Rc};
    use std::{io::prelude::*, sync::Mutex};
    use tempfile::TempDir;
    use test::*;

    #[tokio::test]
    async fn parallel_projection() -> Result<()> {
        let partition_count = 4;
        let results = execute("SELECT c1, c2 FROM test", partition_count).await?;

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

    #[tokio::test]
    async fn create_variable_expr() -> Result<()> {
        let tmp_dir = TempDir::new()?;
        let partition_count = 4;
        let mut ctx = create_ctx(&tmp_dir, partition_count)?;

        let variable_provider = test::variable::SystemVar::new();
        ctx.register_variable(VarType::System, Arc::new(variable_provider));
        let variable_provider = test::variable::UserDefinedVar::new();
        ctx.register_variable(VarType::UserDefined, Arc::new(variable_provider));

        let provider = test::create_table_dual();
        ctx.register_table("dual", provider);

        let results = collect(&mut ctx, "SELECT @@version, @name FROM dual").await?;

        let batch = &results[0];
        assert_eq!(2, batch.num_columns());
        assert_eq!(1, batch.num_rows());
        assert_eq!(field_names(batch), vec!["@@version", "@name"]);

        let version = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("failed to cast version");
        assert_eq!(version.value(0), "system-var-@@version");

        let name = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("failed to cast name");
        assert_eq!(name.value(0), "user-defined-var-@name");

        Ok(())
    }

    #[tokio::test]
    async fn parallel_query_with_filter() -> Result<()> {
        let tmp_dir = TempDir::new()?;
        let partition_count = 4;
        let mut ctx = create_ctx(&tmp_dir, partition_count)?;

        let logical_plan =
            ctx.create_logical_plan("SELECT c1, c2 FROM test WHERE c1 > 0 AND c1 < 3")?;
        let logical_plan = ctx.optimize(&logical_plan)?;

        let physical_plan = ctx.create_physical_plan(&logical_plan)?;

        let results = ctx.collect(physical_plan).await?;

        // there should be one batch per partition
        assert_eq!(results.len(), partition_count);

        let row_count: usize = results.iter().map(|batch| batch.num_rows()).sum();
        assert_eq!(row_count, 20);

        Ok(())
    }

    #[tokio::test]
    async fn projection_on_table_scan() -> Result<()> {
        let tmp_dir = TempDir::new()?;
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

        let batches = ctx.collect(physical_plan).await?;
        assert_eq!(4, batches.len());
        assert_eq!(1, batches[0].num_columns());
        assert_eq!(10, batches[0].num_rows());

        Ok(())
    }

    #[test]
    fn preserve_nullability_on_projection() -> Result<()> {
        let tmp_dir = TempDir::new()?;
        let ctx = create_ctx(&tmp_dir, 1)?;

        let schema = ctx.state.datasources.get("test").unwrap().schema();
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

    #[tokio::test]
    async fn projection_on_memory_scan() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]);
        let schema = SchemaRef::new(schema);
        let plan = LogicalPlanBuilder::from(&LogicalPlan::InMemoryScan {
            data: vec![vec![RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![1, 10, 10, 100])),
                    Arc::new(Int32Array::from(vec![2, 12, 12, 120])),
                    Arc::new(Int32Array::from(vec![3, 12, 12, 120])),
                ],
            )?]],
            schema: schema.clone(),
            projection: None,
            projected_schema: schema.clone(),
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

        let batches = ctx.collect(physical_plan).await?;
        assert_eq!(1, batches.len());
        assert_eq!(1, batches[0].num_columns());
        assert_eq!(4, batches[0].num_rows());

        Ok(())
    }

    #[tokio::test]
    async fn sort() -> Result<()> {
        let results =
            execute("SELECT c1, c2 FROM test ORDER BY c1 DESC, c2 ASC", 4).await?;
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

    #[tokio::test]
    async fn aggregate() -> Result<()> {
        let results = execute("SELECT SUM(c1), SUM(c2) FROM test", 4).await?;
        assert_eq!(results.len(), 1);

        let batch = &results[0];

        assert_eq!(field_names(batch), vec!["SUM(c1)", "SUM(c2)"]);

        let expected: Vec<&str> = vec!["60,220"];
        let mut rows = test::format_batch(&batch);
        rows.sort();
        assert_eq!(rows, expected);

        Ok(())
    }

    #[tokio::test]
    async fn aggregate_avg() -> Result<()> {
        let results = execute("SELECT AVG(c1), AVG(c2) FROM test", 4).await?;
        assert_eq!(results.len(), 1);

        let batch = &results[0];

        assert_eq!(field_names(batch), vec!["AVG(c1)", "AVG(c2)"]);

        let expected: Vec<&str> = vec!["1.5,5.5"];
        let mut rows = test::format_batch(&batch);
        rows.sort();
        assert_eq!(rows, expected);

        Ok(())
    }

    #[tokio::test]
    async fn aggregate_max() -> Result<()> {
        let results = execute("SELECT MAX(c1), MAX(c2) FROM test", 4).await?;
        assert_eq!(results.len(), 1);

        let batch = &results[0];

        assert_eq!(field_names(batch), vec!["MAX(c1)", "MAX(c2)"]);

        let expected: Vec<&str> = vec!["3,10"];
        let mut rows = test::format_batch(&batch);
        rows.sort();
        assert_eq!(rows, expected);

        Ok(())
    }

    #[tokio::test]
    async fn aggregate_min() -> Result<()> {
        let results = execute("SELECT MIN(c1), MIN(c2) FROM test", 4).await?;
        assert_eq!(results.len(), 1);

        let batch = &results[0];

        assert_eq!(field_names(batch), vec!["MIN(c1)", "MIN(c2)"]);

        let expected: Vec<&str> = vec!["0,1"];
        let mut rows = test::format_batch(&batch);
        rows.sort();
        assert_eq!(rows, expected);

        Ok(())
    }

    #[tokio::test]
    async fn aggregate_grouped() -> Result<()> {
        let results = execute("SELECT c1, SUM(c2) FROM test GROUP BY c1", 4).await?;
        assert_eq!(results.len(), 1);

        let batch = &results[0];

        assert_eq!(field_names(batch), vec!["c1", "SUM(c2)"]);

        let expected: Vec<&str> = vec!["0,55", "1,55", "2,55", "3,55"];
        let mut rows = test::format_batch(&batch);
        rows.sort();
        assert_eq!(rows, expected);

        Ok(())
    }

    #[tokio::test]
    async fn aggregate_grouped_avg() -> Result<()> {
        let results = execute("SELECT c1, AVG(c2) FROM test GROUP BY c1", 4).await?;
        assert_eq!(results.len(), 1);

        let batch = &results[0];

        assert_eq!(field_names(batch), vec!["c1", "AVG(c2)"]);

        let expected: Vec<&str> = vec!["0,5.5", "1,5.5", "2,5.5", "3,5.5"];
        let mut rows = test::format_batch(&batch);
        rows.sort();
        assert_eq!(rows, expected);

        Ok(())
    }

    #[tokio::test]
    async fn aggregate_grouped_empty() -> Result<()> {
        let results =
            execute("SELECT c1, AVG(c2) FROM test WHERE c1 = 123 GROUP BY c1", 4).await?;
        assert_eq!(results.len(), 1);

        let batch = &results[0];

        assert_eq!(field_names(batch), vec!["c1", "AVG(c2)"]);

        let expected: Vec<&str> = vec![];
        let mut rows = test::format_batch(&batch);
        rows.sort();
        assert_eq!(rows, expected);

        Ok(())
    }

    #[tokio::test]
    async fn aggregate_grouped_max() -> Result<()> {
        let results = execute("SELECT c1, MAX(c2) FROM test GROUP BY c1", 4).await?;
        assert_eq!(results.len(), 1);

        let batch = &results[0];

        assert_eq!(field_names(batch), vec!["c1", "MAX(c2)"]);

        let expected: Vec<&str> = vec!["0,10", "1,10", "2,10", "3,10"];
        let mut rows = test::format_batch(&batch);
        rows.sort();
        assert_eq!(rows, expected);

        Ok(())
    }

    #[tokio::test]
    async fn aggregate_grouped_min() -> Result<()> {
        let results = execute("SELECT c1, MIN(c2) FROM test GROUP BY c1", 4).await?;
        assert_eq!(results.len(), 1);

        let batch = &results[0];

        assert_eq!(field_names(batch), vec!["c1", "MIN(c2)"]);

        let expected: Vec<&str> = vec!["0,1", "1,1", "2,1", "3,1"];
        let mut rows = test::format_batch(&batch);
        rows.sort();
        assert_eq!(rows, expected);

        Ok(())
    }

    #[tokio::test]
    async fn count_basic() -> Result<()> {
        let results = execute("SELECT COUNT(c1), COUNT(c2) FROM test", 1).await?;
        assert_eq!(results.len(), 1);

        let batch = &results[0];

        assert_eq!(field_names(batch), vec!["COUNT(c1)", "COUNT(c2)"]);

        let expected: Vec<&str> = vec!["10,10"];
        let mut rows = test::format_batch(&batch);
        rows.sort();
        assert_eq!(rows, expected);
        Ok(())
    }

    #[tokio::test]
    async fn count_partitioned() -> Result<()> {
        let results = execute("SELECT COUNT(c1), COUNT(c2) FROM test", 4).await?;
        assert_eq!(results.len(), 1);

        let batch = &results[0];

        assert_eq!(field_names(batch), vec!["COUNT(c1)", "COUNT(c2)"]);

        let expected: Vec<&str> = vec!["40,40"];
        let mut rows = test::format_batch(&batch);
        rows.sort();
        assert_eq!(rows, expected);
        Ok(())
    }

    #[tokio::test]
    async fn count_aggregated() -> Result<()> {
        let results = execute("SELECT c1, COUNT(c2) FROM test GROUP BY c1", 4).await?;
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
        let tmp_dir = TempDir::new()?;
        let ctx = create_ctx(&tmp_dir, 1)?;

        let schema = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Utf8, false),
            Field::new("c2", DataType::UInt32, false),
        ]));

        let plan = LogicalPlanBuilder::scan("default", "test", schema.as_ref(), None)?
            .aggregate(vec![col("c1")], vec![sum(col("c2"))])?
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

    #[tokio::test]
    async fn write_csv_results() -> Result<()> {
        // create partitioned input file and context
        let tmp_dir = TempDir::new()?;
        let mut ctx = create_ctx(&tmp_dir, 4)?;

        // execute a simple query and write the results to CSV
        let out_dir = tmp_dir.as_ref().to_str().unwrap().to_string() + "/out";
        write_csv(&mut ctx, "SELECT c1, c2 FROM test", &out_dir).await?;

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

        let part0 = collect(&mut ctx, "SELECT c1, c2 FROM part0").await?;
        let part1 = collect(&mut ctx, "SELECT c1, c2 FROM part1").await?;
        let part2 = collect(&mut ctx, "SELECT c1, c2 FROM part2").await?;
        let part3 = collect(&mut ctx, "SELECT c1, c2 FROM part3").await?;
        let allparts = collect(&mut ctx, "SELECT c1, c2 FROM allparts").await?;

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

    #[tokio::test]
    async fn query_csv_with_custom_partition_extension() -> Result<()> {
        let tmp_dir = TempDir::new()?;

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
        let results =
            collect(&mut ctx, "SELECT SUM(c1), SUM(c2), COUNT(*) FROM test").await?;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 1);
        assert_eq!(test::format_batch(&results[0]), vec!["10,110,20"]);

        Ok(())
    }

    #[test]
    fn send_context_to_threads() -> Result<()> {
        // ensure ExecutionContexts can be used in a multi-threaded
        // environment. Usecase is for concurrent planing.
        let tmp_dir = TempDir::new()?;
        let partition_count = 4;
        let ctx = Arc::new(Mutex::new(create_ctx(&tmp_dir, partition_count)?));

        let threads: Vec<JoinHandle<Result<_>>> = (0..2)
            .map(|_| ctx.clone())
            .map(|ctx_clone| {
                thread::spawn(move || {
                    let mut ctx = ctx_clone.lock().expect("Locked context");
                    // Ensure we can create logical plan code on a separate thread.
                    ctx.create_logical_plan(
                        "SELECT c1, c2 FROM test WHERE c1 > 0 AND c1 < 3",
                    )
                })
            })
            .collect();

        for thread in threads {
            thread.join().expect("Failed to join thread")?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn scalar_udf() -> Result<()> {
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

        let myfunc: ScalarFunctionImplementation = Arc::new(|args: &[ArrayRef]| {
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

        ctx.register_udf(create_udf(
            "my_add",
            vec![DataType::Int32, DataType::Int32],
            Arc::new(DataType::Int32),
            myfunc,
        ));

        // from here on, we may be in a different scope. We would still like to be able
        // to call UDFs.

        let t = ctx.table("t")?;

        let plan = LogicalPlanBuilder::from(&t.to_logical_plan())
            .project(vec![
                col("a"),
                col("b"),
                ctx.registry().udf("my_add")?.call(vec![col("a"), col("b")]),
            ])?
            .build()?;

        assert_eq!(
            format!("{:?}", plan),
            "Projection: #a, #b, my_add(#a, #b)\n  TableScan: t projection=None"
        );

        let plan = ctx.optimize(&plan)?;
        let plan = ctx.create_physical_plan(&plan)?;
        let result = ctx.collect(plan).await?;

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

    #[tokio::test]
    async fn simple_avg() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

        let batch1 = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )?;
        let batch2 = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(Int32Array::from(vec![4, 5]))],
        )?;

        let mut ctx = ExecutionContext::new();

        let provider = MemTable::new(Arc::new(schema), vec![vec![batch1], vec![batch2]])?;
        ctx.register_table("t", Box::new(provider));

        let result = collect(&mut ctx, "SELECT AVG(a) FROM t").await?;

        let batch = &result[0];
        assert_eq!(1, batch.num_columns());
        assert_eq!(1, batch.num_rows());

        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("failed to cast version");
        assert_eq!(values.len(), 1);
        // avg(1,2,3,4,5) = 3.0
        assert_eq!(values.value(0), 3.0_f64);
        Ok(())
    }

    /// tests the creation, registration and usage of a UDAF
    #[tokio::test]
    async fn simple_udaf() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

        let batch1 = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )?;
        let batch2 = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(Int32Array::from(vec![4, 5]))],
        )?;

        let mut ctx = ExecutionContext::new();

        let provider = MemTable::new(Arc::new(schema), vec![vec![batch1], vec![batch2]])?;
        ctx.register_table("t", Box::new(provider));

        // define a udaf, using a DataFusion's accumulator
        let my_avg = create_udaf(
            "MY_AVG",
            DataType::Float64,
            Arc::new(DataType::Float64),
            Arc::new(|| {
                Ok(Rc::new(RefCell::new(AvgAccumulator::try_new(
                    &DataType::Float64,
                )?)))
            }),
            Arc::new(vec![DataType::UInt64, DataType::Float64]),
        );

        ctx.register_udaf(my_avg);

        let result = collect(&mut ctx, "SELECT MY_AVG(a) FROM t").await?;

        let batch = &result[0];
        assert_eq!(1, batch.num_columns());
        assert_eq!(1, batch.num_rows());

        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("failed to cast version");
        assert_eq!(values.len(), 1);
        // avg(1,2,3,4,5) = 3.0
        assert_eq!(values.value(0), 3.0_f64);
        Ok(())
    }

    #[tokio::test]
    async fn custom_query_planner() -> Result<()> {
        let mut ctx = ExecutionContext::with_config(
            ExecutionConfig::new().with_query_planner(Arc::new(MyQueryPlanner {})),
        );

        let df = ctx.sql("SELECT 1")?;
        df.collect().await.expect_err("query not supported");
        Ok(())
    }

    struct MyPhysicalPlanner {}

    impl PhysicalPlanner for MyPhysicalPlanner {
        fn create_physical_plan(
            &self,
            _logical_plan: &LogicalPlan,
            _ctx_state: &ExecutionContextState,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            Err(ExecutionError::NotImplemented(
                "query not supported".to_string(),
            ))
        }
    }

    struct MyQueryPlanner {}

    impl QueryPlanner for MyQueryPlanner {
        fn create_physical_plan(
            &self,
            logical_plan: &LogicalPlan,
            ctx_state: &ExecutionContextState,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            let physical_planner = MyPhysicalPlanner {};
            physical_planner.create_physical_plan(logical_plan, ctx_state)
        }
    }

    /// Execute SQL and return results
    async fn collect(ctx: &mut ExecutionContext, sql: &str) -> Result<Vec<RecordBatch>> {
        let logical_plan = ctx.create_logical_plan(sql)?;
        let logical_plan = ctx.optimize(&logical_plan)?;
        let physical_plan = ctx.create_physical_plan(&logical_plan)?;
        ctx.collect(physical_plan).await
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
    async fn execute(sql: &str, partition_count: usize) -> Result<Vec<RecordBatch>> {
        let tmp_dir = TempDir::new()?;
        let mut ctx = create_ctx(&tmp_dir, partition_count)?;
        collect(&mut ctx, sql).await
    }

    /// Execute SQL and write results to partitioned csv files
    async fn write_csv(
        ctx: &mut ExecutionContext,
        sql: &str,
        out_dir: &str,
    ) -> Result<()> {
        let logical_plan = ctx.create_logical_plan(sql)?;
        let logical_plan = ctx.optimize(&logical_plan)?;
        let physical_plan = ctx.create_physical_plan(&logical_plan)?;
        ctx.write_csv(physical_plan, out_dir.to_string()).await
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
