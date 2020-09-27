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

//! This module contains an end to end demonstration of creating
//! a user defined operator in DataFusion.
//!
//! Specifically, it shows how to define a `TopKNode` that implements
//! `ExtensionPlanNode`, add an OptimizerRule to rewrite a
//! `LogicalPlan` to use that node a `LogicalPlan`, create an
//! `ExecutionPlan` and finally produce results.
//!
//! # TopK Background:
//!
//! A "Top K" node is a common query optimization which is used for
//! queries such as "find the top 3 customers by revenue". The
//! (simplified) SQL for such a query might be:
//!
//! ```sql
//! CREATE EXTERNAL TABLE sales(customer_id VARCHAR, revenue BIGINT)
//!   STORED AS CSV location 'tests/customer.csv';
//!
//! SELECT customer_id, revenue FROM sales ORDER BY revenue DESC limit 3;
//! ```
//!
//! And a naive plan would be:
//!
//! ```
//! > explain SELECT customer_id, revenue FROM sales ORDER BY revenue DESC limit 3;
//! +--------------+----------------------------------------+
//! | plan_type    | plan                                   |
//! +--------------+----------------------------------------+
//! | logical_plan | Limit: 3                               |
//! |              |   Sort: #revenue DESC NULLS FIRST      |
//! |              |     Projection: #customer_id, #revenue |
//! |              |       TableScan: sales projection=None |
//! +--------------+----------------------------------------+
//! ```
//!
//! While this plan produces the correct answer, the careful reader
//! will note it fully sorts the input before discarding everything
//! other than the top 3 elements.
//!
//! The same answer can be produced by simply keeping track of the top
//! N elements, reducing the total amount of required buffer memory.
//!

use arrow::{
    array::StringBuilder,
    array::{Int64Array, PrimitiveArrayOps, StringArray},
    datatypes::SchemaRef,
    error::ArrowError,
    record_batch::{RecordBatch, RecordBatchReader},
    util::pretty::pretty_format_batches,
};
use datafusion::{
    error::{ExecutionError, Result},
    execution::context::ExecutionContextState,
    execution::context::QueryPlanner,
    logical_plan::{Expr, LogicalPlan, UserDefinedLogicalNode},
    optimizer::{optimizer::OptimizerRule, utils::optimize_explain},
    physical_plan::{
        planner::{DefaultPhysicalPlanner, ExtensionPlanner},
        Distribution, ExecutionPlan, Partitioning, PhysicalPlanner,
    },
    prelude::{ExecutionConfig, ExecutionContext},
};
use fmt::Debug;
use std::{
    any::Any,
    collections::BTreeMap,
    fmt,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;

/// Execute the specified sql and return the resulting record batches
/// pretty printed as a String.
async fn exec_sql(ctx: &mut ExecutionContext, sql: &str) -> Result<String> {
    let df = ctx.sql(sql)?;
    let batches = df.collect().await?;
    pretty_format_batches(&batches).map_err(|e| ExecutionError::ArrowError(e))
}

/// Create a test table.
async fn setup_table(mut ctx: ExecutionContext) -> Result<ExecutionContext> {
    let sql = "CREATE EXTERNAL TABLE sales(customer_id VARCHAR, revenue BIGINT) STORED AS CSV location 'tests/customer.csv'";

    let expected = vec!["++", "++"];

    let s = exec_sql(&mut ctx, sql).await?;
    let actual = s.lines().collect::<Vec<_>>();

    assert_eq!(expected, actual, "Creating table");
    Ok(ctx)
}

const QUERY: &str =
    "SELECT customer_id, revenue FROM sales ORDER BY revenue DESC limit 3";

// Run the query using the specified execution context and compare it
// to the known result
async fn run_and_compare_query(
    mut ctx: ExecutionContext,
    description: &str,
) -> Result<()> {
    let expected = vec![
        "+-------------+---------+",
        "| customer_id | revenue |",
        "+-------------+---------+",
        "| paul        | 300     |",
        "| jorge       | 200     |",
        "| andy        | 150     |",
        "+-------------+---------+",
    ];

    let s = exec_sql(&mut ctx, QUERY).await?;
    let actual = s.lines().collect::<Vec<_>>();

    assert_eq!(
        expected,
        actual,
        "output mismatch for {}. Expectedn\n{}Actual:\n{}",
        description,
        expected.join("\n"),
        s
    );
    Ok(())
}

#[tokio::test]
// Run the query using default planners and optimizer
async fn normal_query() -> Result<()> {
    let ctx = setup_table(ExecutionContext::new()).await?;
    run_and_compare_query(ctx, "Default context").await
}

#[tokio::test]
// Run the query using topk optimization
async fn topk_query() -> Result<()> {
    // Note the only difference is that the top
    let ctx = setup_table(make_topk_context()).await?;
    run_and_compare_query(ctx, "Topk context").await
}

#[tokio::test]
// Run EXPLAIN PLAN and show the plan was in fact rewritten
async fn topk_plan() -> Result<()> {
    let mut ctx = setup_table(make_topk_context()).await?;

    let expected = vec![
        "| logical_plan after topk                 | TopK: k=3                                      |",
        "|                                         |   Projection: #customer_id, #revenue           |",
        "|                                         |     TableScan: sales projection=Some([0, 1])   |",
    ].join("\n");

    let explain_query = format!("EXPLAIN VERBOSE {}", QUERY);
    let actual_output = exec_sql(&mut ctx, &explain_query).await?;

    // normalize newlines (output on windows uses \r\n)
    let actual_output = actual_output.replace("\r\n", "\n");

    assert!(actual_output.contains(&expected) , "Expected output not present in actual output\nExpected:\n---------\n{}\nActual:\n--------\n{}", expected, actual_output);
    Ok(())
}

fn make_topk_context() -> ExecutionContext {
    let config = ExecutionConfig::new().with_query_planner(Arc::new(TopKQueryPlanner {}));

    ExecutionContext::with_config(config)
}

// ------ The implementation of the TopK code follows -----

struct TopKQueryPlanner {}

impl QueryPlanner for TopKQueryPlanner {
    fn rewrite_logical_plan(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        TopKOptimizerRule {}.optimize(&plan)
    }

    /// Given a `LogicalPlan` created from above, create an
    /// `ExecutionPlan` suitable for execution
    fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        ctx_state: &ExecutionContextState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Teach the default physical planner how to plan TopK nodes.
        let physical_planner =
            DefaultPhysicalPlanner::with_extension_planner(Arc::new(TopKPlanner {}));
        // Delegate most work of physical planning to the default physical planner
        physical_planner.create_physical_plan(logical_plan, ctx_state)
    }
}

struct TopKOptimizerRule {}
impl OptimizerRule for TopKOptimizerRule {
    // Example rewrite pass to insert a user defined LogicalPlanNode
    fn optimize(&mut self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        match plan {
            // Note: this code simply looks for the pattern of a Limit followed by a
            // Sort and replaces it by a TopK node. It does not handle many
            // edge cases (e.g multiple sort columns, sort ASC / DESC), etc.
            LogicalPlan::Limit { ref n, ref input } => {
                if let LogicalPlan::Sort {
                    ref expr,
                    ref input,
                } = **input
                {
                    if expr.len() == 1 {
                        // we found a sort with a single sort expr, replace with a a TopK
                        return Ok(LogicalPlan::Extension {
                            node: Arc::new(TopKPlanNode {
                                k: *n,
                                input: self.optimize(input.as_ref())?,
                                expr: expr[0].clone(),
                            }),
                        });
                    }
                }
            }
            // Due to the way explain is implemented, in order to get
            // explain functionality we need to explicitly handle it
            // here.
            LogicalPlan::Explain {
                verbose,
                plan,
                stringified_plans,
                schema,
            } => {
                return optimize_explain(
                    self,
                    *verbose,
                    &*plan,
                    stringified_plans,
                    &*schema,
                )
            }
            _ => {}
        }

        // If we didn't find the Limit/Sort combination, recurse as
        // normal and build the result.
        self.optimize_children(plan)
    }

    fn name(&self) -> &str {
        "topk"
    }
}

struct TopKPlanNode {
    k: usize,
    input: LogicalPlan,
    /// The sort expression (this example only supports a single sort
    /// expr)
    expr: Expr,
}

impl Debug for TopKPlanNode {
    /// For TopK, use explain format for the Debug format. Other types
    /// of nodes may
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.fmt_for_explain(f)
    }
}

impl UserDefinedLogicalNode for TopKPlanNode {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    /// Schema for TopK is the same as the input
    fn schema(&self) -> &SchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![self.expr.clone()]
    }

    /// For example: `TopK: k=10`
    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TopK: k={}", self.k)
    }

    fn from_template(
        &self,
        exprs: &Vec<Expr>,
        inputs: &Vec<LogicalPlan>,
    ) -> Arc<dyn UserDefinedLogicalNode + Send + Sync> {
        assert_eq!(inputs.len(), 1, "input size inconistent");
        assert_eq!(exprs.len(), 1, "expression size inconistent");
        Arc::new(TopKPlanNode {
            k: self.k,
            input: inputs[0].clone(),
            expr: exprs[0].clone(),
        })
    }
}

/// Physical planner for TopK nodes
struct TopKPlanner {}

impl ExtensionPlanner for TopKPlanner {
    /// Create a physical plan for an extension node
    fn plan_extension(
        &self,
        node: &dyn UserDefinedLogicalNode,
        inputs: Vec<Arc<dyn ExecutionPlan>>,
        _ctx_state: &ExecutionContextState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if let Some(topk_node) = node.as_any().downcast_ref::<TopKPlanNode>() {
            assert_eq!(inputs.len(), 1, "Inconsistent number of inputs");
            // figure out input name
            Ok(Arc::new(TopKExec {
                input: inputs[0].clone(),
                k: topk_node.k,
            }))
        } else {
            Err(ExecutionError::General(format!(
                "Unknown extension node type {:?}",
                node
            )))
        }
    }
}

/// Physical operator that implements TopK for u64 data types. This
/// code is not general and is meant as an illustration only
struct TopKExec {
    input: Arc<dyn ExecutionPlan>,
    /// The maxium number of values
    k: usize,
}

impl Debug for TopKExec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TopKExec")
    }
}

#[async_trait]
impl ExecutionPlan for TopKExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn required_child_distribution(&self) -> Distribution {
        Distribution::UnspecifiedDistribution
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match children.len() {
            1 => Ok(Arc::new(TopKExec {
                input: children[0].clone(),
                k: self.k,
            })),
            _ => Err(ExecutionError::General(
                "TopKExec wrong number of children".to_string(),
            )),
        }
    }

    /// Execute one partition and return an iterator over RecordBatch
    async fn execute(
        &self,
        partition: usize,
    ) -> Result<Arc<Mutex<dyn RecordBatchReader + Send + Sync>>> {
        if 0 != partition {
            return Err(ExecutionError::General(format!(
                "TopKExec invalid partition {}",
                partition
            )));
        }

        Ok(Arc::new(Mutex::new(TopKReader {
            input: self.input.execute(partition).await?,
            k: self.k,
            top_values: BTreeMap::new(),
            done: false,
        })))
    }
}

// A very specialized TopK implementation
struct TopKReader {
    /// The input to read data from
    input: Arc<Mutex<dyn RecordBatchReader + Send + Sync>>,
    /// Maximum number of output values
    k: usize,
    /// Hard coded implementation for sales / customer_id example
    top_values: BTreeMap<i64, String>,
    /// Have we produced the output yet?
    done: bool,
}

impl TopKReader {
    /// Keeps track of the revenue from customer_id and stores if it
    /// is the top values we have seen so far.
    fn add_row(&mut self, customer_id: &str, revenue: i64) {
        self.top_values.insert(revenue, customer_id.into());
        // only keep top k
        while self.top_values.len() > self.k {
            self.remove_lowest_value()
        }
    }

    fn remove_lowest_value(&mut self) {
        if !self.top_values.is_empty() {
            let smallest_revenue = {
                let (revenue, _) = self.top_values.iter().next().unwrap();
                *revenue
            };
            self.top_values.remove(&smallest_revenue);
        }
    }
}

impl RecordBatchReader for TopKReader {
    fn schema(&self) -> SchemaRef {
        self.input.lock().expect("locked input reader").schema()
    }

    /// Reads the next `RecordBatch`.
    fn next_batch(&mut self) -> std::result::Result<Option<RecordBatch>, ArrowError> {
        if self.done {
            return Ok(None);
        }

        // use a loop so that we release the mutex once we have read each input_batch
        loop {
            let input_batch = self
                .input
                .lock()
                .expect("locked input mutex")
                .next_batch()?;

            match input_batch {
                Some(input_batch) => {
                    println!("Got an input batch");
                    let num_rows = input_batch.num_rows();

                    // Assuming the input columns are
                    // column[0]: customer_id / UTF8
                    // column[1]: revenue: Int64
                    let customer_id = input_batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .expect("Column 0 is not customer_id");

                    let revenue = input_batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .expect("Column 1 is not revenue");

                    for row in 0..num_rows {
                        self.add_row(customer_id.value(row), revenue.value(row));
                    }
                }
                None => break,
            }
        }

        let mut revenue_builder = Int64Array::builder(self.top_values.len());
        let mut customer_id_builder = StringBuilder::new(self.top_values.len());

        // make output by walking over the map backwards (so values are descending)
        for (revenue, customer_id) in self.top_values.iter().rev() {
            revenue_builder.append_value(*revenue)?;
            customer_id_builder.append_value(customer_id)?;
        }

        let record_batch = RecordBatch::try_new(
            self.schema().clone(),
            vec![
                Arc::new(customer_id_builder.finish()),
                Arc::new(revenue_builder.finish()),
            ],
        )?;

        self.done = true;
        Ok(Some(record_batch))
    }
}
