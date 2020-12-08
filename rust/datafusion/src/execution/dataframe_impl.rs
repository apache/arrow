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

//! Implementation of DataFrame API

use std::sync::Arc;

use crate::arrow::record_batch::RecordBatch;
use crate::dataframe::*;
use crate::error::Result;
use crate::execution::context::{ExecutionContext, ExecutionContextState};
use crate::logical_plan::{
    col, DFSchema, Expr, FunctionRegistry, JoinType, LogicalPlan, LogicalPlanBuilder,
};

use async_trait::async_trait;

/// Implementation of DataFrame API
pub struct DataFrameImpl {
    ctx_state: ExecutionContextState,
    plan: LogicalPlan,
}

impl DataFrameImpl {
    /// Create a new Table based on an existing logical plan
    pub fn new(ctx_state: ExecutionContextState, plan: &LogicalPlan) -> Self {
        Self {
            ctx_state,
            plan: plan.clone(),
        }
    }
}

#[async_trait]
impl DataFrame for DataFrameImpl {
    /// Apply a projection based on a list of column names
    fn select_columns(&self, columns: Vec<&str>) -> Result<Arc<dyn DataFrame>> {
        let fields = columns
            .iter()
            .map(|name| self.plan.schema().field_with_unqualified_name(name))
            .collect::<Result<Vec<_>>>()?;
        let expr = fields.iter().map(|f| col(f.name())).collect();
        self.select(expr)
    }

    /// Create a projection based on arbitrary expressions
    fn select(&self, expr_list: Vec<Expr>) -> Result<Arc<dyn DataFrame>> {
        let plan = LogicalPlanBuilder::from(&self.plan)
            .project(expr_list)?
            .build()?;
        Ok(Arc::new(DataFrameImpl::new(self.ctx_state.clone(), &plan)))
    }

    /// Create a filter based on a predicate expression
    fn filter(&self, predicate: Expr) -> Result<Arc<dyn DataFrame>> {
        let plan = LogicalPlanBuilder::from(&self.plan)
            .filter(predicate)?
            .build()?;
        Ok(Arc::new(DataFrameImpl::new(self.ctx_state.clone(), &plan)))
    }

    /// Perform an aggregate query
    fn aggregate(
        &self,
        group_expr: Vec<Expr>,
        aggr_expr: Vec<Expr>,
    ) -> Result<Arc<dyn DataFrame>> {
        let plan = LogicalPlanBuilder::from(&self.plan)
            .aggregate(group_expr, aggr_expr)?
            .build()?;
        Ok(Arc::new(DataFrameImpl::new(self.ctx_state.clone(), &plan)))
    }

    /// Limit the number of rows
    fn limit(&self, n: usize) -> Result<Arc<dyn DataFrame>> {
        let plan = LogicalPlanBuilder::from(&self.plan).limit(n)?.build()?;
        Ok(Arc::new(DataFrameImpl::new(self.ctx_state.clone(), &plan)))
    }

    /// Sort by specified sorting expressions
    fn sort(&self, expr: Vec<Expr>) -> Result<Arc<dyn DataFrame>> {
        let plan = LogicalPlanBuilder::from(&self.plan).sort(expr)?.build()?;
        Ok(Arc::new(DataFrameImpl::new(self.ctx_state.clone(), &plan)))
    }

    /// Join with another DataFrame
    fn join(
        &self,
        right: Arc<dyn DataFrame>,
        join_type: JoinType,
        left_cols: &[&str],
        right_cols: &[&str],
    ) -> Result<Arc<dyn DataFrame>> {
        let plan = LogicalPlanBuilder::from(&self.plan)
            .join(&right.to_logical_plan(), join_type, left_cols, right_cols)?
            .build()?;
        Ok(Arc::new(DataFrameImpl::new(self.ctx_state.clone(), &plan)))
    }

    /// Convert to logical plan
    fn to_logical_plan(&self) -> LogicalPlan {
        self.plan.clone()
    }

    // Convert the logical plan represented by this DataFrame into a physical plan and
    // execute it
    async fn collect(&self) -> Result<Vec<RecordBatch>> {
        let ctx = ExecutionContext::from(self.ctx_state.clone());
        let plan = ctx.optimize(&self.plan)?;
        let plan = ctx.create_physical_plan(&plan)?;
        Ok(ctx.collect(plan).await?)
    }

    /// Returns the schema from the logical plan
    fn schema(&self) -> &DFSchema {
        self.plan.schema()
    }

    fn explain(&self, verbose: bool) -> Result<Arc<dyn DataFrame>> {
        let plan = LogicalPlanBuilder::from(&self.plan)
            .explain(verbose)?
            .build()?;
        Ok(Arc::new(DataFrameImpl::new(self.ctx_state.clone(), &plan)))
    }

    fn registry(&self) -> &dyn FunctionRegistry {
        &self.ctx_state
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datasource::csv::CsvReadOptions;
    use crate::execution::context::ExecutionContext;
    use crate::logical_plan::*;
    use crate::{physical_plan::functions::ScalarFunctionImplementation, test};
    use arrow::{array::ArrayRef, datatypes::DataType};

    #[test]
    fn select_columns() -> Result<()> {
        // build plan using Table API
        let t = test_table()?;
        let t2 = t.select_columns(vec!["c1", "c2", "c11"])?;
        let plan = t2.to_logical_plan();

        // build query using SQL
        let sql_plan = create_plan("SELECT c1, c2, c11 FROM aggregate_test_100")?;

        // the two plans should be identical
        assert_same_plan(&plan, &sql_plan);

        Ok(())
    }

    #[test]
    fn select_expr() -> Result<()> {
        // build plan using Table API
        let t = test_table()?;
        let t2 = t.select(vec![col("c1"), col("c2"), col("c11")])?;
        let plan = t2.to_logical_plan();

        // build query using SQL
        let sql_plan = create_plan("SELECT c1, c2, c11 FROM aggregate_test_100")?;

        // the two plans should be identical
        assert_same_plan(&plan, &sql_plan);

        Ok(())
    }

    #[test]
    fn aggregate() -> Result<()> {
        // build plan using DataFrame API
        let df = test_table()?;
        let group_expr = vec![col("c1")];
        let aggr_expr = vec![
            min(col("c12")),
            max(col("c12")),
            avg(col("c12")),
            sum(col("c12")),
            count(col("c12")),
        ];

        let df = df.aggregate(group_expr, aggr_expr)?;

        let plan = df.to_logical_plan();

        // build same plan using SQL API
        let sql = "SELECT c1, MIN(c12), MAX(c12), AVG(c12), SUM(c12), COUNT(c12) \
                   FROM aggregate_test_100 \
                   GROUP BY c1";
        let sql_plan = create_plan(sql)?;

        // the two plans should be identical
        assert_same_plan(&plan, &sql_plan);

        Ok(())
    }

    #[tokio::test]
    async fn join() -> Result<()> {
        let left = test_table()?.select_columns(vec!["c1", "c2"])?;
        let right = test_table()?.select_columns(vec!["c1", "c3"])?;
        let left_rows = left.collect().await?;
        let right_rows = right.collect().await?;
        let join = left.join(right, JoinType::Inner, &["c1"], &["c1"])?;
        let join_rows = join.collect().await?;
        assert_eq!(1, left_rows.len());
        assert_eq!(100, left_rows[0].num_rows());
        assert_eq!(1, right_rows.len());
        assert_eq!(100, right_rows[0].num_rows());
        assert_eq!(1, join_rows.len());
        assert_eq!(2008, join_rows[0].num_rows());
        Ok(())
    }

    #[test]
    fn limit() -> Result<()> {
        // build query using Table API
        let t = test_table()?;
        let t2 = t.select_columns(vec!["c1", "c2", "c11"])?.limit(10)?;
        let plan = t2.to_logical_plan();

        // build query using SQL
        let sql_plan =
            create_plan("SELECT c1, c2, c11 FROM aggregate_test_100 LIMIT 10")?;

        // the two plans should be identical
        assert_same_plan(&plan, &sql_plan);

        Ok(())
    }

    #[test]
    fn explain() -> Result<()> {
        // build query using Table API
        let df = test_table()?;
        let df = df
            .select_columns(vec!["c1", "c2", "c11"])?
            .limit(10)?
            .explain(false)?;
        let plan = df.to_logical_plan();

        // build query using SQL
        let sql_plan =
            create_plan("EXPLAIN SELECT c1, c2, c11 FROM aggregate_test_100 LIMIT 10")?;

        // the two plans should be identical
        assert_same_plan(&plan, &sql_plan);

        Ok(())
    }

    #[test]
    fn registry() -> Result<()> {
        let mut ctx = ExecutionContext::new();
        register_aggregate_csv(&mut ctx)?;

        // declare the udf
        let my_fn: ScalarFunctionImplementation =
            Arc::new(|_: &[ArrayRef]| unimplemented!("my_fn is not implemented"));

        // create and register the udf
        ctx.register_udf(create_udf(
            "my_fn",
            vec![DataType::Float64],
            Arc::new(DataType::Float64),
            my_fn,
        ));

        // build query with a UDF using DataFrame API
        let df = ctx.table("aggregate_test_100")?;

        let f = df.registry();

        let df = df.select(vec![f.udf("my_fn")?.call(vec![col("c12")])])?;
        let plan = df.to_logical_plan();

        // build query using SQL
        let sql_plan =
            ctx.create_logical_plan("SELECT my_fn(c12) FROM aggregate_test_100")?;

        // the two plans should be identical
        assert_same_plan(&plan, &sql_plan);

        Ok(())
    }

    /// Compare the formatted string representation of two plans for equality
    fn assert_same_plan(plan1: &LogicalPlan, plan2: &LogicalPlan) {
        assert_eq!(format!("{:?}", plan1), format!("{:?}", plan2));
    }

    /// Create a logical plan from a SQL query
    fn create_plan(sql: &str) -> Result<LogicalPlan> {
        let mut ctx = ExecutionContext::new();
        register_aggregate_csv(&mut ctx)?;
        ctx.create_logical_plan(sql)
    }

    fn test_table() -> Result<Arc<dyn DataFrame + 'static>> {
        let mut ctx = ExecutionContext::new();
        register_aggregate_csv(&mut ctx)?;
        ctx.table("aggregate_test_100")
    }

    fn register_aggregate_csv(ctx: &mut ExecutionContext) -> Result<()> {
        let schema = test::aggr_test_schema();
        let testdata = test::arrow_testdata_path();
        ctx.register_csv(
            "aggregate_test_100",
            &format!("{}/csv/aggregate_test_100.csv", testdata),
            CsvReadOptions::new().schema(&schema.as_ref().to_owned().into()),
        )?;
        Ok(())
    }
}
