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

//! Projection Push Down optimizer rule ensures that only referenced columns are loaded into memory

use crate::logicalplan::Expr;
use crate::logicalplan::LogicalPlan;
use crate::optimizer::optimizer::OptimizerRule;
use std::collections::HashSet;
use std::rc::Rc;

/// Projection Push Down optimizer rule ensures that only referenced columns are loaded into memory
pub struct ProjectionPushDown {}

impl OptimizerRule for ProjectionPushDown {
    fn optimize(&mut self, plan: &LogicalPlan) -> Rc<LogicalPlan> {
        let mut accum = HashSet::new();
        self.optimize_plan(plan, &mut accum)
    }
}

impl ProjectionPushDown {
    pub fn new() -> Self {
        Self {}
    }

    fn optimize_plan(
        &self,
        plan: &LogicalPlan,
        accum: &mut HashSet<usize>,
    ) -> Rc<LogicalPlan> {
        match plan {
            LogicalPlan::Aggregate {
                input,
                group_expr,
                aggr_expr,
                schema,
            } => {
                // collect all columns referenced by grouping and aggregate expressions
                group_expr.iter().for_each(|e| self.collect_expr(e, accum));
                aggr_expr.iter().for_each(|e| self.collect_expr(e, accum));

                Rc::new(LogicalPlan::Aggregate {
                    input: self.optimize_plan(&input, accum),
                    group_expr: group_expr.clone(),
                    aggr_expr: aggr_expr.clone(),
                    schema: schema.clone(),
                })
            }
            LogicalPlan::TableScan {
                schema_name,
                table_name,
                schema,
                ..
            } => {
                // once we reach the table scan, we can use the accumulated set of column indexes as
                // the projection in the table scan
                let mut projection: Vec<usize> = Vec::with_capacity(accum.len());
                accum.iter().for_each(|i| projection.push(*i));
                Rc::new(LogicalPlan::TableScan {
                    schema_name: schema_name.to_string(),
                    table_name: table_name.to_string(),
                    schema: schema.clone(),
                    projection: Some(projection),
                })
            }
            //TODO implement all logical plan variants and remove this unimplemented
            _ => unimplemented!(),
        }
    }

    fn collect_expr(&self, expr: &Expr, accum: &mut HashSet<usize>) {
        match expr {
            Expr::Column(i) => {
                accum.insert(*i);
            }
            //TODO implement all expression variants and remove this unimplemented
            _ => unimplemented!(),
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::logicalplan::Expr::*;
    use crate::logicalplan::LogicalPlan::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::sync::Arc;

    #[test]
    fn aggregate_no_group_by() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::UInt32, false),
            Field::new("b", DataType::UInt32, false),
            Field::new("c", DataType::UInt32, false),
        ]);

        // create unoptimized plan for SELECT MAX(b) FROM default.test

        let table_scan = TableScan {
            schema_name: "default".to_string(),
            table_name: "test".to_string(),
            schema: Arc::new(schema),
            projection: None,
        };

        let aggregate = Aggregate {
            group_expr: vec![],
            aggr_expr: vec![Column(1)],
            schema: Arc::new(Schema::new(vec![Field::new(
                "MAX(b)",
                DataType::UInt32,
                false,
            )])),
            input: Rc::new(table_scan),
        };

        // run optimizer rule

        let rule: Rc<RefCell<OptimizerRule>> =
            Rc::new(RefCell::new(ProjectionPushDown::new()));

        let optimized_plan = rule.borrow_mut().optimize(&aggregate);

        let formatted_plan = format!("{:?}", optimized_plan);

        assert_eq!(formatted_plan, "Aggregate: groupBy=[[]], aggr=[[#1]]\n  TableScan: test projection=Some([1])");
    }
}
