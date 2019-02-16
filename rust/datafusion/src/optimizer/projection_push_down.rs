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
use arrow::error::{ArrowError, Result};
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

/// Projection Push Down optimizer rule ensures that only referenced columns are loaded into memory
pub struct ProjectionPushDown {}

impl OptimizerRule for ProjectionPushDown {
    fn optimize(&mut self, plan: &LogicalPlan) -> Result<Rc<LogicalPlan>> {
        let mut accum = HashSet::new();
        let mut mapping: HashMap<usize, usize> = HashMap::new();
        self.optimize_plan(plan, &mut accum, &mut mapping)
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
        mapping: &mut HashMap<usize, usize>,
    ) -> Result<Rc<LogicalPlan>> {
        match plan {
            LogicalPlan::Selection { expr, input } => {
                // collect all columns referenced by filter expression
                self.collect_expr(expr, accum);

                // push projection down
                let input = self.optimize_plan(&input, accum, mapping)?;

                // rewrite filter expression to use new column indexes
                let new_expr = self.rewrite_expr(expr, mapping)?;

                Ok(Rc::new(LogicalPlan::Selection {
                    expr: new_expr,
                    input,
                }))
            }
            LogicalPlan::Aggregate {
                input,
                group_expr,
                aggr_expr,
                schema,
            } => {
                // collect all columns referenced by grouping and aggregate expressions
                group_expr.iter().for_each(|e| self.collect_expr(e, accum));
                aggr_expr.iter().for_each(|e| self.collect_expr(e, accum));

                // push projection down
                let input = self.optimize_plan(&input, accum, mapping)?;

                // rewrite expressions to use new column indexes
                let new_group_expr: Result<Vec<Expr>> = group_expr
                    .iter()
                    .map(|e| self.rewrite_expr(e, mapping))
                    .collect();
                let new_aggr_expr: Result<Vec<Expr>> = aggr_expr
                    .iter()
                    .map(|e| self.rewrite_expr(e, mapping))
                    .collect();

                Ok(Rc::new(LogicalPlan::Aggregate {
                    input,
                    group_expr: new_group_expr?,
                    aggr_expr: new_aggr_expr?,
                    schema: schema.clone(),
                }))
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

                // sort the projection otherwise we get non-deterministic behavior
                projection.sort();

                // now that the table scan is returning a different schema we need to create a
                // mapping from the original column index to the new column index so that we
                // can rewrite expressions as we walk back up the tree

                if mapping.len() != 0 {
                    return Err(ArrowError::ComputeError("illegal state".to_string()));
                }

                for i in 0..schema.fields().len() {
                    if let Some(n) = projection.iter().position(|v| *v == i) {
                        mapping.insert(i, n);
                    }
                }

                // return the table scan with projection
                Ok(Rc::new(LogicalPlan::TableScan {
                    schema_name: schema_name.to_string(),
                    table_name: table_name.to_string(),
                    schema: schema.clone(),
                    projection: Some(projection),
                }))
            }
            //TODO implement all logical plan variants and remove this unimplemented
            _ => Err(ArrowError::ComputeError("unimplemented".to_string())),
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

    fn rewrite_expr(&self, expr: &Expr, mapping: &HashMap<usize, usize>) -> Result<Expr> {
        match expr {
            Expr::Column(i) => Ok(Expr::Column(self.new_index(mapping, i)?)),
            //TODO implement all expression variants and remove this unimplemented
            _ => unimplemented!(),
        }
    }

    fn new_index(&self, mapping: &HashMap<usize, usize>, i: &usize) -> Result<usize> {
        match mapping.get(i) {
            Some(j) => Ok(*j),
            _ => Err(ArrowError::ComputeError(
                "Internal error computing new column index".to_string(),
            )),
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

        let optimized_plan = rule.borrow_mut().optimize(&aggregate).unwrap();

        let formatted_plan = format!("{:?}", optimized_plan);

        assert_eq!(formatted_plan, "Aggregate: groupBy=[[]], aggr=[[#0]]\n  TableScan: test projection=Some([1])");
    }

    #[test]
    fn aggregate_group_by() {
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
            group_expr: vec![Column(2)],
            aggr_expr: vec![Column(1)],
            schema: Arc::new(Schema::new(vec![
                Field::new("c", DataType::UInt32, false),
                Field::new("MAX(b)", DataType::UInt32, false),
            ])),
            input: Rc::new(table_scan),
        };

        // run optimizer rule

        let rule: Rc<RefCell<OptimizerRule>> =
            Rc::new(RefCell::new(ProjectionPushDown::new()));

        let optimized_plan = rule.borrow_mut().optimize(&aggregate).unwrap();

        let formatted_plan = format!("{:?}", optimized_plan);

        assert_eq!(formatted_plan, "Aggregate: groupBy=[[#1]], aggr=[[#0]]\n  TableScan: test projection=Some([1, 2])");
    }

    #[test]
    fn aggregate_no_group_by_with_selection() {
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

        let selection = Selection {
            expr: Column(2),
            input: Rc::new(table_scan),
        };

        let aggregate = Aggregate {
            group_expr: vec![],
            aggr_expr: vec![Column(1)],
            schema: Arc::new(Schema::new(vec![Field::new(
                "MAX(b)",
                DataType::UInt32,
                false,
            )])),
            input: Rc::new(selection),
        };

        // run optimizer rule

        let rule: Rc<RefCell<OptimizerRule>> =
            Rc::new(RefCell::new(ProjectionPushDown::new()));

        let optimized_plan = rule.borrow_mut().optimize(&aggregate).unwrap();

        let formatted_plan = format!("{:?}", optimized_plan);

        assert_eq!(formatted_plan, "Aggregate: groupBy=[[]], aggr=[[#0]]\n  Selection: #1\n    TableScan: test projection=Some([1, 2])");
    }
}
