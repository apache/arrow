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

//! Defines the projection relation. A projection determines which columns or expressions are
//! returned from a query. The SQL statement `SELECT a, b, a+b FROM t1` is an example of a
//! projection on table `t1` where the expressions `a`, `b`, and `a+b` are the projection
//! expressions.

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::error::Result;
use crate::execution::expression::CompiledExpr;
use crate::execution::relation::Relation;

/// Projection relation
pub(super) struct ProjectRelation {
    /// Schema for the result of the projection
    schema: Arc<Schema>,
    /// The relation that the projection is being applied to
    input: Rc<RefCell<Relation>>,
    /// Projection expressions
    expr: Vec<CompiledExpr>,
}

impl ProjectRelation {
    pub fn new(
        input: Rc<RefCell<Relation>>,
        expr: Vec<CompiledExpr>,
        schema: Arc<Schema>,
    ) -> Self {
        ProjectRelation {
            input,
            expr,
            schema,
        }
    }
}

impl Relation for ProjectRelation {
    fn next(&mut self) -> Result<Option<RecordBatch>> {
        match self.input.borrow_mut().next()? {
            Some(batch) => {
                let projected_columns: Result<Vec<ArrayRef>> =
                    self.expr.iter().map(|e| e.invoke(&batch)).collect();

                let schema = Schema::new(
                    self.expr
                        .iter()
                        .map(|e| Field::new(&e.name(), e.data_type().clone(), true))
                        .collect(),
                );

                let projected_batch: RecordBatch =
                    RecordBatch::try_new(Arc::new(schema), projected_columns?)?;

                Ok(Some(projected_batch))
            }
            None => Ok(None),
        }
    }

    fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datasource::CsvBatchIterator;
    use crate::execution::context::ExecutionContext;
    use crate::execution::expression;
    use crate::execution::relation::DataSourceRelation;
    use crate::logicalplan::Expr;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Mutex;

    #[test]
    fn project_first_column() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Utf8, false),
            Field::new("c2", DataType::UInt32, false),
            Field::new("c3", DataType::Int8, false),
            Field::new("c3", DataType::Int16, false),
            Field::new("c4", DataType::Int32, false),
            Field::new("c5", DataType::Int64, false),
            Field::new("c6", DataType::UInt8, false),
            Field::new("c7", DataType::UInt16, false),
            Field::new("c8", DataType::UInt32, false),
            Field::new("c9", DataType::UInt64, false),
            Field::new("c10", DataType::Float32, false),
            Field::new("c11", DataType::Float64, false),
            Field::new("c12", DataType::Utf8, false),
        ]));

        let ds = CsvBatchIterator::new(
            "../../testing/data/csv/aggregate_test_100.csv",
            schema.clone(),
            true,
            &None,
            1024,
        );
        let relation = Rc::new(RefCell::new(DataSourceRelation::new(Arc::new(
            Mutex::new(ds),
        ))));
        let context = ExecutionContext::new();

        let projection_expr =
            vec![
                expression::compile_expr(&context, &Expr::Column(0), schema.as_ref())
                    .unwrap(),
            ];

        let mut projection = ProjectRelation::new(relation, projection_expr, schema);
        let batch = projection.next().unwrap().unwrap();
        assert_eq!(1, batch.num_columns());

        assert_eq!("c1", batch.schema().field(0).name());
    }

}
