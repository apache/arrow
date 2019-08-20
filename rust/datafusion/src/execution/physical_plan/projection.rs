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

//! Defines the projection execution plan. A projection determines which columns or expressions
//! are returned from a query. The SQL statement `SELECT a, b, a+b FROM t1` is an example
//! of a projection on table `t1` where the expressions `a`, `b`, and `a+b` are the
//! projection expressions.

use std::sync::{Arc, Mutex};

use crate::error::Result;
use crate::execution::physical_plan::{
    BatchIterator, ExecutionPlan, Partition, PhysicalExpr,
};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;

/// Execution plan for a projection
pub struct ProjectionExec {
    /// The projection expressions
    expr: Vec<Arc<dyn PhysicalExpr>>,
    /// The schema once the projection has been applied to the input
    schema: Arc<Schema>,
    /// The input plan
    input: Arc<dyn ExecutionPlan>,
}

impl ProjectionExec {
    /// Create a projection on an input
    pub fn try_new(
        expr: Vec<Arc<dyn PhysicalExpr>>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Self> {
        let input_schema = input.schema();

        let fields: Result<Vec<_>> = expr
            .iter()
            .map(|e| Ok(Field::new(&e.name(), e.data_type(&input_schema)?, true)))
            .collect();

        let schema = Arc::new(Schema::new(fields?));

        Ok(Self {
            expr: expr.clone(),
            schema,
            input: input.clone(),
        })
    }
}

impl ExecutionPlan for ProjectionExec {
    /// Get the schema for this execution plan
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    /// Get the partitions for this execution plan
    fn partitions(&self) -> Result<Vec<Arc<dyn Partition>>> {
        let partitions: Vec<Arc<dyn Partition>> = self
            .input
            .partitions()?
            .iter()
            .map(|p| {
                let expr = self.expr.clone();
                let projection: Arc<dyn Partition> = Arc::new(ProjectionPartition {
                    schema: self.schema.clone(),
                    expr,
                    input: p.clone() as Arc<dyn Partition>,
                });

                projection
            })
            .collect();

        Ok(partitions)
    }
}

/// Represents a single partition of a projection execution plan
struct ProjectionPartition {
    schema: Arc<Schema>,
    expr: Vec<Arc<dyn PhysicalExpr>>,
    input: Arc<dyn Partition>,
}

impl Partition for ProjectionPartition {
    /// Execute the projection
    fn execute(&self) -> Result<Arc<Mutex<dyn BatchIterator>>> {
        Ok(Arc::new(Mutex::new(ProjectionIterator {
            schema: self.schema.clone(),
            expr: self.expr.clone(),
            input: self.input.execute()?,
        })))
    }
}

/// Projection iterator
struct ProjectionIterator {
    schema: Arc<Schema>,
    expr: Vec<Arc<dyn PhysicalExpr>>,
    input: Arc<Mutex<dyn BatchIterator>>,
}

impl BatchIterator for ProjectionIterator {
    /// Get the schema
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    /// Get the next batch
    fn next(&mut self) -> Result<Option<RecordBatch>> {
        let mut input = self.input.lock().unwrap();
        match input.next()? {
            Some(batch) => {
                let arrays: Result<Vec<_>> =
                    self.expr.iter().map(|expr| expr.evaluate(&batch)).collect();
                Ok(Some(RecordBatch::try_new(self.schema.clone(), arrays?)?))
            }
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::execution::physical_plan::csv::CsvExec;
    use crate::execution::physical_plan::expressions::Column;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::env;
    use std::fs;
    use std::fs::File;
    use std::io::prelude::*;
    use std::io::{BufReader, BufWriter};
    use std::path::Path;

    #[test]
    fn project_first_column() -> Result<()> {
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

        let partitions = 4;
        let path = create_partitioned_csv("aggregate_test_100.csv", partitions)?;

        let csv = CsvExec::try_new(&path, schema, true, None, 1024)?;

        let projection =
            ProjectionExec::try_new(vec![Arc::new(Column::new(0))], Arc::new(csv))?;

        let mut partition_count = 0;
        let mut row_count = 0;
        for partition in projection.partitions()? {
            partition_count += 1;
            let iterator = partition.execute()?;
            let mut iterator = iterator.lock().unwrap();
            while let Some(batch) = iterator.next()? {
                assert_eq!(1, batch.num_columns());
                row_count += batch.num_rows();
            }
        }
        assert_eq!(partitions, partition_count);
        assert_eq!(100, row_count);

        Ok(())
    }

    /// Generated partitioned copy of a CSV file
    fn create_partitioned_csv(filename: &str, partitions: usize) -> Result<String> {
        let testdata = env::var("ARROW_TEST_DATA").expect("ARROW_TEST_DATA not defined");
        let path = format!("{}/csv/{}", testdata, filename);

        let mut dir = env::temp_dir();
        dir.push(&format!("{}-{}", filename, partitions));

        if Path::new(&dir).exists() {
            fs::remove_dir_all(&dir).unwrap();
        }
        fs::create_dir(dir.clone()).unwrap();

        let mut writers = vec![];
        for i in 0..partitions {
            let mut filename = dir.clone();
            filename.push(format!("part{}.csv", i));
            let writer = BufWriter::new(File::create(&filename).unwrap());
            writers.push(writer);
        }

        let f = File::open(&path)?;
        let f = BufReader::new(f);
        let mut i = 0;
        for line in f.lines() {
            let line = line.unwrap();

            if i == 0 {
                // write header to all partitions
                for w in writers.iter_mut() {
                    w.write(line.as_bytes()).unwrap();
                    w.write(b"\n").unwrap();
                }
            } else {
                // write data line to single partition
                let partition = i % partitions;
                writers[partition].write(line.as_bytes()).unwrap();
                writers[partition].write(b"\n").unwrap();
            }

            i += 1;
        }
        for w in writers.iter_mut() {
            w.flush().unwrap();
        }

        Ok(dir.as_os_str().to_str().unwrap().to_string())
    }
}
