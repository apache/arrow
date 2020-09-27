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

/// In this example we will declare a single-type, single return type UDAF that computes the geometric mean.
/// The geometric mean is described here: https://en.wikipedia.org/wiki/Geometric_mean
use arrow::{
    array::Float32Array, array::Float64Array, array::PrimitiveArrayOps,
    datatypes::DataType, record_batch::RecordBatch,
};

use datafusion::{error::Result, logical_plan::create_udaf, physical_plan::Accumulator};
use datafusion::{prelude::*, scalar::ScalarValue};
use std::{cell::RefCell, rc::Rc, sync::Arc};

// create local execution context with an in-memory table
fn create_context() -> Result<ExecutionContext> {
    use arrow::datatypes::{Field, Schema};
    use datafusion::datasource::MemTable;
    // define a schema.
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Float32, false)]));

    // define data in two partitions
    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Float32Array::from(vec![2.0, 4.0, 8.0]))],
    )?;
    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Float32Array::from(vec![64.0]))],
    )?;

    // declare a new context. In spark API, this corresponds to a new spark SQLsession
    let mut ctx = ExecutionContext::new();

    // declare a table in memory. In spark API, this corresponds to createDataFrame(...).
    let provider = MemTable::new(schema, vec![vec![batch1], vec![batch2]])?;
    ctx.register_table("t", Box::new(provider));
    Ok(ctx)
}

/// A UDAF has state across multiple rows, and thus we require a `struct` with that state.
#[derive(Debug)]
struct GeometricMean {
    n: u32,
    prod: f64,
}

impl GeometricMean {
    // how the struct is initialized
    pub fn new() -> Self {
        GeometricMean { n: 0, prod: 1.0 }
    }
}

// UDAFs are built using the trait `Accumulator`, that offers DataFusion the necessary functions
// to use them.
impl Accumulator for GeometricMean {
    // this function serializes our state to `ScalarValue`, which DataFusion uses
    // to pass this state between execution stages.
    // Note that this can be arbitrary data.
    fn state(&self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::from(self.prod),
            ScalarValue::from(self.n),
        ])
    }

    // this function receives one entry per argument of this accumulator.
    // DataFusion calls this function on every row, and expects this function to update the accumulator's state.
    fn update(&mut self, values: &Vec<ScalarValue>) -> Result<()> {
        // this is a one-argument UDAF, and thus we use `0`.
        let value = &values[0];
        match value {
            // here we map `ScalarValue` to our internal state. `Float64` indicates that this function
            // only accepts Float64 as its argument (DataFusion does try to coerce arguments to this type)
            //
            // Note that `.map` here ensures that we ignore Nulls.
            ScalarValue::Float64(e) => e.map(|value| {
                self.prod *= value;
                self.n += 1;
            }),
            _ => unreachable!(""),
        };
        Ok(())
    }

    // this function receives states from other accumulators (Vec<ScalarValue>)
    // and updates the accumulator.
    fn merge(&mut self, states: &Vec<ScalarValue>) -> Result<()> {
        let prod = &states[0];
        let n = &states[1];
        match (prod, n) {
            (ScalarValue::Float64(Some(prod)), ScalarValue::UInt32(Some(n))) => {
                self.prod *= prod;
                self.n += n;
            }
            _ => unreachable!(""),
        };
        Ok(())
    }

    // DataFusion expects this function to return the final value of this aggregator.
    // in this case, this is the formula of the geometric mean
    fn evaluate(&self) -> Result<ScalarValue> {
        let value = self.prod.powf(1.0 / self.n as f64);
        Ok(ScalarValue::from(value))
    }

    // Optimization hint: this trait also supports `update_batch` and `merge_batch`,
    // that can be used to perform these operations on arrays instead of single values.
    // By default, these methods call `update` and `merge` row by row
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut ctx = create_context()?;

    // here is where we define the UDAF. We also declare its signature:
    let geometric_mean = create_udaf(
        // the name; used to represent it in plan descriptions and in the registry, to use in SQL.
        "geo_mean",
        // the input type; DataFusion guarantees that the first entry of `values` in `update` has this type.
        DataType::Float64,
        // the return type; DataFusion expects this to match the type returned by `evaluate`.
        Arc::new(DataType::Float64),
        // This is the accumulator factory; DataFusion uses it to create new accumulators.
        Arc::new(|| Ok(Rc::new(RefCell::new(GeometricMean::new())))),
        // This is the description of the state. `state()` must match the types here.
        Arc::new(vec![DataType::Float64, DataType::UInt32]),
    );

    // get a DataFrame from the context
    // this table has 1 column `a` f32 with values {2,4,8,64}, whose geometric mean is 8.0.
    let df = ctx.table("t")?;

    // perform the aggregation
    let df = df.aggregate(vec![], vec![geometric_mean.call(vec![col("a")])])?;

    // note that "a" is f32, not f64. DataFusion coerces it to match the UDAF's signature.

    // execute the query
    let results = df.collect().await?;

    // downcast the array to the expected type
    let result = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();

    // verify that the calculation is correct
    assert_eq!(result.value(0), 8.0);

    Ok(())
}
