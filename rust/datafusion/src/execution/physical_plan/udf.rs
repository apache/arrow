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

//! UDF support

use std::fmt;

use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Schema};

use crate::error::Result;
use crate::execution::physical_plan::PhysicalExpr;

use super::{Accumulator, AggregateExpr, Aggregator};
use arrow::record_batch::RecordBatch;
use fmt::{Debug, Formatter};
use std::{cell::RefCell, rc::Rc, sync::Arc};

/// Scalar UDF
pub type ScalarUdf = Arc<dyn Fn(&[ArrayRef]) -> Result<ArrayRef> + Send + Sync>;

/// Function to construct the return type of a function given its arguments.
pub type ReturnType =
    Arc<dyn Fn(&Vec<Arc<dyn PhysicalExpr>>, &Schema) -> Result<DataType> + Send + Sync>;

/// Scalar UDF Expression
#[derive(Clone)]
pub struct ScalarFunction {
    /// Function name
    pub name: String,
    /// Set of valid argument types.
    /// The first dimension (0) represents specific combinations of valid argument types
    /// The second dimension (1) represents the types of each argument.
    /// For example, [[t1, t2]] is a function of 2 arguments that only accept t1 on the first arg and t2 on the second
    pub arg_types: Vec<Vec<DataType>>,
    /// Return type
    pub return_type: DataType,
    /// UDF implementation
    pub fun: ScalarUdf,
}

impl Debug for ScalarFunction {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScalarFunction")
            .field("name", &self.name)
            .field("arg_types", &self.arg_types)
            .field("return_type", &self.return_type)
            .field("fun", &"<FUNC>")
            .finish()
    }
}

impl ScalarFunction {
    /// Create a new ScalarFunction
    pub fn new(
        name: &str,
        arg_types: Vec<Vec<DataType>>,
        return_type: DataType,
        fun: ScalarUdf,
    ) -> Self {
        Self {
            name: name.to_owned(),
            arg_types,
            return_type,
            fun,
        }
    }
}

/// Scalar UDF Physical Expression
pub struct ScalarFunctionExpr {
    fun: Box<ScalarUdf>,
    name: String,
    args: Vec<Arc<dyn PhysicalExpr>>,
    return_type: DataType,
}

impl Debug for ScalarFunctionExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScalarFunctionExpr")
            .field("fun", &"<FUNC>")
            .field("name", &self.name)
            .field("args", &self.args)
            .field("return_type", &self.return_type)
            .finish()
    }
}

impl ScalarFunctionExpr {
    /// Create a new Scalar function
    pub fn new(
        name: &str,
        fun: Box<ScalarUdf>,
        args: Vec<Arc<dyn PhysicalExpr>>,
        return_type: &DataType,
    ) -> Self {
        Self {
            fun,
            name: name.to_owned(),
            args,
            return_type: return_type.clone(),
        }
    }
}

impl fmt::Display for ScalarFunctionExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}({})",
            self.name,
            self.args
                .iter()
                .map(|e| format!("{}", e))
                .collect::<Vec<String>>()
                .join(", ")
        )
    }
}

impl PhysicalExpr for ScalarFunctionExpr {
    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(self.return_type.clone())
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(true)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        // evaluate the arguments
        let inputs = self
            .args
            .iter()
            .map(|e| e.evaluate(batch))
            .collect::<Result<Vec<_>>>()?;

        // evaluate the function
        let fun = self.fun.as_ref();
        (fun)(&inputs)
    }
}

/// A generic aggregate function
/*
This struct is

An aggregate function accepts an arbitrary number of arguments, of arbitrary data types,
and returns an arbitrary type based on the incoming types.

It is the developer of the function's responsibility to ensure that the aggregator correctly handles the different
types that are presented to them, and that the return type correctly matches the type returned by the
aggregator.

It is the user of the function's responsibility to pass arguments to the function that have valid types.
*/
#[derive(Clone)]
pub struct AggregateFunction {
    /// Function name
    pub name: String,
    /// A list of arguments and their respective types. A function can accept more than one type as argument
    /// (e.g. sum(i8), sum(u8)).
    pub args: Vec<Vec<DataType>>,
    /// Return type. This function takes
    pub return_type: ReturnType,
    /// implementation of the aggregation
    pub aggregate: Arc<dyn Aggregator>,
}

/// An aggregate function physical expression
pub struct AggregateFunctionExpr {
    name: String,
    fun: Box<AggregateFunction>,
    // for now, our AggregateFunctionExpr accepts a single element only.
    arg: Arc<dyn PhysicalExpr>,
}

impl AggregateFunctionExpr {
    /// Create a new AggregateFunctionExpr
    pub fn new(
        name: &str,
        args: Vec<Arc<dyn PhysicalExpr>>,
        fun: Box<AggregateFunction>,
    ) -> Self {
        Self {
            name: name.to_owned(),
            arg: args[0].clone(),
            fun,
        }
    }
}

impl Debug for AggregateFunctionExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("AggregateFunctionExpr")
            .field("fun", &"<FUNC>")
            .field("name", &self.name)
            .field("args", &self.arg)
            .finish()
    }
}

impl PhysicalExpr for AggregateFunctionExpr {
    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        self.fun.as_ref().return_type.as_ref()(&vec![self.arg.clone()], input_schema)
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(false)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        self.arg.evaluate(batch)
    }
}

impl Aggregator for AggregateFunctionExpr {
    fn create_accumulator(&self) -> Rc<RefCell<dyn Accumulator>> {
        self.fun.aggregate.create_accumulator()
    }

    fn create_reducer(&self, column_name: &str) -> Arc<dyn AggregateExpr> {
        self.fun.aggregate.create_reducer(column_name)
    }
}

impl fmt::Display for AggregateFunctionExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}({})",
            self.name,
            [&self.arg]
                .iter()
                .map(|e| format!("{}", e))
                .collect::<Vec<String>>()
                .join(", ")
        )
    }
}
