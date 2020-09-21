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

//! This module contains functions and structs supporting user-defined aggregate functions.

use fmt::{Debug, Formatter};
use std::{cell::RefCell, fmt, rc::Rc};

use arrow::{
    datatypes::Field,
    datatypes::{DataType, Schema},
};

use crate::physical_plan::PhysicalExpr;
use crate::{error::Result, logical_plan::Expr};

use super::{
    aggregates::AccumulatorFunctionImplementation,
    aggregates::StateTypeFunction,
    expressions::format_state_name,
    functions::{ReturnTypeFunction, Signature},
    type_coercion::coerce,
    Accumulator, AggregateExpr,
};
use std::sync::Arc;

/// Logical representation of a user-defined aggregate function (UDAF)
/// A UDAF is different from a UDF in that it is stateful across batches.
#[derive(Clone)]
pub struct AggregateUDF {
    /// name
    pub name: String,
    /// signature
    pub signature: Signature,
    /// Return type
    pub return_type: ReturnTypeFunction,
    /// actual implementation
    pub accumulator: AccumulatorFunctionImplementation,
    /// the accumulator's state's description as a function of the return type
    pub state_type: StateTypeFunction,
}

impl Debug for AggregateUDF {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("AggregateUDF")
            .field("name", &self.name)
            .field("signature", &self.signature)
            .field("fun", &"<FUNC>")
            .finish()
    }
}

impl AggregateUDF {
    /// Create a new AggregateUDF
    pub fn new(
        name: &str,
        signature: &Signature,
        return_type: &ReturnTypeFunction,
        accumulator: &AccumulatorFunctionImplementation,
        state_type: &StateTypeFunction,
    ) -> Self {
        Self {
            name: name.to_owned(),
            signature: signature.clone(),
            return_type: return_type.clone(),
            accumulator: accumulator.clone(),
            state_type: state_type.clone(),
        }
    }

    /// creates a logical expression with a call of the UDAF
    /// This utility allows using the UDAF without requiring access to the registry.
    pub fn call(&self, args: Vec<Expr>) -> Expr {
        Expr::AggregateUDF {
            fun: Arc::new(self.clone()),
            args,
        }
    }
}

/// Creates a physical expression of the UDAF, that includes all necessary type coercion.
/// This function errors when `args`' can't be coerced to a valid argument type of the UDAF.
pub fn create_aggregate_expr(
    fun: &AggregateUDF,
    args: &Vec<Arc<dyn PhysicalExpr>>,
    input_schema: &Schema,
    name: String,
) -> Result<Arc<dyn AggregateExpr>> {
    // coerce
    let args = coerce(args, input_schema, &fun.signature)?;

    let arg_types = args
        .iter()
        .map(|arg| arg.data_type(input_schema))
        .collect::<Result<Vec<_>>>()?;

    Ok(Arc::new(AggregateFunctionExpr {
        fun: fun.clone(),
        args: args.clone(),
        data_type: (fun.return_type)(&arg_types)?.as_ref().clone(),
        name: name.clone(),
    }))
}

/// Physical aggregate expression of a UDAF.
#[derive(Debug)]
pub struct AggregateFunctionExpr {
    fun: AggregateUDF,
    args: Vec<Arc<dyn PhysicalExpr>>,
    data_type: DataType,
    name: String,
}

impl AggregateExpr for AggregateFunctionExpr {
    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        self.args.clone()
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        let fields = (self.fun.state_type)(&self.data_type)?
            .iter()
            .enumerate()
            .map(|(i, data_type)| {
                Field::new(
                    &format_state_name(&self.name, &format!("{}", i)),
                    data_type.clone(),
                    true,
                )
            })
            .collect::<Vec<Field>>();

        Ok(fields)
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new(&self.name, self.data_type.clone(), true))
    }

    fn create_accumulator(&self) -> Result<Rc<RefCell<dyn Accumulator>>> {
        (self.fun.accumulator)()
    }
}
