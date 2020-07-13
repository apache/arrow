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

//! Math expressions

use crate::error::ExecutionError;
use crate::execution::context::ExecutionContext;
use crate::execution::physical_plan::udf::ScalarFunction;

use arrow::array::{Array, ArrayRef, Float64Array, Float64Builder};
use arrow::datatypes::{DataType, Field};

use std::sync::Arc;

macro_rules! math_unary_function {
    ($NAME:expr, $FUNC:ident) => {
        ScalarFunction::new(
            $NAME,
            vec![Field::new("n", DataType::Float64, true)],
            DataType::Float64,
            Arc::new(|args: &[ArrayRef]| {
                let n = &args[0].as_any().downcast_ref::<Float64Array>();
                match n {
                    Some(array) => {
                        let mut builder = Float64Builder::new(array.len());
                        for i in 0..array.len() {
                            if array.is_null(i) {
                                builder.append_null()?;
                            } else {
                                builder.append_value(array.value(i).$FUNC())?;
                            }
                        }
                        Ok(Arc::new(builder.finish()))
                    }
                    _ => Err(ExecutionError::General(format!(
                        "Invalid data type for {}",
                        $NAME
                    ))),
                }
            }),
        )
    };
}

/// Register math scalar functions with the context
pub fn register_math_functions(ctx: &mut ExecutionContext) {
    ctx.register_udf(math_unary_function!("sqrt", sqrt));
    ctx.register_udf(math_unary_function!("sin", sin));
    ctx.register_udf(math_unary_function!("cos", cos));
    ctx.register_udf(math_unary_function!("tan", tan));
    ctx.register_udf(math_unary_function!("asin", asin));
    ctx.register_udf(math_unary_function!("acos", acos));
    ctx.register_udf(math_unary_function!("atan", atan));
    ctx.register_udf(math_unary_function!("floor", floor));
    ctx.register_udf(math_unary_function!("ceil", ceil));
    ctx.register_udf(math_unary_function!("round", round));
    ctx.register_udf(math_unary_function!("trunc", trunc));
    ctx.register_udf(math_unary_function!("abs", abs));
    ctx.register_udf(math_unary_function!("signum", signum));
    ctx.register_udf(math_unary_function!("exp", exp));
    ctx.register_udf(math_unary_function!("log", ln));
    ctx.register_udf(math_unary_function!("log2", log2));
    ctx.register_udf(math_unary_function!("log10", log10));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Result;
    use crate::logicalplan::{sqrt, Expr, LogicalPlanBuilder};
    use arrow::datatypes::Schema;

    #[test]
    fn cast_i8_input() -> Result<()> {
        let schema = Schema::new(vec![Field::new("c0", DataType::Int8, true)]);
        let plan = LogicalPlanBuilder::scan("", "", &schema, None)?
            .project(vec![sqrt(Expr::UnresolvedColumn("c0".to_owned()))])?
            .build()?;
        let ctx = ExecutionContext::new();
        let plan = ctx.optimize(&plan)?;
        let expected = "Projection: sqrt(CAST(#0 AS Float64))\
        \n  TableScan:  projection=Some([0])";
        assert_eq!(format!("{:?}", plan), expected);
        Ok(())
    }

    #[test]
    fn no_cast_f64_input() -> Result<()> {
        let schema = Schema::new(vec![Field::new("c0", DataType::Float64, true)]);
        let plan = LogicalPlanBuilder::scan("", "", &schema, None)?
            .project(vec![sqrt(Expr::UnresolvedColumn("c0".to_owned()))])?
            .build()?;
        let ctx = ExecutionContext::new();
        let plan = ctx.optimize(&plan)?;
        let expected = "Projection: sqrt(#0)\
        \n  TableScan:  projection=Some([0])";
        assert_eq!(format!("{:?}", plan), expected);
        Ok(())
    }
}
