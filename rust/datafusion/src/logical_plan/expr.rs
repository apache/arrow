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

//! This module provides an `Expr` enum for representing expressions such as `col = 5` or `SUM(col)`

pub use super::Operator;

use std::fmt;
use std::sync::Arc;

use aggregates::{AccumulatorFunctionImplementation, StateTypeFunction};
use arrow::{compute::can_cast_types, datatypes::DataType};

use crate::error::{DataFusionError, Result};
use crate::logical_plan::{DFField, DFSchema};
use crate::physical_plan::{
    aggregates, expressions::binary_operator_data_type, functions, udf::ScalarUDF,
};
use crate::{physical_plan::udaf::AggregateUDF, scalar::ScalarValue};
use functions::{ReturnTypeFunction, ScalarFunctionImplementation, Signature};
use std::collections::HashSet;

/// `Expr` is a logical expression. A logical expression is something like `1 + 1`, or `CAST(c1 AS int)`.
/// Logical expressions know how to compute its [arrow::datatypes::DataType] and nullability.
/// `Expr` is a central struct of DataFusion's query API.
///
/// # Examples
///
/// ```
/// # use datafusion::logical_plan::Expr;
/// # use datafusion::error::Result;
/// # fn main() -> Result<()> {
/// let expr = Expr::Column("c1".to_string()) + Expr::Column("c2".to_string());
/// println!("{:?}", expr);
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub enum Expr {
    /// An expression with a specific name.
    Alias(Box<Expr>, String),
    /// A named reference to a field in a schema.
    Column(String),
    /// A named reference to a variable in a registry.
    ScalarVariable(Vec<String>),
    /// A constant value.
    Literal(ScalarValue),
    /// A binary expression such as "age > 21"
    BinaryExpr {
        /// Left-hand side of the expression
        left: Box<Expr>,
        /// The comparison operator
        op: Operator,
        /// Right-hand side of the expression
        right: Box<Expr>,
    },
    /// Negation of an expression. The expression's type must be a boolean to make sense.
    Not(Box<Expr>),
    /// Whether an expression is not Null. This expression is never null.
    IsNotNull(Box<Expr>),
    /// Whether an expression is Null. This expression is never null.
    IsNull(Box<Expr>),
    /// The CASE expression is similar to a series of nested if/else and there are two forms that
    /// can be used. The first form consists of a series of boolean "when" expressions with
    /// corresponding "then" expressions, and an optional "else" expression.
    ///
    /// CASE WHEN condition THEN result
    ///      [WHEN ...]
    ///      [ELSE result]
    /// END
    ///
    /// The second form uses a base expression and then a series of "when" clauses that match on a
    /// literal value.
    ///
    /// CASE expression
    ///     WHEN value THEN result
    ///     [WHEN ...]
    ///     [ELSE result]
    /// END
    Case {
        /// Optional base expression that can be compared to literal values in the "when" expressions
        expr: Option<Box<Expr>>,
        /// One or more when/then expressions
        when_then_expr: Vec<(Box<Expr>, Box<Expr>)>,
        /// Optional "else" expression
        else_expr: Option<Box<Expr>>,
    },
    /// Casts the expression to a given type. This expression is guaranteed to have a fixed type.
    Cast {
        /// The expression being cast
        expr: Box<Expr>,
        /// The `DataType` the expression will yield
        data_type: DataType,
    },
    /// A sort expression, that can be used to sort values.
    Sort {
        /// The expression to sort on
        expr: Box<Expr>,
        /// The direction of the sort
        asc: bool,
        /// Whether to put Nulls before all other data values
        nulls_first: bool,
    },
    /// Represents the call of a built-in scalar function with a set of arguments.
    ScalarFunction {
        /// The function
        fun: functions::BuiltinScalarFunction,
        /// List of expressions to feed to the functions as arguments
        args: Vec<Expr>,
    },
    /// Represents the call of a user-defined scalar function with arguments.
    ScalarUDF {
        /// The function
        fun: Arc<ScalarUDF>,
        /// List of expressions to feed to the functions as arguments
        args: Vec<Expr>,
    },
    /// Represents the call of an aggregate built-in function with arguments.
    AggregateFunction {
        /// Name of the function
        fun: aggregates::AggregateFunction,
        /// List of expressions to feed to the functions as arguments
        args: Vec<Expr>,
        /// Whether this is a DISTINCT aggregation or not
        distinct: bool,
    },
    /// aggregate function
    AggregateUDF {
        /// The function
        fun: Arc<AggregateUDF>,
        /// List of expressions to feed to the functions as arguments
        args: Vec<Expr>,
    },
    /// Represents a reference to all fields in a schema.
    Wildcard,
}

impl Expr {
    /// Returns the [arrow::datatypes::DataType] of the expression based on [arrow::datatypes::Schema].
    ///
    /// # Errors
    ///
    /// This function errors when it is not possible to compute its [arrow::datatypes::DataType].
    /// This happens when e.g. the expression refers to a column that does not exist in the schema, or when
    /// the expression is incorrectly typed (e.g. `[utf8] + [bool]`).
    pub fn get_type(&self, schema: &DFSchema) -> Result<DataType> {
        match self {
            Expr::Alias(expr, _) => expr.get_type(schema),
            Expr::Column(name) => Ok(schema
                .field_with_unqualified_name(name)?
                .data_type()
                .clone()),
            Expr::ScalarVariable(_) => Ok(DataType::Utf8),
            Expr::Literal(l) => Ok(l.get_datatype()),
            Expr::Case { when_then_expr, .. } => when_then_expr[0].1.get_type(schema),
            Expr::Cast { data_type, .. } => Ok(data_type.clone()),
            Expr::ScalarUDF { fun, args } => {
                let data_types = args
                    .iter()
                    .map(|e| e.get_type(schema))
                    .collect::<Result<Vec<_>>>()?;
                Ok((fun.return_type)(&data_types)?.as_ref().clone())
            }
            Expr::ScalarFunction { fun, args } => {
                let data_types = args
                    .iter()
                    .map(|e| e.get_type(schema))
                    .collect::<Result<Vec<_>>>()?;
                functions::return_type(fun, &data_types)
            }
            Expr::AggregateFunction { fun, args, .. } => {
                let data_types = args
                    .iter()
                    .map(|e| e.get_type(schema))
                    .collect::<Result<Vec<_>>>()?;
                aggregates::return_type(fun, &data_types)
            }
            Expr::AggregateUDF { fun, args, .. } => {
                let data_types = args
                    .iter()
                    .map(|e| e.get_type(schema))
                    .collect::<Result<Vec<_>>>()?;
                Ok((fun.return_type)(&data_types)?.as_ref().clone())
            }
            Expr::Not(_) => Ok(DataType::Boolean),
            Expr::IsNull(_) => Ok(DataType::Boolean),
            Expr::IsNotNull(_) => Ok(DataType::Boolean),
            Expr::BinaryExpr {
                ref left,
                ref right,
                ref op,
            } => binary_operator_data_type(
                &left.get_type(schema)?,
                op,
                &right.get_type(schema)?,
            ),
            Expr::Sort { ref expr, .. } => expr.get_type(schema),
            Expr::Wildcard => Err(DataFusionError::Internal(
                "Wildcard expressions are not valid in a logical query plan".to_owned(),
            )),
        }
    }

    /// Returns the nullability of the expression based on [arrow::datatypes::Schema].
    ///
    /// # Errors
    ///
    /// This function errors when it is not possible to compute its nullability.
    /// This happens when the expression refers to a column that does not exist in the schema.
    pub fn nullable(&self, input_schema: &DFSchema) -> Result<bool> {
        match self {
            Expr::Alias(expr, _) => expr.nullable(input_schema),
            Expr::Column(name) => Ok(input_schema
                .field_with_unqualified_name(name)?
                .is_nullable()),
            Expr::Literal(value) => Ok(value.is_null()),
            Expr::ScalarVariable(_) => Ok(true),
            Expr::Case {
                when_then_expr,
                else_expr,
                ..
            } => {
                // this expression is nullable if any of the input expressions are nullable
                let then_nullable = when_then_expr
                    .iter()
                    .map(|(_, t)| t.nullable(input_schema))
                    .collect::<Result<Vec<_>>>()?;
                if then_nullable.contains(&true) {
                    Ok(true)
                } else if let Some(e) = else_expr {
                    e.nullable(input_schema)
                } else {
                    Ok(false)
                }
            }
            Expr::Cast { expr, .. } => expr.nullable(input_schema),
            Expr::ScalarFunction { .. } => Ok(true),
            Expr::ScalarUDF { .. } => Ok(true),
            Expr::AggregateFunction { .. } => Ok(true),
            Expr::AggregateUDF { .. } => Ok(true),
            Expr::Not(expr) => expr.nullable(input_schema),
            Expr::IsNull(_) => Ok(false),
            Expr::IsNotNull(_) => Ok(false),
            Expr::BinaryExpr {
                ref left,
                ref right,
                ..
            } => Ok(left.nullable(input_schema)? || right.nullable(input_schema)?),
            Expr::Sort { ref expr, .. } => expr.nullable(input_schema),
            Expr::Wildcard => Err(DataFusionError::Internal(
                "Wildcard expressions are not valid in a logical query plan".to_owned(),
            )),
        }
    }

    /// Returns the name of this expression based on [arrow::datatypes::Schema].
    ///
    /// This represents how a column with this expression is named when no alias is chosen
    pub fn name(&self, input_schema: &DFSchema) -> Result<String> {
        create_name(self, input_schema)
    }

    /// Returns a [arrow::datatypes::Field] compatible with this expression.
    pub fn to_field(&self, input_schema: &DFSchema) -> Result<DFField> {
        Ok(DFField::new(
            None, //TODO  qualifier
            &self.name(input_schema)?,
            self.get_type(input_schema)?,
            self.nullable(input_schema)?,
        ))
    }

    /// Wraps this expression in a cast to a target [arrow::datatypes::DataType].
    ///
    /// # Errors
    ///
    /// This function errors when it is impossible to cast the
    /// expression to the target [arrow::datatypes::DataType].
    pub fn cast_to(&self, cast_to_type: &DataType, schema: &DFSchema) -> Result<Expr> {
        let this_type = self.get_type(schema)?;
        if this_type == *cast_to_type {
            Ok(self.clone())
        } else if can_cast_types(&this_type, cast_to_type) {
            Ok(Expr::Cast {
                expr: Box::new(self.clone()),
                data_type: cast_to_type.clone(),
            })
        } else {
            Err(DataFusionError::Plan(format!(
                "Cannot automatically convert {:?} to {:?}",
                this_type, cast_to_type
            )))
        }
    }

    /// Equal
    pub fn eq(&self, other: Expr) -> Expr {
        binary_expr(self.clone(), Operator::Eq, other)
    }

    /// Not equal
    pub fn not_eq(&self, other: Expr) -> Expr {
        binary_expr(self.clone(), Operator::NotEq, other)
    }

    /// Greater than
    pub fn gt(&self, other: Expr) -> Expr {
        binary_expr(self.clone(), Operator::Gt, other)
    }

    /// Greater than or equal to
    pub fn gt_eq(&self, other: Expr) -> Expr {
        binary_expr(self.clone(), Operator::GtEq, other)
    }

    /// Less than
    pub fn lt(&self, other: Expr) -> Expr {
        binary_expr(self.clone(), Operator::Lt, other)
    }

    /// Less than or equal to
    pub fn lt_eq(&self, other: Expr) -> Expr {
        binary_expr(self.clone(), Operator::LtEq, other)
    }

    /// And
    pub fn and(&self, other: Expr) -> Expr {
        binary_expr(self.clone(), Operator::And, other)
    }

    /// Or
    pub fn or(&self, other: Expr) -> Expr {
        binary_expr(self.clone(), Operator::Or, other)
    }

    /// Not
    pub fn not(&self) -> Expr {
        Expr::Not(Box::new(self.clone()))
    }

    /// Calculate the modulus of two expressions
    pub fn modulus(&self, other: Expr) -> Expr {
        binary_expr(self.clone(), Operator::Modulus, other)
    }

    /// like (string) another expression
    pub fn like(&self, other: Expr) -> Expr {
        binary_expr(self.clone(), Operator::Like, other)
    }

    /// not like another expression
    pub fn not_like(&self, other: Expr) -> Expr {
        binary_expr(self.clone(), Operator::NotLike, other)
    }

    /// Alias
    pub fn alias(&self, name: &str) -> Expr {
        Expr::Alias(Box::new(self.clone()), name.to_owned())
    }

    /// Create a sort expression from an existing expression.
    ///
    /// ```
    /// # use datafusion::logical_plan::col;
    /// let sort_expr = col("foo").sort(true, true); // SORT ASC NULLS_FIRST
    /// ```
    pub fn sort(&self, asc: bool, nulls_first: bool) -> Expr {
        Expr::Sort {
            expr: Box::new(self.clone()),
            asc,
            nulls_first,
        }
    }
}

pub struct CaseBuilder {
    expr: Option<Box<Expr>>,
    when_expr: Vec<Expr>,
    then_expr: Vec<Expr>,
    else_expr: Option<Box<Expr>>,
}

impl CaseBuilder {
    pub fn when(&mut self, when: Expr, then: Expr) -> CaseBuilder {
        self.when_expr.push(when);
        self.then_expr.push(then);
        CaseBuilder {
            expr: self.expr.clone(),
            when_expr: self.when_expr.clone(),
            then_expr: self.then_expr.clone(),
            else_expr: self.else_expr.clone(),
        }
    }
    pub fn otherwise(&mut self, else_expr: Expr) -> Result<Expr> {
        self.else_expr = Some(Box::new(else_expr));
        self.build()
    }

    pub fn end(&self) -> Result<Expr> {
        self.build()
    }
}

impl CaseBuilder {
    fn build(&self) -> Result<Expr> {
        // collect all "then" expressions
        let mut then_expr = self.then_expr.clone();
        if let Some(e) = &self.else_expr {
            then_expr.push(e.as_ref().to_owned());
        }

        let then_types: Vec<DataType> = then_expr
            .iter()
            .map(|e| match e {
                Expr::Literal(_) => e.get_type(&DFSchema::empty()),
                _ => Ok(DataType::Null),
            })
            .collect::<Result<Vec<_>>>()?;

        if then_types.contains(&DataType::Null) {
            // cannot verify types until execution type
        } else {
            let unique_types: HashSet<&DataType> = then_types.iter().collect();
            if unique_types.len() != 1 {
                return Err(DataFusionError::Plan(format!(
                    "CASE expression 'then' values had multiple data types: {:?}",
                    unique_types
                )));
            }
        }

        Ok(Expr::Case {
            expr: self.expr.clone(),
            when_then_expr: self
                .when_expr
                .iter()
                .zip(self.then_expr.iter())
                .map(|(w, t)| (Box::new(w.clone()), Box::new(t.clone())))
                .collect(),
            else_expr: self.else_expr.clone(),
        })
    }
}

/// Create a CASE WHEN statement with literal WHEN expressions for comparison to the base expression.
pub fn case(expr: Expr) -> CaseBuilder {
    CaseBuilder {
        expr: Some(Box::new(expr)),
        when_expr: vec![],
        then_expr: vec![],
        else_expr: None,
    }
}

/// Create a CASE WHEN statement with boolean WHEN expressions and no base expression.
pub fn when(when: Expr, then: Expr) -> CaseBuilder {
    CaseBuilder {
        expr: None,
        when_expr: vec![when],
        then_expr: vec![then],
        else_expr: None,
    }
}

/// return a new expression l <op> r
pub fn binary_expr(l: Expr, op: Operator, r: Expr) -> Expr {
    Expr::BinaryExpr {
        left: Box::new(l),
        op,
        right: Box::new(r),
    }
}

/// return a new expression with a logical AND
pub fn and(left: Expr, right: Expr) -> Expr {
    Expr::BinaryExpr {
        left: Box::new(left),
        op: Operator::And,
        right: Box::new(right),
    }
}

/// return a new expression with a logical OR
pub fn or(left: Expr, right: Expr) -> Expr {
    Expr::BinaryExpr {
        left: Box::new(left),
        op: Operator::Or,
        right: Box::new(right),
    }
}

/// Create a column expression based on a column name
pub fn col(name: &str) -> Expr {
    Expr::Column(name.to_owned())
}

/// Create an expression to represent the min() aggregate function
pub fn min(expr: Expr) -> Expr {
    Expr::AggregateFunction {
        fun: aggregates::AggregateFunction::Min,
        distinct: false,
        args: vec![expr],
    }
}

/// Create an expression to represent the max() aggregate function
pub fn max(expr: Expr) -> Expr {
    Expr::AggregateFunction {
        fun: aggregates::AggregateFunction::Max,
        distinct: false,
        args: vec![expr],
    }
}

/// Create an expression to represent the sum() aggregate function
pub fn sum(expr: Expr) -> Expr {
    Expr::AggregateFunction {
        fun: aggregates::AggregateFunction::Sum,
        distinct: false,
        args: vec![expr],
    }
}

/// Create an expression to represent the avg() aggregate function
pub fn avg(expr: Expr) -> Expr {
    Expr::AggregateFunction {
        fun: aggregates::AggregateFunction::Avg,
        distinct: false,
        args: vec![expr],
    }
}

/// Create an expression to represent the count() aggregate function
pub fn count(expr: Expr) -> Expr {
    Expr::AggregateFunction {
        fun: aggregates::AggregateFunction::Count,
        distinct: false,
        args: vec![expr],
    }
}

/// Whether it can be represented as a literal expression
pub trait Literal {
    /// convert the value to a Literal expression
    fn lit(&self) -> Expr;
}

impl Literal for &str {
    fn lit(&self) -> Expr {
        Expr::Literal(ScalarValue::Utf8(Some((*self).to_owned())))
    }
}

impl Literal for String {
    fn lit(&self) -> Expr {
        Expr::Literal(ScalarValue::Utf8(Some((*self).to_owned())))
    }
}

macro_rules! make_literal {
    ($TYPE:ty, $SCALAR:ident) => {
        #[allow(missing_docs)]
        impl Literal for $TYPE {
            fn lit(&self) -> Expr {
                Expr::Literal(ScalarValue::$SCALAR(Some(self.clone())))
            }
        }
    };
}

make_literal!(bool, Boolean);
make_literal!(f32, Float32);
make_literal!(f64, Float64);
make_literal!(i8, Int8);
make_literal!(i16, Int16);
make_literal!(i32, Int32);
make_literal!(i64, Int64);
make_literal!(u8, UInt8);
make_literal!(u16, UInt16);
make_literal!(u32, UInt32);
make_literal!(u64, UInt64);

/// Create a literal expression
pub fn lit<T: Literal>(n: T) -> Expr {
    n.lit()
}

/// Create an convenience function representing a unary scalar function
macro_rules! unary_math_expr {
    ($ENUM:ident, $FUNC:ident) => {
        #[allow(missing_docs)]
        pub fn $FUNC(e: Expr) -> Expr {
            Expr::ScalarFunction {
                fun: functions::BuiltinScalarFunction::$ENUM,
                args: vec![e],
            }
        }
    };
}

// generate methods for creating the supported unary math expressions
unary_math_expr!(Sqrt, sqrt);
unary_math_expr!(Sin, sin);
unary_math_expr!(Cos, cos);
unary_math_expr!(Tan, tan);
unary_math_expr!(Asin, asin);
unary_math_expr!(Acos, acos);
unary_math_expr!(Atan, atan);
unary_math_expr!(Floor, floor);
unary_math_expr!(Ceil, ceil);
unary_math_expr!(Round, round);
unary_math_expr!(Trunc, trunc);
unary_math_expr!(Abs, abs);
unary_math_expr!(Signum, signum);
unary_math_expr!(Exp, exp);
unary_math_expr!(Log, ln);
unary_math_expr!(Log2, log2);
unary_math_expr!(Log10, log10);

/// returns the length of a string in bytes
pub fn length(e: Expr) -> Expr {
    Expr::ScalarFunction {
        fun: functions::BuiltinScalarFunction::Length,
        args: vec![e],
    }
}

/// returns the concatenation of string expressions
pub fn concat(args: Vec<Expr>) -> Expr {
    Expr::ScalarFunction {
        fun: functions::BuiltinScalarFunction::Concat,
        args,
    }
}

/// returns an array of fixed size with each argument on it.
pub fn array(args: Vec<Expr>) -> Expr {
    Expr::ScalarFunction {
        fun: functions::BuiltinScalarFunction::Array,
        args,
    }
}

/// Creates a new UDF with a specific signature and specific return type.
/// This is a helper function to create a new UDF.
/// The function `create_udf` returns a subset of all possible `ScalarFunction`:
/// * the UDF has a fixed return type
/// * the UDF has a fixed signature (e.g. [f64, f64])
pub fn create_udf(
    name: &str,
    input_types: Vec<DataType>,
    return_type: Arc<DataType>,
    fun: ScalarFunctionImplementation,
) -> ScalarUDF {
    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(return_type.clone()));
    ScalarUDF::new(name, &Signature::Exact(input_types), &return_type, &fun)
}

/// Creates a new UDAF with a specific signature, state type and return type.
/// The signature and state type must match the `Acumulator's implementation`.
pub fn create_udaf(
    name: &str,
    input_type: DataType,
    return_type: Arc<DataType>,
    accumulator: AccumulatorFunctionImplementation,
    state_type: Arc<Vec<DataType>>,
) -> AggregateUDF {
    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(return_type.clone()));
    let state_type: StateTypeFunction = Arc::new(move |_| Ok(state_type.clone()));
    AggregateUDF::new(
        name,
        &Signature::Exact(vec![input_type]),
        &return_type,
        &accumulator,
        &state_type,
    )
}

fn fmt_function(
    f: &mut fmt::Formatter,
    fun: &String,
    distinct: bool,
    args: &Vec<Expr>,
) -> fmt::Result {
    let args: Vec<String> = args.iter().map(|arg| format!("{:?}", arg)).collect();
    let distinct_str = match distinct {
        true => "DISTINCT ",
        false => "",
    };
    write!(f, "{}({}{})", fun, distinct_str, args.join(", "))
}

impl fmt::Debug for Expr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Expr::Alias(expr, alias) => write!(f, "{:?} AS {}", expr, alias),
            Expr::Column(name) => write!(f, "#{}", name),
            Expr::ScalarVariable(var_names) => write!(f, "{}", var_names.join(".")),
            Expr::Literal(v) => write!(f, "{:?}", v),
            Expr::Case {
                expr,
                when_then_expr,
                else_expr,
                ..
            } => {
                write!(f, "CASE ")?;
                if let Some(e) = expr {
                    write!(f, "{:?} ", e)?;
                }
                for (w, t) in when_then_expr {
                    write!(f, "WHEN {:?} THEN {:?} ", w, t)?;
                }
                if let Some(e) = else_expr {
                    write!(f, "ELSE {:?} ", e)?;
                }
                write!(f, "END")
            }
            Expr::Cast { expr, data_type } => {
                write!(f, "CAST({:?} AS {:?})", expr, data_type)
            }
            Expr::Not(expr) => write!(f, "NOT {:?}", expr),
            Expr::IsNull(expr) => write!(f, "{:?} IS NULL", expr),
            Expr::IsNotNull(expr) => write!(f, "{:?} IS NOT NULL", expr),
            Expr::BinaryExpr { left, op, right } => {
                write!(f, "{:?} {:?} {:?}", left, op, right)
            }
            Expr::Sort {
                expr,
                asc,
                nulls_first,
            } => {
                if *asc {
                    write!(f, "{:?} ASC", expr)?;
                } else {
                    write!(f, "{:?} DESC", expr)?;
                }
                if *nulls_first {
                    write!(f, " NULLS FIRST")
                } else {
                    write!(f, " NULLS LAST")
                }
            }
            Expr::ScalarFunction { fun, args, .. } => {
                fmt_function(f, &fun.to_string(), false, args)
            }
            Expr::ScalarUDF { fun, ref args, .. } => {
                fmt_function(f, &fun.name, false, args)
            }
            Expr::AggregateFunction {
                fun,
                distinct,
                ref args,
                ..
            } => fmt_function(f, &fun.to_string(), *distinct, args),
            Expr::AggregateUDF { fun, ref args, .. } => {
                fmt_function(f, &fun.name, false, args)
            }
            Expr::Wildcard => write!(f, "*"),
        }
    }
}

fn create_function_name(
    fun: &String,
    distinct: bool,
    args: &[Expr],
    input_schema: &DFSchema,
) -> Result<String> {
    let names: Vec<String> = args
        .iter()
        .map(|e| create_name(e, input_schema))
        .collect::<Result<_>>()?;
    let distinct_str = match distinct {
        true => "DISTINCT ",
        false => "",
    };
    Ok(format!("{}({}{})", fun, distinct_str, names.join(",")))
}

/// Returns a readable name of an expression based on the input schema.
/// This function recursively transverses the expression for names such as "CAST(a > 2)".
fn create_name(e: &Expr, input_schema: &DFSchema) -> Result<String> {
    match e {
        Expr::Alias(_, name) => Ok(name.clone()),
        Expr::Column(name) => Ok(name.clone()),
        Expr::ScalarVariable(variable_names) => Ok(variable_names.join(".")),
        Expr::Literal(value) => Ok(format!("{:?}", value)),
        Expr::BinaryExpr { left, op, right } => {
            let left = create_name(left, input_schema)?;
            let right = create_name(right, input_schema)?;
            Ok(format!("{} {:?} {}", left, op, right))
        }
        Expr::Case {
            expr,
            when_then_expr,
            else_expr,
        } => {
            let mut name = "CASE ".to_string();
            if let Some(e) = expr {
                name += &format!("{:?} ", e);
            }
            for (w, t) in when_then_expr {
                name += &format!("WHEN {:?} THEN {:?} ", w, t);
            }
            if let Some(e) = else_expr {
                name += &format!("ELSE {:?} ", e);
            }
            name += "END";
            Ok(name)
        }
        Expr::Cast { expr, data_type } => {
            let expr = create_name(expr, input_schema)?;
            Ok(format!("CAST({} AS {:?})", expr, data_type))
        }
        Expr::Not(expr) => {
            let expr = create_name(expr, input_schema)?;
            Ok(format!("NOT {}", expr))
        }
        Expr::IsNull(expr) => {
            let expr = create_name(expr, input_schema)?;
            Ok(format!("{} IS NULL", expr))
        }
        Expr::IsNotNull(expr) => {
            let expr = create_name(expr, input_schema)?;
            Ok(format!("{} IS NOT NULL", expr))
        }
        Expr::ScalarFunction { fun, args, .. } => {
            create_function_name(&fun.to_string(), false, args, input_schema)
        }
        Expr::ScalarUDF { fun, args, .. } => {
            create_function_name(&fun.name, false, args, input_schema)
        }
        Expr::AggregateFunction {
            fun,
            distinct,
            args,
            ..
        } => create_function_name(&fun.to_string(), *distinct, args, input_schema),
        Expr::AggregateUDF { fun, args } => {
            let mut names = Vec::with_capacity(args.len());
            for e in args {
                names.push(create_name(e, input_schema)?);
            }
            Ok(format!("{}({})", fun.name, names.join(",")))
        }
        other => Err(DataFusionError::NotImplemented(format!(
            "Physical plan does not support logical expression {:?}",
            other
        ))),
    }
}

/// Create field meta-data from an expression, for use in a result set schema
pub fn exprlist_to_fields(
    expr: &[Expr],
    input_schema: &DFSchema,
) -> Result<Vec<DFField>> {
    expr.iter().map(|e| e.to_field(input_schema)).collect()
}

#[cfg(test)]
mod tests {
    use super::super::{col, lit, when};
    use super::*;

    #[test]
    fn case_when_same_literal_then_types() -> Result<()> {
        let _ = when(col("state").eq(lit("CO")), lit(303))
            .when(col("state").eq(lit("NY")), lit(212))
            .end()?;
        Ok(())
    }

    #[test]
    fn case_when_different_literal_then_types() -> Result<()> {
        let maybe_expr = when(col("state").eq(lit("CO")), lit(303))
            .when(col("state").eq(lit("NY")), lit("212"))
            .end();
        assert!(maybe_expr.is_err());
        Ok(())
    }
}
