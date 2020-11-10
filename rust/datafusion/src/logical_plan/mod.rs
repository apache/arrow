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

//! This module provides a logical query plan enum that can describe queries. Logical query
//! plans can be created from a SQL statement or built programmatically via the Table API.
//!
//! Logical query plans can then be optimized and executed directly, or translated into
//! physical query plans and executed.

use std::fmt::{self, Debug, Display};
use std::{any::Any, collections::HashMap, collections::HashSet, sync::Arc};

use aggregates::{AccumulatorFunctionImplementation, StateTypeFunction};
use arrow::{
    compute::can_cast_types,
    datatypes::{DataType, Field, Schema, SchemaRef},
};

use crate::datasource::parquet::ParquetTable;
use crate::datasource::TableProvider;
use crate::error::{DataFusionError, Result};
use crate::{
    datasource::csv::{CsvFile, CsvReadOptions},
    physical_plan::udaf::AggregateUDF,
    scalar::ScalarValue,
};
use crate::{
    physical_plan::{
        aggregates, expressions::binary_operator_data_type, functions, udf::ScalarUDF,
    },
    sql::parser::FileType,
};
use arrow::record_batch::RecordBatch;
use functions::{ReturnTypeFunction, ScalarFunctionImplementation, Signature};

mod operators;
pub use operators::Operator;

fn create_function_name(
    fun: &String,
    distinct: bool,
    args: &[Expr],
    input_schema: &Schema,
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
fn create_name(e: &Expr, input_schema: &Schema) -> Result<String> {
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
pub fn exprlist_to_fields(expr: &[Expr], input_schema: &Schema) -> Result<Vec<Field>> {
    expr.iter().map(|e| e.to_field(input_schema)).collect()
}

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
    /// Parenthesized expression. E.g. `(foo > bar)` or `(1)`
    Nested(Box<Expr>),
    /// Negation of an expression. The expression's type must be a boolean to make sense.
    Not(Box<Expr>),
    /// Whether an expression is not Null. This expression is never null.
    IsNotNull(Box<Expr>),
    /// Whether an expression is Null. This expression is never null.
    IsNull(Box<Expr>),
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
    pub fn get_type(&self, schema: &Schema) -> Result<DataType> {
        match self {
            Expr::Alias(expr, _) => expr.get_type(schema),
            Expr::Column(name) => Ok(schema.field_with_name(name)?.data_type().clone()),
            Expr::ScalarVariable(_) => Ok(DataType::Utf8),
            Expr::Literal(l) => Ok(l.get_datatype()),
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
            Expr::Nested(e) => e.get_type(schema),
        }
    }

    /// Returns the nullability of the expression based on [arrow::datatypes::Schema].
    ///
    /// # Errors
    ///
    /// This function errors when it is not possible to compute its nullability.
    /// This happens when the expression refers to a column that does not exist in the schema.
    pub fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        match self {
            Expr::Alias(expr, _) => expr.nullable(input_schema),
            Expr::Column(name) => Ok(input_schema.field_with_name(name)?.is_nullable()),
            Expr::Literal(value) => Ok(value.is_null()),
            Expr::ScalarVariable(_) => Ok(true),
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
            Expr::Nested(e) => e.nullable(input_schema),
            Expr::Wildcard => Err(DataFusionError::Internal(
                "Wildcard expressions are not valid in a logical query plan".to_owned(),
            )),
        }
    }

    /// Returns the name of this expression based on [arrow::datatypes::Schema].
    ///
    /// This represents how a column with this expression is named when no alias is chosen
    pub fn name(&self, input_schema: &Schema) -> Result<String> {
        create_name(self, input_schema)
    }

    /// Returns a [arrow::datatypes::Field] compatible with this expression.
    pub fn to_field(&self, input_schema: &Schema) -> Result<Field> {
        Ok(Field::new(
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
    pub fn cast_to(&self, cast_to_type: &DataType, schema: &Schema) -> Result<Expr> {
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
        binary_expr(self.clone(), Operator::Eq, other.clone())
    }

    /// Not equal
    pub fn not_eq(&self, other: Expr) -> Expr {
        binary_expr(self.clone(), Operator::NotEq, other.clone())
    }

    /// Greater than
    pub fn gt(&self, other: Expr) -> Expr {
        binary_expr(self.clone(), Operator::Gt, other.clone())
    }

    /// Greater than or equal to
    pub fn gt_eq(&self, other: Expr) -> Expr {
        binary_expr(self.clone(), Operator::GtEq, other.clone())
    }

    /// Less than
    pub fn lt(&self, other: Expr) -> Expr {
        binary_expr(self.clone(), Operator::Lt, other.clone())
    }

    /// Less than or equal to
    pub fn lt_eq(&self, other: Expr) -> Expr {
        binary_expr(self.clone(), Operator::LtEq, other.clone())
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
        binary_expr(self.clone(), Operator::Modulus, other.clone())
    }

    /// like (string) another expression
    pub fn like(&self, other: Expr) -> Expr {
        binary_expr(self.clone(), Operator::Like, other.clone())
    }

    /// not like another expression
    pub fn not_like(&self, other: Expr) -> Expr {
        binary_expr(self.clone(), Operator::NotLike, other.clone())
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

fn binary_expr(l: Expr, op: Operator, r: Expr) -> Expr {
    Expr::BinaryExpr {
        left: Box::new(l),
        op,
        right: Box::new(r),
    }
}

/// return a new expression with a logical AND
pub fn and(left: &Expr, right: &Expr) -> Expr {
    Expr::BinaryExpr {
        left: Box::new(left.clone()),
        op: Operator::And,
        right: Box::new(right.clone()),
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
            Expr::Nested(expr) => write!(f, "({:?})", expr),
        }
    }
}

/// This defines the interface for `LogicalPlan` nodes that can be
/// used to extend DataFusion with custom relational operators.
///
/// See the example in
/// [user_defined_plan.rs](../../tests/user_defined_plan.rs) for an
/// example of how to use this extension API
pub trait UserDefinedLogicalNode: Debug {
    /// Return a reference to self as Any, to support dynamic downcasting
    fn as_any(&self) -> &dyn Any;

    /// Return the logical plan's inputs
    fn inputs(&self) -> Vec<&LogicalPlan>;

    /// Return the output schema of this logical plan node
    fn schema(&self) -> &SchemaRef;

    /// returns all expressions in the current logical plan node. This
    /// should not include expressions of any inputs (aka
    /// non-recursively) These expressions are used for optimizer
    /// passes and rewrites.
    fn expressions(&self) -> Vec<Expr>;

    /// A list of output columns (e.g. the names of columns in
    /// self.schema()) for which predicates can not be pushed below
    /// this node without changing the output.
    ///
    /// By default, this returns all columns and thus prevents any
    /// predicates from being pushed below this node.
    fn prevent_predicate_push_down_columns(&self) -> HashSet<String> {
        // default (safe) is all columns in the schema.
        self.schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect()
    }

    /// Write a single line, human readable string to `f` for use in explain plan
    ///
    /// For example: `TopK: k=10`
    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result;

    /// Create a new `ExtensionPlanNode` with the specified children
    /// and expressions. This function is used during optimization
    /// when the plan is being rewritten and a new instance of the
    /// `ExtensionPlanNode` must be created.
    ///
    /// Note that exprs and inputs are in the same order as the result
    /// of self.inputs and self.exprs.
    ///
    /// So, `self.from_template(exprs, ..).expressions() == exprs
    fn from_template(
        &self,
        exprs: &Vec<Expr>,
        inputs: &Vec<LogicalPlan>,
    ) -> Arc<dyn UserDefinedLogicalNode + Send + Sync>;
}

/// Describes the source of the table, either registered on the context or by reference
#[derive(Clone)]
pub enum TableSource {
    /// The source provider is registered in the context with the corresponding name
    FromContext(String),
    /// The source provider is passed directly by reference
    FromProvider(Arc<dyn TableProvider + Send + Sync>),
}

/// A LogicalPlan represents the different types of relational
/// operators (such as Projection, Filter, etc) and can be created by
/// the SQL query planner and the DataFrame API.
///
/// A LogicalPlan represents transforming an input relation (table) to
/// an output relation (table) with a (potentially) different
/// schema. A plan represents a dataflow tree where data flows
/// from leaves up to the root to produce the query result.
#[derive(Clone)]
pub enum LogicalPlan {
    /// Evaluates an arbitrary list of expressions (essentially a
    /// SELECT with an expression list) on its input.
    Projection {
        /// The list of expressions
        expr: Vec<Expr>,
        /// The incoming logical plan
        input: Arc<LogicalPlan>,
        /// The schema description of the output
        schema: SchemaRef,
    },
    /// Filters rows from its input that do not match an
    /// expression (essentially a WHERE clause with a predicate
    /// expression).
    ///
    /// Semantically, `<predicate>` is evaluated for each row of the input;
    /// If the value of `<predicate>` is true, the input row is passed to
    /// the output. If the value of `<predicate>` is false, the row is
    /// discarded.
    Filter {
        /// The predicate expression, which must have Boolean type.
        predicate: Expr,
        /// The incoming logical plan
        input: Arc<LogicalPlan>,
    },
    /// Aggregates its input based on a set of grouping and aggregate
    /// expressions (e.g. SUM).
    Aggregate {
        /// The incoming logical plan
        input: Arc<LogicalPlan>,
        /// Grouping expressions
        group_expr: Vec<Expr>,
        /// Aggregate expressions
        aggr_expr: Vec<Expr>,
        /// The schema description of the aggregate output
        schema: SchemaRef,
    },
    /// Sorts its input according to a list of sort expressions.
    Sort {
        /// The sort expressions
        expr: Vec<Expr>,
        /// The incoming logical plan
        input: Arc<LogicalPlan>,
    },
    /// Produces rows from a table provider by reference or from the context
    TableScan {
        /// The name of the schema
        schema_name: String,
        /// The source of the table
        source: TableSource,
        /// The schema of the source data
        table_schema: SchemaRef,
        /// Optional column indices to use as a projection
        projection: Option<Vec<usize>>,
        /// The schema description of the output
        projected_schema: SchemaRef,
    },
    /// Produces rows that come from a `Vec` of in memory `RecordBatch`es
    InMemoryScan {
        /// Record batch partitions
        data: Vec<Vec<RecordBatch>>,
        /// The schema of the record batches
        schema: SchemaRef,
        /// Optional column indices to use as a projection
        projection: Option<Vec<usize>>,
        /// The schema description of the output
        projected_schema: SchemaRef,
    },
    /// Produces rows by scanning Parquet file(s)
    ParquetScan {
        /// The path to the files
        path: String,
        /// The schema of the Parquet file(s)
        schema: SchemaRef,
        /// Optional column indices to use as a projection
        projection: Option<Vec<usize>>,
        /// The schema description of the output
        projected_schema: SchemaRef,
    },
    /// Produces rows by scanning a CSV file(s)
    CsvScan {
        /// The path to the files
        path: String,
        /// The underlying table schema
        schema: SchemaRef,
        /// Whether the CSV file(s) have a header containing column names
        has_header: bool,
        /// An optional column delimiter. Defaults to `b','`
        delimiter: Option<u8>,
        /// Optional column indices to use as a projection
        projection: Option<Vec<usize>>,
        /// The schema description of the output
        projected_schema: SchemaRef,
    },
    /// Produces no rows: An empty relation with an empty schema
    EmptyRelation {
        /// The schema description of the output
        schema: SchemaRef,
    },
    /// Produces the first `n` tuples from its input and discards the rest.
    Limit {
        /// The limit
        n: usize,
        /// The logical plan
        input: Arc<LogicalPlan>,
    },
    /// Creates an external table.
    CreateExternalTable {
        /// The table schema
        schema: SchemaRef,
        /// The table name
        name: String,
        /// The physical location
        location: String,
        /// The file type of physical file
        file_type: FileType,
        /// Whether the CSV file contains a header
        has_header: bool,
    },
    /// Produces a relation with string representations of
    /// various parts of the plan
    Explain {
        /// Should extra (detailed, intermediate plans) be included?
        verbose: bool,
        /// The logical plan that is being EXPLAIN'd
        plan: Arc<LogicalPlan>,
        /// Represent the various stages plans have gone through
        stringified_plans: Vec<StringifiedPlan>,
        /// The output schema of the explain (2 columns of text)
        schema: SchemaRef,
    },
    /// Extension operator defined outside of DataFusion
    Extension {
        /// The runtime extension operator
        node: Arc<dyn UserDefinedLogicalNode + Send + Sync>,
    },
}

impl LogicalPlan {
    /// Get a reference to the logical plan's schema
    pub fn schema(&self) -> &SchemaRef {
        match self {
            LogicalPlan::EmptyRelation { schema } => &schema,
            LogicalPlan::InMemoryScan {
                projected_schema, ..
            } => &projected_schema,
            LogicalPlan::CsvScan {
                projected_schema, ..
            } => &projected_schema,
            LogicalPlan::ParquetScan {
                projected_schema, ..
            } => &projected_schema,
            LogicalPlan::TableScan {
                projected_schema, ..
            } => &projected_schema,
            LogicalPlan::Projection { schema, .. } => &schema,
            LogicalPlan::Filter { input, .. } => input.schema(),
            LogicalPlan::Aggregate { schema, .. } => &schema,
            LogicalPlan::Sort { input, .. } => input.schema(),
            LogicalPlan::Limit { input, .. } => input.schema(),
            LogicalPlan::CreateExternalTable { schema, .. } => &schema,
            LogicalPlan::Explain { schema, .. } => &schema,
            LogicalPlan::Extension { node } => &node.schema(),
        }
    }

    /// Returns the (fixed) output schema for explain plans
    pub fn explain_schema() -> SchemaRef {
        SchemaRef::new(Schema::new(vec![
            Field::new("plan_type", DataType::Utf8, false),
            Field::new("plan", DataType::Utf8, false),
        ]))
    }
}

/// Trait that implements the [Visitor
/// pattern](https://en.wikipedia.org/wiki/Visitor_pattern) for a
/// depth first walk of `LogicalPlan` nodes. `pre_visit` is called
/// before any children are visited, and then `post_visit` is called
/// after all children have been visited.
////
/// To use, define a struct that implements this trait and then invoke
/// "LogicalPlan::accept".
///
/// For example, for a logical plan like:
///
/// Projection: #id
///    Filter: #state Eq Utf8(\"CO\")\
///       CsvScan: employee.csv projection=Some([0, 3])";
///
/// The sequence of visit operations would be:
/// ```text
/// visitor.pre_visit(Projection)
/// visitor.pre_visit(Filter)
/// visitor.pre_visit(CsvScan)
/// visitor.post_visit(CsvScan)
/// visitor.post_visit(Filter)
/// visitor.post_visit(Projection)
/// ```
pub trait PlanVisitor {
    /// The type of error returned by this visitor
    type Error;

    /// Invoked on a logical plan before any of its child inputs have been
    /// visited. If Ok(true) is returned, the recursion continues. If
    /// Err(..) or Ok(false) are returned, the recursion stops
    /// immedately and the error, if any, is returned to `accept`
    fn pre_visit(&mut self, plan: &LogicalPlan)
        -> std::result::Result<bool, Self::Error>;

    /// Invoked on a logical plan after all of its child inputs have
    /// been visited. The return value is handled the same as the
    /// return value of `pre_visit`. The provided default implementation
    /// returns `Ok(true)`.
    fn post_visit(
        &mut self,
        _plan: &LogicalPlan,
    ) -> std::result::Result<bool, Self::Error> {
        Ok(true)
    }
}

impl LogicalPlan {
    /// returns all inputs in the logical plan. Returns Ok(true) if
    /// all nodes were visited, and Ok(false) if any call to
    /// `pre_visit` or `post_visit` returned Ok(false) and may have
    /// cut short the recursion
    pub fn accept<V>(&self, visitor: &mut V) -> std::result::Result<bool, V::Error>
    where
        V: PlanVisitor,
    {
        if !visitor.pre_visit(self)? {
            return Ok(false);
        }

        let recurse = match self {
            LogicalPlan::Projection { input, .. } => input.accept(visitor)?,
            LogicalPlan::Filter { input, .. } => input.accept(visitor)?,
            LogicalPlan::Aggregate { input, .. } => input.accept(visitor)?,
            LogicalPlan::Sort { input, .. } => input.accept(visitor)?,
            LogicalPlan::Limit { input, .. } => input.accept(visitor)?,
            LogicalPlan::Extension { node } => {
                for input in node.inputs() {
                    if !input.accept(visitor)? {
                        return Ok(false);
                    }
                }
                true
            }
            // plans without inputs
            LogicalPlan::TableScan { .. }
            | LogicalPlan::InMemoryScan { .. }
            | LogicalPlan::ParquetScan { .. }
            | LogicalPlan::CsvScan { .. }
            | LogicalPlan::EmptyRelation { .. }
            | LogicalPlan::CreateExternalTable { .. }
            | LogicalPlan::Explain { .. } => true,
        };
        if !recurse {
            return Ok(false);
        }

        if !visitor.post_visit(self)? {
            return Ok(false);
        }

        Ok(true)
    }
}

/// Formats plans with a single line per node. For example:
///
/// Projection: #id
///    Filter: #state Eq Utf8(\"CO\")\
///       CsvScan: employee.csv projection=Some([0, 3])";
struct IndentVisitor<'a, 'b> {
    f: &'a mut fmt::Formatter<'b>,
    /// If true, includes summarized schema information
    with_schema: bool,
    indent: u32,
}

impl<'a, 'b> IndentVisitor<'a, 'b> {
    fn write_indent(&mut self) -> fmt::Result {
        for _ in 0..self.indent {
            write!(self.f, "  ")?;
        }
        Ok(())
    }
}

impl<'a, 'b> PlanVisitor for IndentVisitor<'a, 'b> {
    type Error = fmt::Error;

    fn pre_visit(&mut self, plan: &LogicalPlan) -> std::result::Result<bool, fmt::Error> {
        if self.indent > 0 {
            writeln!(self.f)?;
        }
        self.write_indent()?;

        write!(self.f, "{}", plan.display())?;
        if self.with_schema {
            write!(self.f, " {}", display_schema(plan.schema()))?;
        }

        self.indent += 1;
        Ok(true)
    }

    fn post_visit(
        &mut self,
        _plan: &LogicalPlan,
    ) -> std::result::Result<bool, fmt::Error> {
        self.indent -= 1;
        Ok(true)
    }
}

/// Print the schema in a compact representation to `buf`
///
/// For example: `foo:Utf8` if `foo` can not be null, and
/// `foo:Utf8;N` if `foo` is nullable.
///
/// ```
/// use arrow::datatypes::{Field, Schema, DataType};
/// # use datafusion::logical_plan::display_schema;
/// let schema = Schema::new(vec![
///     Field::new("id", DataType::Int32, false),
///     Field::new("first_name", DataType::Utf8, true),
///  ]);
///
///  assert_eq!(
///      "[id:Int32, first_name:Utf8;N]",
///      format!("{}", display_schema(&schema))
///  );
/// ```
pub fn display_schema<'a>(schema: &'a Schema) -> impl fmt::Display + 'a {
    struct Wrapper<'a>(&'a Schema);

    impl<'a> fmt::Display for Wrapper<'a> {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "[")?;
            for (idx, field) in self.0.fields().iter().enumerate() {
                if idx > 0 {
                    write!(f, ", ")?;
                }
                let nullable_str = if field.is_nullable() { ";N" } else { "" };
                write!(
                    f,
                    "{}:{:?}{}",
                    field.name(),
                    field.data_type(),
                    nullable_str
                )?;
            }
            write!(f, "]")
        }
    }
    Wrapper(schema)
}

/// Logic related to creating DOT language graphs.
#[derive(Default)]
struct GraphvizBuilder {
    id_gen: usize,
}

impl GraphvizBuilder {
    fn next_id(&mut self) -> usize {
        self.id_gen += 1;
        self.id_gen
    }

    // write out the start of the subgraph cluster
    fn start_cluster(&mut self, f: &mut fmt::Formatter, title: &str) -> fmt::Result {
        writeln!(f, "  subgraph cluster_{}", self.next_id())?;
        writeln!(f, "  {{")?;
        writeln!(f, "    graph[label={}]", Self::quoted(title))
    }

    // write out the end of the subgraph cluster
    fn end_cluster(&mut self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "  }}")
    }

    /// makes a quoted string suitable for inclusion in a graphviz chart
    fn quoted(label: &str) -> String {
        let label = label.replace('"', "_");
        format!("\"{}\"", label)
    }
}

/// Formats plans for graphical display using the `DOT` language. This
/// format can be visualized using software from
/// [`graphviz`](https://graphviz.org/)
struct GraphvizVisitor<'a, 'b> {
    f: &'a mut fmt::Formatter<'b>,
    graphviz_builder: GraphvizBuilder,
    /// If true, includes summarized schema information
    with_schema: bool,

    /// Holds the ids (as generated from `graphviz_builder` of all
    /// parent nodes
    parent_ids: Vec<usize>,
}

impl<'a, 'b> GraphvizVisitor<'a, 'b> {
    fn new(f: &'a mut fmt::Formatter<'b>) -> Self {
        Self {
            f,
            graphviz_builder: GraphvizBuilder::default(),
            with_schema: false,
            parent_ids: Vec::new(),
        }
    }

    /// Sets a flag which controls if the output schema is displayed
    fn set_with_schema(&mut self, with_schema: bool) {
        self.with_schema = with_schema;
    }

    fn pre_visit_plan(&mut self, label: &str) -> fmt::Result {
        self.graphviz_builder.start_cluster(self.f, label)
    }

    fn post_visit_plan(&mut self) -> fmt::Result {
        self.graphviz_builder.end_cluster(self.f)
    }
}

impl<'a, 'b> PlanVisitor for GraphvizVisitor<'a, 'b> {
    type Error = fmt::Error;

    fn pre_visit(&mut self, plan: &LogicalPlan) -> std::result::Result<bool, fmt::Error> {
        let id = self.graphviz_builder.next_id();

        // Create a new graph node for `plan` such as
        // id [label="foo"]
        let label = if self.with_schema {
            format!(
                "{}\\nSchema: {}",
                plan.display(),
                display_schema(plan.schema())
            )
        } else {
            format!("{}", plan.display())
        };

        writeln!(
            self.f,
            "    {}[shape=box label={}]",
            id,
            GraphvizBuilder::quoted(&label)
        )?;

        // Create an edge to our parent node, if any
        //  parent_id -> id
        if let Some(parent_id) = self.parent_ids.last() {
            writeln!(
                self.f,
                "    {} -> {} [arrowhead=none, arrowtail=normal, dir=back]",
                parent_id, id
            )?;
        }

        self.parent_ids.push(id);
        Ok(true)
    }

    fn post_visit(
        &mut self,
        _plan: &LogicalPlan,
    ) -> std::result::Result<bool, fmt::Error> {
        // always be non-empty as pre_visit always pushes
        self.parent_ids.pop().unwrap();
        Ok(true)
    }
}

// Various implementations for printing out LogicalPlans
impl LogicalPlan {
    /// Return a `format`able structure that produces a single line
    /// per node. For example:
    ///
    /// ```text
    /// Projection: #id
    ///    Filter: #state Eq Utf8(\"CO\")\
    ///       CsvScan: employee.csv projection=Some([0, 3])
    /// ```
    ///
    /// ```
    /// use arrow::datatypes::{Field, Schema, DataType};
    /// use datafusion::logical_plan::{lit, col, LogicalPlanBuilder};
    /// let schema = Schema::new(vec![
    ///     Field::new("id", DataType::Int32, false),
    /// ]);
    /// let plan = LogicalPlanBuilder::scan("default", "foo.csv", &schema, None).unwrap()
    ///     .filter(col("id").eq(lit(5))).unwrap()
    ///     .build().unwrap();
    ///
    /// // Format using display_indent
    /// let display_string = format!("{}", plan.display_indent());
    ///
    /// assert_eq!("Filter: #id Eq Int32(5)\
    ///              \n  TableScan: foo.csv projection=None",
    ///             display_string);
    /// ```
    pub fn display_indent<'a>(&'a self) -> impl fmt::Display + 'a {
        // Boilerplate structure to wrap LogicalPlan with something
        // that that can be formatted
        struct Wrapper<'a>(&'a LogicalPlan);
        impl<'a> fmt::Display for Wrapper<'a> {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                let with_schema = false;
                let mut visitor = IndentVisitor {
                    f,
                    with_schema,
                    indent: 0,
                };
                self.0.accept(&mut visitor).unwrap();
                Ok(())
            }
        }
        Wrapper(self)
    }

    /// Return a `format`able structure that produces a single line
    /// per node that includes the output schema. For example:
    ///
    /// ```text
    /// Projection: #id [id:Int32]\
    ///    Filter: #state Eq Utf8(\"CO\") [id:Int32, state:Utf8]\
    ///      TableScan: employee.csv projection=Some([0, 3]) [id:Int32, state:Utf8]";
    /// ```
    ///
    /// ```
    /// use arrow::datatypes::{Field, Schema, DataType};
    /// use datafusion::logical_plan::{lit, col, LogicalPlanBuilder};
    /// let schema = Schema::new(vec![
    ///     Field::new("id", DataType::Int32, false),
    /// ]);
    /// let plan = LogicalPlanBuilder::scan("default", "foo.csv", &schema, None).unwrap()
    ///     .filter(col("id").eq(lit(5))).unwrap()
    ///     .build().unwrap();
    ///
    /// // Format using display_indent_schema
    /// let display_string = format!("{}", plan.display_indent_schema());
    ///
    /// assert_eq!("Filter: #id Eq Int32(5) [id:Int32]\
    ///             \n  TableScan: foo.csv projection=None [id:Int32]",
    ///             display_string);
    /// ```
    pub fn display_indent_schema<'a>(&'a self) -> impl fmt::Display + 'a {
        // Boilerplate structure to wrap LogicalPlan with something
        // that that can be formatted
        struct Wrapper<'a>(&'a LogicalPlan);
        impl<'a> fmt::Display for Wrapper<'a> {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                let with_schema = true;
                let mut visitor = IndentVisitor {
                    f,
                    with_schema,
                    indent: 0,
                };
                self.0.accept(&mut visitor).unwrap();
                Ok(())
            }
        }
        Wrapper(self)
    }

    /// Return a `format`able structure that produces lines meant for
    /// graphical display using the `DOT` language. This format can be
    /// visualized using software from
    /// [`graphviz`](https://graphviz.org/)
    ///
    /// This currently produces two graphs -- one with the basic
    /// structure, and one with additional details such as schema.
    ///
    /// ```
    /// use arrow::datatypes::{Field, Schema, DataType};
    /// use datafusion::logical_plan::{lit, col, LogicalPlanBuilder};
    /// let schema = Schema::new(vec![
    ///     Field::new("id", DataType::Int32, false),
    /// ]);
    /// let plan = LogicalPlanBuilder::scan("default", "foo.csv", &schema, None).unwrap()
    ///     .filter(col("id").eq(lit(5))).unwrap()
    ///     .build().unwrap();
    ///
    /// // Format using display_graphviz
    /// let graphviz_string = format!("{}", plan.display_graphviz());
    /// ```
    ///
    /// If graphviz string is saved to a file such as `/tmp/example.dot`, the following
    /// commands can be used to render it as a pdf:
    ///
    /// ```bash
    ///   dot -Tpdf < /tmp/example.dot  > /tmp/example.pdf
    /// ```
    ///
    pub fn display_graphviz<'a>(&'a self) -> impl fmt::Display + 'a {
        // Boilerplate structure to wrap LogicalPlan with something
        // that that can be formatted
        struct Wrapper<'a>(&'a LogicalPlan);
        impl<'a> fmt::Display for Wrapper<'a> {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                writeln!(
                    f,
                    "// Begin DataFusion GraphViz Plan (see https://graphviz.org)"
                )?;
                writeln!(f, "digraph {{")?;

                let mut visitor = GraphvizVisitor::new(f);

                visitor.pre_visit_plan("LogicalPlan")?;
                self.0.accept(&mut visitor).unwrap();
                visitor.post_visit_plan()?;

                visitor.set_with_schema(true);
                visitor.pre_visit_plan("Detailed LogicalPlan")?;
                self.0.accept(&mut visitor).unwrap();
                visitor.post_visit_plan()?;

                writeln!(f, "}}")?;
                writeln!(f, "// End DataFusion GraphViz Plan")?;
                Ok(())
            }
        }
        Wrapper(self)
    }

    /// Return a `format`able structure with the a human readable
    /// description of this LogicalPlan node per node, not including
    /// children. For example:
    ///
    /// ```text
    /// Projection: #id
    /// ```
    /// ```
    /// use arrow::datatypes::{Field, Schema, DataType};
    /// use datafusion::logical_plan::{lit, col, LogicalPlanBuilder};
    /// let schema = Schema::new(vec![
    ///     Field::new("id", DataType::Int32, false),
    /// ]);
    /// let plan = LogicalPlanBuilder::scan("default", "foo.csv", &schema, None).unwrap()
    ///     .build().unwrap();
    ///
    /// // Format using display
    /// let display_string = format!("{}", plan.display());
    ///
    /// assert_eq!("TableScan: foo.csv projection=None", display_string);
    /// ```
    pub fn display<'a>(&'a self) -> impl fmt::Display + 'a {
        // Boilerplate structure to wrap LogicalPlan with something
        // that that can be formatted
        struct Wrapper<'a>(&'a LogicalPlan);
        impl<'a> fmt::Display for Wrapper<'a> {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                match *self.0 {
                    LogicalPlan::EmptyRelation { .. } => write!(f, "EmptyRelation"),
                    LogicalPlan::TableScan {
                        ref source,
                        ref projection,
                        ..
                    } => match source {
                        TableSource::FromContext(table_name) => write!(
                            f,
                            "TableScan: {} projection={:?}",
                            table_name, projection
                        ),
                        TableSource::FromProvider(_) => {
                            write!(f, "TableScan: projection={:?}", projection)
                        }
                    },
                    LogicalPlan::InMemoryScan { ref projection, .. } => {
                        write!(f, "InMemoryScan: projection={:?}", projection)
                    }
                    LogicalPlan::CsvScan {
                        ref path,
                        ref projection,
                        ..
                    } => write!(f, "CsvScan: {} projection={:?}", path, projection),
                    LogicalPlan::ParquetScan {
                        ref path,
                        ref projection,
                        ..
                    } => write!(f, "ParquetScan: {} projection={:?}", path, projection),
                    LogicalPlan::Projection { ref expr, .. } => {
                        write!(f, "Projection: ")?;
                        for i in 0..expr.len() {
                            if i > 0 {
                                write!(f, ", ")?;
                            }
                            write!(f, "{:?}", expr[i])?;
                        }
                        Ok(())
                    }
                    LogicalPlan::Filter {
                        predicate: ref expr,
                        ..
                    } => write!(f, "Filter: {:?}", expr),
                    LogicalPlan::Aggregate {
                        ref group_expr,
                        ref aggr_expr,
                        ..
                    } => write!(
                        f,
                        "Aggregate: groupBy=[{:?}], aggr=[{:?}]",
                        group_expr, aggr_expr
                    ),
                    LogicalPlan::Sort { ref expr, .. } => {
                        write!(f, "Sort: ")?;
                        for i in 0..expr.len() {
                            if i > 0 {
                                write!(f, ", ")?;
                            }
                            write!(f, "{:?}", expr[i])?;
                        }
                        Ok(())
                    }
                    LogicalPlan::Limit { ref n, .. } => write!(f, "Limit: {}", n),
                    LogicalPlan::CreateExternalTable { ref name, .. } => {
                        write!(f, "CreateExternalTable: {:?}", name)
                    }
                    LogicalPlan::Explain { .. } => write!(f, "Explain"),
                    LogicalPlan::Extension { ref node } => node.fmt_for_explain(f),
                }
            }
        }
        Wrapper(self)
    }
}

impl fmt::Debug for LogicalPlan {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.display_indent().fmt(f)
    }
}

/// A registry knows how to build logical expressions out of user-defined function' names
pub trait FunctionRegistry {
    /// Set of all available udfs.
    fn udfs(&self) -> HashSet<String>;

    /// Returns a reference to the udf named `name`.
    fn udf(&self, name: &str) -> Result<&ScalarUDF>;

    /// Returns a reference to the udaf named `name`.
    fn udaf(&self, name: &str) -> Result<&AggregateUDF>;
}

/// Builder for logical plans
pub struct LogicalPlanBuilder {
    plan: LogicalPlan,
}

impl LogicalPlanBuilder {
    /// Create a builder from an existing plan
    pub fn from(plan: &LogicalPlan) -> Self {
        Self { plan: plan.clone() }
    }

    /// Create an empty relation
    pub fn empty() -> Self {
        Self::from(&LogicalPlan::EmptyRelation {
            schema: SchemaRef::new(Schema::empty()),
        })
    }

    /// Scan a CSV data source
    pub fn scan_csv(
        path: &str,
        options: CsvReadOptions,
        projection: Option<Vec<usize>>,
    ) -> Result<Self> {
        let has_header = options.has_header;
        let delimiter = options.delimiter;
        let schema: Schema = match options.schema {
            Some(s) => s.to_owned(),
            None => CsvFile::try_new(path, options)?
                .schema()
                .as_ref()
                .to_owned(),
        };

        let projected_schema = SchemaRef::new(
            projection
                .clone()
                .map(|p| {
                    Schema::new(p.iter().map(|i| schema.field(*i).clone()).collect())
                })
                .or(Some(schema.clone()))
                .unwrap(),
        );

        Ok(Self::from(&LogicalPlan::CsvScan {
            path: path.to_owned(),
            schema: SchemaRef::new(schema),
            has_header,
            delimiter: Some(delimiter),
            projection,
            projected_schema,
        }))
    }

    /// Scan a Parquet data source
    pub fn scan_parquet(path: &str, projection: Option<Vec<usize>>) -> Result<Self> {
        let p = ParquetTable::try_new(path)?;
        let schema = p.schema().clone();

        let projected_schema = projection
            .clone()
            .map(|p| Schema::new(p.iter().map(|i| schema.field(*i).clone()).collect()));
        let projected_schema =
            projected_schema.map_or(schema.clone(), |s| SchemaRef::new(s));

        Ok(Self::from(&LogicalPlan::ParquetScan {
            path: path.to_owned(),
            schema,
            projection,
            projected_schema,
        }))
    }

    /// Scan a data source
    pub fn scan(
        schema_name: &str,
        table_name: &str,
        table_schema: &Schema,
        projection: Option<Vec<usize>>,
    ) -> Result<Self> {
        let table_schema = SchemaRef::new(table_schema.clone());
        let projected_schema = projection.clone().map(|p| {
            Schema::new(p.iter().map(|i| table_schema.field(*i).clone()).collect())
        });
        let projected_schema =
            projected_schema.map_or(table_schema.clone(), |s| SchemaRef::new(s));

        Ok(Self::from(&LogicalPlan::TableScan {
            schema_name: schema_name.to_owned(),
            source: TableSource::FromContext(table_name.to_owned()),
            table_schema,
            projected_schema,
            projection,
        }))
    }

    /// Apply a projection.
    ///
    /// # Errors
    /// This function errors under any of the following conditions:
    /// * Two or more expressions have the same name
    /// * An invalid expression is used (e.g. a `sort` expression)
    pub fn project(&self, expr: Vec<Expr>) -> Result<Self> {
        let input_schema = self.plan.schema();
        let mut projected_expr = vec![];
        (0..expr.len()).for_each(|i| match &expr[i] {
            Expr::Wildcard => {
                (0..input_schema.fields().len())
                    .for_each(|i| projected_expr.push(col(input_schema.field(i).name())));
            }
            _ => projected_expr.push(expr[i].clone()),
        });

        validate_unique_names("Projections", &projected_expr, input_schema)?;

        let schema = Schema::new(exprlist_to_fields(&projected_expr, input_schema)?);

        Ok(Self::from(&LogicalPlan::Projection {
            expr: projected_expr,
            input: Arc::new(self.plan.clone()),
            schema: SchemaRef::new(schema),
        }))
    }

    /// Apply a filter
    pub fn filter(&self, expr: Expr) -> Result<Self> {
        Ok(Self::from(&LogicalPlan::Filter {
            predicate: expr,
            input: Arc::new(self.plan.clone()),
        }))
    }

    /// Apply a limit
    pub fn limit(&self, n: usize) -> Result<Self> {
        Ok(Self::from(&LogicalPlan::Limit {
            n,
            input: Arc::new(self.plan.clone()),
        }))
    }

    /// Apply a sort
    pub fn sort(&self, expr: Vec<Expr>) -> Result<Self> {
        Ok(Self::from(&LogicalPlan::Sort {
            expr,
            input: Arc::new(self.plan.clone()),
        }))
    }

    /// Apply an aggregate
    pub fn aggregate(&self, group_expr: Vec<Expr>, aggr_expr: Vec<Expr>) -> Result<Self> {
        let mut all_expr: Vec<Expr> = group_expr.clone();
        aggr_expr.iter().for_each(|x| all_expr.push(x.clone()));

        validate_unique_names("Aggregations", &all_expr, self.plan.schema())?;

        let aggr_schema = Schema::new(exprlist_to_fields(&all_expr, self.plan.schema())?);

        Ok(Self::from(&LogicalPlan::Aggregate {
            input: Arc::new(self.plan.clone()),
            group_expr,
            aggr_expr,
            schema: SchemaRef::new(aggr_schema),
        }))
    }

    /// Create an expression to represent the explanation of the plan
    pub fn explain(&self, verbose: bool) -> Result<Self> {
        let stringified_plans = vec![StringifiedPlan::new(
            PlanType::LogicalPlan,
            format!("{:#?}", self.plan.clone()),
        )];

        let schema = LogicalPlan::explain_schema();

        Ok(Self::from(&LogicalPlan::Explain {
            verbose,
            plan: Arc::new(self.plan.clone()),
            stringified_plans,
            schema,
        }))
    }

    /// Build the plan
    pub fn build(&self) -> Result<LogicalPlan> {
        Ok(self.plan.clone())
    }
}

/// Errors if one or more expressions have equal names.
fn validate_unique_names(
    node_name: &str,
    expressions: &[Expr],
    input_schema: &Schema,
) -> Result<()> {
    let mut unique_names = HashMap::new();
    expressions.iter().enumerate().map(|(position, expr)| {
        let name = expr.name(input_schema)?;
        match unique_names.get(&name) {
            None => {
                unique_names.insert(name, (position, expr));
                Ok(())
            },
            Some((existing_position, existing_expr)) => {
                Err(DataFusionError::Plan(
                    format!("{} require unique expression names \
                             but the expression \"{:?}\" at position {} and \"{:?}\" \
                             at position {} have the same name. Consider aliasing (\"AS\") one of them.",
                             node_name, existing_expr, existing_position, expr, position,
                            )
                ))
            }
        }
    }).collect::<Result<()>>()
}

/// Represents which type of plan
#[derive(Debug, Clone, PartialEq)]
pub enum PlanType {
    /// The initial LogicalPlan provided to DataFusion
    LogicalPlan,
    /// The LogicalPlan which results from applying an optimizer pass
    OptimizedLogicalPlan {
        /// The name of the optimizer which produced this plan
        optimizer_name: String,
    },
    /// The physical plan, prepared for execution
    PhysicalPlan,
}

impl From<&PlanType> for String {
    fn from(t: &PlanType) -> Self {
        match t {
            PlanType::LogicalPlan => "logical_plan".into(),
            PlanType::OptimizedLogicalPlan { optimizer_name } => {
                format!("logical_plan after {}", optimizer_name)
            }
            PlanType::PhysicalPlan => "physical_plan".into(),
        }
    }
}

/// Represents some sort of execution plan, in String form
#[derive(Debug, Clone, PartialEq)]
pub struct StringifiedPlan {
    /// An identifier of what type of plan this string represents
    pub plan_type: PlanType,
    /// The string representation of the plan
    pub plan: Arc<String>,
}

impl StringifiedPlan {
    /// Create a new Stringified plan of `plan_type` with string
    /// representation `plan`
    pub fn new(plan_type: PlanType, plan: impl Into<String>) -> Self {
        StringifiedPlan {
            plan_type,
            plan: Arc::new(plan.into()),
        }
    }

    /// returns true if this plan should be displayed. Generally
    /// `verbose_mode = true` will display all available plans
    pub fn should_display(&self, verbose_mode: bool) -> bool {
        self.plan_type == PlanType::LogicalPlan || verbose_mode
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_builder_simple() -> Result<()> {
        let plan = LogicalPlanBuilder::scan(
            "default",
            "employee.csv",
            &employee_schema(),
            Some(vec![0, 3]),
        )?
        .filter(col("state").eq(lit("CO")))?
        .project(vec![col("id")])?
        .build()?;

        let expected = "Projection: #id\
        \n  Filter: #state Eq Utf8(\"CO\")\
        \n    TableScan: employee.csv projection=Some([0, 3])";

        assert_eq!(expected, format!("{:?}", plan));

        Ok(())
    }

    #[test]
    fn plan_builder_csv() -> Result<()> {
        let plan = LogicalPlanBuilder::scan_csv(
            "employee.csv",
            CsvReadOptions::new().schema(&employee_schema()),
            Some(vec![0, 3]),
        )?
        .filter(col("state").eq(lit("CO")))?
        .project(vec![col("id")])?
        .build()?;

        let expected = "Projection: #id\
        \n  Filter: #state Eq Utf8(\"CO\")\
        \n    CsvScan: employee.csv projection=Some([0, 3])";

        assert_eq!(expected, format!("{:?}", plan));

        Ok(())
    }

    #[test]
    fn plan_builder_aggregate() -> Result<()> {
        let plan = LogicalPlanBuilder::scan(
            "default",
            "employee.csv",
            &employee_schema(),
            Some(vec![3, 4]),
        )?
        .aggregate(
            vec![col("state")],
            vec![sum(col("salary")).alias("total_salary")],
        )?
        .project(vec![col("state"), col("total_salary")])?
        .build()?;

        let expected = "Projection: #state, #total_salary\
        \n  Aggregate: groupBy=[[#state]], aggr=[[SUM(#salary) AS total_salary]]\
        \n    TableScan: employee.csv projection=Some([3, 4])";

        assert_eq!(expected, format!("{:?}", plan));

        Ok(())
    }

    #[test]
    fn plan_builder_sort() -> Result<()> {
        let plan = LogicalPlanBuilder::scan(
            "default",
            "employee.csv",
            &employee_schema(),
            Some(vec![3, 4]),
        )?
        .sort(vec![
            Expr::Sort {
                expr: Box::new(col("state")),
                asc: true,
                nulls_first: true,
            },
            Expr::Sort {
                expr: Box::new(col("total_salary")),
                asc: false,
                nulls_first: false,
            },
        ])?
        .build()?;

        let expected = "Sort: #state ASC NULLS FIRST, #total_salary DESC NULLS LAST\
        \n  TableScan: employee.csv projection=Some([3, 4])";

        assert_eq!(expected, format!("{:?}", plan));

        Ok(())
    }

    #[test]
    fn projection_non_unique_names() -> Result<()> {
        let plan = LogicalPlanBuilder::scan(
            "default",
            "employee.csv",
            &employee_schema(),
            Some(vec![0, 3]),
        )?
        // two columns with the same name => error
        .project(vec![col("id"), col("first_name").alias("id")]);

        match plan {
            Err(DataFusionError::Plan(e)) => {
                assert_eq!(e, "Projections require unique expression names \
                    but the expression \"#id\" at position 0 and \"#first_name AS id\" at \
                    position 1 have the same name. Consider aliasing (\"AS\") one of them.");
                Ok(())
            }
            _ => Err(DataFusionError::Plan(
                "Plan should have returned an DataFusionError::Plan".to_string(),
            )),
        }
    }

    #[test]
    fn aggregate_non_unique_names() -> Result<()> {
        let plan = LogicalPlanBuilder::scan(
            "default",
            "employee.csv",
            &employee_schema(),
            Some(vec![0, 3]),
        )?
        // two columns with the same name => error
        .aggregate(vec![col("state")], vec![sum(col("salary")).alias("state")]);

        match plan {
            Err(DataFusionError::Plan(e)) => {
                assert_eq!(e, "Aggregations require unique expression names \
                    but the expression \"#state\" at position 0 and \"SUM(#salary) AS state\" at \
                    position 1 have the same name. Consider aliasing (\"AS\") one of them.");
                Ok(())
            }
            _ => Err(DataFusionError::Plan(
                "Plan should have returned an DataFusionError::Plan".to_string(),
            )),
        }
    }

    fn employee_schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("first_name", DataType::Utf8, false),
            Field::new("last_name", DataType::Utf8, false),
            Field::new("state", DataType::Utf8, false),
            Field::new("salary", DataType::Int32, false),
        ])
    }

    #[test]
    fn stringified_plan() -> Result<()> {
        let stringified_plan =
            StringifiedPlan::new(PlanType::LogicalPlan, "...the plan...");
        assert!(stringified_plan.should_display(true));
        assert!(stringified_plan.should_display(false)); // display in non verbose mode too

        let stringified_plan =
            StringifiedPlan::new(PlanType::PhysicalPlan, "...the plan...");
        assert!(stringified_plan.should_display(true));
        assert!(!stringified_plan.should_display(false));

        let stringified_plan = StringifiedPlan::new(
            PlanType::OptimizedLogicalPlan {
                optimizer_name: "random opt pass".into(),
            },
            "...the plan...",
        );
        assert!(stringified_plan.should_display(true));
        assert!(!stringified_plan.should_display(false));

        Ok(())
    }

    #[test]
    fn test_visitor() {
        let schema = Schema::new(vec![]);
        assert_eq!("[]", format!("{}", display_schema(&schema)));
    }

    #[test]
    fn test_display_empty_schema() {
        let schema = Schema::new(vec![]);
        assert_eq!("[]", format!("{}", display_schema(&schema)));
    }

    #[test]
    fn test_display_schema() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("first_name", DataType::Utf8, true),
        ]);

        assert_eq!(
            "[id:Int32, first_name:Utf8;N]",
            format!("{}", display_schema(&schema))
        );
    }

    fn display_plan() -> LogicalPlan {
        LogicalPlanBuilder::scan(
            "default",
            "employee.csv",
            &employee_schema(),
            Some(vec![0, 3]),
        )
        .unwrap()
        .filter(col("state").eq(lit("CO")))
        .unwrap()
        .project(vec![col("id")])
        .unwrap()
        .build()
        .unwrap()
    }

    #[test]
    fn test_display_indent() {
        let plan = display_plan();

        let expected = "Projection: #id\
        \n  Filter: #state Eq Utf8(\"CO\")\
        \n    TableScan: employee.csv projection=Some([0, 3])";

        assert_eq!(expected, format!("{}", plan.display_indent()));
    }

    #[test]
    fn test_display_indent_schema() {
        let plan = display_plan();

        let expected = "Projection: #id [id:Int32]\
                        \n  Filter: #state Eq Utf8(\"CO\") [id:Int32, state:Utf8]\
                        \n    TableScan: employee.csv projection=Some([0, 3]) [id:Int32, state:Utf8]";

        assert_eq!(expected, format!("{}", plan.display_indent_schema()));
    }

    #[test]
    fn test_display_graphviz() {
        let plan = display_plan();

        // just test for a few key lines in the output rather than the
        // whole thing to make test mainteance easier.
        let graphviz = format!("{}", plan.display_graphviz());

        assert!(
            graphviz.contains(
                r#"// Begin DataFusion GraphViz Plan (see https://graphviz.org)"#
            ),
            "\n{}",
            plan.display_graphviz()
        );
        assert!(
            graphviz.contains(
                r#"[shape=box label="TableScan: employee.csv projection=Some([0, 3])"]"#
            ),
            "\n{}",
            plan.display_graphviz()
        );
        assert!(graphviz.contains(r#"[shape=box label="TableScan: employee.csv projection=Some([0, 3])\nSchema: [id:Int32, state:Utf8]"]"#),
                "\n{}", plan.display_graphviz());
        assert!(
            graphviz.contains(r#"// End DataFusion GraphViz Plan"#),
            "\n{}",
            plan.display_graphviz()
        );
    }
}

#[cfg(test)]
/// Tests for the Visitor trait and walking logical plan nodes
mod test_visitor {
    use super::*;

    #[derive(Debug, Default)]
    struct OkVisitor {
        strings: Vec<String>,
    }
    impl PlanVisitor for OkVisitor {
        type Error = String;

        fn pre_visit(
            &mut self,
            plan: &LogicalPlan,
        ) -> std::result::Result<bool, Self::Error> {
            let s = match plan {
                LogicalPlan::Projection { .. } => "pre_visit Projection",
                LogicalPlan::Filter { .. } => "pre_visit Filter",
                LogicalPlan::TableScan { .. } => "pre_visit TableScan",
                _ => unimplemented!("unknown plan type"),
            };

            self.strings.push(s.into());
            Ok(true)
        }

        fn post_visit(
            &mut self,
            plan: &LogicalPlan,
        ) -> std::result::Result<bool, Self::Error> {
            let s = match plan {
                LogicalPlan::Projection { .. } => "post_visit Projection",
                LogicalPlan::Filter { .. } => "post_visit Filter",
                LogicalPlan::TableScan { .. } => "post_visit TableScan",
                _ => unimplemented!("unknown plan type"),
            };

            self.strings.push(s.into());
            Ok(true)
        }
    }

    #[test]
    fn visit_order() {
        let mut visitor = OkVisitor::default();
        let plan = test_plan();
        let res = plan.accept(&mut visitor);
        assert!(res.is_ok());

        assert_eq!(
            visitor.strings,
            vec![
                "pre_visit Projection",
                "pre_visit Filter",
                "pre_visit TableScan",
                "post_visit TableScan",
                "post_visit Filter",
                "post_visit Projection"
            ]
        );
    }

    #[derive(Debug, Default)]
    /// Counter than counts to zero and returns true when it gets there
    struct OptionalCounter {
        val: Option<usize>,
    }
    impl OptionalCounter {
        fn new(val: usize) -> Self {
            Self { val: Some(val) }
        }
        // Decrements the counter by 1, if any, returning true if it hits zero
        fn dec(&mut self) -> bool {
            if Some(0) == self.val {
                true
            } else {
                self.val = self.val.take().map(|i| i - 1);
                false
            }
        }
    }

    #[derive(Debug, Default)]
    /// Visitor that returns false after some number of visits
    struct StoppingVisitor {
        inner: OkVisitor,
        /// When Some(0) returns false from pre_visit
        return_false_from_pre_in: OptionalCounter,
        /// When Some(0) returns false from post_visit
        return_false_from_post_in: OptionalCounter,
    }

    impl PlanVisitor for StoppingVisitor {
        type Error = String;

        fn pre_visit(
            &mut self,
            plan: &LogicalPlan,
        ) -> std::result::Result<bool, Self::Error> {
            if self.return_false_from_pre_in.dec() {
                return Ok(false);
            }
            self.inner.pre_visit(plan)
        }

        fn post_visit(
            &mut self,
            plan: &LogicalPlan,
        ) -> std::result::Result<bool, Self::Error> {
            if self.return_false_from_post_in.dec() {
                return Ok(false);
            }

            self.inner.post_visit(plan)
        }
    }

    /// test earliy stopping in pre-visit
    #[test]
    fn early_stoping_pre_visit() {
        let mut visitor = StoppingVisitor::default();
        visitor.return_false_from_pre_in = OptionalCounter::new(2);
        let plan = test_plan();
        let res = plan.accept(&mut visitor);
        assert!(res.is_ok());

        assert_eq!(
            visitor.inner.strings,
            vec!["pre_visit Projection", "pre_visit Filter",]
        );
    }

    #[test]
    fn early_stoping_post_visit() {
        let mut visitor = StoppingVisitor::default();
        visitor.return_false_from_post_in = OptionalCounter::new(1);
        let plan = test_plan();
        let res = plan.accept(&mut visitor);
        assert!(res.is_ok());

        assert_eq!(
            visitor.inner.strings,
            vec![
                "pre_visit Projection",
                "pre_visit Filter",
                "pre_visit TableScan",
                "post_visit TableScan",
            ]
        );
    }

    #[derive(Debug, Default)]
    /// Visitor that returns an error after some number of visits
    struct ErrorVisitor {
        inner: OkVisitor,
        /// When Some(0) returns false from pre_visit
        return_error_from_pre_in: OptionalCounter,
        /// When Some(0) returns false from post_visit
        return_error_from_post_in: OptionalCounter,
    }

    impl PlanVisitor for ErrorVisitor {
        type Error = String;

        fn pre_visit(
            &mut self,
            plan: &LogicalPlan,
        ) -> std::result::Result<bool, Self::Error> {
            if self.return_error_from_pre_in.dec() {
                return Err("Error in pre_visit".into());
            }

            self.inner.pre_visit(plan)
        }

        fn post_visit(
            &mut self,
            plan: &LogicalPlan,
        ) -> std::result::Result<bool, Self::Error> {
            if self.return_error_from_post_in.dec() {
                return Err("Error in post_visit".into());
            }

            self.inner.post_visit(plan)
        }
    }

    #[test]
    fn error_pre_visit() {
        let mut visitor = ErrorVisitor::default();
        visitor.return_error_from_pre_in = OptionalCounter::new(2);
        let plan = test_plan();
        let res = plan.accept(&mut visitor);

        if let Err(e) = res {
            assert_eq!("Error in pre_visit", e);
        } else {
            panic!("Expected an error");
        }

        assert_eq!(
            visitor.inner.strings,
            vec!["pre_visit Projection", "pre_visit Filter",]
        );
    }

    #[test]
    fn error_post_visit() {
        let mut visitor = ErrorVisitor::default();
        visitor.return_error_from_post_in = OptionalCounter::new(1);
        let plan = test_plan();
        let res = plan.accept(&mut visitor);
        if let Err(e) = res {
            assert_eq!("Error in post_visit", e);
        } else {
            panic!("Expected an error");
        }

        assert_eq!(
            visitor.inner.strings,
            vec![
                "pre_visit Projection",
                "pre_visit Filter",
                "pre_visit TableScan",
                "post_visit TableScan",
            ]
        );
    }

    fn test_plan() -> LogicalPlan {
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);

        LogicalPlanBuilder::scan("default", "employee.csv", &schema, Some(vec![0]))
            .unwrap()
            .filter(col("state").eq(lit("CO")))
            .unwrap()
            .project(vec![col("id")])
            .unwrap()
            .build()
            .unwrap()
    }
}
