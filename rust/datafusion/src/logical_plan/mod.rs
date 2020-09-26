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

use fmt::Debug;
use std::{any::Any, collections::HashSet, fmt, sync::Arc};

use aggregates::{AccumulatorFunctionImplementation, StateTypeFunction};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

use crate::datasource::parquet::ParquetTable;
use crate::datasource::TableProvider;
use crate::error::{ExecutionError, Result};
use crate::{
    datasource::csv::{CsvFile, CsvReadOptions},
    physical_plan::udaf::AggregateUDF,
    scalar::ScalarValue,
};
use crate::{
    physical_plan::{
        aggregates, expressions::binary_operator_data_type, functions,
        type_coercion::can_coerce_from, udf::ScalarUDF,
    },
    sql::parser::FileType,
};
use arrow::record_batch::RecordBatch;
use functions::{ReturnTypeFunction, ScalarFunctionImplementation, Signature};

mod operators;
pub use operators::Operator;

fn create_function_name(
    fun: &String,
    args: &[Expr],
    input_schema: &Schema,
) -> Result<String> {
    let names: Vec<String> = args
        .iter()
        .map(|e| create_name(e, input_schema))
        .collect::<Result<_>>()?;
    Ok(format!("{}({})", fun, names.join(",")))
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
            create_function_name(&fun.to_string(), args, input_schema)
        }
        Expr::ScalarUDF { fun, args, .. } => {
            create_function_name(&fun.name, args, input_schema)
        }
        Expr::AggregateFunction { fun, args, .. } => {
            create_function_name(&fun.to_string(), args, input_schema)
        }
        Expr::AggregateUDF { fun, args } => {
            let mut names = Vec::with_capacity(args.len());
            for e in args {
                names.push(create_name(e, input_schema)?);
            }
            Ok(format!("{}({})", fun.name, names.join(",")))
        }
        other => Err(ExecutionError::NotImplemented(format!(
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
            Expr::Wildcard => Err(ExecutionError::General(
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
            Expr::Wildcard => Err(ExecutionError::General(
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
    /// This function errors when it is impossible to cast the expression to the target [arrow::datatypes::DataType].
    pub fn cast_to(&self, cast_to_type: &DataType, schema: &Schema) -> Result<Expr> {
        let this_type = self.get_type(schema)?;
        if this_type == *cast_to_type {
            Ok(self.clone())
        } else if can_coerce_from(cast_to_type, &this_type) {
            Ok(Expr::Cast {
                expr: Box::new(self.clone()),
                data_type: cast_to_type.clone(),
            })
        } else {
            Err(ExecutionError::General(format!(
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
        args: vec![expr],
    }
}

/// Create an expression to represent the max() aggregate function
pub fn max(expr: Expr) -> Expr {
    Expr::AggregateFunction {
        fun: aggregates::AggregateFunction::Max,
        args: vec![expr],
    }
}

/// Create an expression to represent the sum() aggregate function
pub fn sum(expr: Expr) -> Expr {
    Expr::AggregateFunction {
        fun: aggregates::AggregateFunction::Sum,
        args: vec![expr],
    }
}

/// Create an expression to represent the avg() aggregate function
pub fn avg(expr: Expr) -> Expr {
    Expr::AggregateFunction {
        fun: aggregates::AggregateFunction::Avg,
        args: vec![expr],
    }
}

/// Create an expression to represent the count() aggregate function
pub fn count(expr: Expr) -> Expr {
    Expr::AggregateFunction {
        fun: aggregates::AggregateFunction::Count,
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

fn fmt_function(f: &mut fmt::Formatter, fun: &String, args: &Vec<Expr>) -> fmt::Result {
    let args: Vec<String> = args.iter().map(|arg| format!("{:?}", arg)).collect();
    write!(f, "{}({})", fun, args.join(", "))
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
                fmt_function(f, &fun.to_string(), args)
            }
            Expr::ScalarUDF { fun, ref args, .. } => fmt_function(f, &fun.name, args),
            Expr::AggregateFunction { fun, ref args, .. } => {
                fmt_function(f, &fun.to_string(), args)
            }
            Expr::AggregateUDF { fun, ref args, .. } => fmt_function(f, &fun.name, args),
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
/// example of how to use this extenison API
pub trait UserDefinedLogicalNode: Debug {
    /// Return a reference to self as Any, to support dynamic downcasting
    fn as_any(&self) -> &dyn Any;

    /// Return the the logical plan's inputs
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
    /// Produces rows from a table that has been registered on a
    /// context
    TableScan {
        /// The name of the schema
        schema_name: String,
        /// The name of the table
        table_name: String,
        /// The schema of the CSV file(s)
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

impl LogicalPlan {
    fn fmt_with_indent(&self, f: &mut fmt::Formatter, indent: usize) -> fmt::Result {
        if indent > 0 {
            writeln!(f)?;
            for _ in 0..indent {
                write!(f, "  ")?;
            }
        }
        match *self {
            LogicalPlan::EmptyRelation { .. } => write!(f, "EmptyRelation"),
            LogicalPlan::TableScan {
                ref table_name,
                ref projection,
                ..
            } => write!(f, "TableScan: {} projection={:?}", table_name, projection),
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
            LogicalPlan::Projection {
                ref expr,
                ref input,
                ..
            } => {
                write!(f, "Projection: ")?;
                for i in 0..expr.len() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{:?}", expr[i])?;
                }
                input.fmt_with_indent(f, indent + 1)
            }
            LogicalPlan::Filter {
                predicate: ref expr,
                ref input,
                ..
            } => {
                write!(f, "Filter: {:?}", expr)?;
                input.fmt_with_indent(f, indent + 1)
            }
            LogicalPlan::Aggregate {
                ref input,
                ref group_expr,
                ref aggr_expr,
                ..
            } => {
                write!(
                    f,
                    "Aggregate: groupBy=[{:?}], aggr=[{:?}]",
                    group_expr, aggr_expr
                )?;
                input.fmt_with_indent(f, indent + 1)
            }
            LogicalPlan::Sort {
                ref input,
                ref expr,
                ..
            } => {
                write!(f, "Sort: ")?;
                for i in 0..expr.len() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{:?}", expr[i])?;
                }
                input.fmt_with_indent(f, indent + 1)
            }
            LogicalPlan::Limit {
                ref input, ref n, ..
            } => {
                write!(f, "Limit: {}", n)?;
                input.fmt_with_indent(f, indent + 1)
            }
            LogicalPlan::CreateExternalTable { ref name, .. } => {
                write!(f, "CreateExternalTable: {:?}", name)
            }
            LogicalPlan::Explain { ref plan, .. } => {
                write!(f, "Explain")?;
                plan.fmt_with_indent(f, indent + 1)
            }
            LogicalPlan::Extension { ref node } => {
                node.fmt_for_explain(f)?;
                node.inputs()
                    .iter()
                    .map(|input| input.fmt_with_indent(f, indent + 1))
                    .collect()
            }
        }
    }
}

impl fmt::Debug for LogicalPlan {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.fmt_with_indent(f, 0)
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
            table_name: table_name.to_owned(),
            table_schema,
            projected_schema,
            projection,
        }))
    }

    /// Apply a projection
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
}
