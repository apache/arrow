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

use std::{fmt, sync::Arc};

use arrow::datatypes::{DataType, Field, Schema};

use crate::datasource::csv::{CsvFile, CsvReadOptions};
use crate::datasource::parquet::ParquetTable;
use crate::datasource::TableProvider;
use crate::error::{ExecutionError, Result};
use crate::optimizer::utils;
use crate::sql::parser::FileType;
use arrow::record_batch::RecordBatch;

/// Enumeration of supported function types (Scalar and Aggregate)
#[derive(Debug, Clone)]
pub enum FunctionType {
    /// Simple function returning a value per DataFrame
    Scalar,
    /// Aggregate functions produce a value by sampling multiple DataFrames
    Aggregate,
}

/// Logical representation of a UDF (user-defined function)
#[derive(Debug, Clone)]
pub struct FunctionMeta {
    /// Function name
    name: String,
    /// Function arguments
    args: Vec<Field>,
    /// Function return type
    return_type: DataType,
    /// Function type (Scalar or Aggregate)
    function_type: FunctionType,
}

impl FunctionMeta {
    #[allow(missing_docs)]
    pub fn new(
        name: String,
        args: Vec<Field>,
        return_type: DataType,
        function_type: FunctionType,
    ) -> Self {
        FunctionMeta {
            name,
            args,
            return_type,
            function_type,
        }
    }
    /// Getter for the function name
    pub fn name(&self) -> &String {
        &self.name
    }
    /// Getter for the arg list
    pub fn args(&self) -> &Vec<Field> {
        &self.args
    }
    /// Getter for the `DataType` the function returns
    pub fn return_type(&self) -> &DataType {
        &self.return_type
    }
    /// Getter for the `FunctionType`
    pub fn function_type(&self) -> &FunctionType {
        &self.function_type
    }
}

/// Operators applied to expressions
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Operator {
    /// Expressions are equal
    Eq,
    /// Expressions are not equal
    NotEq,
    /// Left side is smaller than right side
    Lt,
    /// Left side is smaller or equal to right side
    LtEq,
    /// Left side is greater than right side
    Gt,
    /// Left side is greater or equal to right side
    GtEq,
    /// Addition
    Plus,
    /// Subtraction
    Minus,
    /// Multiplication operator, like `*`
    Multiply,
    /// Division operator, like `/`
    Divide,
    /// Remainder operator, like `%`
    Modulus,
    /// Logical AND, like `&&`
    And,
    /// Logical OR, like `||`
    Or,
    /// Logical NOT, like `!`
    Not,
    /// Matches a wildcard pattern
    Like,
    /// Does not match a wildcard pattern
    NotLike,
}

impl fmt::Display for Operator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let display = match &self {
            Operator::Eq => "=",
            Operator::NotEq => "!=",
            Operator::Lt => "<",
            Operator::LtEq => "<=",
            Operator::Gt => ">",
            Operator::GtEq => ">=",
            Operator::Plus => "+",
            Operator::Minus => "-",
            Operator::Multiply => "*",
            Operator::Divide => "/",
            Operator::Modulus => "%",
            Operator::And => "AND",
            Operator::Or => "OR",
            Operator::Not => "NOT",
            Operator::Like => "LIKE",
            Operator::NotLike => "NOT LIKE",
        };
        write!(f, "{}", display)
    }
}

/// ScalarValue enumeration
#[derive(Debug, Clone, PartialEq)]
pub enum ScalarValue {
    /// null value
    Null,
    /// true or false value
    Boolean(bool),
    /// 32bit float
    Float32(f32),
    /// 64bit float
    Float64(f64),
    /// signed 8bit int
    Int8(i8),
    /// signed 16bit int
    Int16(i16),
    /// signed 32bit int
    Int32(i32),
    /// signed 64bit int
    Int64(i64),
    /// unsigned 8bit int
    UInt8(u8),
    /// unsigned 16bit int
    UInt16(u16),
    /// unsigned 32bit int
    UInt32(u32),
    /// unsigned 64bit int
    UInt64(u64),
    /// utf-8 encoded string
    Utf8(String),
    /// List of scalars packed as a struct
    Struct(Vec<ScalarValue>),
}

impl ScalarValue {
    /// Getter for the `DataType` of the value
    pub fn get_datatype(&self) -> Result<DataType> {
        match *self {
            ScalarValue::Boolean(_) => Ok(DataType::Boolean),
            ScalarValue::UInt8(_) => Ok(DataType::UInt8),
            ScalarValue::UInt16(_) => Ok(DataType::UInt16),
            ScalarValue::UInt32(_) => Ok(DataType::UInt32),
            ScalarValue::UInt64(_) => Ok(DataType::UInt64),
            ScalarValue::Int8(_) => Ok(DataType::Int8),
            ScalarValue::Int16(_) => Ok(DataType::Int16),
            ScalarValue::Int32(_) => Ok(DataType::Int32),
            ScalarValue::Int64(_) => Ok(DataType::Int64),
            ScalarValue::Float32(_) => Ok(DataType::Float32),
            ScalarValue::Float64(_) => Ok(DataType::Float64),
            ScalarValue::Utf8(_) => Ok(DataType::Utf8),
            _ => Err(ExecutionError::General(format!(
                "Cannot treat {:?} as scalar value",
                self
            ))),
        }
    }
}

impl fmt::Display for ScalarValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ScalarValue::Boolean(value) => write!(f, "{}", value),
            ScalarValue::UInt8(value) => write!(f, "{}", value),
            ScalarValue::UInt16(value) => write!(f, "{}", value),
            ScalarValue::UInt32(value) => write!(f, "{}", value),
            ScalarValue::UInt64(value) => write!(f, "{}", value),
            ScalarValue::Int8(value) => write!(f, "{}", value),
            ScalarValue::Int16(value) => write!(f, "{}", value),
            ScalarValue::Int32(value) => write!(f, "{}", value),
            ScalarValue::Int64(value) => write!(f, "{}", value),
            ScalarValue::Float32(value) => write!(f, "{}", value),
            ScalarValue::Float64(value) => write!(f, "{}", value),
            ScalarValue::Utf8(value) => write!(f, "{}", value),
            ScalarValue::Null => write!(f, "NULL"),
            ScalarValue::Struct(_) => write!(f, "STRUCT"),
        }
    }
}

/// Returns a readable name of an expression based on the input schema.
/// This function recursively transverses the expression for names such as "CAST(a > 2)".
fn create_name(e: &Expr, input_schema: &Schema) -> Result<String> {
    match e {
        Expr::Alias(_, name) => Ok(name.clone()),
        Expr::Column(name) => Ok(name.clone()),
        Expr::Literal(value) => Ok(format!("{:?}", value)),
        Expr::BinaryExpr { left, op, right } => {
            let left = create_name(left, input_schema)?;
            let right = create_name(right, input_schema)?;
            Ok(format!("{} {:?} {}", left, op, right))
        }
        Expr::Cast { expr, data_type } => {
            let expr = create_name(expr, input_schema)?;
            Ok(format!("CAST({} as {:?})", expr, data_type))
        }
        Expr::ScalarFunction { name, args, .. } => {
            let mut names = Vec::with_capacity(args.len());
            for e in args {
                names.push(create_name(e, input_schema)?);
            }
            Ok(format!("{}({})", name, names.join(",")))
        }
        Expr::AggregateFunction { name, args, .. } => {
            let mut names = Vec::with_capacity(args.len());
            for e in args {
                names.push(create_name(e, input_schema)?);
            }
            Ok(format!("{}({})", name, names.join(",")))
        }
        other => Err(ExecutionError::NotImplemented(format!(
            "Physical plan does not support logical expression {:?}",
            other
        ))),
    }
}

/// Returns the datatype of the expression given the input schema
// note: the physical plan derived from an expression must match the datatype on this function.
pub fn expr_to_field(e: &Expr, input_schema: &Schema) -> Result<Field> {
    let data_type = e.get_type(input_schema)?;
    Ok(Field::new(&e.name(input_schema)?, data_type, true))
}

/// Create field meta-data from an expression, for use in a result set schema
pub fn exprlist_to_fields(expr: &[Expr], input_schema: &Schema) -> Result<Vec<Field>> {
    expr.iter()
        .map(|e| expr_to_field(e, input_schema))
        .collect()
}

/// Relation expression
#[derive(Clone, PartialEq)]
pub enum Expr {
    /// An aliased expression
    Alias(Box<Expr>, String),
    /// column of a table scan
    Column(String),
    /// literal value
    Literal(ScalarValue),
    /// binary expression e.g. "age > 21"
    BinaryExpr {
        /// Left-hand side of the expression
        left: Box<Expr>,
        /// The comparison operator
        op: Operator,
        /// Right-hand side of the expression
        right: Box<Expr>,
    },
    /// Nested expression e.g. `(foo > bar)` or `(1)`
    Nested(Box<Expr>),
    /// unary NOT
    Not(Box<Expr>),
    /// unary IS NOT NULL
    IsNotNull(Box<Expr>),
    /// unary IS NULL
    IsNull(Box<Expr>),
    /// cast a value to a different type
    Cast {
        /// The expression being cast
        expr: Box<Expr>,
        /// The `DataType` the expression will yield
        data_type: DataType,
    },
    /// sort expression
    Sort {
        /// The expression to sort on
        expr: Box<Expr>,
        /// The direction of the sort
        asc: bool,
        /// Whether to put Nulls before all other data values
        nulls_first: bool,
    },
    /// scalar function
    ScalarFunction {
        /// Name of the function
        name: String,
        /// List of expressions to feed to the functions as arguments
        args: Vec<Expr>,
        /// The `DataType` the expression will yield
        return_type: DataType,
    },
    /// aggregate function
    AggregateFunction {
        /// Name of the function
        name: String,
        /// List of expressions to feed to the functions as arguments
        args: Vec<Expr>,
    },
    /// Wildcard
    Wildcard,
}

impl Expr {
    /// Find the `DataType` for the expression
    pub fn get_type(&self, schema: &Schema) -> Result<DataType> {
        match self {
            Expr::Alias(expr, _) => expr.get_type(schema),
            Expr::Column(name) => Ok(schema.field_with_name(name)?.data_type().clone()),
            Expr::Literal(l) => l.get_datatype(),
            Expr::Cast { data_type, .. } => Ok(data_type.clone()),
            Expr::ScalarFunction { return_type, .. } => Ok(return_type.clone()),
            Expr::AggregateFunction { name, args, .. } => {
                match name.to_uppercase().as_str() {
                    "MIN" | "MAX" => args[0].get_type(schema),
                    "SUM" => match args[0].get_type(schema)? {
                        DataType::Int8
                        | DataType::Int16
                        | DataType::Int32
                        | DataType::Int64 => Ok(DataType::Int64),
                        DataType::UInt8
                        | DataType::UInt16
                        | DataType::UInt32
                        | DataType::UInt64 => Ok(DataType::UInt64),
                        DataType::Float32 => Ok(DataType::Float32),
                        DataType::Float64 => Ok(DataType::Float64),
                        other => Err(ExecutionError::General(format!(
                            "SUM does not support {:?}",
                            other
                        ))),
                    },
                    "AVG" => match args[0].get_type(schema)? {
                        DataType::Int8
                        | DataType::Int16
                        | DataType::Int32
                        | DataType::Int64
                        | DataType::UInt8
                        | DataType::UInt16
                        | DataType::UInt32
                        | DataType::UInt64
                        | DataType::Float32
                        | DataType::Float64 => Ok(DataType::Float64),
                        other => Err(ExecutionError::General(format!(
                            "AVG does not support {:?}",
                            other
                        ))),
                    },
                    "COUNT" => Ok(DataType::UInt64),
                    other => Err(ExecutionError::General(format!(
                        "Invalid aggregate function '{:?}'",
                        other
                    ))),
                }
            }
            Expr::Not(_) => Ok(DataType::Boolean),
            Expr::IsNull(_) => Ok(DataType::Boolean),
            Expr::IsNotNull(_) => Ok(DataType::Boolean),
            Expr::BinaryExpr {
                ref left,
                ref right,
                ref op,
            } => match op {
                Operator::Not => Ok(DataType::Boolean),
                Operator::Like | Operator::NotLike => Ok(DataType::Boolean),
                Operator::Eq | Operator::NotEq => Ok(DataType::Boolean),
                Operator::Lt | Operator::LtEq => Ok(DataType::Boolean),
                Operator::Gt | Operator::GtEq => Ok(DataType::Boolean),
                Operator::And | Operator::Or => Ok(DataType::Boolean),
                _ => {
                    let left_type = left.get_type(schema)?;
                    let right_type = right.get_type(schema)?;
                    utils::get_supertype(&left_type, &right_type)
                }
            },
            Expr::Sort { ref expr, .. } => expr.get_type(schema),
            Expr::Wildcard => Err(ExecutionError::General(
                "Wildcard expressions are not valid in a logical query plan".to_owned(),
            )),
            Expr::Nested(e) => e.get_type(schema),
        }
    }

    /// Return the name of this expression
    ///
    /// This represents how a column with this expression is named when no alias is chosen
    pub fn name(&self, input_schema: &Schema) -> Result<String> {
        create_name(self, input_schema)
    }

    /// Perform a type cast on the expression value.
    ///
    /// Will `Err` if the type cast cannot be performed.
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

    /// Add the specified expression
    pub fn plus(&self, other: Expr) -> Expr {
        binary_expr(self.clone(), Operator::Plus, other.clone())
    }

    /// Subtract the specified expression
    pub fn minus(&self, other: Expr) -> Expr {
        binary_expr(self.clone(), Operator::Minus, other.clone())
    }

    /// Multiply by the specified expression
    pub fn multiply(&self, other: Expr) -> Expr {
        binary_expr(self.clone(), Operator::Multiply, other.clone())
    }

    /// Divide by the specified expression
    pub fn divide(&self, other: Expr) -> Expr {
        binary_expr(self.clone(), Operator::Divide, other.clone())
    }

    /// Calculate the modulus of two expressions
    pub fn modulus(&self, other: Expr) -> Expr {
        binary_expr(self.clone(), Operator::Modulus, other.clone())
    }

    /// Alias
    pub fn alias(&self, name: &str) -> Expr {
        Expr::Alias(Box::new(self.clone()), name.to_owned())
    }

    /// Create a sort expression from an existing expression.
    ///
    /// ```
    /// # use datafusion::logicalplan::col;
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
pub fn min(expr: Expr) -> Result<Expr> {
    Ok(aggregate_expr("MIN", expr))
}

/// Create an expression to represent the max() aggregate function
pub fn max(expr: Expr) -> Result<Expr> {
    Ok(aggregate_expr("MAX", expr))
}

/// Create an expression to represent the sum() aggregate function
pub fn sum(expr: Expr) -> Result<Expr> {
    Ok(aggregate_expr("SUM", expr))
}

/// Create an expression to represent the avg() aggregate function
pub fn avg(expr: Expr) -> Result<Expr> {
    Ok(aggregate_expr("AVG", expr))
}

/// Create an expression to represent the count() aggregate function
pub fn count(expr: Expr) -> Result<Expr> {
    Ok(aggregate_expr("COUNT", expr))
}

/// Whether it can be represented as a literal expression
pub trait Literal {
    /// convert the value to a Literal expression
    fn lit(&self) -> Expr;
}

impl Literal for &str {
    fn lit(&self) -> Expr {
        Expr::Literal(ScalarValue::Utf8((*self).to_owned()))
    }
}

impl Literal for String {
    fn lit(&self) -> Expr {
        Expr::Literal(ScalarValue::Utf8((*self).to_owned()))
    }
}

macro_rules! make_literal {
    ($TYPE:ty, $SCALAR:ident) => {
        #[allow(missing_docs)]
        impl Literal for $TYPE {
            fn lit(&self) -> Expr {
                Expr::Literal(ScalarValue::$SCALAR(self.clone()))
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
    ($NAME:expr, $FUNC:ident) => {
        #[allow(missing_docs)]
        pub fn $FUNC(e: Expr) -> Expr {
            scalar_function($NAME, vec![e], DataType::Float64)
        }
    };
}

// generate methods for creating the supported unary math expressions
unary_math_expr!("sqrt", sqrt);
unary_math_expr!("sin", sin);
unary_math_expr!("cos", cos);
unary_math_expr!("tan", tan);
unary_math_expr!("asin", asin);
unary_math_expr!("acos", acos);
unary_math_expr!("atan", atan);
unary_math_expr!("floor", floor);
unary_math_expr!("ceil", ceil);
unary_math_expr!("round", round);
unary_math_expr!("trunc", trunc);
unary_math_expr!("abs", abs);
unary_math_expr!("signum", signum);
unary_math_expr!("exp", exp);
unary_math_expr!("log", ln);
unary_math_expr!("log2", log2);
unary_math_expr!("log10", log10);

/// returns the length of a string in bytes
pub fn length(e: Expr) -> Expr {
    scalar_function("length", vec![e], DataType::UInt32)
}

/// Create an aggregate expression
pub fn aggregate_expr(name: &str, expr: Expr) -> Expr {
    Expr::AggregateFunction {
        name: name.to_owned(),
        args: vec![expr],
    }
}

/// Create an aggregate expression
pub fn scalar_function(name: &str, expr: Vec<Expr>, return_type: DataType) -> Expr {
    Expr::ScalarFunction {
        name: name.to_owned(),
        args: expr,
        return_type,
    }
}

impl fmt::Debug for Expr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Expr::Alias(expr, alias) => write!(f, "{:?} AS {}", expr, alias),
            Expr::Column(name) => write!(f, "#{}", name),
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
            Expr::ScalarFunction { name, ref args, .. } => {
                write!(f, "{}(", name)?;
                for i in 0..args.len() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{:?}", args[i])?;
                }

                write!(f, ")")
            }
            Expr::AggregateFunction { name, ref args, .. } => {
                write!(f, "{}(", name)?;
                for i in 0..args.len() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{:?}", args[i])?;
                }

                write!(f, ")")
            }
            Expr::Wildcard => write!(f, "*"),
            Expr::Nested(expr) => write!(f, "({:?})", expr),
        }
    }
}

/// The LogicalPlan represents different types of relations (such as Projection,
/// Selection, etc) and can be created by the SQL query planner and the DataFrame API.
#[derive(Clone)]
pub enum LogicalPlan {
    /// Evaluates an arbitrary list of expressions (essentially a
    /// SELECT with an expression list) on its input.
    Projection {
        /// The list of expressions
        expr: Vec<Expr>,
        /// The incoming logical plan
        input: Box<LogicalPlan>,
        /// The schema description of the output
        schema: Box<Schema>,
    },
    /// Filters rows from its input that do not match an
    /// expression (essentially a WHERE clause with a predicate
    /// expression).
    ///
    /// Semantically, `<expr>` is evaluated for each row of the input;
    /// If the value of `<expr>` is true, the input row is passed to
    /// the output. If the value of `<expr>` is false, the row is
    /// discarded.
    Selection {
        /// The expression. Must have Boolean type.
        expr: Expr,
        /// The incoming logical plan
        input: Box<LogicalPlan>,
    },
    /// Aggregates its input based on a set of grouping and aggregate
    /// expressions (e.g. SUM).
    Aggregate {
        /// The incoming logical plan
        input: Box<LogicalPlan>,
        /// Grouping expressions
        group_expr: Vec<Expr>,
        /// Aggregate expressions
        aggr_expr: Vec<Expr>,
        /// The schema description of the aggregate output
        schema: Box<Schema>,
    },
    /// Sorts its input according to a list of sort expressions.
    Sort {
        /// The sort expressions
        expr: Vec<Expr>,
        /// The incoming logical plan
        input: Box<LogicalPlan>,
    },
    /// Produces rows from a table that has been registered on a
    /// context
    TableScan {
        /// The name of the schema
        schema_name: String,
        /// The name of the table
        table_name: String,
        /// The schema of the CSV file(s)
        table_schema: Box<Schema>,
        /// Optional column indices to use as a projection
        projection: Option<Vec<usize>>,
        /// The schema description of the output
        projected_schema: Box<Schema>,
    },
    /// Produces rows that come from a `Vec` of in memory `RecordBatch`es
    InMemoryScan {
        /// Record batch partitions
        data: Vec<Vec<RecordBatch>>,
        /// The schema of the record batches
        schema: Box<Schema>,
        /// Optional column indices to use as a projection
        projection: Option<Vec<usize>>,
        /// The schema description of the output
        projected_schema: Box<Schema>,
    },
    /// Produces rows by scanning Parquet file(s)
    ParquetScan {
        /// The path to the files
        path: String,
        /// The schema of the Parquet file(s)
        schema: Box<Schema>,
        /// Optional column indices to use as a projection
        projection: Option<Vec<usize>>,
        /// The schema description of the output
        projected_schema: Box<Schema>,
    },
    /// Produces rows by scanning a CSV file(s)
    CsvScan {
        /// The path to the files
        path: String,
        /// The underlying table schema
        schema: Box<Schema>,
        /// Whether the CSV file(s) have a header containing column names
        has_header: bool,
        /// An optional column delimiter. Defaults to `b','`
        delimiter: Option<u8>,
        /// Optional column indices to use as a projection
        projection: Option<Vec<usize>>,
        /// The schema description of the output
        projected_schema: Box<Schema>,
    },
    /// Produces no rows: An empty relation with an empty schema
    EmptyRelation {
        /// The schema description of the output
        schema: Box<Schema>,
    },
    /// Produces the first `n` tuples from its input and discards the rest.
    Limit {
        /// The limit
        n: usize,
        /// The logical plan
        input: Box<LogicalPlan>,
    },
    /// Creates an external table.
    CreateExternalTable {
        /// The table schema
        schema: Box<Schema>,
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
        plan: Box<LogicalPlan>,
        /// Represent the various stages plans have gone through
        stringified_plans: Vec<StringifiedPlan>,
        /// The output schema of the explain (2 columns of text)
        schema: Box<Schema>,
    },
}

impl LogicalPlan {
    /// Get a reference to the logical plan's schema
    pub fn schema(&self) -> &Box<Schema> {
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
            LogicalPlan::Selection { input, .. } => input.schema(),
            LogicalPlan::Aggregate { schema, .. } => &schema,
            LogicalPlan::Sort { input, .. } => input.schema(),
            LogicalPlan::Limit { input, .. } => input.schema(),
            LogicalPlan::CreateExternalTable { schema, .. } => &schema,
            LogicalPlan::Explain { schema, .. } => &schema,
        }
    }

    /// Returns the (fixed) output schema for explain plans
    pub fn explain_schema() -> Box<Schema> {
        Box::new(Schema::new(vec![
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
            LogicalPlan::Selection {
                ref expr,
                ref input,
                ..
            } => {
                write!(f, "Selection: {:?}", expr)?;
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
        }
    }
}

impl fmt::Debug for LogicalPlan {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.fmt_with_indent(f, 0)
    }
}

/// Verify a given type cast can be performed
pub fn can_coerce_from(type_into: &DataType, type_from: &DataType) -> bool {
    use self::DataType::*;
    match type_into {
        Int8 => match type_from {
            Int8 => true,
            _ => false,
        },
        Int16 => match type_from {
            Int8 | Int16 | UInt8 => true,
            _ => false,
        },
        Int32 => match type_from {
            Int8 | Int16 | Int32 | UInt8 | UInt16 => true,
            _ => false,
        },
        Int64 => match type_from {
            Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 => true,
            _ => false,
        },
        UInt8 => match type_from {
            UInt8 => true,
            _ => false,
        },
        UInt16 => match type_from {
            UInt8 | UInt16 => true,
            _ => false,
        },
        UInt32 => match type_from {
            UInt8 | UInt16 | UInt32 => true,
            _ => false,
        },
        UInt64 => match type_from {
            UInt8 | UInt16 | UInt32 | UInt64 => true,
            _ => false,
        },
        Float32 => match type_from {
            Int8 | Int16 | Int32 | Int64 => true,
            UInt8 | UInt16 | UInt32 | UInt64 => true,
            Float32 => true,
            _ => false,
        },
        Float64 => match type_from {
            Int8 | Int16 | Int32 | Int64 => true,
            UInt8 | UInt16 | UInt32 | UInt64 => true,
            Float32 | Float64 => true,
            _ => false,
        },
        Utf8 => true,
        _ => false,
    }
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
            schema: Box::new(Schema::empty()),
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

        let projected_schema = Box::new(
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
            schema: Box::new(schema),
            has_header: has_header,
            delimiter: Some(delimiter),
            projection,
            projected_schema,
        }))
    }

    /// Scan a Parquet data source
    pub fn scan_parquet(path: &str, projection: Option<Vec<usize>>) -> Result<Self> {
        let p = ParquetTable::try_new(path)?;
        let schema = p.schema().as_ref().to_owned();
        let projected_schema = projection
            .clone()
            .map(|p| Schema::new(p.iter().map(|i| schema.field(*i).clone()).collect()));
        Ok(Self::from(&LogicalPlan::ParquetScan {
            path: path.to_owned(),
            schema: Box::new(schema.clone()),
            projection,
            projected_schema: Box::new(
                projected_schema.or(Some(schema.clone())).unwrap(),
            ),
        }))
    }

    /// Scan a data source
    pub fn scan(
        schema_name: &str,
        table_name: &str,
        table_schema: &Schema,
        projection: Option<Vec<usize>>,
    ) -> Result<Self> {
        let projected_schema = projection.clone().map(|p| {
            Schema::new(p.iter().map(|i| table_schema.field(*i).clone()).collect())
        });
        Ok(Self::from(&LogicalPlan::TableScan {
            schema_name: schema_name.to_owned(),
            table_name: table_name.to_owned(),
            table_schema: Box::new(table_schema.clone()),
            projected_schema: Box::new(
                projected_schema.or(Some(table_schema.clone())).unwrap(),
            ),
            projection,
        }))
    }

    /// Apply a projection
    pub fn project(&self, expr: Vec<Expr>) -> Result<Self> {
        let input_schema = self.plan.schema();
        let projected_expr = if expr.contains(&Expr::Wildcard) {
            let mut expr_vec = vec![];
            (0..expr.len()).for_each(|i| match &expr[i] {
                Expr::Wildcard => {
                    (0..input_schema.fields().len())
                        .for_each(|i| expr_vec.push(col(input_schema.field(i).name())));
                }
                _ => expr_vec.push(expr[i].clone()),
            });
            expr_vec
        } else {
            expr.clone()
        };

        let schema =
            Schema::new(exprlist_to_fields(&projected_expr, input_schema.as_ref())?);

        Ok(Self::from(&LogicalPlan::Projection {
            expr: projected_expr,
            input: Box::new(self.plan.clone()),
            schema: Box::new(schema),
        }))
    }

    /// Apply a filter
    pub fn filter(&self, expr: Expr) -> Result<Self> {
        Ok(Self::from(&LogicalPlan::Selection {
            expr,
            input: Box::new(self.plan.clone()),
        }))
    }

    /// Apply a limit
    pub fn limit(&self, n: usize) -> Result<Self> {
        Ok(Self::from(&LogicalPlan::Limit {
            n,
            input: Box::new(self.plan.clone()),
        }))
    }

    /// Apply a sort
    pub fn sort(&self, expr: Vec<Expr>) -> Result<Self> {
        Ok(Self::from(&LogicalPlan::Sort {
            expr,
            input: Box::new(self.plan.clone()),
        }))
    }

    /// Apply an aggregate
    pub fn aggregate(&self, group_expr: Vec<Expr>, aggr_expr: Vec<Expr>) -> Result<Self> {
        let mut all_expr: Vec<Expr> = group_expr.clone();
        aggr_expr.iter().for_each(|x| all_expr.push(x.clone()));

        let aggr_schema = Schema::new(exprlist_to_fields(&all_expr, self.plan.schema())?);

        Ok(Self::from(&LogicalPlan::Aggregate {
            input: Box::new(self.plan.clone()),
            group_expr,
            aggr_expr,
            schema: Box::new(aggr_schema),
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
        \n  Selection: #state Eq Utf8(\"CO\")\
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
        \n  Selection: #state Eq Utf8(\"CO\")\
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
            vec![aggregate_expr("SUM", col("salary")).alias("total_salary")],
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
