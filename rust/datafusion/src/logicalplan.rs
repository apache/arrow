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

use std::fmt;

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
    pub fn get_datatype(&self) -> DataType {
        match *self {
            ScalarValue::Boolean(_) => DataType::Boolean,
            ScalarValue::UInt8(_) => DataType::UInt8,
            ScalarValue::UInt16(_) => DataType::UInt16,
            ScalarValue::UInt32(_) => DataType::UInt32,
            ScalarValue::UInt64(_) => DataType::UInt64,
            ScalarValue::Int8(_) => DataType::Int8,
            ScalarValue::Int16(_) => DataType::Int16,
            ScalarValue::Int32(_) => DataType::Int32,
            ScalarValue::Int64(_) => DataType::Int64,
            ScalarValue::Float32(_) => DataType::Float32,
            ScalarValue::Float64(_) => DataType::Float64,
            ScalarValue::Utf8(_) => DataType::Utf8,
            _ => panic!("Cannot treat {:?} as scalar value", self),
        }
    }
}

/// Relation expression
#[derive(Clone, PartialEq)]
pub enum Expr {
    /// An aliased expression
    Alias(Box<Expr>, String),
    /// index into a value within the row or complex value
    Column(usize),
    /// Reference to column by name
    UnresolvedColumn(String),
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
        /// The `DataType` the expression will yield
        return_type: DataType,
    },
    /// Wildcard
    Wildcard,
}

impl Expr {
    /// Find the `DataType` for the expression
    pub fn get_type(&self, schema: &Schema) -> Result<DataType> {
        match self {
            Expr::Alias(expr, _) => expr.get_type(schema),
            Expr::Column(n) => Ok(schema.field(*n).data_type().clone()),
            Expr::UnresolvedColumn(name) => {
                Ok(schema.field_with_name(&name)?.data_type().clone())
            }
            Expr::Literal(l) => Ok(l.get_datatype()),
            Expr::Cast { data_type, .. } => Ok(data_type.clone()),
            Expr::ScalarFunction { return_type, .. } => Ok(return_type.clone()),
            Expr::AggregateFunction { return_type, .. } => Ok(return_type.clone()),
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
        }
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
    pub fn eq(&self, other: &Expr) -> Expr {
        Expr::BinaryExpr {
            left: Box::new(self.clone()),
            op: Operator::Eq,
            right: Box::new(other.clone()),
        }
    }

    /// Not equal
    pub fn not_eq(&self, other: &Expr) -> Expr {
        Expr::BinaryExpr {
            left: Box::new(self.clone()),
            op: Operator::NotEq,
            right: Box::new(other.clone()),
        }
    }

    /// Greater than
    pub fn gt(&self, other: &Expr) -> Expr {
        Expr::BinaryExpr {
            left: Box::new(self.clone()),
            op: Operator::Gt,
            right: Box::new(other.clone()),
        }
    }

    /// Greater than or equal to
    pub fn gt_eq(&self, other: &Expr) -> Expr {
        Expr::BinaryExpr {
            left: Box::new(self.clone()),
            op: Operator::GtEq,
            right: Box::new(other.clone()),
        }
    }

    /// Less than
    pub fn lt(&self, other: &Expr) -> Expr {
        Expr::BinaryExpr {
            left: Box::new(self.clone()),
            op: Operator::Lt,
            right: Box::new(other.clone()),
        }
    }

    /// Less than or equal to
    pub fn lt_eq(&self, other: &Expr) -> Expr {
        Expr::BinaryExpr {
            left: Box::new(self.clone()),
            op: Operator::LtEq,
            right: Box::new(other.clone()),
        }
    }

    /// Not
    pub fn not(&self) -> Expr {
        Expr::Not(Box::new(self.clone()))
    }

    /// Alias
    pub fn alias(&self, name: &str) -> Expr {
        Expr::Alias(Box::new(self.clone()), name.to_owned())
    }
}

/// Create a column expression based on a column index
pub fn col_index(index: usize) -> Expr {
    Expr::Column(index)
}

/// Create a column expression based on a column name
pub fn col(name: &str) -> Expr {
    Expr::UnresolvedColumn(name.to_owned())
}

/// Create a literal string expression
pub fn lit_str(str: &str) -> Expr {
    Expr::Literal(ScalarValue::Utf8(str.to_owned()))
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

/// Create an aggregate expression
pub fn aggregate_expr(name: &str, expr: Expr, return_type: DataType) -> Expr {
    Expr::AggregateFunction {
        name: name.to_owned(),
        args: vec![expr],
        return_type,
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
            Expr::Column(i) => write!(f, "#{}", i),
            Expr::UnresolvedColumn(name) => write!(f, "#{}", name),
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
        }
    }
}

/// The LogicalPlan represents different types of relations (such as Projection,
/// Selection, etc) and can be created by the SQL query planner and the DataFrame API.
#[derive(Clone)]
pub enum LogicalPlan {
    /// A Projection (essentially a SELECT with an expression list)
    Projection {
        /// The list of expressions
        expr: Vec<Expr>,
        /// The incoming logic plan
        input: Box<LogicalPlan>,
        /// The schema description
        schema: Box<Schema>,
    },
    /// A Selection (essentially a WHERE clause with a predicate expression)
    Selection {
        /// The expression
        expr: Expr,
        /// The incoming logic plan
        input: Box<LogicalPlan>,
    },
    /// Represents a list of aggregate expressions with optional grouping expressions
    Aggregate {
        /// The incoming logic plan
        input: Box<LogicalPlan>,
        /// Grouping expressions
        group_expr: Vec<Expr>,
        /// Aggregate expressions
        aggr_expr: Vec<Expr>,
        /// The schema description
        schema: Box<Schema>,
    },
    /// Represents a list of sort expressions to be applied to a relation
    Sort {
        /// The sort expressions
        expr: Vec<Expr>,
        /// The incoming logic plan
        input: Box<LogicalPlan>,
        /// The schema description
        schema: Box<Schema>,
    },
    /// A table scan against a table that has been registered on a context
    TableScan {
        /// The name of the schema
        schema_name: String,
        /// The name of the table
        table_name: String,
        /// The schema of the CSV file(s)
        table_schema: Box<Schema>,
        /// Optional column indices to use as a projection
        projection: Option<Vec<usize>>,
        /// The projected schema
        projected_schema: Box<Schema>,
    },
    /// A table scan against a vector of record batches
    InMemoryScan {
        /// Record batch partitions
        data: Vec<Vec<RecordBatch>>,
        /// The schema of the record batches
        schema: Box<Schema>,
        /// Optional column indices to use as a projection
        projection: Option<Vec<usize>>,
        /// The projected schema
        projected_schema: Box<Schema>,
    },
    /// A table scan against a Parquet data source
    ParquetScan {
        /// The path to the files
        path: String,
        /// The schema of the Parquet file(s)
        schema: Box<Schema>,
        /// Optional column indices to use as a projection
        projection: Option<Vec<usize>>,
        /// The projected schema
        projected_schema: Box<Schema>,
    },
    /// A table scan against a CSV data source
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
        /// The projected schema
        projected_schema: Box<Schema>,
    },
    /// An empty relation with an empty schema
    EmptyRelation {
        /// The schema description
        schema: Box<Schema>,
    },
    /// Represents the maximum number of records to return
    Limit {
        /// The limit
        n: usize,
        /// The logical plan
        input: Box<LogicalPlan>,
        /// The schema description
        schema: Box<Schema>,
    },
    /// Represents a create external table expression.
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
            LogicalPlan::Sort { schema, .. } => &schema,
            LogicalPlan::Limit { schema, .. } => &schema,
            LogicalPlan::CreateExternalTable { schema, .. } => &schema,
        }
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
                        .for_each(|i| expr_vec.push(col_index(i).clone()));
                }
                _ => expr_vec.push(expr[i].clone()),
            });
            expr_vec
        } else {
            expr.clone()
        };

        let schema = Schema::new(utils::exprlist_to_fields(
            &projected_expr,
            input_schema.as_ref(),
        )?);

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
            schema: self.plan.schema().clone(),
        }))
    }

    /// Apply a sort
    pub fn sort(&self, expr: Vec<Expr>) -> Result<Self> {
        Ok(Self::from(&LogicalPlan::Sort {
            expr,
            input: Box::new(self.plan.clone()),
            schema: self.plan.schema().clone(),
        }))
    }

    /// Apply an aggregate
    pub fn aggregate(&self, group_expr: Vec<Expr>, aggr_expr: Vec<Expr>) -> Result<Self> {
        let mut all_fields: Vec<Expr> = group_expr.clone();
        aggr_expr.iter().for_each(|x| all_fields.push(x.clone()));

        let aggr_schema =
            Schema::new(utils::exprlist_to_fields(&all_fields, self.plan.schema())?);

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
        .filter(col("state").eq(&lit_str("CO")))?
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
        .filter(col("state").eq(&lit_str("CO")))?
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
            vec![aggregate_expr("SUM", col("salary"), DataType::Int32)
                .alias("total_salary")],
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

    fn employee_schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("first_name", DataType::Utf8, false),
            Field::new("last_name", DataType::Utf8, false),
            Field::new("state", DataType::Utf8, false),
            Field::new("salary", DataType::Int32, false),
        ])
    }
}
