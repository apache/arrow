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

//! Logical query plan

use std::fmt;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};

use crate::error::{ExecutionError, Result};
use crate::optimizer::utils;
use crate::sql::parser::FileType;

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
    Utf8(Arc<String>),
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
    /// index into a value within the row or complex value
    Column(usize),
    /// literal value
    Literal(ScalarValue),
    /// binary expression e.g. "age > 21"
    BinaryExpr {
        /// Left-hand side of the expression
        left: Arc<Expr>,
        /// The comparison operator
        op: Operator,
        /// Right-hand side of the expression
        right: Arc<Expr>,
    },
    /// unary IS NOT NULL
    IsNotNull(Arc<Expr>),
    /// unary IS NULL
    IsNull(Arc<Expr>),
    /// cast a value to a different type
    Cast {
        /// The expression being cast
        expr: Arc<Expr>,
        /// The `DataType` the expression will yield
        data_type: DataType,
    },
    /// sort expression
    Sort {
        /// The expression to sort on
        expr: Arc<Expr>,
        /// The direction of the sort
        asc: bool,
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
}

impl Expr {
    /// Find the `DataType` for the expression
    pub fn get_type(&self, schema: &Schema) -> DataType {
        match self {
            Expr::Column(n) => schema.field(*n).data_type().clone(),
            Expr::Literal(l) => l.get_datatype(),
            Expr::Cast { data_type, .. } => data_type.clone(),
            Expr::ScalarFunction { return_type, .. } => return_type.clone(),
            Expr::AggregateFunction { return_type, .. } => return_type.clone(),
            Expr::IsNull(_) => DataType::Boolean,
            Expr::IsNotNull(_) => DataType::Boolean,
            Expr::BinaryExpr {
                ref left,
                ref right,
                ref op,
            } => match op {
                Operator::Eq | Operator::NotEq => DataType::Boolean,
                Operator::Lt | Operator::LtEq => DataType::Boolean,
                Operator::Gt | Operator::GtEq => DataType::Boolean,
                Operator::And | Operator::Or => DataType::Boolean,
                _ => {
                    let left_type = left.get_type(schema);
                    let right_type = right.get_type(schema);
                    utils::get_supertype(&left_type, &right_type).unwrap()
                }
            },
            Expr::Sort { ref expr, .. } => expr.get_type(schema),
        }
    }

    /// Perform a type cast on the expression value.
    ///
    /// Will `Err` if the type cast cannot be performed.
    pub fn cast_to(&self, cast_to_type: &DataType, schema: &Schema) -> Result<Expr> {
        let this_type = self.get_type(schema);
        if this_type == *cast_to_type {
            Ok(self.clone())
        } else if can_coerce_from(cast_to_type, &this_type) {
            Ok(Expr::Cast {
                expr: Arc::new(self.clone()),
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
            left: Arc::new(self.clone()),
            op: Operator::Eq,
            right: Arc::new(other.clone()),
        }
    }

    /// Not equal
    pub fn not_eq(&self, other: &Expr) -> Expr {
        Expr::BinaryExpr {
            left: Arc::new(self.clone()),
            op: Operator::NotEq,
            right: Arc::new(other.clone()),
        }
    }

    /// Greater than
    pub fn gt(&self, other: &Expr) -> Expr {
        Expr::BinaryExpr {
            left: Arc::new(self.clone()),
            op: Operator::Gt,
            right: Arc::new(other.clone()),
        }
    }

    /// Greater than or equal to
    pub fn gt_eq(&self, other: &Expr) -> Expr {
        Expr::BinaryExpr {
            left: Arc::new(self.clone()),
            op: Operator::GtEq,
            right: Arc::new(other.clone()),
        }
    }

    /// Less than
    pub fn lt(&self, other: &Expr) -> Expr {
        Expr::BinaryExpr {
            left: Arc::new(self.clone()),
            op: Operator::Lt,
            right: Arc::new(other.clone()),
        }
    }

    /// Less than or equal to
    pub fn lt_eq(&self, other: &Expr) -> Expr {
        Expr::BinaryExpr {
            left: Arc::new(self.clone()),
            op: Operator::LtEq,
            right: Arc::new(other.clone()),
        }
    }
}

impl fmt::Debug for Expr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Expr::Column(i) => write!(f, "#{}", i),
            Expr::Literal(v) => write!(f, "{:?}", v),
            Expr::Cast { expr, data_type } => {
                write!(f, "CAST({:?} AS {:?})", expr, data_type)
            }
            Expr::IsNull(expr) => write!(f, "{:?} IS NULL", expr),
            Expr::IsNotNull(expr) => write!(f, "{:?} IS NOT NULL", expr),
            Expr::BinaryExpr { left, op, right } => {
                write!(f, "{:?} {:?} {:?}", left, op, right)
            }
            Expr::Sort { expr, asc } => {
                if *asc {
                    write!(f, "{:?} ASC", expr)
                } else {
                    write!(f, "{:?} DESC", expr)
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
        input: Arc<LogicalPlan>,
        /// The schema description
        schema: Arc<Schema>,
    },
    /// A Selection (essentially a WHERE clause with a predicate expression)
    Selection {
        /// The expression
        expr: Expr,
        /// The incoming logic plan
        input: Arc<LogicalPlan>,
    },
    /// Represents a list of aggregate expressions with optional grouping expressions
    Aggregate {
        /// The incoming logic plan
        input: Arc<LogicalPlan>,
        /// Grouping expressions
        group_expr: Vec<Expr>,
        /// Aggregate expressions
        aggr_expr: Vec<Expr>,
        /// The schema description
        schema: Arc<Schema>,
    },
    /// Represents a list of sort expressions to be applied to a relation
    Sort {
        /// The sort expressions
        expr: Vec<Expr>,
        /// The incoming logic plan
        input: Arc<LogicalPlan>,
        /// The schema description
        schema: Arc<Schema>,
    },
    /// A table scan against a table that has been registered on a context
    TableScan {
        /// The name of the schema
        schema_name: String,
        /// The name of the table
        table_name: String,
        /// The underlying table schema
        table_schema: Arc<Schema>,
        /// The projected schema
        projected_schema: Arc<Schema>,
        /// Optional column indices to use as a projection
        projection: Option<Vec<usize>>,
    },
    /// An empty relation with an empty schema
    EmptyRelation {
        /// The schema description
        schema: Arc<Schema>,
    },
    /// Represents the maximum number of records to return
    Limit {
        /// The expression
        expr: Expr,
        /// The logical plan
        input: Arc<LogicalPlan>,
        /// The schema description
        schema: Arc<Schema>,
    },
    /// Represents a create external table expression.
    CreateExternalTable {
        /// The table schema
        schema: Arc<Schema>,
        /// The table name
        name: String,
        /// The physical location
        location: String,
        /// The file type of physical file
        file_type: FileType,
        /// Whether the CSV file contains a header
        header_row: bool,
    },
}

impl LogicalPlan {
    /// Get a reference to the logical plan's schema
    pub fn schema(&self) -> &Arc<Schema> {
        match self {
            LogicalPlan::EmptyRelation { schema } => &schema,
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
                ref input,
                ref expr,
                ..
            } => {
                write!(f, "Limit: {:?}", expr)?;
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
            Int8 | Int16 => true,
            _ => false,
        },
        Int32 => match type_from {
            Int8 | Int16 | Int32 => true,
            _ => false,
        },
        Int64 => match type_from {
            Int8 | Int16 | Int32 | Int64 => true,
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn logical_plan_can_be_shared_between_threads() {
        let schema = Schema::new(vec![]);
        let plan = Arc::new(LogicalPlan::TableScan {
            schema_name: "".to_string(),
            table_name: "people".to_string(),
            table_schema: Arc::new(schema.clone()),
            projected_schema: Arc::new(schema),
            projection: Some(vec![0, 1, 4]),
        });

        // prove that a plan can be passed to a thread
        let plan1 = plan.clone();
        thread::spawn(move || {
            println!("plan: {:?}", plan1);
        });
    }

}
