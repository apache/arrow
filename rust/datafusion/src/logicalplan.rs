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
use std::fmt::{Error, Formatter};
use std::rc::Rc;
use std::sync::Arc;

use arrow::datatypes::*;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum FunctionType {
    Scalar,
    Aggregate,
}

#[derive(Debug, Clone)]
pub struct FunctionMeta {
    name: String,
    args: Vec<Field>,
    return_type: DataType,
    function_type: FunctionType,
}

impl FunctionMeta {
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
    pub fn name(&self) -> &String {
        &self.name
    }
    pub fn args(&self) -> &Vec<Field> {
        &self.args
    }
    pub fn return_type(&self) -> &DataType {
        &self.return_type
    }
    pub fn function_type(&self) -> &FunctionType {
        &self.function_type
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum Operator {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    Plus,
    Minus,
    Multiply,
    Divide,
    Modulus,
    And,
    Or,
    Not,
    Like,
    NotLike,
}

impl Operator {
    /// Get the result type of applying this operation to its left and right inputs
    pub fn get_datatype(&self, l: &Expr, _r: &Expr, schema: &Schema) -> DataType {
        //TODO: implement correctly, just go with left side for now
        l.get_type(schema).clone()
    }
}

/// ScalarValue enumeration
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum ScalarValue {
    Null,
    Boolean(bool),
    Float32(f32),
    Float64(f64),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Utf8(Rc<String>),
    Struct(Vec<ScalarValue>),
}

impl ScalarValue {
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
            ScalarValue::Struct(_) => unimplemented!(),
            ScalarValue::Null => unimplemented!(),
        }
    }
}

/// Relation Expression
#[derive(Serialize, Deserialize, Clone, PartialEq)]
pub enum Expr {
    /// index into a value within the row or complex value
    Column(usize),
    /// literal value
    Literal(ScalarValue),
    /// binary expression e.g. "age > 21"
    BinaryExpr {
        left: Rc<Expr>,
        op: Operator,
        right: Rc<Expr>,
    },
    /// unary IS NOT NULL
    IsNotNull(Rc<Expr>),
    /// unary IS NULL
    IsNull(Rc<Expr>),
    /// cast a value to a different type
    Cast { expr: Rc<Expr>, data_type: DataType },
    /// sort expression
    Sort { expr: Rc<Expr>, asc: bool },
    /// scalar function
    ScalarFunction {
        name: String,
        args: Vec<Expr>,
        return_type: DataType,
    },
    /// aggregate function
    AggregateFunction {
        name: String,
        args: Vec<Expr>,
        return_type: DataType,
    },
}

impl Expr {
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
            } => {
                match op {
                    Operator::Eq | Operator::NotEq => DataType::Boolean,
                    Operator::Lt | Operator::LtEq => DataType::Boolean,
                    Operator::Gt | Operator::GtEq => DataType::Boolean,
                    Operator::And | Operator::Or => DataType::Boolean,
                    _ => {
                        let left_type = left.get_type(schema);
                        let right_type = right.get_type(schema);
                        get_supertype(&left_type, &right_type).unwrap_or(DataType::Utf8) //TODO ???
                    }
                }
            }
            Expr::Sort { ref expr, .. } => expr.get_type(schema),
        }
    }

    pub fn cast_to(
        &self,
        cast_to_type: &DataType,
        schema: &Schema,
    ) -> Result<Expr, String> {
        let this_type = self.get_type(schema);
        if this_type == *cast_to_type {
            Ok(self.clone())
        } else if can_coerce_from(cast_to_type, &this_type) {
            Ok(Expr::Cast {
                expr: Rc::new(self.clone()),
                data_type: cast_to_type.clone(),
            })
        } else {
            Err(format!(
                "Cannot automatically convert {:?} to {:?}",
                this_type, cast_to_type
            ))
        }
    }

    pub fn eq(&self, other: &Expr) -> Expr {
        Expr::BinaryExpr {
            left: Rc::new(self.clone()),
            op: Operator::Eq,
            right: Rc::new(other.clone()),
        }
    }

    pub fn not_eq(&self, other: &Expr) -> Expr {
        Expr::BinaryExpr {
            left: Rc::new(self.clone()),
            op: Operator::NotEq,
            right: Rc::new(other.clone()),
        }
    }

    pub fn gt(&self, other: &Expr) -> Expr {
        Expr::BinaryExpr {
            left: Rc::new(self.clone()),
            op: Operator::Gt,
            right: Rc::new(other.clone()),
        }
    }

    pub fn gt_eq(&self, other: &Expr) -> Expr {
        Expr::BinaryExpr {
            left: Rc::new(self.clone()),
            op: Operator::GtEq,
            right: Rc::new(other.clone()),
        }
    }

    pub fn lt(&self, other: &Expr) -> Expr {
        Expr::BinaryExpr {
            left: Rc::new(self.clone()),
            op: Operator::Lt,
            right: Rc::new(other.clone()),
        }
    }

    pub fn lt_eq(&self, other: &Expr) -> Expr {
        Expr::BinaryExpr {
            left: Rc::new(self.clone()),
            op: Operator::LtEq,
            right: Rc::new(other.clone()),
        }
    }
}

impl fmt::Debug for Expr {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
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

/// The LogicalPlan represents different types of relations (such as Projection, Selection, etc) and
/// can be created by the SQL query planner and the DataFrame API.
#[derive(Serialize, Deserialize, Clone)]
pub enum LogicalPlan {
    /// A Projection (essentially a SELECT with an expression list)
    Projection {
        expr: Vec<Expr>,
        input: Rc<LogicalPlan>,
        schema: Arc<Schema>,
    },
    /// A Selection (essentially a WHERE clause with a predicate expression)
    Selection { expr: Expr, input: Rc<LogicalPlan> },
    /// Represents a list of aggregate expressions with optional grouping expressions
    Aggregate {
        input: Rc<LogicalPlan>,
        group_expr: Vec<Expr>,
        aggr_expr: Vec<Expr>,
        schema: Arc<Schema>,
    },
    /// Represents a list of sort expressions to be applied to a relation
    Sort {
        expr: Vec<Expr>,
        input: Rc<LogicalPlan>,
        schema: Arc<Schema>,
    },
    /// A table scan against a table that has been registered on a context
    TableScan {
        schema_name: String,
        table_name: String,
        schema: Arc<Schema>,
        projection: Option<Vec<usize>>,
    },
    /// An empty relation with an empty schema
    EmptyRelation { schema: Arc<Schema> },
    // Represents the maximum number of records to return
    Limit {
        expr: Expr,
        input: Rc<LogicalPlan>,
        schema: Arc<Schema>,
    },
}

impl LogicalPlan {
    /// Get a reference to the logical plan's schema
    pub fn schema(&self) -> &Arc<Schema> {
        match self {
            LogicalPlan::EmptyRelation { schema } => &schema,
            LogicalPlan::TableScan { schema, .. } => &schema,
            LogicalPlan::Projection { schema, .. } => &schema,
            LogicalPlan::Selection { input, .. } => input.schema(),
            LogicalPlan::Aggregate { schema, .. } => &schema,
            LogicalPlan::Sort { schema, .. } => &schema,
            LogicalPlan::Limit { schema, .. } => &schema,
        }
    }
}

impl LogicalPlan {
    fn fmt_with_indent(&self, f: &mut Formatter, indent: usize) -> Result<(), Error> {
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
        }
    }
}

impl fmt::Debug for LogicalPlan {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        self.fmt_with_indent(f, 0)
    }
}

//TODO move to Arrow DataType impl?
pub fn get_supertype(l: &DataType, r: &DataType) -> Option<DataType> {
    match _get_supertype(l, r) {
        Some(dt) => Some(dt),
        None => match _get_supertype(r, l) {
            Some(dt) => Some(dt),
            None => None,
        },
    }
}

fn _get_supertype(l: &DataType, r: &DataType) -> Option<DataType> {
    use self::DataType::*;
    match (l, r) {
        (UInt8, Int8) => Some(Int8),
        (UInt8, Int16) => Some(Int16),
        (UInt8, Int32) => Some(Int32),
        (UInt8, Int64) => Some(Int64),

        (UInt16, Int16) => Some(Int16),
        (UInt16, Int32) => Some(Int32),
        (UInt16, Int64) => Some(Int64),

        (UInt32, Int32) => Some(Int32),
        (UInt32, Int64) => Some(Int64),

        (UInt64, Int64) => Some(Int64),

        (Int8, UInt8) => Some(Int8),

        (Int16, UInt8) => Some(Int16),
        (Int16, UInt16) => Some(Int16),

        (Int32, UInt8) => Some(Int32),
        (Int32, UInt16) => Some(Int32),
        (Int32, UInt32) => Some(Int32),

        (Int64, UInt8) => Some(Int64),
        (Int64, UInt16) => Some(Int64),
        (Int64, UInt32) => Some(Int64),
        (Int64, UInt64) => Some(Int64),

        (UInt8, UInt8) => Some(UInt8),
        (UInt8, UInt16) => Some(UInt16),
        (UInt8, UInt32) => Some(UInt32),
        (UInt8, UInt64) => Some(UInt64),
        (UInt8, Float32) => Some(Float32),
        (UInt8, Float64) => Some(Float64),

        (UInt16, UInt8) => Some(UInt16),
        (UInt16, UInt16) => Some(UInt16),
        (UInt16, UInt32) => Some(UInt32),
        (UInt16, UInt64) => Some(UInt64),
        (UInt16, Float32) => Some(Float32),
        (UInt16, Float64) => Some(Float64),

        (UInt32, UInt8) => Some(UInt32),
        (UInt32, UInt16) => Some(UInt32),
        (UInt32, UInt32) => Some(UInt32),
        (UInt32, UInt64) => Some(UInt64),
        (UInt32, Float32) => Some(Float32),
        (UInt32, Float64) => Some(Float64),

        (UInt64, UInt8) => Some(UInt64),
        (UInt64, UInt16) => Some(UInt64),
        (UInt64, UInt32) => Some(UInt64),
        (UInt64, UInt64) => Some(UInt64),
        (UInt64, Float32) => Some(Float32),
        (UInt64, Float64) => Some(Float64),

        (Int8, Int8) => Some(Int8),
        (Int8, Int16) => Some(Int16),
        (Int8, Int32) => Some(Int32),
        (Int8, Int64) => Some(Int64),
        (Int8, Float32) => Some(Float32),
        (Int8, Float64) => Some(Float64),

        (Int16, Int8) => Some(Int16),
        (Int16, Int16) => Some(Int16),
        (Int16, Int32) => Some(Int32),
        (Int16, Int64) => Some(Int64),
        (Int16, Float32) => Some(Float32),
        (Int16, Float64) => Some(Float64),

        (Int32, Int8) => Some(Int32),
        (Int32, Int16) => Some(Int32),
        (Int32, Int32) => Some(Int32),
        (Int32, Int64) => Some(Int64),
        (Int32, Float32) => Some(Float32),
        (Int32, Float64) => Some(Float64),

        (Int64, Int8) => Some(Int64),
        (Int64, Int16) => Some(Int64),
        (Int64, Int32) => Some(Int64),
        (Int64, Int64) => Some(Int64),
        (Int64, Float32) => Some(Float32),
        (Int64, Float64) => Some(Float64),

        (Float32, Float32) => Some(Float32),
        (Float32, Float64) => Some(Float64),
        (Float64, Float32) => Some(Float64),
        (Float64, Float64) => Some(Float64),

        (Utf8, Utf8) => Some(Utf8),

        (Boolean, Boolean) => Some(Boolean),

        _ => None,
    }
}

pub fn can_coerce_from(left: &DataType, other: &DataType) -> bool {
    use self::DataType::*;
    match left {
        Int8 => match other {
            Int8 => true,
            _ => false,
        },
        Int16 => match other {
            Int8 | Int16 => true,
            _ => false,
        },
        Int32 => match other {
            Int8 | Int16 | Int32 => true,
            _ => false,
        },
        Int64 => match other {
            Int8 | Int16 | Int32 | Int64 => true,
            _ => false,
        },
        UInt8 => match other {
            UInt8 => true,
            _ => false,
        },
        UInt16 => match other {
            UInt8 | UInt16 => true,
            _ => false,
        },
        UInt32 => match other {
            UInt8 | UInt16 | UInt32 => true,
            _ => false,
        },
        UInt64 => match other {
            UInt8 | UInt16 | UInt32 | UInt64 => true,
            _ => false,
        },
        Float32 => match other {
            Int8 | Int16 | Int32 | Int64 => true,
            UInt8 | UInt16 | UInt32 | UInt64 => true,
            Float32 => true,
            _ => false,
        },
        Float64 => match other {
            Int8 | Int16 | Int32 | Int64 => true,
            UInt8 | UInt16 | UInt32 | UInt64 => true,
            Float32 | Float64 => true,
            _ => false,
        },
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;

    #[test]
    fn serialize_plan() {
        let schema = Schema::new(vec![
            Field::new("first_name", DataType::Utf8, false),
            Field::new("last_name", DataType::Utf8, false),
            Field::new(
                "address",
                DataType::Struct(vec![
                    Field::new("street", DataType::Utf8, false),
                    Field::new("zip", DataType::UInt16, false),
                ]),
                false,
            ),
        ]);

        let plan = LogicalPlan::TableScan {
            schema_name: "".to_string(),
            table_name: "people".to_string(),
            schema: Arc::new(schema),
            projection: Some(vec![0, 1, 4]),
        };

        let serialized = serde_json::to_string(&plan).unwrap();

        assert_eq!(
            "{\"TableScan\":{\
             \"schema_name\":\"\",\
             \"table_name\":\"people\",\
             \"schema\":{\"fields\":[\
             {\"name\":\"first_name\",\"data_type\":\"Utf8\",\"nullable\":false},\
             {\"name\":\"last_name\",\"data_type\":\"Utf8\",\"nullable\":false},\
             {\"name\":\"address\",\"data_type\":{\"Struct\":\
             [\
             {\"name\":\"street\",\"data_type\":\"Utf8\",\"nullable\":false},\
             {\"name\":\"zip\",\"data_type\":\"UInt16\",\"nullable\":false}]},\"nullable\":false}\
             ]},\
             \"projection\":[0,1,4]}}",
            serialized
        );
    }
}
