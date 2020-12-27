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

//! This module provides ScalarValue, an enum that can be used for storage of single elements

use std::{convert::TryFrom, fmt, sync::Arc};

use arrow::array::{
    Int16Builder, Int32Builder, Int64Builder, Int8Builder, ListBuilder, UInt16Builder,
    UInt32Builder, UInt64Builder, UInt8Builder,
};
use arrow::{
    array::ArrayRef,
    datatypes::{DataType, Field},
};
use arrow::{
    array::{
        Array, BooleanArray, Date32Array, Float32Array, Float64Array, Int16Array,
        Int32Array, Int64Array, Int8Array, LargeStringArray, ListArray, StringArray,
        UInt16Array, UInt32Array, UInt64Array, UInt8Array,
    },
    datatypes::DateUnit,
};

use crate::error::{DataFusionError, Result};

/// Represents a dynamically typed, nullable single value.
/// This is the single-valued counter-part of arrow’s `Array`.
#[derive(Clone, PartialEq)]
pub enum ScalarValue {
    /// true or false value
    Boolean(Option<bool>),
    /// 32bit float
    Float32(Option<f32>),
    /// 64bit float
    Float64(Option<f64>),
    /// signed 8bit int
    Int8(Option<i8>),
    /// signed 16bit int
    Int16(Option<i16>),
    /// signed 32bit int
    Int32(Option<i32>),
    /// signed 64bit int
    Int64(Option<i64>),
    /// unsigned 8bit int
    UInt8(Option<u8>),
    /// unsigned 16bit int
    UInt16(Option<u16>),
    /// unsigned 32bit int
    UInt32(Option<u32>),
    /// unsigned 64bit int
    UInt64(Option<u64>),
    /// utf-8 encoded string.
    Utf8(Option<String>),
    /// utf-8 encoded string representing a LargeString's arrow type.
    LargeUtf8(Option<String>),
    /// list of nested ScalarValue
    List(Option<Vec<ScalarValue>>, DataType),
    /// Date stored as a signed 32bit int
    Date32(Option<i32>),
}

macro_rules! typed_cast {
    ($array:expr, $index:expr, $ARRAYTYPE:ident, $SCALAR:ident) => {{
        let array = $array.as_any().downcast_ref::<$ARRAYTYPE>().unwrap();
        ScalarValue::$SCALAR(match array.is_null($index) {
            true => None,
            false => Some(array.value($index).into()),
        })
    }};
}

macro_rules! build_list {
    ($VALUE_BUILDER_TY:ident, $SCALAR_TY:ident, $VALUES:expr, $SIZE:expr) => {{
        match $VALUES {
            None => {
                let mut builder = ListBuilder::new($VALUE_BUILDER_TY::new(0));
                for _ in 0..$SIZE {
                    builder.append(false).unwrap();
                }
                builder.finish()
            }
            Some(values) => {
                let mut builder = ListBuilder::new($VALUE_BUILDER_TY::new(values.len()));

                for _ in 0..$SIZE {
                    for scalar_value in values {
                        match scalar_value {
                            ScalarValue::$SCALAR_TY(Some(v)) => {
                                builder.values().append_value(*v).unwrap()
                            }
                            ScalarValue::$SCALAR_TY(None) => {
                                builder.values().append_null().unwrap();
                            }
                            _ => panic!("Incompatible ScalarValue for list"),
                        };
                    }
                    builder.append(true).unwrap();
                }

                builder.finish()
            }
        }
    }};
}

impl ScalarValue {
    /// Getter for the `DataType` of the value
    pub fn get_datatype(&self) -> DataType {
        match self {
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
            ScalarValue::LargeUtf8(_) => DataType::LargeUtf8,
            ScalarValue::List(_, data_type) => {
                DataType::List(Box::new(Field::new("item", data_type.clone(), true)))
            }
            ScalarValue::Date32(_) => DataType::Date32(DateUnit::Day),
        }
    }

    /// Calculate arithmetic negation for a scalar value
    pub fn arithmetic_negate(&self) -> Self {
        match self {
            ScalarValue::Boolean(None)
            | ScalarValue::Int8(None)
            | ScalarValue::Int16(None)
            | ScalarValue::Int32(None)
            | ScalarValue::Int64(None)
            | ScalarValue::Float32(None) => self.clone(),
            ScalarValue::Float64(Some(v)) => ScalarValue::Float64(Some(-v)),
            ScalarValue::Float32(Some(v)) => ScalarValue::Float32(Some(-v)),
            ScalarValue::Int8(Some(v)) => ScalarValue::Int8(Some(-v)),
            ScalarValue::Int16(Some(v)) => ScalarValue::Int16(Some(-v)),
            ScalarValue::Int32(Some(v)) => ScalarValue::Int32(Some(-v)),
            ScalarValue::Int64(Some(v)) => ScalarValue::Int64(Some(-v)),
            _ => panic!("Cannot run arithmetic negate on scala value: {:?}", self),
        }
    }

    /// whether this value is null or not.
    pub fn is_null(&self) -> bool {
        matches!(
            *self,
            ScalarValue::Boolean(None)
                | ScalarValue::UInt8(None)
                | ScalarValue::UInt16(None)
                | ScalarValue::UInt32(None)
                | ScalarValue::UInt64(None)
                | ScalarValue::Int8(None)
                | ScalarValue::Int16(None)
                | ScalarValue::Int32(None)
                | ScalarValue::Int64(None)
                | ScalarValue::Float32(None)
                | ScalarValue::Float64(None)
                | ScalarValue::Utf8(None)
                | ScalarValue::LargeUtf8(None)
                | ScalarValue::List(None, _)
        )
    }

    /// Converts a scalar value into an 1-row array.
    pub fn to_array(&self) -> ArrayRef {
        self.to_array_of_size(1)
    }

    /// Converts a scalar value into an array of `size` rows.
    pub fn to_array_of_size(&self, size: usize) -> ArrayRef {
        match self {
            ScalarValue::Boolean(e) => {
                Arc::new(BooleanArray::from(vec![*e; size])) as ArrayRef
            }
            ScalarValue::Float64(e) => {
                Arc::new(Float64Array::from(vec![*e; size])) as ArrayRef
            }
            ScalarValue::Float32(e) => Arc::new(Float32Array::from(vec![*e; size])),
            ScalarValue::Int8(e) => Arc::new(Int8Array::from(vec![*e; size])),
            ScalarValue::Int16(e) => Arc::new(Int16Array::from(vec![*e; size])),
            ScalarValue::Int32(e) => Arc::new(Int32Array::from(vec![*e; size])),
            ScalarValue::Int64(e) => Arc::new(Int64Array::from(vec![*e; size])),
            ScalarValue::UInt8(e) => Arc::new(UInt8Array::from(vec![*e; size])),
            ScalarValue::UInt16(e) => Arc::new(UInt16Array::from(vec![*e; size])),
            ScalarValue::UInt32(e) => Arc::new(UInt32Array::from(vec![*e; size])),
            ScalarValue::UInt64(e) => Arc::new(UInt64Array::from(vec![*e; size])),
            ScalarValue::Utf8(e) => Arc::new(StringArray::from(vec![e.as_deref(); size])),
            ScalarValue::LargeUtf8(e) => {
                Arc::new(LargeStringArray::from(vec![e.as_deref(); size]))
            }
            ScalarValue::List(values, data_type) => Arc::new(match data_type {
                DataType::Int8 => build_list!(Int8Builder, Int8, values, size),
                DataType::Int16 => build_list!(Int16Builder, Int16, values, size),
                DataType::Int32 => build_list!(Int32Builder, Int32, values, size),
                DataType::Int64 => build_list!(Int64Builder, Int64, values, size),
                DataType::UInt8 => build_list!(UInt8Builder, UInt8, values, size),
                DataType::UInt16 => build_list!(UInt16Builder, UInt16, values, size),
                DataType::UInt32 => build_list!(UInt32Builder, UInt32, values, size),
                DataType::UInt64 => build_list!(UInt64Builder, UInt64, values, size),
                _ => panic!("Unexpected DataType for list"),
            }),
            ScalarValue::Date32(e) => Arc::new(Date32Array::from(vec![*e; size])),
        }
    }

    /// Converts a value in `array` at `index` into a ScalarValue
    pub fn try_from_array(array: &ArrayRef, index: usize) -> Result<Self> {
        Ok(match array.data_type() {
            DataType::Boolean => typed_cast!(array, index, BooleanArray, Boolean),
            DataType::Float64 => typed_cast!(array, index, Float64Array, Float64),
            DataType::Float32 => typed_cast!(array, index, Float32Array, Float32),
            DataType::UInt64 => typed_cast!(array, index, UInt64Array, UInt64),
            DataType::UInt32 => typed_cast!(array, index, UInt32Array, UInt32),
            DataType::UInt16 => typed_cast!(array, index, UInt16Array, UInt16),
            DataType::UInt8 => typed_cast!(array, index, UInt8Array, UInt8),
            DataType::Int64 => typed_cast!(array, index, Int64Array, Int64),
            DataType::Int32 => typed_cast!(array, index, Int32Array, Int32),
            DataType::Int16 => typed_cast!(array, index, Int16Array, Int16),
            DataType::Int8 => typed_cast!(array, index, Int8Array, Int8),
            DataType::Utf8 => typed_cast!(array, index, StringArray, Utf8),
            DataType::LargeUtf8 => typed_cast!(array, index, LargeStringArray, LargeUtf8),
            DataType::List(nested_type) => {
                let list_array =
                    array.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
                        DataFusionError::Internal(
                            "Failed to downcast ListArray".to_string(),
                        )
                    })?;
                let value = match list_array.is_null(index) {
                    true => None,
                    false => {
                        let nested_array = list_array.value(index);
                        let scalar_vec = (0..nested_array.len())
                            .map(|i| ScalarValue::try_from_array(&nested_array, i))
                            .collect::<Result<Vec<_>>>()?;
                        Some(scalar_vec)
                    }
                };
                ScalarValue::List(value, nested_type.data_type().clone())
            }
            DataType::Date32(DateUnit::Day) => {
                typed_cast!(array, index, Date32Array, Date32)
            }
            other => {
                return Err(DataFusionError::NotImplemented(format!(
                    "Can't create a scalar of array of type \"{:?}\"",
                    other
                )))
            }
        })
    }
}

impl From<f64> for ScalarValue {
    fn from(value: f64) -> Self {
        ScalarValue::Float64(Some(value))
    }
}

impl From<f32> for ScalarValue {
    fn from(value: f32) -> Self {
        ScalarValue::Float32(Some(value))
    }
}

impl From<i8> for ScalarValue {
    fn from(value: i8) -> Self {
        ScalarValue::Int8(Some(value))
    }
}

impl From<i16> for ScalarValue {
    fn from(value: i16) -> Self {
        ScalarValue::Int16(Some(value))
    }
}

impl From<i32> for ScalarValue {
    fn from(value: i32) -> Self {
        ScalarValue::Int32(Some(value))
    }
}

impl From<i64> for ScalarValue {
    fn from(value: i64) -> Self {
        ScalarValue::Int64(Some(value))
    }
}

impl From<bool> for ScalarValue {
    fn from(value: bool) -> Self {
        ScalarValue::Boolean(Some(value))
    }
}

impl From<u8> for ScalarValue {
    fn from(value: u8) -> Self {
        ScalarValue::UInt8(Some(value))
    }
}

impl From<u16> for ScalarValue {
    fn from(value: u16) -> Self {
        ScalarValue::UInt16(Some(value))
    }
}

impl From<u32> for ScalarValue {
    fn from(value: u32) -> Self {
        ScalarValue::UInt32(Some(value))
    }
}

impl From<u64> for ScalarValue {
    fn from(value: u64) -> Self {
        ScalarValue::UInt64(Some(value))
    }
}

macro_rules! impl_try_from {
    ($SCALAR:ident, $NATIVE:ident) => {
        impl TryFrom<ScalarValue> for $NATIVE {
            type Error = DataFusionError;

            fn try_from(value: ScalarValue) -> Result<Self> {
                match value {
                    ScalarValue::$SCALAR(Some(inner_value)) => Ok(inner_value),
                    _ => Err(DataFusionError::Internal(format!(
                        "Cannot convert {:?} to {}",
                        value,
                        std::any::type_name::<Self>()
                    ))),
                }
            }
        }
    };
}

impl_try_from!(Int8, i8);
impl_try_from!(Int16, i16);

// special implementation for i32 because of Date32
impl TryFrom<ScalarValue> for i32 {
    type Error = DataFusionError;

    fn try_from(value: ScalarValue) -> Result<Self> {
        match value {
            ScalarValue::Int32(Some(inner_value))
            | ScalarValue::Date32(Some(inner_value)) => Ok(inner_value),
            _ => Err(DataFusionError::Internal(format!(
                "Cannot convert {:?} to {}",
                value,
                std::any::type_name::<Self>()
            ))),
        }
    }
}

impl_try_from!(Int64, i64);
impl_try_from!(UInt8, u8);
impl_try_from!(UInt16, u16);
impl_try_from!(UInt32, u32);
impl_try_from!(UInt64, u64);
impl_try_from!(Float32, f32);
impl_try_from!(Float64, f64);
impl_try_from!(Boolean, bool);

impl TryFrom<&DataType> for ScalarValue {
    type Error = DataFusionError;

    fn try_from(datatype: &DataType) -> Result<Self> {
        Ok(match datatype {
            DataType::Boolean => ScalarValue::Boolean(None),
            DataType::Float64 => ScalarValue::Float64(None),
            DataType::Float32 => ScalarValue::Float32(None),
            DataType::Int8 => ScalarValue::Int8(None),
            DataType::Int16 => ScalarValue::Int16(None),
            DataType::Int32 => ScalarValue::Int32(None),
            DataType::Int64 => ScalarValue::Int64(None),
            DataType::UInt8 => ScalarValue::UInt8(None),
            DataType::UInt16 => ScalarValue::UInt16(None),
            DataType::UInt32 => ScalarValue::UInt32(None),
            DataType::UInt64 => ScalarValue::UInt64(None),
            DataType::Utf8 => ScalarValue::Utf8(None),
            DataType::LargeUtf8 => ScalarValue::LargeUtf8(None),
            DataType::List(ref nested_type) => {
                ScalarValue::List(None, nested_type.data_type().clone())
            }
            _ => {
                return Err(DataFusionError::NotImplemented(format!(
                    "Can't create a scalar of type \"{:?}\"",
                    datatype
                )))
            }
        })
    }
}

macro_rules! format_option {
    ($F:expr, $EXPR:expr) => {{
        match $EXPR {
            Some(e) => write!($F, "{}", e),
            None => write!($F, "NULL"),
        }
    }};
}

impl fmt::Display for ScalarValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ScalarValue::Boolean(e) => format_option!(f, e)?,
            ScalarValue::Float32(e) => format_option!(f, e)?,
            ScalarValue::Float64(e) => format_option!(f, e)?,
            ScalarValue::Int8(e) => format_option!(f, e)?,
            ScalarValue::Int16(e) => format_option!(f, e)?,
            ScalarValue::Int32(e) => format_option!(f, e)?,
            ScalarValue::Int64(e) => format_option!(f, e)?,
            ScalarValue::UInt8(e) => format_option!(f, e)?,
            ScalarValue::UInt16(e) => format_option!(f, e)?,
            ScalarValue::UInt32(e) => format_option!(f, e)?,
            ScalarValue::UInt64(e) => format_option!(f, e)?,
            ScalarValue::Utf8(e) => format_option!(f, e)?,
            ScalarValue::LargeUtf8(e) => format_option!(f, e)?,
            ScalarValue::List(e, _) => match e {
                Some(l) => write!(
                    f,
                    "{}",
                    l.iter()
                        .map(|v| format!("{}", v))
                        .collect::<Vec<_>>()
                        .join(",")
                )?,
                None => write!(f, "NULL")?,
            },
            ScalarValue::Date32(e) => format_option!(f, e)?,
        };
        Ok(())
    }
}

impl fmt::Debug for ScalarValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ScalarValue::Boolean(_) => write!(f, "Boolean({})", self),
            ScalarValue::Float32(_) => write!(f, "Float32({})", self),
            ScalarValue::Float64(_) => write!(f, "Float64({})", self),
            ScalarValue::Int8(_) => write!(f, "Int8({})", self),
            ScalarValue::Int16(_) => write!(f, "Int16({})", self),
            ScalarValue::Int32(_) => write!(f, "Int32({})", self),
            ScalarValue::Int64(_) => write!(f, "Int64({})", self),
            ScalarValue::UInt8(_) => write!(f, "UInt8({})", self),
            ScalarValue::UInt16(_) => write!(f, "UInt16({})", self),
            ScalarValue::UInt32(_) => write!(f, "UInt32({})", self),
            ScalarValue::UInt64(_) => write!(f, "UInt64({})", self),
            ScalarValue::Utf8(_) => write!(f, "Utf8(\"{}\")", self),
            ScalarValue::LargeUtf8(_) => write!(f, "LargeUtf8(\"{}\")", self),
            ScalarValue::List(_, _) => write!(f, "List([{}])", self),
            ScalarValue::Date32(_) => write!(f, "Date32(\"{}\")", self),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scalar_list_null_to_array() -> Result<()> {
        let list_array_ref = ScalarValue::List(None, DataType::UInt64).to_array();
        let list_array = list_array_ref.as_any().downcast_ref::<ListArray>().unwrap();

        assert!(list_array.is_null(0));
        assert_eq!(list_array.len(), 1);
        assert_eq!(list_array.values().len(), 0);

        Ok(())
    }

    #[test]
    fn scalar_list_to_array() -> Result<()> {
        let list_array_ref = ScalarValue::List(
            Some(vec![
                ScalarValue::UInt64(Some(100)),
                ScalarValue::UInt64(None),
                ScalarValue::UInt64(Some(101)),
            ]),
            DataType::UInt64,
        )
        .to_array();

        let list_array = list_array_ref.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(list_array.len(), 1);
        assert_eq!(list_array.values().len(), 3);

        let prim_array_ref = list_array.value(0);
        let prim_array = prim_array_ref
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(prim_array.len(), 3);
        assert_eq!(prim_array.value(0), 100);
        assert!(prim_array.is_null(1));
        assert_eq!(prim_array.value(2), 101);

        Ok(())
    }
}
