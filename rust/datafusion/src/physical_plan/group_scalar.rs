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

//! Defines scalars used to construct groups, ex. in GROUP BY clauses.

use ordered_float::OrderedFloat;
use std::convert::{From, TryFrom};

use crate::error::{DataFusionError, Result};
use crate::scalar::ScalarValue;

/// Enumeration of types that can be used in a GROUP BY expression
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub(crate) enum GroupByScalar {
    Float32(OrderedFloat<f32>),
    Float64(OrderedFloat<f64>),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Utf8(Box<String>),
    Boolean(bool),
    TimeMicrosecond(i64),
    TimeNanosecond(i64),
}

impl TryFrom<&ScalarValue> for GroupByScalar {
    type Error = DataFusionError;

    fn try_from(scalar_value: &ScalarValue) -> Result<Self> {
        Ok(match scalar_value {
            ScalarValue::Float32(Some(v)) => {
                GroupByScalar::Float32(OrderedFloat::from(*v))
            }
            ScalarValue::Float64(Some(v)) => {
                GroupByScalar::Float64(OrderedFloat::from(*v))
            }
            ScalarValue::Boolean(Some(v)) => GroupByScalar::Boolean(*v),
            ScalarValue::Int8(Some(v)) => GroupByScalar::Int8(*v),
            ScalarValue::Int16(Some(v)) => GroupByScalar::Int16(*v),
            ScalarValue::Int32(Some(v)) => GroupByScalar::Int32(*v),
            ScalarValue::Int64(Some(v)) => GroupByScalar::Int64(*v),
            ScalarValue::UInt8(Some(v)) => GroupByScalar::UInt8(*v),
            ScalarValue::UInt16(Some(v)) => GroupByScalar::UInt16(*v),
            ScalarValue::UInt32(Some(v)) => GroupByScalar::UInt32(*v),
            ScalarValue::UInt64(Some(v)) => GroupByScalar::UInt64(*v),
            ScalarValue::Utf8(Some(v)) => GroupByScalar::Utf8(Box::new(v.clone())),
            ScalarValue::Float32(None)
            | ScalarValue::Float64(None)
            | ScalarValue::Boolean(None)
            | ScalarValue::Int8(None)
            | ScalarValue::Int16(None)
            | ScalarValue::Int32(None)
            | ScalarValue::Int64(None)
            | ScalarValue::UInt8(None)
            | ScalarValue::UInt16(None)
            | ScalarValue::UInt32(None)
            | ScalarValue::UInt64(None)
            | ScalarValue::Utf8(None) => {
                return Err(DataFusionError::Internal(format!(
                    "Cannot convert a ScalarValue holding NULL ({:?})",
                    scalar_value
                )));
            }
            v => {
                return Err(DataFusionError::Internal(format!(
                    "Cannot convert a ScalarValue with associated DataType {:?}",
                    v.get_datatype()
                )))
            }
        })
    }
}

impl From<&GroupByScalar> for ScalarValue {
    fn from(group_by_scalar: &GroupByScalar) -> Self {
        match group_by_scalar {
            GroupByScalar::Float32(v) => ScalarValue::Float32(Some((*v).into())),
            GroupByScalar::Float64(v) => ScalarValue::Float64(Some((*v).into())),
            GroupByScalar::Boolean(v) => ScalarValue::Boolean(Some(*v)),
            GroupByScalar::Int8(v) => ScalarValue::Int8(Some(*v)),
            GroupByScalar::Int16(v) => ScalarValue::Int16(Some(*v)),
            GroupByScalar::Int32(v) => ScalarValue::Int32(Some(*v)),
            GroupByScalar::Int64(v) => ScalarValue::Int64(Some(*v)),
            GroupByScalar::UInt8(v) => ScalarValue::UInt8(Some(*v)),
            GroupByScalar::UInt16(v) => ScalarValue::UInt16(Some(*v)),
            GroupByScalar::UInt32(v) => ScalarValue::UInt32(Some(*v)),
            GroupByScalar::UInt64(v) => ScalarValue::UInt64(Some(*v)),
            GroupByScalar::Utf8(v) => ScalarValue::Utf8(Some(v.to_string())),
            GroupByScalar::TimeMicrosecond(v) => ScalarValue::TimeMicrosecond(Some(*v)),
            GroupByScalar::TimeNanosecond(v) => ScalarValue::TimeNanosecond(Some(*v)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::error::{DataFusionError, Result};

    macro_rules! scalar_eq_test {
        ($TYPE:expr, $VALUE:expr) => {{
            let scalar_value = $TYPE($VALUE);
            let a = GroupByScalar::try_from(&scalar_value).unwrap();

            let scalar_value = $TYPE($VALUE);
            let b = GroupByScalar::try_from(&scalar_value).unwrap();

            assert_eq!(a, b);
        }};
    }

    #[test]
    fn test_scalar_ne_non_std() -> Result<()> {
        // Test only Scalars with non native Eq, Hash
        scalar_eq_test!(ScalarValue::Float32, Some(1.0));
        scalar_eq_test!(ScalarValue::Float64, Some(1.0));

        Ok(())
    }

    macro_rules! scalar_ne_test {
        ($TYPE:expr, $LVALUE:expr, $RVALUE:expr) => {{
            let scalar_value = $TYPE($LVALUE);
            let a = GroupByScalar::try_from(&scalar_value).unwrap();

            let scalar_value = $TYPE($RVALUE);
            let b = GroupByScalar::try_from(&scalar_value).unwrap();

            assert_ne!(a, b);
        }};
    }

    #[test]
    fn test_scalar_eq_non_std() -> Result<()> {
        // Test only Scalars with non native Eq, Hash
        scalar_ne_test!(ScalarValue::Float32, Some(1.0), Some(2.0));
        scalar_ne_test!(ScalarValue::Float64, Some(1.0), Some(2.0));

        Ok(())
    }

    #[test]
    fn from_scalar_holding_none() -> Result<()> {
        let scalar_value = ScalarValue::Int8(None);
        let result = GroupByScalar::try_from(&scalar_value);

        match result {
            Err(DataFusionError::Internal(error_message)) => assert_eq!(
                error_message,
                String::from("Cannot convert a ScalarValue holding NULL (Int8(NULL))")
            ),
            _ => panic!("Unexpected result"),
        }

        Ok(())
    }

    #[test]
    fn from_scalar_unsupported() -> Result<()> {
        // Use any ScalarValue type not supported by GroupByScalar.
        let scalar_value = ScalarValue::LargeUtf8(Some("1.1".to_string()));
        let result = GroupByScalar::try_from(&scalar_value);

        match result {
            Err(DataFusionError::Internal(error_message)) => assert_eq!(
                error_message,
                String::from(
                    "Cannot convert a ScalarValue with associated DataType LargeUtf8"
                )
            ),
            _ => panic!("Unexpected result"),
        }

        Ok(())
    }

    #[test]
    fn size_of_group_by_scalar() {
        assert_eq!(std::mem::size_of::<GroupByScalar>(), 16);
    }
}
