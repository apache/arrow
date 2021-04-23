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

//! Contains functionality to load an ArrayData from the C Data Interface

use std::convert::TryFrom;

use crate::{
    error::{ArrowError, Result},
    ffi,
};

use super::ArrayData;
use crate::datatypes::DataType;
use crate::ffi::ArrowArray;

impl TryFrom<ffi::ArrowArray> for ArrayData {
    type Error = ArrowError;

    fn try_from(value: ffi::ArrowArray) -> Result<Self> {
        let child_data = value.children()?;

        let child_type = if !child_data.is_empty() {
            Some(child_data[0].data_type().clone())
        } else {
            None
        };

        let data_type = value.data_type(child_type)?;

        let len = value.len();
        let offset = value.offset();
        let null_count = value.null_count();
        let buffers = value.buffers()?;
        let null_bit_buffer = value.null_bit_buffer();

        Ok(ArrayData::new(
            data_type,
            len,
            Some(null_count),
            null_bit_buffer,
            offset,
            buffers,
            child_data,
        ))
    }
}

impl TryFrom<ArrayData> for ffi::ArrowArray {
    type Error = ArrowError;

    fn try_from(value: ArrayData) -> Result<Self> {
        // If parent is nullable, then children also must be nullable
        // so we pass this nullable to the creation of hte child data
        let nullable = match value.data_type() {
            DataType::List(field) => field.is_nullable(),
            DataType::LargeList(field) => field.is_nullable(),
            _ => false,
        };

        let len = value.len();
        let offset = value.offset() as usize;
        let null_count = value.null_count();
        let buffers = value.buffers().to_vec();
        let null_buffer = value.null_buffer().cloned();
        let child_data = value
            .child_data()
            .iter()
            .map(|arr| {
                let len = arr.len();
                let offset = arr.offset() as usize;
                let null_count = arr.null_count();
                let buffers = arr.buffers().to_vec();
                let null_buffer = arr.null_buffer().cloned();

                // Note: the nullable comes from the parent data.
                unsafe {
                    ArrowArray::try_new(
                        arr.data_type(),
                        len,
                        null_count,
                        null_buffer,
                        offset,
                        buffers,
                        vec![],
                        nullable,
                    )
                    .expect("infallible")
                }
            })
            .collect::<Vec<_>>();

        unsafe {
            ffi::ArrowArray::try_new(
                value.data_type(),
                len,
                null_count,
                null_buffer,
                offset,
                buffers,
                child_data,
                nullable,
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::error::Result;
    use crate::{
        array::{Array, ArrayData, Int64Array, UInt32Array, UInt64Array},
        ffi::ArrowArray,
    };
    use std::convert::TryFrom;

    fn test_round_trip(expected: &ArrayData) -> Result<()> {
        // create a `ArrowArray` from the data.
        let d1 = ArrowArray::try_from(expected.clone())?;

        // here we export the array as 2 pointers. We would have no control over ownership if it was not for
        // the release mechanism.
        let (array, schema) = ArrowArray::into_raw(d1);

        // simulate an external consumer by being the consumer
        let d1 = unsafe { ArrowArray::try_from_raw(array, schema) }?;

        let result = &ArrayData::try_from(d1)?;

        assert_eq!(result, expected);
        Ok(())
    }

    #[test]
    fn test_u32() -> Result<()> {
        let array = UInt32Array::from(vec![Some(2), None, Some(1), None]);
        let data = array.data();
        test_round_trip(data)
    }

    #[test]
    fn test_u64() -> Result<()> {
        let array = UInt64Array::from(vec![Some(2), None, Some(1), None]);
        let data = array.data();
        test_round_trip(data)
    }

    #[test]
    fn test_i64() -> Result<()> {
        let array = Int64Array::from(vec![Some(2), None, Some(1), None]);
        let data = array.data();
        test_round_trip(data)
    }
}
