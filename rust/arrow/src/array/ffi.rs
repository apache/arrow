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

impl TryFrom<ffi::ArrowArray> for ArrayData {
    type Error = ArrowError;

    fn try_from(value: ffi::ArrowArray) -> Result<Self> {
        let data_type = value.data_type()?;
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
            // this is empty because ffi still does not support it.
            // this is ok because FFI only supports datatypes without childs
            vec![],
        ))
    }
}

impl TryFrom<ArrayData> for ffi::ArrowArray {
    type Error = ArrowError;

    fn try_from(value: ArrayData) -> Result<Self> {
        let len = value.len();
        let offset = value.offset() as usize;
        let null_count = value.null_count();
        let buffers = value.buffers().to_vec();
        let null_buffer = value.null_buffer().cloned();

        unsafe {
            ffi::ArrowArray::try_new(
                value.data_type(),
                len,
                null_count,
                null_buffer,
                offset,
                buffers,
                // this is empty because ffi still does not support it.
                // this is ok because FFI only supports datatypes without childs
                vec![],
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
        let data = UInt32Array::from(vec![Some(2), None, Some(1), None]).data();
        test_round_trip(data.as_ref())
    }

    #[test]
    fn test_u64() -> Result<()> {
        let data = UInt64Array::from(vec![Some(2), None, Some(1), None]).data();
        test_round_trip(data.as_ref())
    }

    #[test]
    fn test_i64() -> Result<()> {
        let data = Int64Array::from(vec![Some(2), None, Some(1), None]).data();
        test_round_trip(data.as_ref())
    }
}
