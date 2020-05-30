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

//! Contains the `NullArray` type.
//!
//! A `NullArray` is a simplified array where all values are null.
//! Although the [specification](https://arrow.apache.org/docs/format/Columnar.html#null-layout)
//! allows for not allocating any memory buffers, the Rust implementation allocates null buffers
//! to ensure consistency of null checks.
//!
//! # Example: Create an array
//!
//! ```
//! use arrow::array::{Array, NullArray};
//!
//! # fn main() -> arrow::error::Result<()> {
//! let array = NullArray::new(10);
//!
//! assert_eq!(array.len(), 10);
//! assert_eq!(array.null_count(), 10);
//!
//! # Ok(())
//! # }
//! ```

use std::any::Any;
use std::fmt;

use crate::array::{Array, ArrayData, ArrayDataRef};
use crate::buffer::MutableBuffer;
use crate::datatypes::*;
use crate::util::bit_util;

/// An Array where all elements are nulls
pub struct NullArray {
    data: ArrayDataRef,
}

impl NullArray {
    /// Create a new null array of the specified length
    pub fn new(length: usize) -> Self {
        let num_bytes = bit_util::ceil(length, 8);
        let null_buf = MutableBuffer::new(num_bytes).with_bitset(num_bytes, false);
        let array_data = ArrayData::builder(DataType::Null)
            .len(length)
            .null_count(length)
            .null_bit_buffer(null_buf.freeze())
            .build();
        NullArray::from(array_data)
    }
}

impl Array for NullArray {
    fn as_any(&self) -> &Any {
        self
    }

    fn data(&self) -> ArrayDataRef {
        self.data.clone()
    }

    fn data_ref(&self) -> &ArrayDataRef {
        &self.data
    }
}

impl From<ArrayDataRef> for NullArray {
    fn from(data: ArrayDataRef) -> Self {
        assert_eq!(
            data.buffers().len(),
            0,
            "NullArray data should contain 0 buffers"
        );
        assert!(
            data.null_buffer().is_some(),
            "NullArray data should contain a null buffer"
        );
        Self { data }
    }
}

impl fmt::Debug for NullArray {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "NullArray")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_null_array() {
        let array1 = NullArray::new(32);

        assert_eq!(array1.len(), 32);
        assert_eq!(array1.null_count(), 32);
        assert_eq!(array1.is_valid(0), false);
    }

    #[test]
    fn test_null_array_slice() {
        let array1 = NullArray::new(32);

        let array2 = array1.slice(8, 16);
        assert_eq!(array2.len(), 16);
        assert_eq!(array2.null_count(), 16);
        assert_eq!(array2.offset(), 8);
    }
}
