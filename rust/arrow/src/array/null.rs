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
use std::mem;

use crate::array::{Array, ArrayData, ArrayDataRef};
use crate::datatypes::*;

/// An Array where all elements are nulls
pub struct NullArray {
    data: ArrayDataRef,
}

impl NullArray {
    /// Create a new null array of the specified length
    pub fn new(length: usize) -> Self {
        let array_data = ArrayData::builder(DataType::Null).len(length).build();
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

    /// Returns whether the element at `index` is null.
    /// All elements of a `NullArray` are always null.
    fn is_null(&self, _index: usize) -> bool {
        true
    }

    /// Returns whether the element at `index` is valid.
    /// All elements of a `NullArray` are always invalid.
    fn is_valid(&self, _index: usize) -> bool {
        false
    }

    /// Returns the total number of null values in this array.
    /// The null count of a `NullArray` always equals its length.
    fn null_count(&self) -> usize {
        self.data_ref().len()
    }

    /// Returns the total number of bytes of memory occupied by the buffers owned by this [NullArray].
    fn get_buffer_memory_size(&self) -> usize {
        self.data.get_buffer_memory_size()
    }

    /// Returns the total number of bytes of memory occupied physically by this [NullArray].
    fn get_array_memory_size(&self) -> usize {
        self.data.get_array_memory_size() + mem::size_of_val(self)
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
            data.null_buffer().is_none(),
            "NullArray data should not contain a null buffer, as no buffers are required"
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
        let null_arr = NullArray::new(32);

        assert_eq!(null_arr.len(), 32);
        assert_eq!(null_arr.null_count(), 32);
        assert_eq!(null_arr.is_valid(0), false);

        assert_eq!(0, null_arr.get_buffer_memory_size());
        let internals_of_null_array = 64; // Arc<ArrayData>
        assert_eq!(
            null_arr.get_buffer_memory_size() + internals_of_null_array,
            null_arr.get_array_memory_size()
        );
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
