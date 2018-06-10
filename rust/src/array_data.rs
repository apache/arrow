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

use std::sync::Arc;

use bitmap::Bitmap;
use buffer::Buffer;
use datatypes::DataType;
use util::bit_util::count_set_bits;

#[derive(PartialEq, Debug)]
pub struct ArrayData {
    /// The data type for this array data
    data_type: DataType,

    /// The number of elements in this array data
    length: i64,

    /// The number of null elements in this array data
    null_count: i64,

    /// The offset into this array data.
    offset: i64,

    /// The buffers for this array data. Note that depending on the array types, this
    /// could hold different types of buffers (e.g., null bit buffer, value buffer, value
    /// offset buffer) at different positions.
    buffers: Vec<Buffer>,

    /// The child(ren) of this array. Only non-empty for `ListArray` and `StructArray`.
    child_data: Vec<ArrayDataRef>,

    /// The null bit map. A `None` value for this indicates all values are non-null in
    /// this array.
    null_bitmap: Option<Bitmap>,
}

pub type ArrayDataRef = Arc<ArrayData>;
pub const UNKNOWN_NULL_COUNT: i64 = -1;

impl ArrayData {
    pub fn new(
        data_type: DataType, length: i64, mut null_count: i64,
        null_bit_buffer: Option<Buffer>, offset: i64, buffers: Vec<Buffer>,
        child_data: Vec<ArrayDataRef>,
    ) -> Self {
        if null_count < 0 {
            null_count = if let Some(ref buf) = null_bit_buffer {
                count_set_bits(buf.data())
            } else {
                0
            }
        }
        let null_bitmap = null_bit_buffer.map(Bitmap::from);
        Self {
            data_type, length, null_count, offset, buffers, child_data, null_bitmap
        }
    }

    /// Returns a builder to construct a `ArrayData` instance.
    pub fn builder(data_type: DataType) -> ArrayDataBuilder {
        ArrayDataBuilder::new(data_type)
    }

    /// Returns a copy of the data type for this array data.
    pub fn data_type(&self) -> DataType {
        self.data_type.clone()
    }

    /// Returns a slice of buffers for this array data.
    pub fn buffers(&self) -> &[Buffer] {
        &self.buffers[..]
    }

    /// Returns a slice of children data arrays.
    pub fn child_data(&self) -> &[ArrayDataRef] {
        &self.child_data[..]
    }

    /// Returns whether the element at index `i` is null.
    pub fn is_null(&self, i: usize) -> bool {
        if let Some(ref b) = self.null_bitmap {
            return b.is_set(i)
        }
        false
    }

    /// Returns whether the element at index `i` is not null.
    pub fn is_valid(&self, i: usize) -> bool {
        if let Some(ref b) = self.null_bitmap {
            return !b.is_set(i)
        }
        true
    }

    /// Returns the length (i.e., number of elements) of this array.
    pub fn length(&self) -> usize {
        self.length as usize
    }

    /// Returns the offset of this array.
    pub fn offset(&self) -> i64 {
        self.offset
    }

    /// Returns the total number of nulls in this array.
    pub fn null_count(&self) -> i64 {
        self.null_count
    }
}

pub struct ArrayDataBuilder {
    data_type: DataType,
    length: i64,
    null_count: i64,
    null_bit_buffer: Option<Buffer>,
    offset: i64,
    buffers: Vec<Buffer>,
    child_data: Vec<ArrayDataRef>,
}

impl ArrayDataBuilder {
    pub fn new(data_type: DataType) -> Self {
        Self {
            data_type: data_type,
            length: 0,
            null_count: UNKNOWN_NULL_COUNT,
            null_bit_buffer: None,
            offset: 0,
            buffers: vec![],
            child_data: vec![],
        }
    }

    pub fn length(mut self, n: i64) -> Self {
        self.length = n;
        self
    }

    pub fn null_count(mut self, n: i64) -> Self {
        self.null_count = n;
        self
    }

    pub fn null_bit_buffer(mut self, buf: Buffer) -> Self {
        self.null_bit_buffer = Some(buf);
        self
    }

    pub fn offset(mut self, n: i64) -> Self {
        self.offset = n;
        self
    }

    pub fn buffers(mut self, v: Vec<Buffer>) -> Self {
        self.buffers = v;
        self
    }

    pub fn add_buffer(mut self, b: Buffer) -> Self {
        self.buffers.push(b);
        self
    }

    pub fn child_data(mut self, v: Vec<ArrayDataRef>) -> Self {
        self.child_data = v;
        self
    }

    pub fn add_child_data(mut self, r: ArrayDataRef) -> Self {
        self.child_data.push(r);
        self
    }

    pub fn build(self) -> ArrayDataRef {
        let data = ArrayData::new(
            self.data_type, self.length, self.null_count, self.null_bit_buffer,
            self.offset, self.buffers, self.child_data);
        Arc::new(data)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use buffer::Buffer;
    use super::{ArrayData, DataType};

    #[test]
    fn test_new() {
        let arr_data = ArrayData::new(DataType::Boolean, 10, 1, None, 2, vec![], vec![]);
        assert_eq!(10, arr_data.length());
        assert_eq!(1, arr_data.null_count());
        assert_eq!(2, arr_data.offset());
        assert_eq!(0, arr_data.buffers().len());
        assert_eq!(0, arr_data.child_data().len());
    }

    #[test]
    fn test_builder() {
        let v = vec![0, 1, 2, 3];
        let child_arr_data = Arc::new(
            ArrayData::new(DataType::Int32, 10, 0, None, 0, vec![], vec![]));
        let b1 = Buffer::from(&v[..]);
        let arr_data = ArrayData::builder(DataType::Int32)
            .length(20)
            .null_count(10)
            .offset(5)
            .add_buffer(b1)
            .add_child_data(child_arr_data.clone())
            .build();

        assert_eq!(20, arr_data.length());
        assert_eq!(10, arr_data.null_count());
        assert_eq!(5, arr_data.offset());
        assert_eq!(1, arr_data.buffers().len());
        assert_eq!(&[0, 1, 2, 3], arr_data.buffers()[0].data());
        assert_eq!(1, arr_data.child_data().len());
        assert_eq!(child_arr_data, arr_data.child_data()[0]);
    }
}
