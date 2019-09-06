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

///! Example of using binary arrays
extern crate arrow;

use arrow::array::{ArrayData, BinaryArray};
use arrow::buffer::Buffer;
use arrow::datatypes::{DataType, ToByteSlice};

fn main() {
    let values: [u8; 12] = [
        b'h', b'e', b'l', b'l', b'o', b'p', b'a', b'r', b'q', b'u', b'e', b't',
    ];
    let offsets: [i32; 4] = [0, 5, 5, 12];

    // Array data: ["hello", "", "parquet"]
    let array_data = ArrayData::builder(DataType::Utf8)
        .len(3)
        .add_buffer(Buffer::from(offsets.to_byte_slice()))
        .add_buffer(Buffer::from(&values[..]))
        .null_bit_buffer(Buffer::from([0b00000101]))
        .build();
    let binary_array = BinaryArray::from(array_data);
    println!("{:?}", binary_array);
}
