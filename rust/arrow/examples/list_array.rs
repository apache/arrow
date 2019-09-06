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

///! Example of using list arrays
extern crate arrow;

use arrow::array::{ArrayData, ListArray};
use arrow::buffer::Buffer;
use arrow::datatypes::{DataType, ToByteSlice};

fn main() {
    // Construct a value array
    let value_data = ArrayData::builder(DataType::Int32)
        .len(8)
        .add_buffer(Buffer::from(&[0, 1, 2, 3, 4, 5, 6, 7].to_byte_slice()))
        .build();

    // Construct a buffer for value offsets, for the nested array:
    //  [[0, 1, 2], [3, 4, 5], [6, 7]]
    let value_offsets = Buffer::from(&[0, 3, 6, 8].to_byte_slice());

    // Construct a list array from the above two
    let list_data_type = DataType::List(Box::new(DataType::Int32));
    let list_data = ArrayData::builder(list_data_type.clone())
        .len(3)
        .add_buffer(value_offsets.clone())
        .add_child_data(value_data.clone())
        .build();
    let list_array = ListArray::from(list_data);

    println!("{:?}", list_array);
}
