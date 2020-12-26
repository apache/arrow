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

///! Tensor builder example
extern crate arrow;

use arrow::array::*; //{Int32BufferBuilder, Float32BufferBuilder};
use arrow::buffer::Buffer;
use arrow::datatypes::ToByteSlice;
use arrow::error::Result;
use arrow::tensor::{Float32Tensor, Int32Tensor};

fn main() -> Result<()> {
    // Building a tensor using the buffer builder for Int32
    // The buffer builder will pad the appended numbers
    // to match the required size for each buffer
    let mut builder = Int32BufferBuilder::new(16);
    for i in 0..16 {
        builder.append(i);
    }
    let buf = builder.finish();

    // When building a tensor the buffer and shape are required
    // The new function will estimate the expected stride for the
    // storage data
    let tensor = Int32Tensor::try_new(buf, Some(vec![2, 8]), None, None)?;
    println!("Int32 Tensor");
    println!("{:?}", tensor);

    // Creating a tensor using float type buffer builder
    let mut builder = Float32BufferBuilder::new(4);
    builder.append(1.0);
    builder.append(2.0);
    builder.append(3.0);
    builder.append(4.0);
    let buf = builder.finish();

    // When building the tensor the buffer and shape are necessary
    // The new function will estimate the expected stride for the
    // storage data
    let tensor = Float32Tensor::try_new(buf, Some(vec![2, 2]), None, None)?;
    println!("\nFloat32 Tensor");
    println!("{:?}", tensor);

    // In order to build a tensor from an array the function to_byte_slice add the
    // required padding to the elements in the array.
    let buf = Buffer::from(&[0, 1, 2, 3, 4, 5, 6, 7, 9, 10].to_byte_slice());
    let tensor = Int32Tensor::try_new(buf, Some(vec![2, 5]), None, None)?;
    println!("\nInt32 Tensor");
    println!("{:?}", tensor);

    Ok(())
}
