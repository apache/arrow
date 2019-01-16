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

///! Many builders are available to easily create different types of arrow arrays
extern crate arrow;

use arrow::builder::Int32Builder;

fn main() {
    // Primitive Arrays
    //
    // Primitive arrays are arrays of fixed-width primitive types (bool, u8, u16, u32, u64, i8, i16,
    // i32, i64, f32, f64)

    // Create a new builder with a capacity of 100
    let mut primitive_array_builder = Int32Builder::new(100);

    // Append an individual primitive value
    primitive_array_builder.append_value(55).unwrap();

    // Append a null value
    primitive_array_builder.append_null().unwrap();

    // Append a slice of primitive values
    primitive_array_builder.append_slice(&[39, 89, 12]).unwrap();

    // Build the `PrimitiveArray`
    let _primitive_array = primitive_array_builder.finish();
}
