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

extern crate arrow;

use arrow::array::*;
use arrow::builder::*;

fn main() {
    let mut builder: Builder<i32> = Builder::new();
    for i in 0..10 {
        builder.push(i);
    }
    let buffer = builder.finish();

    println!("buffer length: {}", buffer.len());
    println!("buffer contents: {:?}", buffer.iter().collect::<Vec<i32>>());

    // note that the builder can no longer be used once it has built a buffer, so either
    // of the following calls will fail

    //    builder.push(123);
    //    builder.build();

    // create a memory-aligned Arrow from the builder (zero-copy)
    let array = PrimitiveArray::from(buffer);
    println!("array contents: {:?}", array.iter().collect::<Vec<i32>>());
}
