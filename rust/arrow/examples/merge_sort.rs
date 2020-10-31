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

///! Example demonstrating a merge sort of two un-sorted arrays
use std::sync::Arc;

extern crate arrow;

use arrow::compute;
use arrow::error::Result;
use arrow::{
    array::{ArrayRef, UInt32Array},
    compute::SortOptions,
};

fn main() -> Result<()> {
    // the arrays
    let array1 = Arc::new(UInt32Array::from(vec![1, 4, 2])) as ArrayRef;
    let array2 = Arc::new(UInt32Array::from(vec![6, 3, 5])) as ArrayRef;

    // the sort options we are using
    let options = SortOptions::default();

    // the expectation
    let expected = UInt32Array::from(vec![1, 2, 3, 4, 5, 6]);

    // step 1: sort each of the arrays
    let array1 = compute::sort(&array1, Some(options))?;
    let array2 = compute::sort(&array2, Some(options))?;

    // step 2: compute how to merge them (which side to take at every index)
    let is_left = compute::merge_indices(
        &[array1.clone()],
        &[array2.clone()],
        &[SortOptions::default()],
    )?;

    // step 3: merge them
    let result = compute::merge(array1.as_ref(), array2.as_ref(), &is_left)?;

    // verify result
    let result = result.as_any().downcast_ref::<UInt32Array>().unwrap();

    assert_eq!(&expected, result);
    Ok(())
}
