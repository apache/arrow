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

use libc::size_t;
use std::os::raw::{c_char, c_void};

/// Arrow buffer representation compatible with c memory layout.
#[repr(C)]
struct CBuffer {
    buffer_addr: *const u8,
    buffer_len: size_t,
}

/// Arrow array data representation compatible with c memory layout.
#[repr(C)]
struct CArrayData {
    validity_buffer: CBuffer,
    data_buffer: CBuffer,
    offset_buffer: CBuffer,
}

/// Utility list struct.
#[repr(C)]
struct CList {
    ptrs: *mut c_void,
    len: size_t,
}

/// List with error message.
#[repr(C)]
struct CListResult {
    /// error_msg is null when Ok, otherwise contains error message.
    error_msg: *const c_char,
    list: CList,
}

/// Pointer with error message.
#[repr(C)]
struct CPointerResult {
    error_msg: *const c_char,
    ptr: *mut c_void,
}

#[link_name = "gandiva"]
extern "C" {
    /// Create a projector with serialized schema and expressions.
    fn make_projector(schema_data: CBuffer, expression_data: CBuffer) -> CPointerResult;
    /// Evaluate a record batch with projector.
    fn eval_projector(projector: *mut c_void, input_arrays: CList) -> CListResult;
    /// Destroy projector.
    fn release_projector(projector: *mut c_void);

    /// Create a filter with serialized schema and expression.
    fn make_filter(schema_data: CBuffer, expression_data: CBuffer) -> CPointerResult;
    /// Evaluate a filter against input.
    fn eval_filter(filter: *mut c_void, input_arrays: CList) -> CListResult;
    /// Destroy a filter.
    fn release_filter(filter: *mut c_void);
}
