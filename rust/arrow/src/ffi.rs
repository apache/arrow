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

//! Contains declarations of the C Data Interface and how to import them.
//! Generally, this module is divided in two main interfaces:
//! One interface maps C ABI to native Rust types, i.e. convert c-pointers, c_char, to native rust.
//! This is handled by [FFI_ArrowSchema] and [FFI_ArrowArray].
//! The other interface maps native Rust types to the Rust-specific implementation of Arrow such as `format` to [Datatype],
//! `Buffer`, etc. This is handled by [ArrowArray].

use std::{ffi::CStr, ffi::CString, iter, mem, ptr};

use crate::datatypes::DataType;
use crate::error::{ArrowError, Result};
use crate::{buffer::Buffer, memory};

/// ABI-compatible struct for FFI_ArrowSchema from C Data Interface
/// See https://arrow.apache.org/docs/format/CDataInterface.html#structure-definitions
/// This was created by bindgen
#[repr(C)]
#[derive(Debug, Clone)]
pub struct FFI_ArrowSchema {
    format: *const ::std::os::raw::c_char,
    name: *const ::std::os::raw::c_char,
    metadata: *const ::std::os::raw::c_char,
    flags: i64,
    n_children: i64,
    children: *mut *mut FFI_ArrowSchema,
    dictionary: *mut FFI_ArrowSchema,
    release: ::std::option::Option<unsafe extern "C" fn(arg1: *mut FFI_ArrowSchema)>,
    private_data: *mut ::std::os::raw::c_void,
}

impl FFI_ArrowSchema {
    /// create a new [FFI_ArrowSchema] from a format. Atm we are only using `format`.
    pub(crate) fn new(format: &str) -> FFI_ArrowSchema {
        // https://arrow.apache.org/docs/format/CDataInterface.html#c.ArrowSchema
        FFI_ArrowSchema {
            format: CString::new(format).unwrap().into_raw(),
            name: std::ptr::null_mut(),
            metadata: std::ptr::null_mut(),
            flags: 0,
            n_children: 0,
            children: ptr::null_mut(),
            dictionary: std::ptr::null_mut(),
            release: None,
            private_data: std::ptr::null_mut(),
        }
    }

    pub(crate) fn format(&self) -> &str {
        unsafe { CStr::from_ptr(self.format) }
            .to_str()
            .expect("The external API has a non-utf8 as format")
    }
}

impl Drop for FFI_ArrowSchema {
    fn drop(&mut self) {
        match self.release {
            None => (),
            Some(release) => unsafe { release(self) },
        };
    }
}

/// maps a DataType `format` to a [DataType](arrow::datatypes::DataType).
/// See https://arrow.apache.org/docs/format/CDataInterface.html#data-type-description-format-strings
fn to_datatype(format: &str) -> Result<DataType> {
    Ok(match format {
        "n" => DataType::Null,
        "b" => DataType::Boolean,
        "c" => DataType::Int8,
        "C" => DataType::UInt8,
        "s" => DataType::Int16,
        "S" => DataType::UInt16,
        "i" => DataType::Int32,
        "I" => DataType::UInt32,
        "l" => DataType::Int64,
        "L" => DataType::UInt64,
        "e" => DataType::Float16,
        "f" => DataType::Float32,
        "g" => DataType::Float64,
        "z" => DataType::Binary,
        "Z" => DataType::LargeBinary,
        "u" => DataType::Utf8,
        "U" => DataType::LargeUtf8,
        _ => {
            return Err(ArrowError::CDataInterface(
                "The datatype \"{}\" is still not supported in Rust implementation"
                    .to_string(),
            ))
        }
    })
}

/// the inverse of [to_datatype]
fn from_datatype(datatype: &DataType) -> Result<String> {
    Ok(match datatype {
        DataType::Null => "n",
        DataType::Boolean => "b",
        DataType::Int8 => "c",
        DataType::UInt8 => "C",
        DataType::Int16 => "s",
        DataType::UInt16 => "S",
        DataType::Int32 => "i",
        DataType::UInt32 => "I",
        DataType::Int64 => "l",
        DataType::UInt64 => "L",
        DataType::Float16 => "e",
        DataType::Float32 => "f",
        DataType::Float64 => "g",
        DataType::Binary => "z",
        DataType::LargeBinary => "Z",
        DataType::Utf8 => "u",
        DataType::LargeUtf8 => "U",
        _ => {
            return Err(ArrowError::CDataInterface(
                "The datatype \"{:?}\" is still not supported in Rust implementation"
                    .to_string(),
            ))
        }
    }
    .to_string())
}

// returns the number of bytes of a given type, when stored in a buffer.
// applicable to types that can be represented as buffers
fn byte_width(data_type: &DataType) -> Result<usize> {
    Ok(match data_type {
        DataType::UInt32 => 4,
        DataType::UInt64 => 8,
        DataType::Int64 => 8,
        _ => {
            return Err(ArrowError::CDataInterface(format!(
                "The datatype \"{:?}\" is still not supported in Rust implementation",
                data_type
            )))
        }
    })
}

/// ABI-compatible struct for ArrowArray from C Data Interface
/// See https://arrow.apache.org/docs/format/CDataInterface.html#structure-definitions
/// This was created by bindgen
#[repr(C)]
#[derive(Debug, Clone)]
pub struct FFI_ArrowArray {
    pub length: i64,
    pub null_count: i64,
    pub offset: i64,
    pub n_buffers: i64,
    pub n_children: i64,
    pub buffers: *mut *const ::std::os::raw::c_void,
    pub children: *mut *mut FFI_ArrowArray,
    pub dictionary: *mut FFI_ArrowArray,
    pub release: ::std::option::Option<unsafe extern "C" fn(arg1: *mut FFI_ArrowArray)>,
    pub private_data: *mut ::std::os::raw::c_void,
}

// callback used to drop [FFI_ArrowArray].
extern "C" fn release(array: *mut FFI_ArrowArray) {
    if array.is_null() {
        return ();
    }
    let value = unsafe { &*array };

    (0..value.n_buffers).for_each(|i| {
        let ptr = unsafe { *value.buffers.add(i as usize) } as *mut u8;
        if !ptr.is_null() {
            // todo: store in the private data the buffer's capacity, as it needs to be passed here.
            unsafe { memory::free_aligned(ptr, 4) };
        }
    });
}

impl FFI_ArrowArray {
    /// creates a new `FFI_ArrowArray` from existing data.
    /// This is used to export an `FFI_ArrowArray`.
    fn new(
        length: i64,
        null_count: i64,
        offset: i64,
        n_buffers: i64,
        buffers: Vec<Option<Buffer>>,
    ) -> Self {
        // build an array of pointers to the buffers, forgetting the buffers
        let mut buffers_ptr = buffers
            .iter()
            .map(|maybe_buffer| {
                match maybe_buffer {
                    Some(b) => {
                        let ptr = b.raw_data() as *const std::os::raw::c_void;
                        // forget the buffer
                        mem::forget(b);
                        ptr
                    }
                    None => std::ptr::null(),
                }
            })
            .collect::<Box<[_]>>();
        mem::forget(buffers);
        let buffers = buffers_ptr.as_mut_ptr();
        // forget the array of buffers as `release` will take care of them.
        mem::forget(buffers_ptr);

        Self {
            length,
            null_count,
            offset,
            n_buffers,
            n_children: 0,
            buffers,
            children: std::ptr::null_mut(),
            dictionary: std::ptr::null_mut(),
            release: Some(release),
            private_data: std::ptr::null_mut(),
        }
    }

    /// returns the buffer at index `i`. It may not exist (null pointer)
    /// `offset` must match the offset of each of the valid types, in bytes.
    /// # Panic
    /// This function panics if i is larger or equal to `n_buffers`.
    fn buffer(&self, i: usize, offset: usize) -> Option<Buffer> {
        assert!(i < self.n_buffers as usize);
        if self.buffers.is_null() {
            return None;
        }
        let buffers = self.buffers as *mut *const u8;

        let ptr = unsafe { *buffers.add(i) };

        if ptr.is_null() {
            return None;
        } else {
            let len = self.length as usize * offset;
            Some(unsafe { Buffer::from_unowned(ptr, len, self.clone()) })
        }
    }
}

impl Drop for FFI_ArrowArray {
    fn drop(&mut self) {
        match self.release {
            None => (),
            Some(release) => unsafe { release(self) },
        };
    }
}

/// Struct containing the necessary data and schema to move an Array from and to the C Data Interface.
/// Its main responsibility is to expose functionality that requires
/// both [FFI_ArrowArray] and [FFI_ArrowSchema].
///
/// To export to the C Data Interface, use [ArrowArray::new] and [ArrowArray::to_raw].
/// To import from the C Data Interface, use [ArrowArray::try_from_raw].
#[derive(Debug)]
pub struct ArrowArray {
    // invariant: neither of them is null. We just do not own them and their lifetime is not monitored
    // by Rust
    array: *const FFI_ArrowArray,
    schema: *const FFI_ArrowSchema,
}

impl ArrowArray {
    /// creates a new `ArrowArray`. This is used to export to the C Data Interface.
    pub fn try_new(
        data_type: &DataType,
        len: usize,
        null_count: usize,
        null_buffer: Option<Buffer>,
        offset: usize,
        buffers: Vec<Buffer>,
        child_data: Vec<ArrowArray>,
    ) -> Result<Self> {
        let format = from_datatype(data_type)?;
        // * insert the null buffer at the start
        // * make all others `Option<Buffer>`.
        let new_buffers = iter::once(null_buffer)
            .chain(buffers.iter().map(|b| Some(b.clone())))
            .collect::<Vec<_>>();

        // allocate both structures in the heap
        let schema = Box::into_raw(Box::new(FFI_ArrowSchema::new(&format)));
        let array = Box::into_raw(Box::new(FFI_ArrowArray::new(
            len as i64,
            null_count as i64,
            offset as i64,
            new_buffers.len() as i64,
            new_buffers,
        )));

        Ok(ArrowArray { schema, array })
    }

    /// creates a new `ArrowArray`. This is used to import from the C Data Interface
    pub unsafe fn try_from_raw(
        array: *const FFI_ArrowArray,
        schema: *const FFI_ArrowSchema,
    ) -> Result<Self> {
        if array.is_null() || schema.is_null() {
            return Err(ArrowError::MemoryError(
                "At least one of the pointers passed to `try_from_raw` is null"
                    .to_string(),
            ));
        };
        Ok(Self { array, schema })
    }

    // private method to access `array`. This is safe because [ArrowArray::try_from_raw] asserts that the pointer is not null.
    #[inline]
    fn get_array(&self) -> &FFI_ArrowArray {
        unsafe { self.array.as_ref() }.unwrap()
    }

    /// exports [ArrowArray]] to the C Data Interface
    pub fn to_raw(this: ArrowArray) -> (*const FFI_ArrowArray, *const FFI_ArrowSchema) {
        (this.array, this.schema)
    }

    /// returns the null bit buffer.
    /// Rust implementation uses a buffer that is not part of the array of buffers.
    /// The C Data interface's null buffer is part of the array of buffers.
    pub fn null_bit_buffer(&self) -> Option<Buffer> {
        // the null bitmaps are always 4 bytes?
        self.get_array().buffer(0, 4)
    }

    /// returns all buffers, as organized by Rust.
    pub fn buffers(&self) -> Result<Vec<Buffer>> {
        // todo: width depends on the buffer...
        let width = byte_width(&self.data_type()?)?;
        let array = self.get_array();
        Ok((1..array.n_buffers)
            .map(|i| array.buffer(i as usize, width).unwrap())
            .collect())
    }

    /// the length of the array
    pub fn len(&self) -> usize {
        self.get_array().length as usize
    }

    /// the offset of the array
    pub fn offset(&self) -> usize {
        self.get_array().offset as usize
    }

    /// the null count of the array
    pub fn null_count(&self) -> usize {
        self.get_array().null_count as usize
    }

    /// the data_type as declared in the schema
    pub fn data_type(&self) -> Result<DataType> {
        to_datatype(unsafe { self.schema.as_ref() }.unwrap().format())
    }
}

// todo: add testing
