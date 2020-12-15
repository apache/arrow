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

//! Contains declarations to bind to the [C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html).
//!
//! Generally, this module is divided in two main interfaces:
//! One interface maps C ABI to native Rust types, i.e. convert c-pointers, c_char, to native rust.
//! This is handled by [FFI_ArrowSchema] and [FFI_ArrowArray].
//!
//! The second interface maps native Rust types to the Rust-specific implementation of Arrow such as `format` to [Datatype],
//! `Buffer`, etc. This is handled by [ArrowArray].
//!
//! ```rust
//! # use std::sync::Arc;
//! # use arrow::array::{Int32Array, Array, ArrayData, make_array_from_raw};
//! # use arrow::error::{Result, ArrowError};
//! # use arrow::compute::kernels::arithmetic;
//! # use std::convert::TryFrom;
//! # fn main() -> Result<()> {
//! // create an array natively
//! let array = Int32Array::from(vec![Some(1), None, Some(3)]);
//!
//! // export it
//! let (array_ptr, schema_ptr) = array.to_raw()?;
//!
//! // consumed and used by something else...
//!
//! // import it
//! let array = unsafe { make_array_from_raw(array_ptr, schema_ptr)? };
//!
//! // perform some operation
//! let array = array.as_any().downcast_ref::<Int32Array>().ok_or(
//!     ArrowError::ParseError("Expects an int32".to_string()),
//! )?;
//! let array = arithmetic::add(&array, &array)?;
//!
//! // verify
//! assert_eq!(array, Int32Array::from(vec![Some(2), None, Some(6)]));
//!
//! // (drop/release)
//! Ok(())
//! }
//! ```

/*
# Design:

Main assumptions:
* A memory region is deallocated according it its own release mechanism.
* Rust shares memory regions between arrays.
* A memory region should be deallocated when no-one is using it.

The design of this module is as follows:

`ArrowArray` contains two `Arc`s, one per ABI-compatible `struct`, each containing data
according to the C Data Interface. These Arcs are used for ref counting of the structs
within Rust and lifetime management.

Each ABI-compatible `struct` knowns how to `drop` itself, calling `release`.

To import an array, unsafely create an `ArrowArray` from two pointers using [ArrowArray::try_from_raw].
To export an array, create an `ArrowArray` using [ArrowArray::try_new].
*/

use std::{ffi::CStr, ffi::CString, iter, mem::size_of, ptr, sync::Arc};

use crate::buffer::Buffer;
use crate::datatypes::DataType;
use crate::error::{ArrowError, Result};
use crate::util::bit_util;

/// ABI-compatible struct for `ArrowSchema` from C Data Interface
/// See https://arrow.apache.org/docs/format/CDataInterface.html#structure-definitions
/// This was created by bindgen
#[repr(C)]
#[derive(Debug)]
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

// callback used to drop [FFI_ArrowSchema] when it is exported.
unsafe extern "C" fn release_schema(schema: *mut FFI_ArrowSchema) {
    let schema = &mut *schema;

    // take ownership back to release it.
    CString::from_raw(schema.format as *mut std::os::raw::c_char);

    schema.release = None;
}

impl FFI_ArrowSchema {
    /// create a new [FFI_ArrowSchema] from a format.
    fn new(format: &str) -> FFI_ArrowSchema {
        // https://arrow.apache.org/docs/format/CDataInterface.html#c.ArrowSchema
        FFI_ArrowSchema {
            format: CString::new(format).unwrap().into_raw(),
            name: std::ptr::null_mut(),
            metadata: std::ptr::null_mut(),
            flags: 0,
            n_children: 0,
            children: ptr::null_mut(),
            dictionary: std::ptr::null_mut(),
            release: Some(release_schema),
            private_data: std::ptr::null_mut(),
        }
    }

    /// create an empty [FFI_ArrowSchema]
    fn empty() -> Self {
        Self {
            format: std::ptr::null_mut(),
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

    /// returns the format of this schema.
    pub fn format(&self) -> &str {
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
        z => {
            return Err(ArrowError::CDataInterface(format!(
                "The datatype \"{:?}\" is still not supported in Rust implementation",
                z
            )))
        }
    }
    .to_string())
}

// returns the number of bits that buffer `i` (in the C data interface) is expected to have.
// This is set by the Arrow specification
fn bit_width(data_type: &DataType, i: usize) -> Result<usize> {
    Ok(match (data_type, i) {
        // the null buffer is bit sized
        (_, 0) => 1,
        // primitive types first buffer's size is given by the native types
        (DataType::Boolean, 1) => 1,
        (DataType::UInt8, 1) => size_of::<u8>() * 8,
        (DataType::UInt16, 1) => size_of::<u16>() * 8,
        (DataType::UInt32, 1) => size_of::<u32>() * 8,
        (DataType::UInt64, 1) => size_of::<u64>() * 8,
        (DataType::Int8, 1) => size_of::<i8>() * 8,
        (DataType::Int16, 1) => size_of::<i16>() * 8,
        (DataType::Int32, 1) => size_of::<i32>() * 8,
        (DataType::Int64, 1) => size_of::<i64>() * 8,
        (DataType::Float32, 1) => size_of::<f32>() * 8,
        (DataType::Float64, 1) => size_of::<f64>() * 8,
        // primitive types have a single buffer
        (DataType::Boolean, _) |
        (DataType::UInt8, _) |
        (DataType::UInt16, _) |
        (DataType::UInt32, _) |
        (DataType::UInt64, _) |
        (DataType::Int8, _) |
        (DataType::Int16, _) |
        (DataType::Int32, _) |
        (DataType::Int64, _) |
        (DataType::Float32, _) |
        (DataType::Float64, _) => {
            return Err(ArrowError::CDataInterface(format!(
                "The datatype \"{:?}\" expects 2 buffers, but requested {}. Please verify that the C data interface is correctly implemented.",
                data_type, i
            )))
        }
        // Variable-sized binaries: have two buffers.
        // Utf8: first buffer is i32, second is in bytes
        (DataType::Utf8, 1) => size_of::<i32>() * 8,
        (DataType::Utf8, 2) => size_of::<u8>() * 8,
        (DataType::Utf8, _) => {
            return Err(ArrowError::CDataInterface(format!(
                "The datatype \"{:?}\" expects 3 buffers, but requested {}. Please verify that the C data interface is correctly implemented.",
                data_type, i
            )))
        }
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
#[derive(Debug)]
pub struct FFI_ArrowArray {
    pub(crate) length: i64,
    pub(crate) null_count: i64,
    pub(crate) offset: i64,
    pub(crate) n_buffers: i64,
    pub(crate) n_children: i64,
    pub(crate) buffers: *mut *const ::std::os::raw::c_void,
    children: *mut *mut FFI_ArrowArray,
    dictionary: *mut FFI_ArrowArray,
    release: ::std::option::Option<unsafe extern "C" fn(arg1: *mut FFI_ArrowArray)>,
    // When exported, this MUST contain everything that is owned by this array.
    // for example, any buffer pointed to in `buffers` must be here, as well as the `buffers` pointer
    // itself.
    // In other words, everything in [FFI_ArrowArray] must be owned by `private_data` and can assume
    // that they do not outlive `private_data`.
    private_data: *mut ::std::os::raw::c_void,
}

// callback used to drop [FFI_ArrowArray] when it is exported
unsafe extern "C" fn release_array(array: *mut FFI_ArrowArray) {
    if array.is_null() {
        return;
    }
    let array = &mut *array;
    // take ownership of `private_data`, therefore dropping it
    Box::from_raw(array.private_data as *mut PrivateData);

    array.release = None;
}

struct PrivateData {
    buffers: Vec<Option<Buffer>>,
    buffers_ptr: Box<[*const std::os::raw::c_void]>,
}

impl FFI_ArrowArray {
    /// creates a new `FFI_ArrowArray` from existing data.
    /// # Safety
    /// This method releases `buffers`. Consumers of this struct *must* call `release` before
    /// releasing this struct, or contents in `buffers` leak.
    unsafe fn new(
        length: i64,
        null_count: i64,
        offset: i64,
        n_buffers: i64,
        buffers: Vec<Option<Buffer>>,
    ) -> Self {
        let buffers_ptr = buffers
            .iter()
            .map(|maybe_buffer| match maybe_buffer {
                // note that `raw_data` takes into account the buffer's offset
                Some(b) => b.raw_data() as *const std::os::raw::c_void,
                None => std::ptr::null(),
            })
            .collect::<Box<[_]>>();
        let pointer = buffers_ptr.as_ptr() as *mut *const std::ffi::c_void;

        // create the private data owning everything.
        // any other data must be added here, e.g. via a struct, to track lifetime.
        let private_data = Box::new(PrivateData {
            buffers,
            buffers_ptr,
        });

        Self {
            length,
            null_count,
            offset,
            n_buffers,
            n_children: 0,
            buffers: pointer,
            children: std::ptr::null_mut(),
            dictionary: std::ptr::null_mut(),
            release: Some(release_array),
            private_data: Box::into_raw(private_data) as *mut ::std::os::raw::c_void,
        }
    }

    // create an empty `FFI_ArrowArray`, which can be used to import data into
    fn empty() -> Self {
        Self {
            length: 0,
            null_count: 0,
            offset: 0,
            n_buffers: 0,
            n_children: 0,
            buffers: std::ptr::null_mut(),
            children: std::ptr::null_mut(),
            dictionary: std::ptr::null_mut(),
            release: None,
            private_data: std::ptr::null_mut(),
        }
    }
}

/// returns a new buffer corresponding to the index `i` of the FFI array. It may not exist (null pointer).
/// `bits` is the number of bits that the native type of this buffer has.
/// The size of the buffer will be `ceil(self.length * bits, 8)`.
/// # Panic
/// This function panics if `i` is larger or equal to `n_buffers`.
/// # Safety
/// This function assumes that `ceil(self.length * bits, 8)` is the size of the buffer
unsafe fn create_buffer(
    array: Arc<FFI_ArrowArray>,
    index: usize,
    len: usize,
) -> Option<Buffer> {
    if array.buffers.is_null() {
        return None;
    }
    let buffers = array.buffers as *mut *const u8;

    assert!(index < array.n_buffers as usize);
    let ptr = *buffers.add(index);

    if ptr.is_null() {
        None
    } else {
        Some(Buffer::from_unowned(ptr, len, array))
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

/// Struct used to move an Array from and to the C Data Interface.
/// Its main responsibility is to expose functionality that requires
/// both [FFI_ArrowArray] and [FFI_ArrowSchema].
///
/// This struct has two main paths:
///
/// ## Import from the C Data Interface
/// * [ArrowArray::empty] to allocate memory to be filled by an external call
/// * [ArrowArray::try_from_raw] to consume two non-null allocated pointers
/// ## Export to the C Data Interface
/// * [ArrowArray::try_new] to create a new [ArrowArray] from Rust-specific information
/// * [ArrowArray::into_raw] to expose two pointers for [FFI_ArrowArray] and [FFI_ArrowSchema].
///
/// # Safety
/// Whoever creates this struct is responsible for releasing their resources. Specifically,
/// consumers *must* call [ArrowArray::into_raw] and take ownership of the individual pointers,
/// calling [FFI_ArrowArray::release] and [FFI_ArrowSchema::release] accordingly.
///
/// Furthermore, this struct assumes that the incoming data agrees with the C data interface.
#[derive(Debug)]
pub struct ArrowArray {
    // these are ref-counted because they can be shared by multiple buffers.
    array: Arc<FFI_ArrowArray>,
    schema: Arc<FFI_ArrowSchema>,
}

impl ArrowArray {
    /// creates a new `ArrowArray`. This is used to export to the C Data Interface.
    /// # Safety
    /// See safety of [ArrowArray]
    pub unsafe fn try_new(
        data_type: &DataType,
        len: usize,
        null_count: usize,
        null_buffer: Option<Buffer>,
        offset: usize,
        buffers: Vec<Buffer>,
        _child_data: Vec<ArrowArray>,
    ) -> Result<Self> {
        let format = from_datatype(data_type)?;
        // * insert the null buffer at the start
        // * make all others `Option<Buffer>`.
        let new_buffers = iter::once(null_buffer)
            .chain(buffers.iter().map(|b| Some(b.clone())))
            .collect::<Vec<_>>();

        let schema = Arc::new(FFI_ArrowSchema::new(&format));
        let array = Arc::new(FFI_ArrowArray::new(
            len as i64,
            null_count as i64,
            offset as i64,
            new_buffers.len() as i64,
            new_buffers,
        ));

        Ok(ArrowArray { schema, array })
    }

    /// creates a new [ArrowArray] from two pointers. Used to import from the C Data Interface.
    /// # Safety
    /// See safety of [ArrowArray]
    /// # Error
    /// Errors if any of the pointers is null
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
        Ok(Self {
            array: Arc::from_raw(array as *mut FFI_ArrowArray),
            schema: Arc::from_raw(schema as *mut FFI_ArrowSchema),
        })
    }

    /// creates a new empty [ArrowArray]. Used to import from the C Data Interface.
    /// # Safety
    /// See safety of [ArrowArray]
    pub unsafe fn empty() -> Self {
        let schema = Arc::new(FFI_ArrowSchema::empty());
        let array = Arc::new(FFI_ArrowArray::empty());
        ArrowArray { schema, array }
    }

    /// exports [ArrowArray] to the C Data Interface
    pub fn into_raw(this: ArrowArray) -> (*const FFI_ArrowArray, *const FFI_ArrowSchema) {
        (Arc::into_raw(this.array), Arc::into_raw(this.schema))
    }

    /// returns the null bit buffer.
    /// Rust implementation uses a buffer that is not part of the array of buffers.
    /// The C Data interface's null buffer is part of the array of buffers.
    pub fn null_bit_buffer(&self) -> Option<Buffer> {
        // similar to `self.buffer_len(0)`, but without `Result`.
        let buffer_len = bit_util::ceil(self.array.length as usize, 8);

        unsafe { create_buffer(self.array.clone(), 0, buffer_len) }
    }

    /// Returns the length, in bytes, of the buffer `i` (indexed according to the C data interface)
    // Rust implementation uses fixed-sized buffers, which require knowledge of their `len`.
    // for variable-sized buffers, such as the second buffer of a stringArray, we need
    // to fetch offset buffer's len to build the second buffer.
    fn buffer_len(&self, i: usize) -> Result<usize> {
        let data_type = &self.data_type()?;

        Ok(match (data_type, i) {
            (DataType::Utf8, 1) => {
                // the len of the offset buffer (buffer 1) equals length + 1
                let bits = bit_width(data_type, i)?;
                bit_util::ceil((self.array.length as usize + 1) * bits, 8)
            }
            (DataType::Utf8, 2) => {
                // the len of the data buffer (buffer 2) equals the last value of the offset buffer (buffer 1)
                let len = self.buffer_len(1)?;
                // first buffer is the null buffer => add(1)
                // we assume that pointer is aligned for `i32`, as Utf8 uses `i32` offsets.
                #[allow(clippy::cast_ptr_alignment)]
                let offset_buffer = unsafe {
                    *(self.array.buffers as *mut *const u8).add(1) as *const i32
                };
                // get last offset
                (unsafe { *offset_buffer.add(len / size_of::<i32>() - 1) }) as usize
            }
            // buffer len of primitive types
            _ => {
                let bits = bit_width(data_type, i)?;
                bit_util::ceil(self.array.length as usize * bits, 8)
            }
        })
    }

    /// returns all buffers, as organized by Rust (i.e. null buffer is skipped)
    pub fn buffers(&self) -> Result<Vec<Buffer>> {
        (0..self.array.n_buffers - 1)
            .map(|index| {
                // + 1: skip null buffer
                let index = (index + 1) as usize;

                let len = self.buffer_len(index)?;

                unsafe { create_buffer(self.array.clone(), index, len) }.ok_or_else(
                    || {
                        ArrowError::CDataInterface(format!(
                            "The external buffer at position {} is null.",
                            index - 1
                        ))
                    },
                )
            })
            .collect()
    }

    /// the length of the array
    pub fn len(&self) -> usize {
        self.array.length as usize
    }

    /// whether the array is empty
    pub fn is_empty(&self) -> bool {
        self.array.length == 0
    }

    /// the offset of the array
    pub fn offset(&self) -> usize {
        self.array.offset as usize
    }

    /// the null count of the array
    pub fn null_count(&self) -> usize {
        self.array.null_count as usize
    }

    /// the data_type as declared in the schema
    pub fn data_type(&self) -> Result<DataType> {
        to_datatype(self.schema.format())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::{make_array, Array, ArrayData, Int32Array, StringArray};
    use crate::compute::kernels;
    use std::convert::TryFrom;
    use std::sync::Arc;

    #[test]
    fn test_round_trip() -> Result<()> {
        // create an array natively
        let array = Int32Array::from(vec![1, 2, 3]);

        // export it
        let array = ArrowArray::try_from(array.data().as_ref().clone())?;

        // (simulate consumer) import it
        let data = Arc::new(ArrayData::try_from(array)?);
        let array = make_array(data);

        // perform some operation
        let array = array.as_any().downcast_ref::<Int32Array>().unwrap();
        let array = kernels::arithmetic::add(&array, &array).unwrap();

        // verify
        assert_eq!(array, Int32Array::from(vec![2, 4, 6]));

        // (drop/release)
        Ok(())
    }
    // case with nulls is tested in the docs, through the example on this module.

    #[test]
    fn test_string() -> Result<()> {
        // create an array natively
        let array = StringArray::from(vec![Some("a"), None, Some("aaa")]);

        // export it
        let array = ArrowArray::try_from(array.data().as_ref().clone())?;

        // (simulate consumer) import it
        let data = Arc::new(ArrayData::try_from(array)?);
        let array = make_array(data);

        // perform some operation
        let array = kernels::concat::concat(&[array.as_ref(), array.as_ref()]).unwrap();
        let array = array.as_any().downcast_ref::<StringArray>().unwrap();

        // verify
        let expected = StringArray::from(vec![
            Some("a"),
            None,
            Some("aaa"),
            Some("a"),
            None,
            Some("aaa"),
        ]);
        assert_eq!(array, &expected);

        // (drop/release)
        Ok(())
    }
}
