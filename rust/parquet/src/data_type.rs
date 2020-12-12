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

//! Data types that connect Parquet physical types with their Rust-specific
//! representations.
use std::cmp::Ordering;
use std::fmt;
use std::mem;
use std::ops::{Deref, DerefMut};
use std::str::from_utf8;

use byteorder::{BigEndian, ByteOrder};

use crate::basic::Type;
use crate::column::reader::{ColumnReader, ColumnReaderImpl};
use crate::column::writer::{ColumnWriter, ColumnWriterImpl};
use crate::errors::{ParquetError, Result};
use crate::util::{
    bit_util::{from_ne_slice, FromBytes},
    memory::{ByteBuffer, ByteBufferPtr},
};

/// Rust representation for logical type INT96, value is backed by an array of `u32`.
/// The type only takes 12 bytes, without extra padding.
#[derive(Clone, Debug, PartialOrd)]
pub struct Int96 {
    value: Option<[u32; 3]>,
}

impl Int96 {
    /// Creates new INT96 type struct with no data set.
    pub fn new() -> Self {
        Self { value: None }
    }

    /// Returns underlying data as slice of [`u32`].
    #[inline]
    pub fn data(&self) -> &[u32] {
        self.value
            .as_ref()
            .expect("set_data should have been called")
    }

    /// Sets data for this INT96 type.
    #[inline]
    pub fn set_data(&mut self, elem0: u32, elem1: u32, elem2: u32) {
        self.value = Some([elem0, elem1, elem2]);
    }

    /// Converts this INT96 into an i64 representing the number of MILLISECONDS since Epoch
    pub fn to_i64(&self) -> i64 {
        const JULIAN_DAY_OF_EPOCH: i64 = 2_440_588;
        const SECONDS_PER_DAY: i64 = 86_400;
        const MILLIS_PER_SECOND: i64 = 1_000;

        let day = self.data()[2] as i64;
        let nanoseconds = ((self.data()[1] as i64) << 32) + self.data()[0] as i64;
        let seconds = (day - JULIAN_DAY_OF_EPOCH) * SECONDS_PER_DAY;

        seconds * MILLIS_PER_SECOND + nanoseconds / 1_000_000
    }
}

impl Default for Int96 {
    fn default() -> Self {
        Self { value: None }
    }
}

impl PartialEq for Int96 {
    fn eq(&self, other: &Int96) -> bool {
        match (&self.value, &other.value) {
            (Some(v1), Some(v2)) => v1 == v2,
            (None, None) => true,
            _ => false,
        }
    }
}

impl From<Vec<u32>> for Int96 {
    fn from(buf: Vec<u32>) -> Self {
        assert_eq!(buf.len(), 3);
        let mut result = Self::new();
        result.set_data(buf[0], buf[1], buf[2]);
        result
    }
}

impl fmt::Display for Int96 {
    #[cold]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self.data())
    }
}

/// Rust representation for BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY Parquet physical types.
/// Value is backed by a byte buffer.
#[derive(Clone, Debug)]
pub struct ByteArray {
    data: Option<ByteBufferPtr>,
}

impl PartialOrd for ByteArray {
    fn partial_cmp(&self, other: &ByteArray) -> Option<Ordering> {
        if self.data.is_some() && other.data.is_some() {
            if self.len() > other.len() {
                Some(Ordering::Greater)
            } else if self.len() < other.len() {
                Some(Ordering::Less)
            } else {
                for (v1, v2) in self.data().iter().zip(other.data().iter()) {
                    if *v1 > *v2 {
                        return Some(Ordering::Greater);
                    } else if *v1 < *v2 {
                        return Some(Ordering::Less);
                    }
                }
                Some(Ordering::Equal)
            }
        } else {
            None
        }
    }
}

impl ByteArray {
    /// Creates new byte array with no data set.
    #[inline]
    pub fn new() -> Self {
        ByteArray { data: None }
    }

    /// Gets length of the underlying byte buffer.
    #[inline]
    pub fn len(&self) -> usize {
        assert!(self.data.is_some());
        self.data.as_ref().unwrap().len()
    }

    /// Checks if the underlying buffer is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns slice of data.
    #[inline]
    pub fn data(&self) -> &[u8] {
        self.data
            .as_ref()
            .expect("set_data should have been called")
            .as_ref()
    }

    /// Set data from another byte buffer.
    #[inline]
    pub fn set_data(&mut self, data: ByteBufferPtr) {
        self.data = Some(data);
    }

    /// Returns `ByteArray` instance with slice of values for a data.
    #[inline]
    pub fn slice(&self, start: usize, len: usize) -> Self {
        Self::from(
            self.data
                .as_ref()
                .expect("set_data should have been called")
                .range(start, len),
        )
    }

    pub fn as_utf8(&self) -> Result<&str> {
        self.data
            .as_ref()
            .map(|ptr| ptr.as_ref())
            .ok_or_else(|| general_err!("Can't convert empty byte array to utf8"))
            .and_then(|bytes| from_utf8(bytes).map_err(|e| e.into()))
    }
}

impl From<Vec<u8>> for ByteArray {
    fn from(buf: Vec<u8>) -> ByteArray {
        Self {
            data: Some(ByteBufferPtr::new(buf)),
        }
    }
}

impl<'a> From<&'a str> for ByteArray {
    fn from(s: &'a str) -> ByteArray {
        let mut v = Vec::new();
        v.extend_from_slice(s.as_bytes());
        Self {
            data: Some(ByteBufferPtr::new(v)),
        }
    }
}

impl From<ByteBufferPtr> for ByteArray {
    fn from(ptr: ByteBufferPtr) -> ByteArray {
        Self { data: Some(ptr) }
    }
}

impl From<ByteBuffer> for ByteArray {
    fn from(mut buf: ByteBuffer) -> ByteArray {
        Self {
            data: Some(buf.consume()),
        }
    }
}

impl Default for ByteArray {
    fn default() -> Self {
        ByteArray { data: None }
    }
}

impl PartialEq for ByteArray {
    fn eq(&self, other: &ByteArray) -> bool {
        match (&self.data, &other.data) {
            (Some(d1), Some(d2)) => d1.as_ref() == d2.as_ref(),
            (None, None) => true,
            _ => false,
        }
    }
}

impl fmt::Display for ByteArray {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self.data())
    }
}

/// Wrapper type for performance reasons, this represents `FIXED_LEN_BYTE_ARRAY` but in all other
/// considerations behaves the same as `ByteArray`
///
/// # Performance notes:
/// This type is a little unfortunate, without it the compiler generates code that takes quite a
/// big hit on the CPU pipeline. Essentially the previous version stalls awaiting the result of
/// `T::get_physical_type() == Type::FIXED_LEN_BYTE_ARRAY`.
///
/// Its debatable if this is wanted, it is out of spec for what parquet documents as its base
/// types, although there are code paths in the Rust (and potentially the C++) versions that
/// warrant this.
///
/// With this wrapper type the compiler generates more targetted code paths matching the higher
/// level logical types, removing the data-hazard from all decoding and encoding paths.
#[repr(transparent)]
#[derive(Clone, Debug, Default)]
pub struct FixedLenByteArray(ByteArray);

impl PartialEq for FixedLenByteArray {
    fn eq(&self, other: &FixedLenByteArray) -> bool {
        self.0.eq(&other.0)
    }
}

impl PartialEq<ByteArray> for FixedLenByteArray {
    fn eq(&self, other: &ByteArray) -> bool {
        self.0.eq(other)
    }
}

impl PartialEq<FixedLenByteArray> for ByteArray {
    fn eq(&self, other: &FixedLenByteArray) -> bool {
        self.eq(&other.0)
    }
}

impl fmt::Display for FixedLenByteArray {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl PartialOrd for FixedLenByteArray {
    fn partial_cmp(&self, other: &FixedLenByteArray) -> Option<Ordering> {
        self.0.partial_cmp(&other.0)
    }
}

impl PartialOrd<FixedLenByteArray> for ByteArray {
    fn partial_cmp(&self, other: &FixedLenByteArray) -> Option<Ordering> {
        self.partial_cmp(&other.0)
    }
}

impl PartialOrd<ByteArray> for FixedLenByteArray {
    fn partial_cmp(&self, other: &ByteArray) -> Option<Ordering> {
        self.0.partial_cmp(other)
    }
}

impl Deref for FixedLenByteArray {
    type Target = ByteArray;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for FixedLenByteArray {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<ByteArray> for FixedLenByteArray {
    fn from(other: ByteArray) -> Self {
        Self(other)
    }
}

impl From<FixedLenByteArray> for ByteArray {
    fn from(other: FixedLenByteArray) -> Self {
        other.0
    }
}

/// Rust representation for Decimal values.
///
/// This is not a representation of Parquet physical type, but rather a wrapper for
/// DECIMAL logical type, and serves as container for raw parts of decimal values:
/// unscaled value in bytes, precision and scale.
#[derive(Clone, Debug)]
pub enum Decimal {
    /// Decimal backed by `i32`.
    Int32 {
        value: [u8; 4],
        precision: i32,
        scale: i32,
    },
    /// Decimal backed by `i64`.
    Int64 {
        value: [u8; 8],
        precision: i32,
        scale: i32,
    },
    /// Decimal backed by byte array.
    Bytes {
        value: ByteArray,
        precision: i32,
        scale: i32,
    },
}

impl Decimal {
    /// Creates new decimal value from `i32`.
    pub fn from_i32(value: i32, precision: i32, scale: i32) -> Self {
        let mut bytes = [0; 4];
        BigEndian::write_i32(&mut bytes, value);
        Decimal::Int32 {
            value: bytes,
            precision,
            scale,
        }
    }

    /// Creates new decimal value from `i64`.
    pub fn from_i64(value: i64, precision: i32, scale: i32) -> Self {
        let mut bytes = [0; 8];
        BigEndian::write_i64(&mut bytes, value);
        Decimal::Int64 {
            value: bytes,
            precision,
            scale,
        }
    }

    /// Creates new decimal value from `ByteArray`.
    pub fn from_bytes(value: ByteArray, precision: i32, scale: i32) -> Self {
        Decimal::Bytes {
            value,
            precision,
            scale,
        }
    }

    /// Returns bytes of unscaled value.
    pub fn data(&self) -> &[u8] {
        match *self {
            Decimal::Int32 { ref value, .. } => value,
            Decimal::Int64 { ref value, .. } => value,
            Decimal::Bytes { ref value, .. } => value.data(),
        }
    }

    /// Returns decimal precision.
    pub fn precision(&self) -> i32 {
        match *self {
            Decimal::Int32 { precision, .. } => precision,
            Decimal::Int64 { precision, .. } => precision,
            Decimal::Bytes { precision, .. } => precision,
        }
    }

    /// Returns decimal scale.
    pub fn scale(&self) -> i32 {
        match *self {
            Decimal::Int32 { scale, .. } => scale,
            Decimal::Int64 { scale, .. } => scale,
            Decimal::Bytes { scale, .. } => scale,
        }
    }
}

impl Default for Decimal {
    fn default() -> Self {
        Self::from_i32(0, 0, 0)
    }
}

impl PartialEq for Decimal {
    fn eq(&self, other: &Decimal) -> bool {
        self.precision() == other.precision()
            && self.scale() == other.scale()
            && self.data() == other.data()
    }
}

/// Converts an instance of data type to a slice of bytes as `u8`.
pub trait AsBytes {
    /// Returns slice of bytes for this data type.
    fn as_bytes(&self) -> &[u8];
}

/// Converts an slice of a data type to a slice of bytes.
pub trait SliceAsBytes: Sized {
    /// Returns slice of bytes for a slice of this data type.
    fn slice_as_bytes(self_: &[Self]) -> &[u8];
    /// Return the internal representation as a mutable slice
    ///
    /// # Safety
    /// If modified you are _required_ to ensure the internal representation
    /// is valid and correct for the actual raw data
    unsafe fn slice_as_bytes_mut(self_: &mut [Self]) -> &mut [u8];
}

impl AsBytes for [u8] {
    fn as_bytes(&self) -> &[u8] {
        self
    }
}

macro_rules! gen_as_bytes {
    ($source_ty:ident) => {
        impl AsBytes for $source_ty {
            fn as_bytes(&self) -> &[u8] {
                unsafe {
                    std::slice::from_raw_parts(
                        self as *const $source_ty as *const u8,
                        std::mem::size_of::<$source_ty>(),
                    )
                }
            }
        }

        impl SliceAsBytes for $source_ty {
            #[inline]
            fn slice_as_bytes(self_: &[Self]) -> &[u8] {
                unsafe {
                    std::slice::from_raw_parts(
                        self_.as_ptr() as *const u8,
                        std::mem::size_of::<$source_ty>() * self_.len(),
                    )
                }
            }

            #[inline]
            unsafe fn slice_as_bytes_mut(self_: &mut [Self]) -> &mut [u8] {
                std::slice::from_raw_parts_mut(
                    self_.as_mut_ptr() as *mut u8,
                    std::mem::size_of::<$source_ty>() * self_.len(),
                )
            }
        }
    };
}

gen_as_bytes!(i8);
gen_as_bytes!(i16);
gen_as_bytes!(i32);
gen_as_bytes!(i64);
gen_as_bytes!(u8);
gen_as_bytes!(u16);
gen_as_bytes!(u32);
gen_as_bytes!(u64);
gen_as_bytes!(f32);
gen_as_bytes!(f64);

macro_rules! unimplemented_slice_as_bytes {
    ($ty: ty) => {
        impl SliceAsBytes for $ty {
            fn slice_as_bytes(_self: &[Self]) -> &[u8] {
                unimplemented!()
            }

            unsafe fn slice_as_bytes_mut(_self: &mut [Self]) -> &mut [u8] {
                unimplemented!()
            }
        }
    };
}

// TODO - Can Int96 and bool be implemented in these terms?
unimplemented_slice_as_bytes!(Int96);
unimplemented_slice_as_bytes!(bool);
unimplemented_slice_as_bytes!(ByteArray);
unimplemented_slice_as_bytes!(FixedLenByteArray);

impl AsBytes for bool {
    fn as_bytes(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self as *const bool as *const u8, 1) }
    }
}

impl AsBytes for Int96 {
    fn as_bytes(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(self.data() as *const [u32] as *const u8, 12)
        }
    }
}

impl AsBytes for ByteArray {
    fn as_bytes(&self) -> &[u8] {
        self.data()
    }
}

impl AsBytes for FixedLenByteArray {
    fn as_bytes(&self) -> &[u8] {
        self.data()
    }
}

impl AsBytes for Decimal {
    fn as_bytes(&self) -> &[u8] {
        self.data()
    }
}

impl AsBytes for Vec<u8> {
    fn as_bytes(&self) -> &[u8] {
        self.as_slice()
    }
}

impl<'a> AsBytes for &'a str {
    fn as_bytes(&self) -> &[u8] {
        (self as &str).as_bytes()
    }
}

impl AsBytes for str {
    fn as_bytes(&self) -> &[u8] {
        (self as &str).as_bytes()
    }
}

pub(crate) mod private {
    use crate::encodings::decoding::PlainDecoderDetails;
    use crate::util::bit_util::{BitReader, BitWriter};
    use crate::util::memory::ByteBufferPtr;

    use byteorder::ByteOrder;
    use std::convert::TryInto;

    use super::{ParquetError, Result, SliceAsBytes};

    pub type BitIndex = u64;

    /// Sealed trait to start to remove specialisation from implementations
    ///
    /// This is done to force the associated value type to be unimplementable outside of this
    /// crate, and thus hint to the type system (and end user) traits are public for the contract
    /// and not for extension.
    pub trait ParquetValueType:
        std::cmp::PartialEq
        + std::fmt::Debug
        + std::fmt::Display
        + std::default::Default
        + std::clone::Clone
        + super::AsBytes
        + super::FromBytes
        + super::SliceAsBytes
        + PartialOrd
    {
        /// Encode the value directly from a higher level encoder
        fn encode<W: std::io::Write>(
            values: &[Self],
            writer: &mut W,
            bit_writer: &mut BitWriter,
        ) -> Result<()>;

        /// Establish the data that will be decoded in a buffer
        fn set_data(
            decoder: &mut PlainDecoderDetails,
            data: ByteBufferPtr,
            num_values: usize,
        );

        /// Decode the value from a given buffer for a higher level decoder
        fn decode(
            buffer: &mut [Self],
            decoder: &mut PlainDecoderDetails,
        ) -> Result<usize>;

        /// Return the encoded size for a type
        fn dict_encoding_size(&self) -> (usize, usize) {
            (std::mem::size_of::<Self>(), 1)
        }

        /// Return the value as i64 if possible
        ///
        /// This is essentially the same as `std::convert::TryInto<i64>` but can
        /// implemented for `f32` and `f64`, types that would fail orphan rules
        fn as_i64(&self) -> Result<i64> {
            Err(general_err!("Type cannot be converted to i64"))
        }

        /// Return the value as u64 if possible
        ///
        /// This is essentially the same as `std::convert::TryInto<u64>` but can
        /// implemented for `f32` and `f64`, types that would fail orphan rules
        fn as_u64(&self) -> Result<u64> {
            self.as_i64()
                .map_err(|_| general_err!("Type cannot be converted to u64"))
                .map(|x| x as u64)
        }

        /// Return the value as an Any to allow for downcasts without transmutation
        fn as_any(&self) -> &dyn std::any::Any;

        /// Return the value as an mutable Any to allow for downcasts without transmutation
        fn as_mut_any(&mut self) -> &mut dyn std::any::Any;
    }

    impl ParquetValueType for bool {
        #[inline]
        fn encode<W: std::io::Write>(
            values: &[Self],
            _: &mut W,
            bit_writer: &mut BitWriter,
        ) -> Result<()> {
            for value in values {
                bit_writer.put_value(*value as u64, 1);
            }
            Ok(())
        }

        #[inline]
        fn set_data(
            decoder: &mut PlainDecoderDetails,
            data: ByteBufferPtr,
            num_values: usize,
        ) {
            decoder.bit_reader.replace(BitReader::new(data));
            decoder.num_values = num_values;
        }

        #[inline]
        fn decode(
            buffer: &mut [Self],
            decoder: &mut PlainDecoderDetails,
        ) -> Result<usize> {
            let bit_reader = decoder.bit_reader.as_mut().unwrap();
            let num_values = std::cmp::min(buffer.len(), decoder.num_values);
            let values_read = bit_reader.get_batch(&mut buffer[..num_values], 1);
            decoder.num_values -= values_read;
            Ok(values_read)
        }

        #[inline]
        fn as_i64(&self) -> Result<i64> {
            Ok(*self as i64)
        }

        #[inline]
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        #[inline]
        fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
            self
        }
    }

    /// Hopelessly unsafe function that emulates `num::as_ne_bytes`
    ///
    /// It is not recommended to use this outside of this private module as, while it
    /// _should_ work for primitive values, it is little better than a transmutation
    /// and can act as a backdoor into mis-interpreting types as arbitary byte slices
    #[inline]
    fn as_raw<'a, T>(value: *const T) -> &'a [u8] {
        unsafe {
            let value = value as *const u8;
            std::slice::from_raw_parts(value, std::mem::size_of::<T>())
        }
    }

    macro_rules! impl_from_raw {
        ($ty: ty, $self: ident => $as_i64: block) => {
            impl ParquetValueType for $ty {
                #[inline]
                fn encode<W: std::io::Write>(values: &[Self], writer: &mut W, _: &mut BitWriter) -> Result<()> {
                    let raw = unsafe {
                        std::slice::from_raw_parts(
                            values.as_ptr() as *const u8,
                            std::mem::size_of::<$ty>() * values.len(),
                        )
                    };
                    writer.write_all(raw)?;

                    Ok(())
                }

                #[inline]
                fn set_data(decoder: &mut PlainDecoderDetails, data: ByteBufferPtr, num_values: usize) {
                    decoder.data.replace(data);
                    decoder.start = 0;
                    decoder.num_values = num_values;
                }

                #[inline]
                fn decode(buffer: &mut [Self], decoder: &mut PlainDecoderDetails) -> Result<usize> {
                    let data = decoder.data.as_ref().expect("set_data should have been called");
                    let num_values = std::cmp::min(buffer.len(), decoder.num_values);
                    let bytes_left = data.len() - decoder.start;
                    let bytes_to_decode = std::mem::size_of::<Self>() * num_values;

                    if bytes_left < bytes_to_decode {
                        return Err(eof_err!("Not enough bytes to decode"));
                    }

                    // SAFETY: Raw types should be as per the standard rust bit-vectors
                    unsafe {
                        let raw_buffer = &mut Self::slice_as_bytes_mut(buffer)[..bytes_to_decode];
                        raw_buffer.copy_from_slice(data.range(decoder.start, bytes_to_decode).as_ref());
                    };
                    decoder.start += bytes_to_decode;
                    decoder.num_values -= num_values;

                    Ok(num_values)
                }

                #[inline]
                fn as_i64(&$self) -> Result<i64> {
                    $as_i64
                }

                #[inline]
                fn as_any(&self) -> &dyn std::any::Any {
                    self
                }

                #[inline]
                fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
                    self
                }
            }
        }
    }

    impl_from_raw!(i32, self => { Ok(*self as i64) });
    impl_from_raw!(i64, self => { Ok(*self) });
    impl_from_raw!(f32, self => { Err(general_err!("Type cannot be converted to i64")) });
    impl_from_raw!(f64, self => { Err(general_err!("Type cannot be converted to i64")) });

    impl ParquetValueType for super::Int96 {
        #[inline]
        fn encode<W: std::io::Write>(
            values: &[Self],
            writer: &mut W,
            _: &mut BitWriter,
        ) -> Result<()> {
            for value in values {
                let raw = unsafe {
                    std::slice::from_raw_parts(
                        value.data() as *const [u32] as *const u8,
                        12,
                    )
                };
                writer.write_all(raw)?;
            }
            Ok(())
        }

        #[inline]
        fn set_data(
            decoder: &mut PlainDecoderDetails,
            data: ByteBufferPtr,
            num_values: usize,
        ) {
            decoder.data.replace(data);
            decoder.start = 0;
            decoder.num_values = num_values;
        }

        #[inline]
        fn decode(
            buffer: &mut [Self],
            decoder: &mut PlainDecoderDetails,
        ) -> Result<usize> {
            // TODO - Remove the duplication between this and the general slice method
            let data = decoder
                .data
                .as_ref()
                .expect("set_data should have been called");
            let num_values = std::cmp::min(buffer.len(), decoder.num_values);
            let bytes_left = data.len() - decoder.start;
            let bytes_to_decode = 12 * num_values;

            if bytes_left < bytes_to_decode {
                return Err(eof_err!("Not enough bytes to decode"));
            }

            let data_range = data.range(decoder.start, bytes_to_decode);
            let bytes: &[u8] = data_range.data();
            decoder.start += bytes_to_decode;

            let mut pos = 0; // position in byte array
            for i in 0..num_values {
                let elem0 = byteorder::LittleEndian::read_u32(&bytes[pos..pos + 4]);
                let elem1 = byteorder::LittleEndian::read_u32(&bytes[pos + 4..pos + 8]);
                let elem2 = byteorder::LittleEndian::read_u32(&bytes[pos + 8..pos + 12]);

                buffer[i]
                    .as_mut_any()
                    .downcast_mut::<Self>()
                    .unwrap()
                    .set_data(elem0, elem1, elem2);

                pos += 12;
            }
            decoder.num_values -= num_values;

            Ok(num_values)
        }

        #[inline]
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        #[inline]
        fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
            self
        }
    }

    // TODO - Why does macro importing fail?
    /// Reads `$size` of bytes from `$src`, and reinterprets them as type `$ty`, in
    /// little-endian order. `$ty` must implement the `Default` trait. Otherwise this won't
    /// compile.
    /// This is copied and modified from byteorder crate.
    macro_rules! read_num_bytes {
        ($ty:ty, $size:expr, $src:expr) => {{
            assert!($size <= $src.len());
            let mut buffer =
                <$ty as $crate::util::bit_util::FromBytes>::Buffer::default();
            buffer.as_mut()[..$size].copy_from_slice(&$src[..$size]);
            <$ty>::from_ne_bytes(buffer)
        }};
    }

    impl ParquetValueType for super::ByteArray {
        #[inline]
        fn encode<W: std::io::Write>(
            values: &[Self],
            writer: &mut W,
            _: &mut BitWriter,
        ) -> Result<()> {
            for value in values {
                let len: u32 = value.len().try_into().unwrap();
                writer.write_all(&len.to_ne_bytes())?;
                let raw = value.data();
                writer.write_all(raw)?;
            }
            Ok(())
        }

        #[inline]
        fn set_data(
            decoder: &mut PlainDecoderDetails,
            data: ByteBufferPtr,
            num_values: usize,
        ) {
            decoder.data.replace(data);
            decoder.start = 0;
            decoder.num_values = num_values;
        }

        #[inline]
        fn decode(
            buffer: &mut [Self],
            decoder: &mut PlainDecoderDetails,
        ) -> Result<usize> {
            let data = decoder
                .data
                .as_mut()
                .expect("set_data should have been called");
            let num_values = std::cmp::min(buffer.len(), decoder.num_values);
            for i in 0..num_values {
                let len: usize =
                    read_num_bytes!(u32, 4, data.start_from(decoder.start).as_ref())
                        as usize;
                decoder.start += std::mem::size_of::<u32>();

                if data.len() < decoder.start + len {
                    return Err(eof_err!("Not enough bytes to decode"));
                }

                let val: &mut Self = buffer[i].as_mut_any().downcast_mut().unwrap();

                val.set_data(data.range(decoder.start, len));
                decoder.start += len;
            }
            decoder.num_values -= num_values;

            Ok(num_values)
        }

        #[inline]
        fn dict_encoding_size(&self) -> (usize, usize) {
            (std::mem::size_of::<u32>(), self.len())
        }

        #[inline]
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        #[inline]
        fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
            self
        }
    }

    impl ParquetValueType for super::FixedLenByteArray {
        #[inline]
        fn encode<W: std::io::Write>(
            values: &[Self],
            writer: &mut W,
            _: &mut BitWriter,
        ) -> Result<()> {
            for value in values {
                let raw = value.data();
                writer.write_all(raw)?;
            }
            Ok(())
        }

        #[inline]
        fn set_data(
            decoder: &mut PlainDecoderDetails,
            data: ByteBufferPtr,
            num_values: usize,
        ) {
            decoder.data.replace(data);
            decoder.start = 0;
            decoder.num_values = num_values;
        }

        #[inline]
        fn decode(
            buffer: &mut [Self],
            decoder: &mut PlainDecoderDetails,
        ) -> Result<usize> {
            assert!(decoder.type_length > 0);

            let data = decoder
                .data
                .as_mut()
                .expect("set_data should have been called");
            let num_values = std::cmp::min(buffer.len(), decoder.num_values);
            for i in 0..num_values {
                let len = decoder.type_length as usize;

                if data.len() < decoder.start + len {
                    return Err(eof_err!("Not enough bytes to decode"));
                }

                let val: &mut Self = buffer[i].as_mut_any().downcast_mut().unwrap();

                val.set_data(data.range(decoder.start, len));
                decoder.start += len;
            }
            decoder.num_values -= num_values;

            Ok(num_values)
        }

        #[inline]
        fn dict_encoding_size(&self) -> (usize, usize) {
            (std::mem::size_of::<u32>(), self.len())
        }

        #[inline]
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        #[inline]
        fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
            self
        }
    }
}

/// Contains the Parquet physical type information as well as the Rust primitive type
/// presentation.
pub trait DataType: 'static {
    type T: private::ParquetValueType;

    /// Returns Parquet physical type.
    fn get_physical_type() -> Type;

    /// Returns size in bytes for Rust representation of the physical type.
    fn get_type_size() -> usize;

    fn get_column_reader(column_writer: ColumnReader) -> Option<ColumnReaderImpl<Self>>
    where
        Self: Sized;

    fn get_column_writer(column_writer: ColumnWriter) -> Option<ColumnWriterImpl<Self>>
    where
        Self: Sized;

    fn get_column_writer_ref(
        column_writer: &ColumnWriter,
    ) -> Option<&ColumnWriterImpl<Self>>
    where
        Self: Sized;

    fn get_column_writer_mut(
        column_writer: &mut ColumnWriter,
    ) -> Option<&mut ColumnWriterImpl<Self>>
    where
        Self: Sized;
}

// Workaround bug in specialization
pub trait SliceAsBytesDataType: DataType
where
    Self::T: SliceAsBytes,
{
}

impl<T> SliceAsBytesDataType for T
where
    T: DataType,
    <T as DataType>::T: SliceAsBytes,
{
}

macro_rules! make_type {
    ($name:ident, $physical_ty:path, $reader_ident: ident, $writer_ident: ident, $native_ty:ty, $size:expr) => {
        #[derive(Clone)]
        pub struct $name {}

        impl DataType for $name {
            type T = $native_ty;

            fn get_physical_type() -> Type {
                $physical_ty
            }

            fn get_type_size() -> usize {
                $size
            }

            fn get_column_reader(
                column_writer: ColumnReader,
            ) -> Option<ColumnReaderImpl<Self>> {
                match column_writer {
                    ColumnReader::$reader_ident(w) => Some(w),
                    _ => None,
                }
            }

            fn get_column_writer(
                column_writer: ColumnWriter,
            ) -> Option<ColumnWriterImpl<Self>> {
                match column_writer {
                    ColumnWriter::$writer_ident(w) => Some(w),
                    _ => None,
                }
            }

            fn get_column_writer_ref(
                column_writer: &ColumnWriter,
            ) -> Option<&ColumnWriterImpl<Self>> {
                match column_writer {
                    ColumnWriter::$writer_ident(w) => Some(w),
                    _ => None,
                }
            }

            fn get_column_writer_mut(
                column_writer: &mut ColumnWriter,
            ) -> Option<&mut ColumnWriterImpl<Self>> {
                match column_writer {
                    ColumnWriter::$writer_ident(w) => Some(w),
                    _ => None,
                }
            }
        }
    };
}

// Generate struct definitions for all physical types

make_type!(
    BoolType,
    Type::BOOLEAN,
    BoolColumnReader,
    BoolColumnWriter,
    bool,
    1
);
make_type!(
    Int32Type,
    Type::INT32,
    Int32ColumnReader,
    Int32ColumnWriter,
    i32,
    4
);
make_type!(
    Int64Type,
    Type::INT64,
    Int64ColumnReader,
    Int64ColumnWriter,
    i64,
    8
);
make_type!(
    Int96Type,
    Type::INT96,
    Int96ColumnReader,
    Int96ColumnWriter,
    Int96,
    mem::size_of::<Int96>()
);
make_type!(
    FloatType,
    Type::FLOAT,
    FloatColumnReader,
    FloatColumnWriter,
    f32,
    4
);
make_type!(
    DoubleType,
    Type::DOUBLE,
    DoubleColumnReader,
    DoubleColumnWriter,
    f64,
    8
);
make_type!(
    ByteArrayType,
    Type::BYTE_ARRAY,
    ByteArrayColumnReader,
    ByteArrayColumnWriter,
    ByteArray,
    mem::size_of::<ByteArray>()
);
make_type!(
    FixedLenByteArrayType,
    Type::FIXED_LEN_BYTE_ARRAY,
    FixedLenByteArrayColumnReader,
    FixedLenByteArrayColumnWriter,
    FixedLenByteArray,
    mem::size_of::<FixedLenByteArray>()
);

impl FromBytes for Int96 {
    type Buffer = [u8; 12];
    fn from_le_bytes(_bs: Self::Buffer) -> Self {
        unimplemented!()
    }
    fn from_be_bytes(_bs: Self::Buffer) -> Self {
        unimplemented!()
    }
    fn from_ne_bytes(bs: Self::Buffer) -> Self {
        let mut i = Int96::new();
        i.set_data(
            from_ne_slice(&bs[0..4]),
            from_ne_slice(&bs[4..8]),
            from_ne_slice(&bs[8..12]),
        );
        i
    }
}

// FIXME Needed to satisfy the constraint of many decoding functions but ByteArray does not
// appear to actual be converted directly from bytes
impl FromBytes for ByteArray {
    type Buffer = [u8; 8];
    fn from_le_bytes(_bs: Self::Buffer) -> Self {
        unreachable!()
    }
    fn from_be_bytes(_bs: Self::Buffer) -> Self {
        unreachable!()
    }
    fn from_ne_bytes(bs: Self::Buffer) -> Self {
        ByteArray::from(bs.to_vec())
    }
}

impl FromBytes for FixedLenByteArray {
    type Buffer = [u8; 8];

    fn from_le_bytes(_bs: Self::Buffer) -> Self {
        unreachable!()
    }
    fn from_be_bytes(_bs: Self::Buffer) -> Self {
        unreachable!()
    }
    fn from_ne_bytes(bs: Self::Buffer) -> Self {
        Self(ByteArray::from(bs.to_vec()))
    }
}

/// Macro to reduce repetition in making type assertions on the physical type against `T`
macro_rules! ensure_phys_ty {
    ($($ty: pat)|+ , $err: literal) => {
        match T::get_physical_type() {
            $($ty => (),)*
            _ => panic!($err),
        };
    }
}

#[cfg(test)]
#[allow(clippy::float_cmp, clippy::approx_constant)]
mod tests {
    use super::*;

    #[test]
    #[allow(clippy::string_lit_as_bytes)]
    fn test_as_bytes() {
        assert_eq!(false.as_bytes(), &[0]);
        assert_eq!(true.as_bytes(), &[1]);
        assert_eq!(7_i32.as_bytes(), &[7, 0, 0, 0]);
        assert_eq!(555_i32.as_bytes(), &[43, 2, 0, 0]);
        assert_eq!(555_u32.as_bytes(), &[43, 2, 0, 0]);
        assert_eq!(i32::max_value().as_bytes(), &[255, 255, 255, 127]);
        assert_eq!(i32::min_value().as_bytes(), &[0, 0, 0, 128]);
        assert_eq!(7_i64.as_bytes(), &[7, 0, 0, 0, 0, 0, 0, 0]);
        assert_eq!(555_i64.as_bytes(), &[43, 2, 0, 0, 0, 0, 0, 0]);
        assert_eq!(
            (i64::max_value()).as_bytes(),
            &[255, 255, 255, 255, 255, 255, 255, 127]
        );
        assert_eq!((i64::min_value()).as_bytes(), &[0, 0, 0, 0, 0, 0, 0, 128]);
        assert_eq!(3.14_f32.as_bytes(), &[195, 245, 72, 64]);
        assert_eq!(3.14_f64.as_bytes(), &[31, 133, 235, 81, 184, 30, 9, 64]);
        assert_eq!("hello".as_bytes(), &[b'h', b'e', b'l', b'l', b'o']);
        assert_eq!(
            Vec::from("hello".as_bytes()).as_bytes(),
            &[b'h', b'e', b'l', b'l', b'o']
        );

        // Test Int96
        let i96 = Int96::from(vec![1, 2, 3]);
        assert_eq!(i96.as_bytes(), &[1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0]);

        // Test ByteArray
        let ba = ByteArray::from(vec![1, 2, 3]);
        assert_eq!(ba.as_bytes(), &[1, 2, 3]);

        // Test Decimal
        let decimal = Decimal::from_i32(123, 5, 2);
        assert_eq!(decimal.as_bytes(), &[0, 0, 0, 123]);
        let decimal = Decimal::from_i64(123, 5, 2);
        assert_eq!(decimal.as_bytes(), &[0, 0, 0, 0, 0, 0, 0, 123]);
        let decimal = Decimal::from_bytes(ByteArray::from(vec![1, 2, 3]), 5, 2);
        assert_eq!(decimal.as_bytes(), &[1, 2, 3]);
    }

    #[test]
    fn test_int96_from() {
        assert_eq!(
            Int96::from(vec![1, 12345, 1234567890]).data(),
            &[1, 12345, 1234567890]
        );
    }

    #[test]
    fn test_byte_array_from() {
        assert_eq!(
            ByteArray::from(vec![b'A', b'B', b'C']).data(),
            &[b'A', b'B', b'C']
        );
        assert_eq!(ByteArray::from("ABC").data(), &[b'A', b'B', b'C']);
        assert_eq!(
            ByteArray::from(ByteBufferPtr::new(vec![1u8, 2u8, 3u8, 4u8, 5u8])).data(),
            &[1u8, 2u8, 3u8, 4u8, 5u8]
        );
        let mut buf = ByteBuffer::new();
        buf.set_data(vec![6u8, 7u8, 8u8, 9u8, 10u8]);
        assert_eq!(ByteArray::from(buf).data(), &[6u8, 7u8, 8u8, 9u8, 10u8]);
    }

    #[test]
    fn test_decimal_partial_eq() {
        assert_eq!(Decimal::default(), Decimal::from_i32(0, 0, 0));
        assert_eq!(Decimal::from_i32(222, 5, 2), Decimal::from_i32(222, 5, 2));
        assert_eq!(
            Decimal::from_bytes(ByteArray::from(vec![0, 0, 0, 3]), 5, 2),
            Decimal::from_i32(3, 5, 2)
        );

        assert!(Decimal::from_i32(222, 5, 2) != Decimal::from_i32(111, 5, 2));
        assert!(Decimal::from_i32(222, 5, 2) != Decimal::from_i32(222, 6, 2));
        assert!(Decimal::from_i32(222, 5, 2) != Decimal::from_i32(222, 5, 3));

        assert!(Decimal::from_i64(222, 5, 2) != Decimal::from_i32(222, 5, 2));
    }

    #[test]
    fn test_byte_array_ord() {
        let ba1 = ByteArray::from(vec![1, 2, 3]);
        let ba11 = ByteArray::from(vec![1, 2, 3]);
        let ba2 = ByteArray::from(vec![3, 4]);
        let ba3 = ByteArray::from(vec![1, 2, 4]);
        let ba4 = ByteArray::from(vec![]);
        let ba5 = ByteArray::from(vec![2, 2, 3]);

        assert!(ba1 > ba2);
        assert!(ba3 > ba1);
        assert!(ba1 > ba4);
        assert_eq!(ba1, ba11);
        assert!(ba5 > ba1);
    }
}
