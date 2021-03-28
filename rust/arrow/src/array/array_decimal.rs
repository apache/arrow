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

use std::any::Any;
use std::convert::From;
use std::fmt;
use std::mem;

use super::{
    array::print_long_array, raw_pointer::RawPtrBox, Array, ArrayData, FixedSizeListArray,
};
use crate::array::{DecimalBuilder, DecimalIter};
use crate::buffer::Buffer;

use crate::datatypes::{
    ArrowDecimalNativeType, ArrowDecimalType, DataType, Decimal128Type, Decimal256Type,
};

/// Array whose elements are of decimal types.
pub struct DecimalArray<T: 'static + ArrowDecimalType> {
    /// Underlying ArrayData
    /// # Safety
    /// must have exactly one buffer, aligned to type T
    data: ArrayData,
    /// Pointer to the value array. The lifetime of this must be <= to the value buffer
    /// stored in `data`, so it's safe to store.
    /// # Safety
    /// raw_values must have a value equivalent to `data.buffers()[0].raw_data()`
    /// raw_values must have alignment for type T::NativeType
    raw_values: RawPtrBox<T::Native>,
    //
    precision: usize,
    //
    scale: usize,
}

impl<T: 'static + ArrowDecimalType> DecimalArray<T> {
    /// Returns the length of this array.
    #[inline]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Returns whether this array is empty.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Returns a slice of the values of this array
    #[inline]
    pub fn values(&self) -> &[T::Native] {
        // Soundness
        //     raw_values alignment & location is ensured by fn from(ArrayDataRef)
        //     buffer bounds/offset is ensured by the ArrayData instance.
        unsafe {
            std::slice::from_raw_parts(
                self.raw_values.as_ptr().add(self.data.offset()),
                self.len(),
            )
        }
    }

    // Returns a new primitive array builder
    pub fn builder(capacity: usize, precision: usize, scale: usize) -> DecimalBuilder {
        DecimalBuilder::new(capacity, precision, scale)
    }

    pub fn from_fixed_size_list_array(
        v: FixedSizeListArray,
        precision: usize,
        scale: usize,
    ) -> Self {
        assert_eq!(
            v.data_ref().child_data()[0].child_data().len(),
            0,
            "DecimalArray can only be created from list array of u8 values \
             (i.e. FixedSizeList<PrimitiveArray<u8>>)."
        );
        assert_eq!(
            v.data_ref().child_data()[0].data_type(),
            &DataType::UInt8,
            "DecimalArray can only be created from FixedSizeList<u8> arrays, mismatched data types."
        );

        let mut builder = ArrayData::builder(DataType::Decimal128(precision, scale))
            .len(v.len())
            .add_buffer(v.data_ref().child_data()[0].buffers()[0].clone());
        if let Some(bitmap) = v.data_ref().null_bitmap() {
            builder = builder.null_bit_buffer(bitmap.bits.clone())
        }

        let data = builder.build();
        Self::from(data)
    }

    /// Returns the primitive value at index `i`.
    ///
    /// Note this doesn't do any bound checking, for performance reason.
    /// # Safety
    /// caller must ensure that the passed in offset is less than the array len()
    pub fn value(&self, i: usize) -> T::Native {
        let offset = i + self.offset();
        unsafe { *self.raw_values.as_ptr().add(offset) }
    }

    /// Creates a PrimitiveArray based on an iterator of values without nulls
    pub fn from_iter_values<I: IntoIterator<Item = T::Native>>(iter: I) -> Self {
        let val_buf: Buffer = iter.into_iter().collect();
        let data = ArrayData::new(
            DataType::Decimal128(1, 1),
            val_buf.len() / mem::size_of::<<T as ArrowDecimalType>::Native>(),
            None,
            None,
            0,
            vec![val_buf],
            vec![],
        );
        DecimalArray::<T>::from(data)
    }

    pub fn precision(&self) -> usize {
        self.precision
    }

    pub fn scale(&self) -> usize {
        self.scale
    }
}

impl<T: 'static + ArrowDecimalType> From<ArrayData> for DecimalArray<T> {
    fn from(data: ArrayData) -> Self {
        assert_eq!(
            data.buffers().len(),
            1,
            "DecimalArray data should contain 1 buffer only (values)"
        );
        let values = data.buffers()[0].as_ptr();

        let (precision, scale) = match data.data_type() {
            DataType::Decimal128(precision, scale) => (*precision, *scale),
            _ => panic!("Expected data type to be Decimal"),
        };

        Self {
            data,
            raw_values: unsafe { RawPtrBox::new(values) },
            precision,
            scale,
        }
    }
}

impl<T: 'static + ArrowDecimalType> fmt::Debug for DecimalArray<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DecimalArray<{}, {}>\n[\n", self.precision, self.scale)?;

        print_long_array(self, f, |array, index, f| {
            fmt::Debug::fmt(&array.value(index).as_string(self.scale), f)
        })?;

        write!(f, "]")
    }
}

impl<T: 'static + ArrowDecimalType> Array for DecimalArray<T> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data(&self) -> &ArrayData {
        &self.data
    }

    /// Returns the total number of bytes of memory occupied by the buffers owned by this [DecimalArray].
    fn get_buffer_memory_size(&self) -> usize {
        self.data.get_buffer_memory_size()
    }

    /// Returns the total number of bytes of memory occupied physically by this [DecimalArray].
    fn get_array_memory_size(&self) -> usize {
        self.data.get_array_memory_size() + mem::size_of_val(self)
    }
}

impl<'a, T: ArrowDecimalType> DecimalArray<T> {
    /// constructs a new iterator
    pub fn iter(&'a self) -> DecimalIter<'a, T> {
        DecimalIter::<'a, T>::new(&self)
    }
}

impl<'a, T: ArrowDecimalType> IntoIterator for &'a DecimalArray<T> {
    type Item = Option<T::Native>;
    type IntoIter = DecimalIter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        DecimalIter::<'a, T>::new(self)
    }
}

pub type Decimal128Array = DecimalArray<Decimal128Type>;
pub type Decimal256Array = DecimalArray<Decimal256Type>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decimal_128_array() {
        // let val_8887: [u8; 16] = [192, 219, 180, 17, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        // let val_neg_8887: [u8; 16] = [64, 36, 75, 238, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255];
        let values: [u8; 32] = [
            192, 219, 180, 17, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 64, 36, 75, 238, 253,
            255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        ];
        let array_data = ArrayData::builder(DataType::Decimal128(23, 0))
            .len(2)
            .add_buffer(Buffer::from(&values[..]))
            .build();
        let decimal_array = Decimal128Array::from(array_data);

        assert_eq!(8_887_000_000_i128, decimal_array.value(0));
        assert_eq!(-8_887_000_000_i128, decimal_array.value(1));
        assert_eq!(2, decimal_array.len());
    }

    #[test]
    fn test_decimal_128_array_fmt_debug() {
        let values: [u8; 32] = [
            192, 219, 180, 17, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 64, 36, 75, 238, 253,
            255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        ];
        let array_data = ArrayData::builder(DataType::Decimal128(23, 6))
            .len(2)
            .add_buffer(Buffer::from(&values[..]))
            .build();
        let arr = Decimal128Array::from(array_data);
        assert_eq!(
            "DecimalArray<23, 6>\n[\n  \"8887.000000\",\n  \"-8887.000000\",\n]",
            format!("{:?}", arr)
        );
    }
}
