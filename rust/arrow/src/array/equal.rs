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

use super::*;
use crate::datatypes::*;
use crate::util::bit_util;
use array::{
    Array, BinaryOffsetSizeTrait, GenericBinaryArray, GenericListArray,
    GenericStringArray, ListArrayOps, OffsetSizeTrait, StringOffsetSizeTrait,
};

/// Trait for `Array` equality.
pub trait ArrayEqual {
    /// Returns true if this array is equal to the `other` array
    fn equals(&self, other: &dyn Array) -> bool;

    /// Returns true if the range [start_idx, end_idx) is equal to
    /// [other_start_idx, other_start_idx + end_idx - start_idx) in the `other` array
    fn range_equals(
        &self,
        other: &dyn Array,
        start_idx: usize,
        end_idx: usize,
        other_start_idx: usize,
    ) -> bool;
}

impl<T: ArrowPrimitiveType> ArrayEqual for PrimitiveArray<T> {
    fn equals(&self, other: &dyn Array) -> bool {
        if !base_equal(&self.data(), &other.data()) {
            return false;
        }

        if T::DATA_TYPE == DataType::Boolean {
            return bool_equal(self, other);
        }

        let value_buf = self.data_ref().buffers()[0].clone();
        let other_value_buf = other.data_ref().buffers()[0].clone();
        let byte_width = T::get_bit_width() / 8;

        if self.null_count() > 0 {
            let values = value_buf.data();
            let other_values = other_value_buf.data();

            for i in 0..self.len() {
                if self.is_valid(i) {
                    let start = (i + self.offset()) * byte_width;
                    let data = &values[start..(start + byte_width)];
                    let other_start = (i + other.offset()) * byte_width;
                    let other_data =
                        &other_values[other_start..(other_start + byte_width)];
                    if data != other_data {
                        return false;
                    }
                }
            }
        } else {
            let start = self.offset() * byte_width;
            let other_start = other.offset() * byte_width;
            let len = self.len() * byte_width;
            let data = &value_buf.data()[start..(start + len)];
            let other_data = &other_value_buf.data()[other_start..(other_start + len)];
            if data != other_data {
                return false;
            }
        }

        true
    }

    fn range_equals(
        &self,
        other: &dyn Array,
        start_idx: usize,
        end_idx: usize,
        other_start_idx: usize,
    ) -> bool {
        assert!(other_start_idx + (end_idx - start_idx) <= other.len());
        let other = other.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();

        let mut j = other_start_idx;
        for i in start_idx..end_idx {
            let is_null = self.is_null(i);
            let other_is_null = other.is_null(j);
            if is_null != other_is_null || (!is_null && self.value(i) != other.value(j)) {
                return false;
            }
            j += 1;
        }

        true
    }
}

fn bool_equal(lhs: &Array, rhs: &Array) -> bool {
    let values = lhs.data_ref().buffers()[0].data();
    let other_values = rhs.data_ref().buffers()[0].data();

    // TODO: we can do this more efficiently if all values are not-null
    for i in 0..lhs.len() {
        if lhs.is_valid(i)
            && bit_util::get_bit(values, i + lhs.offset())
                != bit_util::get_bit(other_values, i + rhs.offset())
        {
            return false;
        }
    }
    true
}

impl<T: ArrowNumericType> PartialEq for PrimitiveArray<T> {
    fn eq(&self, other: &PrimitiveArray<T>) -> bool {
        self.equals(other)
    }
}

impl PartialEq for BooleanArray {
    fn eq(&self, other: &BooleanArray) -> bool {
        self.equals(other)
    }
}

impl<OffsetSize: StringOffsetSizeTrait> PartialEq for GenericStringArray<OffsetSize> {
    fn eq(&self, other: &Self) -> bool {
        self.equals(other)
    }
}

impl<OffsetSize: BinaryOffsetSizeTrait> PartialEq for GenericBinaryArray<OffsetSize> {
    fn eq(&self, other: &Self) -> bool {
        self.equals(other)
    }
}

impl PartialEq for FixedSizeBinaryArray {
    fn eq(&self, other: &Self) -> bool {
        self.equals(other)
    }
}

impl<OffsetSize: OffsetSizeTrait> ArrayEqual for GenericListArray<OffsetSize> {
    fn equals(&self, other: &dyn Array) -> bool {
        if !base_equal(&self.data(), &other.data()) {
            return false;
        }

        let other = other
            .as_any()
            .downcast_ref::<GenericListArray<OffsetSize>>()
            .unwrap();

        if !value_offset_equal(self, other) {
            return false;
        }

        if !self.values().range_equals(
            &*other.values(),
            self.value_offset(0).to_usize().unwrap(),
            self.value_offset(self.len()).to_usize().unwrap(),
            other.value_offset(0).to_usize().unwrap(),
        ) {
            return false;
        }

        true
    }

    fn range_equals(
        &self,
        other: &dyn Array,
        start_idx: usize,
        end_idx: usize,
        other_start_idx: usize,
    ) -> bool {
        assert!(other_start_idx + (end_idx - start_idx) <= other.len());

        let other = other
            .as_any()
            .downcast_ref::<GenericListArray<OffsetSize>>()
            .unwrap();

        let mut j = other_start_idx;
        for i in start_idx..end_idx {
            let is_null = self.is_null(i);
            let other_is_null = other.is_null(j);

            if is_null != other_is_null {
                return false;
            }

            if is_null {
                continue;
            }

            let start_offset = self.value_offset(i).to_usize().unwrap();
            let end_offset = self.value_offset(i + 1).to_usize().unwrap();
            let other_start_offset = other.value_offset(j).to_usize().unwrap();
            let other_end_offset = other.value_offset(j + 1).to_usize().unwrap();

            if end_offset - start_offset != other_end_offset - other_start_offset {
                return false;
            }

            if !self.values().range_equals(
                other,
                start_offset,
                end_offset,
                other_start_offset,
            ) {
                return false;
            }

            j += 1;
        }

        true
    }
}

impl<T: ArrowPrimitiveType> ArrayEqual for DictionaryArray<T> {
    fn equals(&self, other: &dyn Array) -> bool {
        self.range_equals(other, 0, self.len(), 0)
    }

    fn range_equals(
        &self,
        other: &dyn Array,
        start_idx: usize,
        end_idx: usize,
        other_start_idx: usize,
    ) -> bool {
        assert!(other_start_idx + (end_idx - start_idx) <= other.len());
        let other = other.as_any().downcast_ref::<DictionaryArray<T>>().unwrap();

        // For now, all the values must be the same
        self.keys()
            .range_equals(other.keys(), start_idx, end_idx, other_start_idx)
            && self
                .values()
                .range_equals(&*other.values(), 0, other.values().len(), 0)
    }
}

impl ArrayEqual for FixedSizeListArray {
    fn equals(&self, other: &dyn Array) -> bool {
        if !base_equal(&self.data(), &other.data()) {
            return false;
        }

        let other = other.as_any().downcast_ref::<FixedSizeListArray>().unwrap();

        if !self.values().range_equals(
            &*other.values(),
            self.value_offset(0) as usize,
            self.value_offset(self.len()) as usize,
            other.value_offset(0) as usize,
        ) {
            return false;
        }

        true
    }

    fn range_equals(
        &self,
        other: &dyn Array,
        start_idx: usize,
        end_idx: usize,
        other_start_idx: usize,
    ) -> bool {
        assert!(other_start_idx + (end_idx - start_idx) <= other.len());
        let other = other.as_any().downcast_ref::<FixedSizeListArray>().unwrap();

        let mut j = other_start_idx;
        for i in start_idx..end_idx {
            let is_null = self.is_null(i);
            let other_is_null = other.is_null(j);

            if is_null != other_is_null {
                return false;
            }

            if is_null {
                continue;
            }

            let start_offset = self.value_offset(i) as usize;
            let end_offset = self.value_offset(i + 1) as usize;
            let other_start_offset = other.value_offset(j) as usize;
            let other_end_offset = other.value_offset(j + 1) as usize;

            if end_offset - start_offset != other_end_offset - other_start_offset {
                return false;
            }

            if !self.values().range_equals(
                &*other.values(),
                start_offset,
                end_offset,
                other_start_offset,
            ) {
                return false;
            }

            j += 1;
        }

        true
    }
}

impl<OffsetSize: BinaryOffsetSizeTrait> ArrayEqual for GenericBinaryArray<OffsetSize> {
    fn equals(&self, other: &dyn Array) -> bool {
        if !base_equal(&self.data(), &other.data()) {
            return false;
        }

        let other = other
            .as_any()
            .downcast_ref::<GenericBinaryArray<OffsetSize>>()
            .unwrap();

        if !value_offset_equal(self, other) {
            return false;
        }

        // TODO: handle null & length == 0 case?

        let value_buf = self.value_data();
        let other_value_buf = other.value_data();
        let value_data = value_buf.data();
        let other_value_data = other_value_buf.data();

        if self.null_count() == 0 {
            // No offset in both - just do memcmp
            if self.offset() == 0 && other.offset() == 0 {
                let len = self.value_offset(self.len()).to_usize().unwrap();
                return value_data[..len] == other_value_data[..len];
            } else {
                let start = self.value_offset(0).to_usize().unwrap();
                let other_start = other.value_offset(0).to_usize().unwrap();
                let len = (self.value_offset(self.len()) - self.value_offset(0))
                    .to_usize()
                    .unwrap();
                return value_data[start..(start + len)]
                    == other_value_data[other_start..(other_start + len)];
            }
        } else {
            for i in 0..self.len() {
                if self.is_null(i) {
                    continue;
                }

                let start = self.value_offset(i).to_usize().unwrap();
                let other_start = other.value_offset(i).to_usize().unwrap();
                let len = self.value_length(i).to_usize().unwrap();
                if value_data[start..(start + len)]
                    != other_value_data[other_start..(other_start + len)]
                {
                    return false;
                }
            }
        }

        true
    }

    fn range_equals(
        &self,
        other: &dyn Array,
        start_idx: usize,
        end_idx: usize,
        other_start_idx: usize,
    ) -> bool {
        assert!(other_start_idx + (end_idx - start_idx) <= other.len());
        let other = other
            .as_any()
            .downcast_ref::<GenericBinaryArray<OffsetSize>>()
            .unwrap();

        let mut j = other_start_idx;
        for i in start_idx..end_idx {
            let is_null = self.is_null(i);
            let other_is_null = other.is_null(j);

            if is_null != other_is_null {
                return false;
            }

            if is_null {
                continue;
            }

            let start_offset = self.value_offset(i).to_usize().unwrap();
            let end_offset = self.value_offset(i + 1).to_usize().unwrap();
            let other_start_offset = other.value_offset(j).to_usize().unwrap();
            let other_end_offset = other.value_offset(j + 1).to_usize().unwrap();

            if end_offset - start_offset != other_end_offset - other_start_offset {
                return false;
            }

            let value_buf = self.value_data();
            let other_value_buf = other.value_data();
            let value_data = value_buf.data();
            let other_value_data = other_value_buf.data();

            if end_offset - start_offset > 0 {
                let len = end_offset - start_offset;
                if value_data[start_offset..(start_offset + len)]
                    != other_value_data[other_start_offset..(other_start_offset + len)]
                {
                    return false;
                }
            }

            j += 1;
        }

        true
    }
}

impl<OffsetSize: StringOffsetSizeTrait> ArrayEqual for GenericStringArray<OffsetSize> {
    fn equals(&self, other: &dyn Array) -> bool {
        if !base_equal(&self.data(), &other.data()) {
            return false;
        }

        let other = other
            .as_any()
            .downcast_ref::<GenericStringArray<OffsetSize>>()
            .unwrap();

        if !value_offset_equal(self, other) {
            return false;
        }

        // TODO: handle null & length == 0 case?

        let value_buf = self.value_data();
        let other_value_buf = other.value_data();
        let value_data = value_buf.data();
        let other_value_data = other_value_buf.data();

        if self.null_count() == 0 {
            // No offset in both - just do memcmp
            if self.offset() == 0 && other.offset() == 0 {
                let len = self.value_offset(self.len()).to_usize().unwrap();
                return value_data[..len] == other_value_data[..len];
            } else {
                let start = self.value_offset(0).to_usize().unwrap();
                let other_start = other.value_offset(0).to_usize().unwrap();
                let len = (self.value_offset(self.len()) - self.value_offset(0))
                    .to_usize()
                    .unwrap();
                return value_data[start..(start + len)]
                    == other_value_data[other_start..(other_start + len)];
            }
        } else {
            for i in 0..self.len() {
                if self.is_null(i) {
                    continue;
                }

                let start = self.value_offset(i).to_usize().unwrap();
                let other_start = other.value_offset(i).to_usize().unwrap();
                let len = self.value_length(i).to_usize().unwrap();
                if value_data[start..(start + len)]
                    != other_value_data[other_start..(other_start + len)]
                {
                    return false;
                }
            }
        }

        true
    }

    fn range_equals(
        &self,
        other: &dyn Array,
        start_idx: usize,
        end_idx: usize,
        other_start_idx: usize,
    ) -> bool {
        assert!(other_start_idx + (end_idx - start_idx) <= other.len());
        let other = other
            .as_any()
            .downcast_ref::<GenericStringArray<OffsetSize>>()
            .unwrap();

        let mut j = other_start_idx;
        for i in start_idx..end_idx {
            let is_null = self.is_null(i);
            let other_is_null = other.is_null(j);

            if is_null != other_is_null {
                return false;
            }

            if is_null {
                continue;
            }

            let start_offset = self.value_offset(i).to_usize().unwrap();
            let end_offset = self.value_offset(i + 1).to_usize().unwrap();
            let other_start_offset = other.value_offset(j).to_usize().unwrap();
            let other_end_offset = other.value_offset(j + 1).to_usize().unwrap();

            if end_offset - start_offset != other_end_offset - other_start_offset {
                return false;
            }

            let value_buf = self.value_data();
            let other_value_buf = other.value_data();
            let value_data = value_buf.data();
            let other_value_data = other_value_buf.data();

            if end_offset - start_offset > 0 {
                let len = end_offset - start_offset;
                if value_data[start_offset..(start_offset + len)]
                    != other_value_data[other_start_offset..(other_start_offset + len)]
                {
                    return false;
                }
            }

            j += 1;
        }

        true
    }
}

impl ArrayEqual for FixedSizeBinaryArray {
    fn equals(&self, other: &dyn Array) -> bool {
        if !base_equal(&self.data(), &other.data()) {
            return false;
        }

        let other = other
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();

        let this = self
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();

        if !value_offset_equal(this, other) {
            return false;
        }

        // TODO: handle null & length == 0 case?

        let value_buf = self.value_data();
        let other_value_buf = other.value_data();
        let value_data = value_buf.data();
        let other_value_data = other_value_buf.data();

        if self.null_count() == 0 {
            // No offset in both - just do memcmp
            if self.offset() == 0 && other.offset() == 0 {
                let len = self.value_offset(self.len()) as usize;
                return value_data[..len] == other_value_data[..len];
            } else {
                let start = self.value_offset(0) as usize;
                let other_start = other.value_offset(0) as usize;
                let len = (self.value_offset(self.len()) - self.value_offset(0)) as usize;
                return value_data[start..(start + len)]
                    == other_value_data[other_start..(other_start + len)];
            }
        } else {
            for i in 0..self.len() {
                if self.is_null(i) {
                    continue;
                }

                let start = self.value_offset(i) as usize;
                let other_start = other.value_offset(i) as usize;
                let len = self.value_length() as usize;
                if value_data[start..(start + len)]
                    != other_value_data[other_start..(other_start + len)]
                {
                    return false;
                }
            }
        }

        true
    }

    fn range_equals(
        &self,
        other: &dyn Array,
        start_idx: usize,
        end_idx: usize,
        other_start_idx: usize,
    ) -> bool {
        assert!(other_start_idx + (end_idx - start_idx) <= other.len());
        let other = other
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();

        let mut j = other_start_idx;
        for i in start_idx..end_idx {
            let is_null = self.is_null(i);
            let other_is_null = other.is_null(j);

            if is_null != other_is_null {
                return false;
            }

            if is_null {
                continue;
            }

            let start_offset = self.value_offset(i) as usize;
            let end_offset = self.value_offset(i + 1) as usize;
            let other_start_offset = other.value_offset(j) as usize;
            let other_end_offset = other.value_offset(j + 1) as usize;

            if end_offset - start_offset != other_end_offset - other_start_offset {
                return false;
            }

            let value_buf = self.value_data();
            let other_value_buf = other.value_data();
            let value_data = value_buf.data();
            let other_value_data = other_value_buf.data();

            if end_offset - start_offset > 0 {
                let len = end_offset - start_offset;
                if value_data[start_offset..(start_offset + len)]
                    != other_value_data[other_start_offset..(other_start_offset + len)]
                {
                    return false;
                }
            }

            j += 1;
        }

        true
    }
}

impl ArrayEqual for StructArray {
    fn equals(&self, other: &dyn Array) -> bool {
        if !base_equal(&self.data(), &other.data()) {
            return false;
        }

        let other = other.as_any().downcast_ref::<StructArray>().unwrap();

        for i in 0..self.len() {
            let is_null = self.is_null(i);
            let other_is_null = other.is_null(i);

            if is_null != other_is_null {
                return false;
            }

            if is_null {
                continue;
            }
            for j in 0..self.num_columns() {
                if !self.column(j).range_equals(&**other.column(j), i, i + 1, i) {
                    return false;
                }
            }
        }

        true
    }

    fn range_equals(
        &self,
        other: &dyn Array,
        start_idx: usize,
        end_idx: usize,
        other_start_idx: usize,
    ) -> bool {
        assert!(other_start_idx + (end_idx - start_idx) <= other.len());
        let other = other.as_any().downcast_ref::<StructArray>().unwrap();

        let mut j = other_start_idx;
        for i in start_idx..end_idx {
            let is_null = self.is_null(i);
            let other_is_null = other.is_null(i);

            if is_null != other_is_null {
                return false;
            }

            if is_null {
                continue;
            }
            for k in 0..self.num_columns() {
                if !self.column(k).range_equals(&**other.column(k), i, i + 1, j) {
                    return false;
                }
            }

            j += 1;
        }

        true
    }
}

impl ArrayEqual for UnionArray {
    fn equals(&self, _other: &dyn Array) -> bool {
        unimplemented!(
            "Added to allow UnionArray to implement the Array trait: see ARROW-8576"
        )
    }

    fn range_equals(
        &self,
        _other: &dyn Array,
        _start_idx: usize,
        _end_idx: usize,
        _other_start_idx: usize,
    ) -> bool {
        unimplemented!(
            "Added to allow UnionArray to implement the Array trait: see ARROW-8576"
        )
    }
}

impl ArrayEqual for NullArray {
    fn equals(&self, other: &dyn Array) -> bool {
        if other.data_type() != &DataType::Null {
            return false;
        }

        if self.len() != other.len() {
            return false;
        }
        if self.null_count() != other.null_count() {
            return false;
        }

        true
    }

    fn range_equals(
        &self,
        _other: &dyn Array,
        _start_idx: usize,
        _end_idx: usize,
        _other_start_idx: usize,
    ) -> bool {
        unimplemented!("Range comparison for null array not yet supported")
    }
}

// Compare if the common basic fields between the two arrays are equal
fn base_equal(this: &ArrayDataRef, other: &ArrayDataRef) -> bool {
    if this.data_type() != other.data_type() {
        return false;
    }
    if this.len() != other.len() {
        return false;
    }
    if this.null_count() != other.null_count() {
        return false;
    }
    if this.null_count() > 0 {
        let null_bitmap = this.null_bitmap().as_ref().unwrap();
        let other_null_bitmap = other.null_bitmap().as_ref().unwrap();
        let null_buf = null_bitmap.bits.data();
        let other_null_buf = other_null_bitmap.bits.data();
        for i in 0..this.len() {
            if bit_util::get_bit(null_buf, i + this.offset())
                != bit_util::get_bit(other_null_buf, i + other.offset())
            {
                return false;
            }
        }
    }
    true
}

// Compare if the value offsets are equal between the two list arrays
fn value_offset_equal<K: OffsetSizeTrait, T: Array + ListArrayOps<K>>(
    this: &T,
    other: &T,
) -> bool {
    // Check if offsets differ
    if this.offset() == 0 && other.offset() == 0 {
        let offset_data = &this.data_ref().buffers()[0];
        let other_offset_data = &other.data_ref().buffers()[0];
        return offset_data.data()[0..((this.len() + 1) * 4)]
            == other_offset_data.data()[0..((other.len() + 1) * 4)];
    }

    // The expensive case
    for i in 0..=this.len() {
        if this.value_offset_at(i) - this.value_offset_at(0)
            != other.value_offset_at(i) - other.value_offset_at(0)
        {
            return false;
        }
    }

    true
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::error::Result;
    use std::{convert::TryFrom, sync::Arc};

    #[test]
    fn test_primitive_equal() {
        let a = Int32Array::from(vec![1, 2, 3]);
        let b = Int32Array::from(vec![1, 2, 3]);
        assert!(a.equals(&b));
        assert!(b.equals(&a));

        let b = Int32Array::from(vec![1, 2, 4]);
        assert!(!a.equals(&b));
        assert!(!b.equals(&a));

        // Test the case where null_count > 0

        let a = Int32Array::from(vec![Some(1), None, Some(2), Some(3)]);
        let b = Int32Array::from(vec![Some(1), None, Some(2), Some(3)]);
        assert!(a.equals(&b));
        assert!(b.equals(&a));

        let b = Int32Array::from(vec![Some(1), None, None, Some(3)]);
        assert!(!a.equals(&b));
        assert!(!b.equals(&a));

        let b = Int32Array::from(vec![Some(1), None, Some(2), Some(4)]);
        assert!(!a.equals(&b));
        assert!(!b.equals(&a));

        // Test the case where offset != 0

        let a_slice = a.slice(1, 2);
        let b_slice = b.slice(1, 2);
        assert!(a_slice.equals(&*b_slice));
        assert!(b_slice.equals(&*a_slice));
    }

    #[test]
    fn test_boolean_equal() {
        let a = BooleanArray::from(vec![false, false, true]);
        let b = BooleanArray::from(vec![false, false, true]);
        assert!(a.equals(&b));
        assert!(b.equals(&a));

        let b = BooleanArray::from(vec![false, false, false]);
        assert!(!a.equals(&b));
        assert!(!b.equals(&a));

        // Test the case where null_count > 0

        let a = BooleanArray::from(vec![Some(false), None, None, Some(true)]);
        let b = BooleanArray::from(vec![Some(false), None, None, Some(true)]);
        assert!(a.equals(&b));
        assert!(b.equals(&a));

        let b = BooleanArray::from(vec![None, None, None, Some(true)]);
        assert!(!a.equals(&b));
        assert!(!b.equals(&a));

        let b = BooleanArray::from(vec![Some(true), None, None, Some(true)]);
        assert!(!a.equals(&b));
        assert!(!b.equals(&a));

        // Test the case where offset != 0

        let a = BooleanArray::from(vec![false, true, false, true, false, false, true]);
        let b = BooleanArray::from(vec![false, false, false, true, false, true, true]);
        assert!(!a.equals(&b));
        assert!(!b.equals(&a));

        let a_slice = a.slice(2, 3);
        let b_slice = b.slice(2, 3);
        assert!(a_slice.equals(&*b_slice));
        assert!(b_slice.equals(&*a_slice));

        let a_slice = a.slice(3, 4);
        let b_slice = b.slice(3, 4);
        assert!(!a_slice.equals(&*b_slice));
        assert!(!b_slice.equals(&*a_slice));
    }

    #[test]
    fn test_list_equal() {
        let mut a_builder = ListBuilder::new(Int32Builder::new(10));
        let mut b_builder = ListBuilder::new(Int32Builder::new(10));

        let a = create_list_array(&mut a_builder, &[Some(&[1, 2, 3]), Some(&[4, 5, 6])])
            .unwrap();
        let b = create_list_array(&mut b_builder, &[Some(&[1, 2, 3]), Some(&[4, 5, 6])])
            .unwrap();

        assert!(a.equals(&b));
        assert!(b.equals(&a));

        let b = create_list_array(&mut a_builder, &[Some(&[1, 2, 3]), Some(&[4, 5, 7])])
            .unwrap();
        assert!(!a.equals(&b));
        assert!(!b.equals(&a));

        // Test the case where null_count > 0

        let a = create_list_array(
            &mut a_builder,
            &[Some(&[1, 2]), None, None, Some(&[3, 4]), None, None],
        )
        .unwrap();
        let b = create_list_array(
            &mut a_builder,
            &[Some(&[1, 2]), None, None, Some(&[3, 4]), None, None],
        )
        .unwrap();
        assert!(a.equals(&b));
        assert!(b.equals(&a));

        let b = create_list_array(
            &mut a_builder,
            &[
                Some(&[1, 2]),
                None,
                Some(&[5, 6]),
                Some(&[3, 4]),
                None,
                None,
            ],
        )
        .unwrap();
        assert!(!a.equals(&b));
        assert!(!b.equals(&a));

        let b = create_list_array(
            &mut a_builder,
            &[Some(&[1, 2]), None, None, Some(&[3, 5]), None, None],
        )
        .unwrap();
        assert!(!a.equals(&b));
        assert!(!b.equals(&a));

        // Test the case where offset != 0

        let a_slice = a.slice(0, 3);
        let b_slice = b.slice(0, 3);
        assert!(a_slice.equals(&*b_slice));
        assert!(b_slice.equals(&*a_slice));

        let a_slice = a.slice(0, 5);
        let b_slice = b.slice(0, 5);
        assert!(!a_slice.equals(&*b_slice));
        assert!(!b_slice.equals(&*a_slice));

        let a_slice = a.slice(4, 1);
        let b_slice = b.slice(4, 1);
        assert!(a_slice.equals(&*b_slice));
        assert!(b_slice.equals(&*a_slice));
    }

    #[test]
    fn test_fixed_size_list_equal() {
        let mut a_builder = FixedSizeListBuilder::new(Int32Builder::new(10), 3);
        let mut b_builder = FixedSizeListBuilder::new(Int32Builder::new(10), 3);

        let a = create_fixed_size_list_array(
            &mut a_builder,
            &[Some(&[1, 2, 3]), Some(&[4, 5, 6])],
        )
        .unwrap();
        let b = create_fixed_size_list_array(
            &mut b_builder,
            &[Some(&[1, 2, 3]), Some(&[4, 5, 6])],
        )
        .unwrap();

        assert!(a.equals(&b));
        assert!(b.equals(&a));

        let b = create_fixed_size_list_array(
            &mut a_builder,
            &[Some(&[1, 2, 3]), Some(&[4, 5, 7])],
        )
        .unwrap();
        assert!(!a.equals(&b));
        assert!(!b.equals(&a));

        // Test the case where null_count > 0

        let a = create_fixed_size_list_array(
            &mut a_builder,
            &[Some(&[1, 2, 3]), None, None, Some(&[4, 5, 6]), None, None],
        )
        .unwrap();
        let b = create_fixed_size_list_array(
            &mut a_builder,
            &[Some(&[1, 2, 3]), None, None, Some(&[4, 5, 6]), None, None],
        )
        .unwrap();
        assert!(a.equals(&b));
        assert!(b.equals(&a));

        let b = create_fixed_size_list_array(
            &mut a_builder,
            &[
                Some(&[1, 2, 3]),
                None,
                Some(&[7, 8, 9]),
                Some(&[4, 5, 6]),
                None,
                None,
            ],
        )
        .unwrap();
        assert!(!a.equals(&b));
        assert!(!b.equals(&a));

        let b = create_fixed_size_list_array(
            &mut a_builder,
            &[Some(&[1, 2, 3]), None, None, Some(&[3, 6, 9]), None, None],
        )
        .unwrap();
        assert!(!a.equals(&b));
        assert!(!b.equals(&a));

        // Test the case where offset != 0

        let a_slice = a.slice(0, 3);
        let b_slice = b.slice(0, 3);
        assert!(a_slice.equals(&*b_slice));
        assert!(b_slice.equals(&*a_slice));

        // let a_slice = a.slice(0, 5);
        // let b_slice = b.slice(0, 5);
        // assert!(!a_slice.equals(&*b_slice));
        // assert!(!b_slice.equals(&*a_slice));

        // let a_slice = a.slice(4, 1);
        // let b_slice = b.slice(4, 1);
        // assert!(a_slice.equals(&*b_slice));
        // assert!(b_slice.equals(&*a_slice));
    }

    fn test_generic_string_equal<OffsetSize: StringOffsetSizeTrait>() {
        let a = GenericStringArray::<OffsetSize>::from_vec(vec!["hello", "world"]);
        let b = GenericStringArray::<OffsetSize>::from_vec(vec!["hello", "world"]);
        assert!(a.equals(&b));
        assert!(b.equals(&a));

        let b = GenericStringArray::<OffsetSize>::from_vec(vec!["hello", "arrow"]);
        assert!(!a.equals(&b));
        assert!(!b.equals(&a));

        // Test the case where null_count > 0

        let a = GenericStringArray::<OffsetSize>::from_opt_vec(vec![
            Some("hello"),
            None,
            None,
            Some("world"),
            None,
            None,
        ]);

        let b = GenericStringArray::<OffsetSize>::from_opt_vec(vec![
            Some("hello"),
            None,
            None,
            Some("world"),
            None,
            None,
        ]);
        assert!(a.equals(&b));
        assert!(b.equals(&a));

        let b = GenericStringArray::<OffsetSize>::from_opt_vec(vec![
            Some("hello"),
            Some("foo"),
            None,
            Some("world"),
            None,
            None,
        ]);
        assert!(!a.equals(&b));
        assert!(!b.equals(&a));

        let b = GenericStringArray::<OffsetSize>::from_opt_vec(vec![
            Some("hello"),
            None,
            None,
            Some("arrow"),
            None,
            None,
        ]);
        assert!(!a.equals(&b));
        assert!(!b.equals(&a));

        // Test the case where offset != 0

        let a_slice = a.slice(0, 3);
        let b_slice = b.slice(0, 3);
        assert!(a_slice.equals(&*b_slice));
        assert!(b_slice.equals(&*a_slice));

        let a_slice = a.slice(0, 5);
        let b_slice = b.slice(0, 5);
        assert!(!a_slice.equals(&*b_slice));
        assert!(!b_slice.equals(&*a_slice));

        let a_slice = a.slice(4, 1);
        let b_slice = b.slice(4, 1);
        assert!(a_slice.equals(&*b_slice));
        assert!(b_slice.equals(&*a_slice));
    }

    #[test]
    fn test_string_equal() {
        test_generic_string_equal::<i32>()
    }

    #[test]
    fn test_large_string_equal() {
        test_generic_string_equal::<i64>()
    }

    #[test]
    fn test_struct_equal() {
        let strings: ArrayRef = Arc::new(StringArray::from(vec![
            Some("joe"),
            None,
            None,
            Some("mark"),
            Some("doe"),
        ]));
        let ints: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(1),
            Some(2),
            None,
            Some(4),
            Some(5),
        ]));

        let a =
            StructArray::try_from(vec![("f1", strings.clone()), ("f2", ints.clone())])
                .unwrap();

        let b = StructArray::try_from(vec![("f1", strings), ("f2", ints)]).unwrap();

        assert!(a.equals(&b));
        assert!(b.equals(&a));
    }

    #[test]
    fn test_null_equal() {
        let a = NullArray::new(12);
        let b = NullArray::new(12);
        assert!(a.equals(&b));
        assert!(b.equals(&a));

        let b = NullArray::new(10);
        assert!(!a.equals(&b));
        assert!(!b.equals(&a));

        // Test the case where offset != 0

        let a_slice = a.slice(2, 3);
        let b_slice = b.slice(1, 3);
        assert!(a_slice.equals(&*b_slice));
        assert!(b_slice.equals(&*a_slice));

        let a_slice = a.slice(5, 4);
        let b_slice = b.slice(3, 3);
        assert!(!a_slice.equals(&*b_slice));
        assert!(!b_slice.equals(&*a_slice));
    }

    fn create_list_array<'a, U: AsRef<[i32]>, T: AsRef<[Option<U>]>>(
        builder: &'a mut ListBuilder<Int32Builder>,
        data: T,
    ) -> Result<ListArray> {
        for d in data.as_ref() {
            if let Some(v) = d {
                builder.values().append_slice(v.as_ref())?;
                builder.append(true)?
            } else {
                builder.append(false)?
            }
        }
        Ok(builder.finish())
    }

    /// Create a fixed size list of 2 value lengths
    fn create_fixed_size_list_array<'a, U: AsRef<[i32]>, T: AsRef<[Option<U>]>>(
        builder: &'a mut FixedSizeListBuilder<Int32Builder>,
        data: T,
    ) -> Result<FixedSizeListArray> {
        for d in data.as_ref() {
            if let Some(v) = d {
                builder.values().append_slice(v.as_ref())?;
                builder.append(true)?
            } else {
                for _ in 0..builder.value_length() {
                    builder.values().append_null()?;
                }
                builder.append(false)?
            }
        }
        Ok(builder.finish())
    }
}
