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

//! Module containing functionality to compute array equality.
//! This module uses [ArrayData] and does not
//! depend on dynamic casting of `Array`.

use super::{
    Array, ArrayData, BinaryOffsetSizeTrait, BooleanArray, DecimalArray,
    FixedSizeBinaryArray, FixedSizeListArray, GenericBinaryArray, GenericListArray,
    GenericStringArray, NullArray, OffsetSizeTrait, PrimitiveArray,
    StringOffsetSizeTrait, StructArray,
};

use crate::{
    buffer::Buffer,
    datatypes::{ArrowPrimitiveType, DataType, IntervalUnit},
};

mod boolean;
mod decimal;
mod dictionary;
mod fixed_binary;
mod fixed_list;
mod list;
mod null;
mod primitive;
mod structure;
mod utils;
mod variable_size;

// these methods assume the same type, len and null count.
// For this reason, they are not exposed and are instead used
// to build the generic functions below (`equal_range` and `equal`).
use boolean::boolean_equal;
use decimal::decimal_equal;
use dictionary::dictionary_equal;
use fixed_binary::fixed_binary_equal;
use fixed_list::fixed_list_equal;
use list::list_equal;
use null::null_equal;
use primitive::primitive_equal;
use structure::struct_equal;
use variable_size::variable_sized_equal;

impl PartialEq for dyn Array {
    fn eq(&self, other: &Self) -> bool {
        equal(self.data().as_ref(), other.data().as_ref())
    }
}

impl<T: Array> PartialEq<T> for dyn Array {
    fn eq(&self, other: &T) -> bool {
        equal(self.data().as_ref(), other.data().as_ref())
    }
}

impl PartialEq for NullArray {
    fn eq(&self, other: &NullArray) -> bool {
        equal(self.data().as_ref(), other.data().as_ref())
    }
}

impl<T: ArrowPrimitiveType> PartialEq for PrimitiveArray<T> {
    fn eq(&self, other: &PrimitiveArray<T>) -> bool {
        equal(self.data().as_ref(), other.data().as_ref())
    }
}

impl PartialEq for BooleanArray {
    fn eq(&self, other: &BooleanArray) -> bool {
        equal(self.data().as_ref(), other.data().as_ref())
    }
}

impl<OffsetSize: StringOffsetSizeTrait> PartialEq for GenericStringArray<OffsetSize> {
    fn eq(&self, other: &Self) -> bool {
        equal(self.data().as_ref(), other.data().as_ref())
    }
}

impl<OffsetSize: BinaryOffsetSizeTrait> PartialEq for GenericBinaryArray<OffsetSize> {
    fn eq(&self, other: &Self) -> bool {
        equal(self.data().as_ref(), other.data().as_ref())
    }
}

impl PartialEq for FixedSizeBinaryArray {
    fn eq(&self, other: &Self) -> bool {
        equal(self.data().as_ref(), other.data().as_ref())
    }
}

impl PartialEq for DecimalArray {
    fn eq(&self, other: &Self) -> bool {
        equal(self.data().as_ref(), other.data().as_ref())
    }
}

impl<OffsetSize: OffsetSizeTrait> PartialEq for GenericListArray<OffsetSize> {
    fn eq(&self, other: &Self) -> bool {
        equal(self.data().as_ref(), other.data().as_ref())
    }
}

impl PartialEq for FixedSizeListArray {
    fn eq(&self, other: &Self) -> bool {
        equal(self.data().as_ref(), other.data().as_ref())
    }
}

impl PartialEq for StructArray {
    fn eq(&self, other: &Self) -> bool {
        equal(self.data().as_ref(), other.data().as_ref())
    }
}

/// Compares the values of two [ArrayData] starting at `lhs_start` and `rhs_start` respectively
/// for `len` slots. The null buffers `lhs_nulls` and `rhs_nulls` inherit parent nullability.
///
/// If an array is a child of a struct or list, the array's nulls have to be merged with the parent.
/// This then affects the null count of the array, thus the merged nulls are passed separately
/// as `lhs_nulls` and `rhs_nulls` variables to functions.
/// The nulls are merged with a bitwise AND, and null counts are recomputed where necessary.
#[inline]
fn equal_values(
    lhs: &ArrayData,
    rhs: &ArrayData,
    lhs_nulls: Option<&Buffer>,
    rhs_nulls: Option<&Buffer>,
    lhs_start: usize,
    rhs_start: usize,
    len: usize,
) -> bool {
    // compute the nested buffer of the parent and child
    // if the array has no parent, the child is computed with itself
    #[allow(unused_assignments)]
    let mut temp_lhs: Option<Buffer> = None;
    #[allow(unused_assignments)]
    let mut temp_rhs: Option<Buffer> = None;
    let lhs_merged_nulls = match (lhs_nulls, lhs.null_buffer()) {
        (None, None) => None,
        (None, Some(c)) => Some(c),
        (Some(p), None) => Some(p),
        (Some(p), Some(c)) => {
            let merged = (p & c).unwrap();
            temp_lhs = Some(merged);
            temp_lhs.as_ref()
        }
    };
    let rhs_merged_nulls = match (rhs_nulls, rhs.null_buffer()) {
        (None, None) => None,
        (None, Some(c)) => Some(c),
        (Some(p), None) => Some(p),
        (Some(p), Some(c)) => {
            let merged = (p & c).unwrap();
            temp_rhs = Some(merged);
            temp_rhs.as_ref()
        }
    };

    match lhs.data_type() {
        DataType::Null => null_equal(lhs, rhs, lhs_start, rhs_start, len),
        DataType::Boolean => boolean_equal(lhs, rhs, lhs_start, rhs_start, len),
        DataType::UInt8 => primitive_equal::<u8>(lhs, rhs, lhs_start, rhs_start, len),
        DataType::UInt16 => primitive_equal::<u16>(lhs, rhs, lhs_start, rhs_start, len),
        DataType::UInt32 => primitive_equal::<u32>(lhs, rhs, lhs_start, rhs_start, len),
        DataType::UInt64 => primitive_equal::<u64>(lhs, rhs, lhs_start, rhs_start, len),
        DataType::Int8 => primitive_equal::<i8>(lhs, rhs, lhs_start, rhs_start, len),
        DataType::Int16 => primitive_equal::<i16>(lhs, rhs, lhs_start, rhs_start, len),
        DataType::Int32 => primitive_equal::<i32>(lhs, rhs, lhs_start, rhs_start, len),
        DataType::Int64 => primitive_equal::<i64>(lhs, rhs, lhs_start, rhs_start, len),
        DataType::Float32 => primitive_equal::<f32>(lhs, rhs, lhs_start, rhs_start, len),
        DataType::Float64 => primitive_equal::<f64>(lhs, rhs, lhs_start, rhs_start, len),
        DataType::Date32(_)
        | DataType::Time32(_)
        | DataType::Interval(IntervalUnit::YearMonth) => {
            primitive_equal::<i32>(lhs, rhs, lhs_start, rhs_start, len)
        }
        DataType::Date64(_)
        | DataType::Interval(IntervalUnit::DayTime)
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_) => {
            primitive_equal::<i64>(lhs, rhs, lhs_start, rhs_start, len)
        }
        DataType::Utf8 | DataType::Binary => variable_sized_equal::<i32>(
            lhs,
            rhs,
            lhs_merged_nulls,
            rhs_merged_nulls,
            lhs_start,
            rhs_start,
            len,
        ),
        DataType::LargeUtf8 | DataType::LargeBinary => variable_sized_equal::<i64>(
            lhs,
            rhs,
            lhs_merged_nulls,
            rhs_merged_nulls,
            lhs_start,
            rhs_start,
            len,
        ),
        DataType::FixedSizeBinary(_) => {
            fixed_binary_equal(lhs, rhs, lhs_start, rhs_start, len)
        }
        DataType::Decimal(_, _) => decimal_equal(lhs, rhs, lhs_start, rhs_start, len),
        DataType::List(_) => list_equal::<i32>(lhs, rhs, lhs_start, rhs_start, len),
        DataType::LargeList(_) => list_equal::<i64>(lhs, rhs, lhs_start, rhs_start, len),
        DataType::FixedSizeList(_, _) => {
            fixed_list_equal(lhs, rhs, lhs_start, rhs_start, len)
        }
        DataType::Struct(_) => struct_equal(
            lhs,
            rhs,
            lhs_merged_nulls,
            rhs_merged_nulls,
            lhs_start,
            rhs_start,
            len,
        ),
        DataType::Union(_) => unimplemented!("See ARROW-8576"),
        DataType::Dictionary(data_type, _) => match data_type.as_ref() {
            DataType::Int8 => dictionary_equal::<i8>(lhs, rhs, lhs_start, rhs_start, len),
            DataType::Int16 => {
                dictionary_equal::<i16>(lhs, rhs, lhs_start, rhs_start, len)
            }
            DataType::Int32 => {
                dictionary_equal::<i32>(lhs, rhs, lhs_start, rhs_start, len)
            }
            DataType::Int64 => {
                dictionary_equal::<i64>(lhs, rhs, lhs_start, rhs_start, len)
            }
            DataType::UInt8 => {
                dictionary_equal::<u8>(lhs, rhs, lhs_start, rhs_start, len)
            }
            DataType::UInt16 => {
                dictionary_equal::<u16>(lhs, rhs, lhs_start, rhs_start, len)
            }
            DataType::UInt32 => {
                dictionary_equal::<u32>(lhs, rhs, lhs_start, rhs_start, len)
            }
            DataType::UInt64 => {
                dictionary_equal::<u64>(lhs, rhs, lhs_start, rhs_start, len)
            }
            _ => unreachable!(),
        },
        DataType::Float16 => unreachable!(),
    }
}

fn equal_range(
    lhs: &ArrayData,
    rhs: &ArrayData,
    lhs_nulls: Option<&Buffer>,
    rhs_nulls: Option<&Buffer>,
    lhs_start: usize,
    rhs_start: usize,
    len: usize,
) -> bool {
    utils::base_equal(lhs, rhs)
        && utils::equal_nulls(lhs, rhs, lhs_nulls, rhs_nulls, lhs_start, rhs_start, len)
        && equal_values(lhs, rhs, lhs_nulls, rhs_nulls, lhs_start, rhs_start, len)
}

/// Logically compares two [ArrayData].
/// Two arrays are logically equal if and only if:
/// * their data types are equal
/// * their lengths are equal
/// * their null counts are equal
/// * their null bitmaps are equal
/// * each of their items are equal
/// two items are equal when their in-memory representation is physically equal (i.e. same bit content).
/// The physical comparison depend on the data type.
/// # Panics
/// This function may panic whenever any of the [ArrayData] does not follow the Arrow specification.
/// (e.g. wrong number of buffers, buffer `len` does not correspond to the declared `len`)
pub fn equal(lhs: &ArrayData, rhs: &ArrayData) -> bool {
    let lhs_nulls = lhs.null_buffer();
    let rhs_nulls = rhs.null_buffer();
    utils::base_equal(lhs, rhs)
        && lhs.null_count() == rhs.null_count()
        && utils::equal_nulls(lhs, rhs, lhs_nulls, rhs_nulls, 0, 0, lhs.len())
        && equal_values(lhs, rhs, lhs_nulls, rhs_nulls, 0, 0, lhs.len())
}

#[cfg(test)]
mod tests {
    use std::convert::TryFrom;
    use std::sync::Arc;

    use crate::array::{
        array::Array, ArrayDataRef, ArrayRef, BinaryOffsetSizeTrait, BooleanArray,
        DecimalBuilder, FixedSizeBinaryBuilder, FixedSizeListBuilder, GenericBinaryArray,
        Int32Builder, ListBuilder, NullArray, PrimitiveBuilder, StringArray,
        StringDictionaryBuilder, StringOffsetSizeTrait, StructArray,
    };
    use crate::array::{GenericStringArray, Int32Array};
    use crate::buffer::Buffer;
    use crate::datatypes::{Field, Int16Type};

    use super::*;

    #[test]
    fn test_null_equal() {
        let a = NullArray::new(12).data();
        let b = NullArray::new(12).data();
        test_equal(&a, &b, true);

        let b = NullArray::new(10).data();
        test_equal(&a, &b, false);

        // Test the case where offset != 0

        let a_slice = a.slice(2, 3);
        let b_slice = b.slice(1, 3);
        test_equal(&a_slice, &b_slice, true);

        let a_slice = a.slice(5, 4);
        let b_slice = b.slice(3, 3);
        test_equal(&a_slice, &b_slice, false);
    }

    #[test]
    fn test_boolean_equal() {
        let a = BooleanArray::from(vec![false, false, true]).data();
        let b = BooleanArray::from(vec![false, false, true]).data();
        test_equal(a.as_ref(), b.as_ref(), true);

        let b = BooleanArray::from(vec![false, false, false]).data();
        test_equal(a.as_ref(), b.as_ref(), false);

        // Test the case where null_count > 0

        let a = BooleanArray::from(vec![Some(false), None, None, Some(true)]).data();
        let b = BooleanArray::from(vec![Some(false), None, None, Some(true)]).data();
        test_equal(a.as_ref(), b.as_ref(), true);

        let b = BooleanArray::from(vec![None, None, None, Some(true)]).data();
        test_equal(a.as_ref(), b.as_ref(), false);

        let b = BooleanArray::from(vec![Some(true), None, None, Some(true)]).data();
        test_equal(a.as_ref(), b.as_ref(), false);

        // Test the case where offset != 0

        let a =
            BooleanArray::from(vec![false, true, false, true, false, false, true]).data();
        let b =
            BooleanArray::from(vec![false, false, false, true, false, true, true]).data();
        assert_eq!(equal(a.as_ref(), b.as_ref()), false);
        assert_eq!(equal(b.as_ref(), a.as_ref()), false);

        let a_slice = a.slice(2, 3);
        let b_slice = b.slice(2, 3);
        assert_eq!(equal(&a_slice, &b_slice), true);
        assert_eq!(equal(&b_slice, &a_slice), true);

        let a_slice = a.slice(3, 4);
        let b_slice = b.slice(3, 4);
        assert_eq!(equal(&a_slice, &b_slice), false);
        assert_eq!(equal(&b_slice, &a_slice), false);
    }

    #[test]
    fn test_primitive() {
        let cases = vec![
            (
                vec![Some(1), Some(2), Some(3)],
                vec![Some(1), Some(2), Some(3)],
                true,
            ),
            (
                vec![Some(1), Some(2), Some(3)],
                vec![Some(1), Some(2), Some(4)],
                false,
            ),
            (
                vec![Some(1), Some(2), None],
                vec![Some(1), Some(2), None],
                true,
            ),
            (
                vec![Some(1), None, Some(3)],
                vec![Some(1), Some(2), None],
                false,
            ),
            (
                vec![Some(1), None, None],
                vec![Some(1), Some(2), None],
                false,
            ),
        ];

        for (lhs, rhs, expected) in cases {
            let lhs = Int32Array::from(lhs).data();
            let rhs = Int32Array::from(rhs).data();
            test_equal(&lhs, &rhs, expected);
        }
    }

    #[test]
    fn test_primitive_slice() {
        let cases = vec![
            (
                vec![Some(1), Some(2), Some(3)],
                (0, 1),
                vec![Some(1), Some(2), Some(3)],
                (0, 1),
                true,
            ),
            (
                vec![Some(1), Some(2), Some(3)],
                (1, 1),
                vec![Some(1), Some(2), Some(3)],
                (2, 1),
                false,
            ),
            (
                vec![Some(1), Some(2), None],
                (1, 1),
                vec![Some(1), None, Some(2)],
                (2, 1),
                true,
            ),
        ];

        for (lhs, slice_lhs, rhs, slice_rhs, expected) in cases {
            let lhs = Int32Array::from(lhs).data();
            let lhs = lhs.slice(slice_lhs.0, slice_lhs.1);
            let rhs = Int32Array::from(rhs).data();
            let rhs = rhs.slice(slice_rhs.0, slice_rhs.1);

            test_equal(&lhs, &rhs, expected);
        }
    }

    fn test_equal(lhs: &ArrayData, rhs: &ArrayData, expected: bool) {
        // equality is symmetric
        assert_eq!(equal(lhs, lhs), true, "\n{:?}\n{:?}", lhs, lhs);
        assert_eq!(equal(rhs, rhs), true, "\n{:?}\n{:?}", rhs, rhs);

        assert_eq!(equal(lhs, rhs), expected, "\n{:?}\n{:?}", lhs, rhs);
        assert_eq!(equal(rhs, lhs), expected, "\n{:?}\n{:?}", rhs, lhs);
    }

    fn binary_cases() -> Vec<(Vec<Option<String>>, Vec<Option<String>>, bool)> {
        let base = vec![
            Some("hello".to_owned()),
            None,
            None,
            Some("world".to_owned()),
            None,
            None,
        ];
        let not_base = vec![
            Some("hello".to_owned()),
            Some("foo".to_owned()),
            None,
            Some("world".to_owned()),
            None,
            None,
        ];
        vec![
            (
                vec![Some("hello".to_owned()), Some("world".to_owned())],
                vec![Some("hello".to_owned()), Some("world".to_owned())],
                true,
            ),
            (
                vec![Some("hello".to_owned()), Some("world".to_owned())],
                vec![Some("hello".to_owned()), Some("arrow".to_owned())],
                false,
            ),
            (base.clone(), base.clone(), true),
            (base, not_base, false),
        ]
    }

    fn test_generic_string_equal<OffsetSize: StringOffsetSizeTrait>() {
        let cases = binary_cases();

        for (lhs, rhs, expected) in cases {
            let lhs = lhs.iter().map(|x| x.as_deref()).collect();
            let rhs = rhs.iter().map(|x| x.as_deref()).collect();
            let lhs = GenericStringArray::<OffsetSize>::from_opt_vec(lhs).data();
            let rhs = GenericStringArray::<OffsetSize>::from_opt_vec(rhs).data();
            test_equal(lhs.as_ref(), rhs.as_ref(), expected);
        }
    }

    #[test]
    fn test_string_equal() {
        test_generic_string_equal::<i32>()
    }

    #[test]
    fn test_large_string_equal() {
        test_generic_string_equal::<i64>()
    }

    fn test_generic_binary_equal<OffsetSize: BinaryOffsetSizeTrait>() {
        let cases = binary_cases();

        for (lhs, rhs, expected) in cases {
            let lhs = lhs
                .iter()
                .map(|x| x.as_deref().map(|x| x.as_bytes()))
                .collect();
            let rhs = rhs
                .iter()
                .map(|x| x.as_deref().map(|x| x.as_bytes()))
                .collect();
            let lhs = GenericBinaryArray::<OffsetSize>::from_opt_vec(lhs).data();
            let rhs = GenericBinaryArray::<OffsetSize>::from_opt_vec(rhs).data();
            test_equal(lhs.as_ref(), rhs.as_ref(), expected);
        }
    }

    #[test]
    fn test_binary_equal() {
        test_generic_binary_equal::<i32>()
    }

    #[test]
    fn test_large_binary_equal() {
        test_generic_binary_equal::<i64>()
    }

    #[test]
    fn test_null() {
        let a = NullArray::new(2).data();
        let b = NullArray::new(2).data();
        test_equal(a.as_ref(), b.as_ref(), true);

        let b = NullArray::new(1).data();
        test_equal(a.as_ref(), b.as_ref(), false);
    }

    fn create_list_array<U: AsRef<[i32]>, T: AsRef<[Option<U>]>>(
        data: T,
    ) -> ArrayDataRef {
        let mut builder = ListBuilder::new(Int32Builder::new(10));
        for d in data.as_ref() {
            if let Some(v) = d {
                builder.values().append_slice(v.as_ref()).unwrap();
                builder.append(true).unwrap()
            } else {
                builder.append(false).unwrap()
            }
        }
        builder.finish().data()
    }

    #[test]
    fn test_list_equal() {
        let a = create_list_array(&[Some(&[1, 2, 3]), Some(&[4, 5, 6])]);
        let b = create_list_array(&[Some(&[1, 2, 3]), Some(&[4, 5, 6])]);
        test_equal(a.as_ref(), b.as_ref(), true);

        let b = create_list_array(&[Some(&[1, 2, 3]), Some(&[4, 5, 7])]);
        test_equal(a.as_ref(), b.as_ref(), false);
    }

    // Test the case where null_count > 0
    #[test]
    fn test_list_null() {
        let a =
            create_list_array(&[Some(&[1, 2]), None, None, Some(&[3, 4]), None, None]);
        let b =
            create_list_array(&[Some(&[1, 2]), None, None, Some(&[3, 4]), None, None]);
        test_equal(a.as_ref(), b.as_ref(), true);

        let b = create_list_array(&[
            Some(&[1, 2]),
            None,
            Some(&[5, 6]),
            Some(&[3, 4]),
            None,
            None,
        ]);
        test_equal(a.as_ref(), b.as_ref(), false);

        let b =
            create_list_array(&[Some(&[1, 2]), None, None, Some(&[3, 5]), None, None]);
        test_equal(a.as_ref(), b.as_ref(), false);
    }

    // Test the case where offset != 0
    #[test]
    fn test_list_offsets() {
        let a =
            create_list_array(&[Some(&[1, 2]), None, None, Some(&[3, 4]), None, None]);
        let b =
            create_list_array(&[Some(&[1, 2]), None, None, Some(&[3, 5]), None, None]);

        let a_slice = a.slice(0, 3);
        let b_slice = b.slice(0, 3);
        test_equal(&a_slice, &b_slice, true);

        let a_slice = a.slice(0, 5);
        let b_slice = b.slice(0, 5);
        test_equal(&a_slice, &b_slice, false);

        let a_slice = a.slice(4, 1);
        let b_slice = b.slice(4, 1);
        test_equal(&a_slice, &b_slice, true);
    }

    fn create_fixed_size_binary_array<U: AsRef<[u8]>, T: AsRef<[Option<U>]>>(
        data: T,
    ) -> ArrayDataRef {
        let mut builder = FixedSizeBinaryBuilder::new(15, 5);

        for d in data.as_ref() {
            if let Some(v) = d {
                builder.append_value(v.as_ref()).unwrap();
            } else {
                builder.append_null().unwrap();
            }
        }
        builder.finish().data()
    }

    #[test]
    fn test_fixed_size_binary_equal() {
        let a = create_fixed_size_binary_array(&[Some(b"hello"), Some(b"world")]);
        let b = create_fixed_size_binary_array(&[Some(b"hello"), Some(b"world")]);
        test_equal(a.as_ref(), b.as_ref(), true);

        let b = create_fixed_size_binary_array(&[Some(b"hello"), Some(b"arrow")]);
        test_equal(a.as_ref(), b.as_ref(), false);
    }

    // Test the case where null_count > 0
    #[test]
    fn test_fixed_size_binary_null() {
        let a = create_fixed_size_binary_array(&[Some(b"hello"), None, Some(b"world")]);
        let b = create_fixed_size_binary_array(&[Some(b"hello"), None, Some(b"world")]);
        test_equal(a.as_ref(), b.as_ref(), true);

        let b = create_fixed_size_binary_array(&[Some(b"hello"), Some(b"world"), None]);
        test_equal(a.as_ref(), b.as_ref(), false);

        let b = create_fixed_size_binary_array(&[Some(b"hello"), None, Some(b"arrow")]);
        test_equal(a.as_ref(), b.as_ref(), false);
    }

    #[test]
    fn test_fixed_size_binary_offsets() {
        // Test the case where offset != 0
        let a = create_fixed_size_binary_array(&[
            Some(b"hello"),
            None,
            None,
            Some(b"world"),
            None,
            None,
        ]);
        let b = create_fixed_size_binary_array(&[
            Some(b"hello"),
            None,
            None,
            Some(b"arrow"),
            None,
            None,
        ]);

        let a_slice = a.slice(0, 3);
        let b_slice = b.slice(0, 3);
        test_equal(&a_slice, &b_slice, true);

        let a_slice = a.slice(0, 5);
        let b_slice = b.slice(0, 5);
        test_equal(&a_slice, &b_slice, false);

        let a_slice = a.slice(4, 1);
        let b_slice = b.slice(4, 1);
        test_equal(&a_slice, &b_slice, true);

        let a_slice = a.slice(3, 1);
        let b_slice = b.slice(3, 1);
        test_equal(&a_slice, &b_slice, false);
    }

    fn create_decimal_array(data: &[Option<i128>]) -> ArrayDataRef {
        let mut builder = DecimalBuilder::new(20, 23, 6);

        for d in data {
            if let Some(v) = d {
                builder.append_value(*v).unwrap();
            } else {
                builder.append_null().unwrap();
            }
        }
        builder.finish().data()
    }

    #[test]
    fn test_decimal_equal() {
        let a = create_decimal_array(&[Some(8_887_000_000), Some(-8_887_000_000)]);
        let b = create_decimal_array(&[Some(8_887_000_000), Some(-8_887_000_000)]);
        test_equal(a.as_ref(), b.as_ref(), true);

        let b = create_decimal_array(&[Some(15_887_000_000), Some(-8_887_000_000)]);
        test_equal(a.as_ref(), b.as_ref(), false);
    }

    // Test the case where null_count > 0
    #[test]
    fn test_decimal_null() {
        let a = create_decimal_array(&[Some(8_887_000_000), None, Some(-8_887_000_000)]);
        let b = create_decimal_array(&[Some(8_887_000_000), None, Some(-8_887_000_000)]);
        test_equal(a.as_ref(), b.as_ref(), true);

        let b = create_decimal_array(&[Some(8_887_000_000), Some(-8_887_000_000), None]);
        test_equal(a.as_ref(), b.as_ref(), false);

        let b = create_decimal_array(&[Some(15_887_000_000), None, Some(-8_887_000_000)]);
        test_equal(a.as_ref(), b.as_ref(), false);
    }

    #[test]
    fn test_decimal_offsets() {
        // Test the case where offset != 0
        let a = create_decimal_array(&[
            Some(8_887_000_000),
            None,
            None,
            Some(-8_887_000_000),
            None,
            None,
        ]);
        let b = create_decimal_array(&[
            Some(8_887_000_000),
            None,
            None,
            Some(15_887_000_000),
            None,
            None,
        ]);

        let a_slice = a.slice(0, 3);
        let b_slice = b.slice(0, 3);
        test_equal(&a_slice, &b_slice, true);

        let a_slice = a.slice(0, 5);
        let b_slice = b.slice(0, 5);
        test_equal(&a_slice, &b_slice, false);

        let a_slice = a.slice(4, 1);
        let b_slice = b.slice(4, 1);
        test_equal(&a_slice, &b_slice, true);

        let a_slice = a.slice(3, 3);
        let b_slice = b.slice(3, 3);
        test_equal(&a_slice, &b_slice, false);

        let a_slice = a.slice(1, 3);
        let b_slice = b.slice(1, 3);
        test_equal(&a_slice, &b_slice, false);

        let b = create_decimal_array(&[
            None,
            None,
            None,
            Some(-8_887_000_000),
            Some(-3_000),
            None,
        ]);
        let a_slice = a.slice(1, 3);
        let b_slice = b.slice(1, 3);
        test_equal(&a_slice, &b_slice, true);
    }

    /// Create a fixed size list of 2 value lengths
    fn create_fixed_size_list_array<U: AsRef<[i32]>, T: AsRef<[Option<U>]>>(
        data: T,
    ) -> ArrayDataRef {
        let mut builder = FixedSizeListBuilder::new(Int32Builder::new(10), 3);

        for d in data.as_ref() {
            if let Some(v) = d {
                builder.values().append_slice(v.as_ref()).unwrap();
                builder.append(true).unwrap()
            } else {
                for _ in 0..builder.value_length() {
                    builder.values().append_null().unwrap();
                }
                builder.append(false).unwrap()
            }
        }
        builder.finish().data()
    }

    #[test]
    fn test_fixed_size_list_equal() {
        let a = create_fixed_size_list_array(&[Some(&[1, 2, 3]), Some(&[4, 5, 6])]);
        let b = create_fixed_size_list_array(&[Some(&[1, 2, 3]), Some(&[4, 5, 6])]);
        test_equal(a.as_ref(), b.as_ref(), true);

        let b = create_fixed_size_list_array(&[Some(&[1, 2, 3]), Some(&[4, 5, 7])]);
        test_equal(a.as_ref(), b.as_ref(), false);
    }

    // Test the case where null_count > 0
    #[test]
    fn test_fixed_list_null() {
        let a = create_fixed_size_list_array(&[
            Some(&[1, 2, 3]),
            None,
            None,
            Some(&[4, 5, 6]),
            None,
            None,
        ]);
        let b = create_fixed_size_list_array(&[
            Some(&[1, 2, 3]),
            None,
            None,
            Some(&[4, 5, 6]),
            None,
            None,
        ]);
        test_equal(a.as_ref(), b.as_ref(), true);

        let b = create_fixed_size_list_array(&[
            Some(&[1, 2, 3]),
            None,
            Some(&[7, 8, 9]),
            Some(&[4, 5, 6]),
            None,
            None,
        ]);
        test_equal(a.as_ref(), b.as_ref(), false);

        let b = create_fixed_size_list_array(&[
            Some(&[1, 2, 3]),
            None,
            None,
            Some(&[3, 6, 9]),
            None,
            None,
        ]);
        test_equal(a.as_ref(), b.as_ref(), false);
    }

    #[test]
    fn test_fixed_list_offsets() {
        // Test the case where offset != 0
        let a = create_fixed_size_list_array(&[
            Some(&[1, 2, 3]),
            None,
            None,
            Some(&[4, 5, 6]),
            None,
            None,
        ]);
        let b = create_fixed_size_list_array(&[
            Some(&[1, 2, 3]),
            None,
            None,
            Some(&[3, 6, 9]),
            None,
            None,
        ]);

        let a_slice = a.slice(0, 3);
        let b_slice = b.slice(0, 3);
        test_equal(&a_slice, &b_slice, true);

        let a_slice = a.slice(0, 5);
        let b_slice = b.slice(0, 5);
        test_equal(&a_slice, &b_slice, false);

        let a_slice = a.slice(4, 1);
        let b_slice = b.slice(4, 1);
        test_equal(&a_slice, &b_slice, true);
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
                .unwrap()
                .data();

        let b = StructArray::try_from(vec![("f1", strings), ("f2", ints)])
            .unwrap()
            .data();

        test_equal(a.as_ref(), b.as_ref(), true);
    }

    #[test]
    fn test_struct_equal_null() {
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
        let ints_non_null: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 0]));

        let a = ArrayData::builder(DataType::Struct(vec![
            Field::new("f1", DataType::Utf8, true),
            Field::new("f2", DataType::Int32, true),
        ]))
        .null_bit_buffer(Buffer::from(vec![0b00001011]))
        .len(5)
        .null_count(2)
        .add_child_data(strings.data_ref().clone())
        .add_child_data(ints.data_ref().clone())
        .build();
        let a = crate::array::make_array(a);

        let b = ArrayData::builder(DataType::Struct(vec![
            Field::new("f1", DataType::Utf8, true),
            Field::new("f2", DataType::Int32, true),
        ]))
        .null_bit_buffer(Buffer::from(vec![0b00001011]))
        .len(5)
        .null_count(2)
        .add_child_data(strings.data_ref().clone())
        .add_child_data(ints_non_null.data_ref().clone())
        .build();
        let b = crate::array::make_array(b);

        test_equal(a.data_ref(), b.data_ref(), true);

        // test with arrays that are not equal
        let c_ints_non_null: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 0, 4]));
        let c = ArrayData::builder(DataType::Struct(vec![
            Field::new("f1", DataType::Utf8, true),
            Field::new("f2", DataType::Int32, true),
        ]))
        .null_bit_buffer(Buffer::from(vec![0b00001011]))
        .len(5)
        .null_count(2)
        .add_child_data(strings.data_ref().clone())
        .add_child_data(c_ints_non_null.data_ref().clone())
        .build();
        let c = crate::array::make_array(c);

        test_equal(a.data_ref(), c.data_ref(), false);

        // test a nested struct
        let a = ArrayData::builder(DataType::Struct(vec![Field::new(
            "f3",
            a.data_type().clone(),
            true,
        )]))
        .null_bit_buffer(Buffer::from(vec![0b00011110]))
        .len(5)
        .null_count(1)
        .add_child_data(a.data_ref().clone())
        .build();
        let a = crate::array::make_array(a);

        // reconstruct b, but with different data where the first struct is null
        let strings: ArrayRef = Arc::new(StringArray::from(vec![
            Some("joanne"), // difference
            None,
            None,
            Some("mark"),
            Some("doe"),
        ]));
        let b = ArrayData::builder(DataType::Struct(vec![
            Field::new("f1", DataType::Utf8, true),
            Field::new("f2", DataType::Int32, true),
        ]))
        .null_bit_buffer(Buffer::from(vec![0b00001011]))
        .len(5)
        .null_count(2)
        .add_child_data(strings.data_ref().clone())
        .add_child_data(ints_non_null.data_ref().clone())
        .build();

        let b = ArrayData::builder(DataType::Struct(vec![Field::new(
            "f3",
            b.data_type().clone(),
            true,
        )]))
        .null_bit_buffer(Buffer::from(vec![0b00011110]))
        .len(5)
        .null_count(1)
        .add_child_data(b)
        .build();
        let b = crate::array::make_array(b);

        test_equal(a.data_ref(), b.data_ref(), true);
    }

    #[test]
    fn test_struct_equal_null_variable_size() {
        // the string arrays differ, but where the struct array is null
        let strings1: ArrayRef = Arc::new(StringArray::from(vec![
            Some("joe"),
            None,
            None,
            Some("mark"),
            Some("doel"),
        ]));
        let strings2: ArrayRef = Arc::new(StringArray::from(vec![
            Some("joel"),
            None,
            None,
            Some("mark"),
            Some("doe"),
        ]));

        let a = ArrayData::builder(DataType::Struct(vec![Field::new(
            "f1",
            DataType::Utf8,
            true,
        )]))
        .null_bit_buffer(Buffer::from(vec![0b00001010]))
        .len(5)
        .null_count(3)
        .add_child_data(strings1.data_ref().clone())
        .build();
        let a = crate::array::make_array(a);

        let b = ArrayData::builder(DataType::Struct(vec![Field::new(
            "f1",
            DataType::Utf8,
            true,
        )]))
        .null_bit_buffer(Buffer::from(vec![0b00001010]))
        .len(5)
        .null_count(3)
        .add_child_data(strings2.data_ref().clone())
        .build();
        let b = crate::array::make_array(b);

        test_equal(a.data_ref(), b.data_ref(), true);

        // test with arrays that are not equal
        let strings3: ArrayRef = Arc::new(StringArray::from(vec![
            Some("mark"),
            None,
            None,
            Some("doe"),
            Some("joe"),
        ]));
        let c = ArrayData::builder(DataType::Struct(vec![Field::new(
            "f1",
            DataType::Utf8,
            true,
        )]))
        .null_bit_buffer(Buffer::from(vec![0b00001011]))
        .len(5)
        .null_count(2)
        .add_child_data(strings3.data_ref().clone())
        .build();
        let c = crate::array::make_array(c);

        test_equal(a.data_ref(), c.data_ref(), false);
    }

    fn create_dictionary_array(values: &[&str], keys: &[Option<&str>]) -> ArrayDataRef {
        let values = StringArray::from(values.to_vec());
        let mut builder = StringDictionaryBuilder::new_with_dictionary(
            PrimitiveBuilder::<Int16Type>::new(3),
            &values,
        )
        .unwrap();
        for key in keys {
            if let Some(v) = key {
                builder.append(v).unwrap();
            } else {
                builder.append_null().unwrap()
            }
        }
        builder.finish().data()
    }

    #[test]
    fn test_dictionary_equal() {
        // (a, b, c), (1, 2, 1, 3) => (a, b, a, c)
        let a = create_dictionary_array(
            &["a", "b", "c"],
            &[Some("a"), Some("b"), Some("a"), Some("c")],
        );
        // different representation (values and keys are swapped), same result
        let b = create_dictionary_array(
            &["a", "c", "b"],
            &[Some("a"), Some("b"), Some("a"), Some("c")],
        );
        test_equal(a.as_ref(), b.as_ref(), true);

        // different len
        let b =
            create_dictionary_array(&["a", "c", "b"], &[Some("a"), Some("b"), Some("a")]);
        test_equal(a.as_ref(), b.as_ref(), false);

        // different key
        let b = create_dictionary_array(
            &["a", "c", "b"],
            &[Some("a"), Some("b"), Some("a"), Some("a")],
        );
        test_equal(a.as_ref(), b.as_ref(), false);

        // different values, same keys
        let b = create_dictionary_array(
            &["a", "b", "d"],
            &[Some("a"), Some("b"), Some("a"), Some("d")],
        );
        test_equal(a.as_ref(), b.as_ref(), false);
    }

    #[test]
    fn test_dictionary_equal_null() {
        // (a, b, c), (1, 2, 1, 3) => (a, b, a, c)
        let a = create_dictionary_array(
            &["a", "b", "c"],
            &[Some("a"), None, Some("a"), Some("c")],
        );

        // equal to self
        test_equal(a.as_ref(), a.as_ref(), true);

        // different representation (values and keys are swapped), same result
        let b = create_dictionary_array(
            &["a", "c", "b"],
            &[Some("a"), None, Some("a"), Some("c")],
        );
        test_equal(a.as_ref(), b.as_ref(), true);

        // different null position
        let b = create_dictionary_array(
            &["a", "c", "b"],
            &[Some("a"), Some("b"), Some("a"), None],
        );
        test_equal(a.as_ref(), b.as_ref(), false);

        // different key
        let b = create_dictionary_array(
            &["a", "c", "b"],
            &[Some("a"), None, Some("a"), Some("a")],
        );
        test_equal(a.as_ref(), b.as_ref(), false);

        // different values, same keys
        let b = create_dictionary_array(
            &["a", "b", "d"],
            &[Some("a"), None, Some("a"), Some("d")],
        );
        test_equal(a.as_ref(), b.as_ref(), false);
    }
}
