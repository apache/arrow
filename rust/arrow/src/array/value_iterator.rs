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

use super::{
    Array, ArrayRef, BinaryOffsetSizeTrait, BooleanArray, GenericBinaryArray,
    GenericListArray, GenericStringArray, OffsetSizeTrait, StringOffsetSizeTrait,
};

/// an iterator that returns Some(bool) or None.
// Note: This implementation is based on std's [Vec]s' [IntoIter].
#[derive(Debug)]
pub struct BooleanValueIter<'a> {
    array: &'a BooleanArray,
    current: usize,
    current_end: usize,
}

impl<'a> BooleanValueIter<'a> {
    /// create a new iterator
    pub fn new(array: &'a BooleanArray) -> Self {
        BooleanValueIter {
            array,
            current: 0,
            current_end: array.len(),
        }
    }
}

impl<'a> std::iter::Iterator for BooleanValueIter<'a> {
    type Item = bool;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current == self.current_end {
            None
        } else {
            let old = self.current;
            self.current += 1;
            Some(self.array.value(old))
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (
            self.array.len() - self.current,
            Some(self.array.len() - self.current),
        )
    }
}

impl<'a> std::iter::DoubleEndedIterator for BooleanValueIter<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.current_end == self.current {
            None
        } else {
            self.current_end -= 1;
            Some(self.array.value(self.current_end))
        }
    }
}

/// all arrays have known size.
impl<'a> std::iter::ExactSizeIterator for BooleanValueIter<'a> {}

/// an iterator that returns `Some(&str)` or `None`, for string arrays
#[derive(Debug)]
pub struct GenericStringValueIter<'a, T>
where
    T: StringOffsetSizeTrait,
{
    array: &'a GenericStringArray<T>,
    i: usize,
    len: usize,
}

impl<'a, T: StringOffsetSizeTrait> GenericStringValueIter<'a, T> {
    /// create a new iterator
    pub fn new(array: &'a GenericStringArray<T>) -> Self {
        GenericStringValueIter::<T> {
            array,
            i: 0,
            len: array.len(),
        }
    }
}

impl<'a, T: StringOffsetSizeTrait> std::iter::Iterator for GenericStringValueIter<'a, T> {
    type Item = &'a str;

    fn next(&mut self) -> Option<Self::Item> {
        let i = self.i;
        if i >= self.len {
            None
        } else {
            self.i += 1;
            Some(self.array.value(i))
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.len - self.i, Some(self.len - self.i))
    }
}

/// all arrays have known size.
impl<'a, T: StringOffsetSizeTrait> std::iter::ExactSizeIterator
    for GenericStringValueIter<'a, T>
{
}

/// an iterator that returns `Some(&[u8])` or `None`, for binary arrays
#[derive(Debug)]
pub struct GenericBinaryValueIter<'a, T>
where
    T: BinaryOffsetSizeTrait,
{
    array: &'a GenericBinaryArray<T>,
    i: usize,
    len: usize,
}

impl<'a, T: BinaryOffsetSizeTrait> GenericBinaryValueIter<'a, T> {
    /// create a new iterator
    pub fn new(array: &'a GenericBinaryArray<T>) -> Self {
        GenericBinaryValueIter::<T> {
            array,
            i: 0,
            len: array.len(),
        }
    }
}

impl<'a, T: BinaryOffsetSizeTrait> std::iter::Iterator for GenericBinaryValueIter<'a, T> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        let i = self.i;
        if i >= self.len {
            None
        } else {
            self.i += 1;
            Some(self.array.value(i))
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.len - self.i, Some(self.len - self.i))
    }
}

#[derive(Debug)]
pub struct GenericListArrayValueIter<'a, S>
where
    S: OffsetSizeTrait,
{
    array: &'a GenericListArray<S>,
    i: usize,
    len: usize,
}

impl<'a, S: OffsetSizeTrait> GenericListArrayValueIter<'a, S> {
    pub fn new(array: &'a GenericListArray<S>) -> Self {
        GenericListArrayValueIter::<S> {
            array,
            i: 0,
            len: array.len(),
        }
    }
}

impl<'a, S: OffsetSizeTrait> std::iter::Iterator for GenericListArrayValueIter<'a, S> {
    type Item = ArrayRef;

    fn next(&mut self) -> Option<Self::Item> {
        let i = self.i;
        if i >= self.len {
            None
        } else {
            self.i += 1;
            Some(self.array.value(i))
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.len - self.i, Some(self.len - self.i))
    }
}

/// all arrays have known size.
impl<'a, T: BinaryOffsetSizeTrait> std::iter::ExactSizeIterator
    for GenericBinaryValueIter<'a, T>
{
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::array::{
        ArrayRef, BinaryArray, BooleanArray, Int32Array, StringArray, TypedArrayRef,
    };

    #[test]
    fn test_primitive_value_array_iter_round_trip() {
        let array = Int32Array::from(vec![Some(0), None, Some(2), None, Some(4)]);
        let array = Arc::new(array) as ArrayRef;

        let array = array.as_any().downcast_ref::<Int32Array>().unwrap();

        // to and from iter, with a +1
        let result: Int32Array = array.iter_values().map(|e| e + 1).map(Some).collect();

        let expected = Int32Array::from(vec![1, 1, 3, 1, 5]);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_primitive_value_array_double_ended() {
        let array = Int32Array::from(vec![Some(0), None, Some(2), None, Some(4)]);
        let mut a = array.iter_values();
        assert_eq!(a.next(), Some(0));
        assert_eq!(a.next(), Some(i32::default()));
        assert_eq!(a.next_back(), Some(4));
        assert_eq!(a.next_back(), Some(0));
        assert_eq!(a.next_back(), Some(2));
        // the two sides have met: None is returned by both
        assert_eq!(a.next_back(), None);
        assert_eq!(a.next(), None);
    }

    #[test]
    fn test_string_array_iter_round_trip() {
        let array =
            StringArray::from(vec![Some("a"), None, Some("aaa"), None, Some("aaaaa")]);
        let array = Arc::new(array) as ArrayRef;

        let array = array.as_any().downcast_ref::<StringArray>().unwrap();

        // to and from iter, with a +1
        let result: StringArray = array
            .iter_values()
            .map(|e| {
                let mut a = e.to_string();
                a.push('b');
                Some(a)
            })
            .collect();

        let expected = StringArray::from(vec!["ab", "b", "aaab", "b", "aaaaab"]);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_binary_array_iter_round_trip() {
        let array = BinaryArray::from(vec![
            Some(b"a" as &[u8]),
            None,
            Some(b"aaa"),
            None,
            Some(b"aaaaa"),
        ]);

        // to and from iter
        let result: BinaryArray = array.iter_values().map(Some).collect();

        let expected = BinaryArray::from(vec![
            Some(b"a" as &[u8]),
            Some(b""),
            Some(b"aaa"),
            Some(b""),
            Some(b"aaaaa"),
        ]);

        assert_eq!(result, expected);
    }

    #[test]
    fn test_boolean_array_iter_approx_round_trip() {
        let array = BooleanArray::from(vec![Some(true), None, Some(false)]);

        // to and from iter
        let result: BooleanArray = array.iter_values().map(Some).collect();

        let expected = BooleanArray::from(vec![Some(true), Some(false), Some(false)]);

        assert_eq!(result, expected);
    }
}
