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

use crate::datatypes::ArrowPrimitiveType;

use super::{
    Array, ArrayRef, BinaryOffsetSizeTrait, BooleanArray, GenericBinaryArray,
    GenericListArray, GenericStringArray, OffsetSizeTrait, PrimitiveArray,
    StringOffsetSizeTrait,
};

/// an iterator that returns Some(T) or None, that can be used on any PrimitiveArray
// Note: This implementation is based on std's [Vec]s' [IntoIter].
#[derive(Debug)]
pub struct PrimitiveIter<'a, T: ArrowPrimitiveType> {
    array: &'a PrimitiveArray<T>,
    current: usize,
    current_end: usize,
}

impl<'a, T: ArrowPrimitiveType> PrimitiveIter<'a, T> {
    /// create a new iterator
    pub fn new(array: &'a PrimitiveArray<T>) -> Self {
        PrimitiveIter::<T> {
            array,
            current: 0,
            current_end: array.len(),
        }
    }
}

impl<'a, T: ArrowPrimitiveType> std::iter::Iterator for PrimitiveIter<'a, T> {
    type Item = Option<T::Native>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.current == self.current_end {
            None
        } else if self.array.is_null(self.current) {
            self.current += 1;
            Some(None)
        } else {
            let old = self.current;
            self.current += 1;
            // Safety:
            // we just checked bounds in `self.current_end == self.current`
            // this is safe on the premise that this struct is initialized with
            // current = array.len()
            // and that current_end is ever only decremented
            unsafe { Some(Some(self.array.value_unchecked(old))) }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (
            self.array.len() - self.current,
            Some(self.array.len() - self.current),
        )
    }
}

impl<'a, T: ArrowPrimitiveType> std::iter::DoubleEndedIterator for PrimitiveIter<'a, T> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.current_end == self.current {
            None
        } else {
            self.current_end -= 1;
            Some(if self.array.is_null(self.current_end) {
                None
            } else {
                // Safety:
                // we just checked bounds in `self.current_end == self.current`
                // this is safe on the premise that this struct is initialized with
                // current = array.len()
                // and that current_end is ever only decremented
                unsafe { Some(self.array.value_unchecked(self.current_end)) }
            })
        }
    }
}

/// all arrays have known size.
impl<'a, T: ArrowPrimitiveType> std::iter::ExactSizeIterator for PrimitiveIter<'a, T> {}

/// an iterator that returns Some(bool) or None.
// Note: This implementation is based on std's [Vec]s' [IntoIter].
#[derive(Debug)]
pub struct BooleanIter<'a> {
    array: &'a BooleanArray,
    current: usize,
    current_end: usize,
}

impl<'a> BooleanIter<'a> {
    /// create a new iterator
    pub fn new(array: &'a BooleanArray) -> Self {
        BooleanIter {
            array,
            current: 0,
            current_end: array.len(),
        }
    }
}

impl<'a> std::iter::Iterator for BooleanIter<'a> {
    type Item = Option<bool>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current == self.current_end {
            None
        } else if self.array.is_null(self.current) {
            self.current += 1;
            Some(None)
        } else {
            let old = self.current;
            self.current += 1;
            // Safety:
            // we just checked bounds in `self.current_end == self.current`
            // this is safe on the premise that this struct is initialized with
            // current = array.len()
            // and that current_end is ever only decremented
            unsafe { Some(Some(self.array.value_unchecked(old))) }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (
            self.array.len() - self.current,
            Some(self.array.len() - self.current),
        )
    }
}

impl<'a> std::iter::DoubleEndedIterator for BooleanIter<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.current_end == self.current {
            None
        } else {
            self.current_end -= 1;
            Some(if self.array.is_null(self.current_end) {
                None
            } else {
                // Safety:
                // we just checked bounds in `self.current_end == self.current`
                // this is safe on the premise that this struct is initialized with
                // current = array.len()
                // and that current_end is ever only decremented
                unsafe { Some(self.array.value_unchecked(self.current_end)) }
            })
        }
    }
}

/// all arrays have known size.
impl<'a> std::iter::ExactSizeIterator for BooleanIter<'a> {}

/// an iterator that returns `Some(&str)` or `None`, for string arrays
#[derive(Debug)]
pub struct GenericStringIter<'a, T>
where
    T: StringOffsetSizeTrait,
{
    array: &'a GenericStringArray<T>,
    current: usize,
    current_end: usize,
}

impl<'a, T: StringOffsetSizeTrait> GenericStringIter<'a, T> {
    /// create a new iterator
    pub fn new(array: &'a GenericStringArray<T>) -> Self {
        GenericStringIter::<T> {
            array,
            current: 0,
            current_end: array.len(),
        }
    }
}

impl<'a, T: StringOffsetSizeTrait> std::iter::Iterator for GenericStringIter<'a, T> {
    type Item = Option<&'a str>;

    fn next(&mut self) -> Option<Self::Item> {
        let i = self.current;
        if i >= self.current_end {
            None
        } else if self.array.is_null(i) {
            self.current += 1;
            Some(None)
        } else {
            self.current += 1;
            // Safety:
            // we just checked bounds in `self.current_end == self.current`
            // this is safe on the premise that this struct is initialized with
            // current = array.len()
            // and that current_end is ever only decremented
            unsafe { Some(Some(self.array.value_unchecked(i))) }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (
            self.current_end - self.current,
            Some(self.current_end - self.current),
        )
    }
}

impl<'a, T: StringOffsetSizeTrait> std::iter::DoubleEndedIterator
    for GenericStringIter<'a, T>
{
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.current_end == self.current {
            None
        } else {
            self.current_end -= 1;
            Some(if self.array.is_null(self.current_end) {
                None
            } else {
                // Safety:
                // we just checked bounds in `self.current_end == self.current`
                // this is safe on the premise that this struct is initialized with
                // current = array.len()
                // and that current_end is ever only decremented
                unsafe { Some(self.array.value_unchecked(self.current_end)) }
            })
        }
    }
}

/// all arrays have known size.
impl<'a, T: StringOffsetSizeTrait> std::iter::ExactSizeIterator
    for GenericStringIter<'a, T>
{
}

/// an iterator that returns `Some(&[u8])` or `None`, for binary arrays
#[derive(Debug)]
pub struct GenericBinaryIter<'a, T>
where
    T: BinaryOffsetSizeTrait,
{
    array: &'a GenericBinaryArray<T>,
    current: usize,
    current_end: usize,
}

impl<'a, T: BinaryOffsetSizeTrait> GenericBinaryIter<'a, T> {
    /// create a new iterator
    pub fn new(array: &'a GenericBinaryArray<T>) -> Self {
        GenericBinaryIter::<T> {
            array,
            current: 0,
            current_end: array.len(),
        }
    }
}

impl<'a, T: BinaryOffsetSizeTrait> std::iter::Iterator for GenericBinaryIter<'a, T> {
    type Item = Option<&'a [u8]>;

    fn next(&mut self) -> Option<Self::Item> {
        let i = self.current;
        if i >= self.current_end {
            None
        } else if self.array.is_null(i) {
            self.current += 1;
            Some(None)
        } else {
            self.current += 1;
            // Safety:
            // we just checked bounds in `self.current_end == self.current`
            // this is safe on the premise that this struct is initialized with
            // current = array.len()
            // and that current_end is ever only decremented
            unsafe { Some(Some(self.array.value_unchecked(i))) }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (
            self.current_end - self.current,
            Some(self.current_end - self.current),
        )
    }
}

impl<'a, T: BinaryOffsetSizeTrait> std::iter::DoubleEndedIterator
    for GenericBinaryIter<'a, T>
{
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.current_end == self.current {
            None
        } else {
            self.current_end -= 1;
            Some(if self.array.is_null(self.current_end) {
                None
            } else {
                // Safety:
                // we just checked bounds in `self.current_end == self.current`
                // this is safe on the premise that this struct is initialized with
                // current = array.len()
                // and that current_end is ever only decremented
                unsafe { Some(self.array.value_unchecked(self.current_end)) }
            })
        }
    }
}

/// all arrays have known size.
impl<'a, T: BinaryOffsetSizeTrait> std::iter::ExactSizeIterator
    for GenericBinaryIter<'a, T>
{
}

#[derive(Debug)]
pub struct GenericListArrayIter<'a, S>
where
    S: OffsetSizeTrait,
{
    array: &'a GenericListArray<S>,
    current: usize,
    current_end: usize,
}

impl<'a, S: OffsetSizeTrait> GenericListArrayIter<'a, S> {
    pub fn new(array: &'a GenericListArray<S>) -> Self {
        GenericListArrayIter::<S> {
            array,
            current: 0,
            current_end: array.len(),
        }
    }
}

impl<'a, S: OffsetSizeTrait> std::iter::Iterator for GenericListArrayIter<'a, S> {
    type Item = Option<ArrayRef>;

    fn next(&mut self) -> Option<Self::Item> {
        let i = self.current;
        if i >= self.current_end {
            None
        } else if self.array.is_null(i) {
            self.current += 1;
            Some(None)
        } else {
            self.current += 1;
            // Safety:
            // we just checked bounds in `self.current_end == self.current`
            // this is safe on the premise that this struct is initialized with
            // current = array.len()
            // and that current_end is ever only decremented
            unsafe { Some(Some(self.array.value_unchecked(i))) }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (
            self.current_end - self.current,
            Some(self.current_end - self.current),
        )
    }
}

impl<'a, S: OffsetSizeTrait> std::iter::DoubleEndedIterator
    for GenericListArrayIter<'a, S>
{
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.current_end == self.current {
            None
        } else {
            self.current_end -= 1;
            Some(if self.array.is_null(self.current_end) {
                None
            } else {
                // Safety:
                // we just checked bounds in `self.current_end == self.current`
                // this is safe on the premise that this struct is initialized with
                // current = array.len()
                // and that current_end is ever only decremented
                unsafe { Some(self.array.value_unchecked(self.current_end)) }
            })
        }
    }
}

/// all arrays have known size.
impl<'a, S: OffsetSizeTrait> std::iter::ExactSizeIterator
    for GenericListArrayIter<'a, S>
{
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::array::{ArrayRef, BinaryArray, BooleanArray, Int32Array, StringArray};

    #[test]
    fn test_primitive_array_iter_round_trip() {
        let array = Int32Array::from(vec![Some(0), None, Some(2), None, Some(4)]);
        let array = Arc::new(array) as ArrayRef;

        let array = array.as_any().downcast_ref::<Int32Array>().unwrap();

        // to and from iter, with a +1
        let result: Int32Array = array.iter().map(|e| e.map(|e| e + 1)).collect();

        let expected = Int32Array::from(vec![Some(1), None, Some(3), None, Some(5)]);
        assert_eq!(result, expected);

        // check if DoubleEndedIterator is implemented
        let result: Int32Array = array.iter().rev().collect();
        let rev_array = Int32Array::from(vec![Some(4), None, Some(2), None, Some(0)]);
        assert_eq!(result, rev_array);
        // check if ExactSizeIterator is implemented
        let _ = array.iter().rposition(|opt_b| opt_b == Some(1));
    }

    #[test]
    fn test_double_ended() {
        let array = Int32Array::from(vec![Some(0), None, Some(2), None, Some(4)]);
        let mut a = array.iter();
        assert_eq!(a.next(), Some(Some(0)));
        assert_eq!(a.next(), Some(None));
        assert_eq!(a.next_back(), Some(Some(4)));
        assert_eq!(a.next_back(), Some(None));
        assert_eq!(a.next_back(), Some(Some(2)));
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
            .iter()
            .map(|e| {
                e.map(|e| {
                    let mut a = e.to_string();
                    a.push('b');
                    a
                })
            })
            .collect();

        let expected =
            StringArray::from(vec![Some("ab"), None, Some("aaab"), None, Some("aaaaab")]);
        assert_eq!(result, expected);

        // check if DoubleEndedIterator is implemented
        let result: StringArray = array.iter().rev().collect();
        let rev_array =
            StringArray::from(vec![Some("aaaaa"), None, Some("aaa"), None, Some("a")]);
        assert_eq!(result, rev_array);
        // check if ExactSizeIterator is implemented
        let _ = array.iter().rposition(|opt_b| opt_b == Some("a"));
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
        let result: BinaryArray = array.iter().collect();

        assert_eq!(result, array);

        // check if DoubleEndedIterator is implemented
        let result: BinaryArray = array.iter().rev().collect();
        let rev_array = BinaryArray::from(vec![
            Some(b"aaaaa" as &[u8]),
            None,
            Some(b"aaa"),
            None,
            Some(b"a"),
        ]);
        assert_eq!(result, rev_array);

        // check if ExactSizeIterator is implemented
        let _ = array.iter().rposition(|opt_b| opt_b == Some(&[9]));
    }

    #[test]
    fn test_boolean_array_iter_round_trip() {
        let array = BooleanArray::from(vec![Some(true), None, Some(false)]);

        // to and from iter
        let result: BooleanArray = array.iter().collect();

        assert_eq!(result, array);

        // check if DoubleEndedIterator is implemented
        let result: BooleanArray = array.iter().rev().collect();
        let rev_array = BooleanArray::from(vec![Some(false), None, Some(true)]);
        assert_eq!(result, rev_array);

        // check if ExactSizeIterator is implemented
        let _ = array.iter().rposition(|opt_b| opt_b == Some(true));
    }
}
