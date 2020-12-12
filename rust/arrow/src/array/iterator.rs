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
    Array, BinaryOffsetSizeTrait, BooleanArray, GenericBinaryArray, GenericStringArray,
    PrimitiveArray, StringOffsetSizeTrait,
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

    fn next(&mut self) -> Option<Self::Item> {
        if self.current == self.current_end {
            None
        } else if self.array.is_null(self.current) {
            self.current += 1;
            Some(None)
        } else {
            let old = self.current;
            self.current += 1;
            Some(Some(self.array.value(old)))
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.array.len(), Some(self.array.len()))
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
                Some(self.array.value(self.current_end))
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
            Some(Some(self.array.value(old)))
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.array.len(), Some(self.array.len()))
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
                Some(self.array.value(self.current_end))
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
    i: usize,
    len: usize,
}

impl<'a, T: StringOffsetSizeTrait> GenericStringIter<'a, T> {
    /// create a new iterator
    pub fn new(array: &'a GenericStringArray<T>) -> Self {
        GenericStringIter::<T> {
            array,
            i: 0,
            len: array.len(),
        }
    }
}

impl<'a, T: StringOffsetSizeTrait> std::iter::Iterator for GenericStringIter<'a, T> {
    type Item = Option<&'a str>;

    fn next(&mut self) -> Option<Self::Item> {
        let i = self.i;
        if i >= self.len {
            None
        } else if self.array.is_null(i) {
            self.i += 1;
            Some(None)
        } else {
            self.i += 1;
            Some(Some(self.array.value(i)))
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.len, Some(self.len))
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
    i: usize,
    len: usize,
}

impl<'a, T: BinaryOffsetSizeTrait> GenericBinaryIter<'a, T> {
    /// create a new iterator
    pub fn new(array: &'a GenericBinaryArray<T>) -> Self {
        GenericBinaryIter::<T> {
            array,
            i: 0,
            len: array.len(),
        }
    }
}

impl<'a, T: BinaryOffsetSizeTrait> std::iter::Iterator for GenericBinaryIter<'a, T> {
    type Item = Option<&'a [u8]>;

    fn next(&mut self) -> Option<Self::Item> {
        let i = self.i;
        if i >= self.len {
            None
        } else if self.array.is_null(i) {
            self.i += 1;
            Some(None)
        } else {
            self.i += 1;
            Some(Some(self.array.value(i)))
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.len, Some(self.len))
    }
}

/// all arrays have known size.
impl<'a, T: BinaryOffsetSizeTrait> std::iter::ExactSizeIterator
    for GenericBinaryIter<'a, T>
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
    }

    #[test]
    fn test_boolean_array_iter_round_trip() {
        let array = BooleanArray::from(vec![Some(true), None, Some(false)]);

        // to and from iter
        let result: BooleanArray = array.iter().collect();

        assert_eq!(result, array);
    }
}
