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

use super::{Array, PrimitiveArray};

/// an iterator that returns Some(T) or None, that can be used on any non-boolean PrimitiveArray
#[derive(Debug)]
pub struct PrimitiveIter<'a, T: ArrowPrimitiveType> {
    array: &'a PrimitiveArray<T>,
    i: usize,
    len: usize,
}

impl<'a, T: ArrowPrimitiveType> PrimitiveIter<'a, T> {
    /// create a new iterator
    pub fn new(array: &'a PrimitiveArray<T>) -> Self {
        PrimitiveIter::<T> {
            array,
            i: 0,
            len: array.len(),
        }
    }
}

impl<'a, T: ArrowPrimitiveType> std::iter::Iterator for PrimitiveIter<'a, T> {
    type Item = Option<T::Native>;

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
impl<'a, T: ArrowPrimitiveType> std::iter::ExactSizeIterator for PrimitiveIter<'a, T> {}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::array::{ArrayRef, Int32Array};

    #[test]
    fn test_primitive_array_iter_round_trip() {
        let array = Int32Array::from(vec![Some(0), None, Some(2), None, Some(4)]);
        let array = Arc::new(array) as ArrayRef;

        let array = array.as_any().downcast_ref::<Int32Array>().unwrap();

        // to and from iter, with a +1
        let result: Int32Array =
            array.iter().map(|e| e.and_then(|e| Some(e + 1))).collect();

        let expected = Int32Array::from(vec![Some(1), None, Some(3), None, Some(5)]);
        assert_eq!(result, expected);
    }
}
