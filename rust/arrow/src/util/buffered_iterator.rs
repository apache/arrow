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

//! [Buffered] is an iterator useful to build an [arrow::array::Array] and other
//! containers that benefit from batching or chunking.

use std::marker::PhantomData;

/// An iterator that buffers results in a vector so that the iterator returns a vector of `size` items.
/// The items must be a [std::result::Result] and if an error is returned, tha error is returned
/// and the iterator continues.
/// An invariant of this iterator is that every returned vector's size is at most the specified size.
#[derive(Debug)]
pub struct Buffered<I, T, R>
where
    T: Clone,
    I: Iterator<Item = Result<T, R>>,
{
    iter: I,
    size: usize,
    buffer: Vec<T>,
    phantom: PhantomData<R>,
}

impl<I, T, R> Buffered<I, T, R>
where
    T: Clone,
    I: Iterator<Item = Result<T, R>>,
{
    pub fn new(iter: I, size: usize) -> Self {
        Buffered {
            iter,
            size,
            buffer: Vec::with_capacity(size),
            phantom: PhantomData,
        }
    }

    /// returns the number of items buffered so far.
    /// Useful to extract the exact item where an error occurred
    #[inline]
    pub fn n(&self) -> usize {
        return self.buffer.len();
    }
}

impl<I, T, R> Iterator for Buffered<I, T, R>
where
    T: Clone,
    I: Iterator<Item = Result<T, R>>,
{
    type Item = Result<Vec<T>, R>;

    fn next(&mut self) -> Option<Self::Item> {
        for _ in 0..(self.size - self.n()) {
            match self.iter.next() {
                Some(Ok(item)) => self.buffer.push(item),
                Some(Err(error)) => return Some(Err(error)),
                None => break,
            }
        }
        if self.buffer.is_empty() {
            None
        } else {
            let result = self.buffer.clone();
            self.buffer.clear();
            Some(Ok(result))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, PartialEq)]
    struct AError {}

    impl std::fmt::Display for AError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "Bla")
        }
    }
    impl std::error::Error for AError {}

    #[test]
    fn test_basic() {
        let a: Vec<Result<i32, AError>> = vec![Ok(1), Ok(2), Ok(3)];
        let iter = a.into_iter();
        let mut iter = Buffered::new(iter, 2);

        assert_eq!(iter.next(), Some(Ok(vec![1, 2])));
        assert_eq!(iter.next(), Some(Ok(vec![3])));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_error_first() {
        let a: Vec<Result<i32, AError>> =
            vec![Ok(1), Ok(2), Err(AError {}), Ok(4), Ok(5)];
        let iter = a.into_iter();
        let mut iter = Buffered::new(iter, 2);

        assert_eq!(iter.next(), Some(Ok(vec![1, 2])));
        assert_eq!(iter.next(), Some(Err(AError {})));
        // 4 is here: it was not skipped on the previous
        assert_eq!(iter.n(), 0);
        assert_eq!(iter.next(), Some(Ok(vec![4, 5])));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_error_last() {
        let a: Vec<Result<i32, AError>> = vec![Ok(1), Err(AError {}), Ok(3), Ok(4)];
        let iter = a.into_iter();
        let mut iter = Buffered::new(iter, 2);

        assert_eq!(iter.next(), Some(Err(AError {})));
        assert_eq!(iter.n(), 1);
        assert_eq!(iter.next(), Some(Ok(vec![1, 3])));
        assert_eq!(iter.next(), Some(Ok(vec![4])));
        assert_eq!(iter.next(), None);
    }
}
