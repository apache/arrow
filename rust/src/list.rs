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

use std::str;

use super::buffer::Buffer;
use super::datatypes::*;
use super::list_builder::ListBuilder;

/// List<T> is a nested type in which each array slot contains a variable-size sequence of values of
/// the same type T
pub struct List<T>
where
    T: ArrowPrimitiveType,
{
    /// Contiguous region of memory holding contents of the lists
    data: Buffer<T>,
    /// offsets to start of each array slot
    offsets: Buffer<i32>,
}

impl<T> List<T>
where
    T: ArrowPrimitiveType,
{
    /// Create a List from raw parts
    pub fn from_raw_parts(data: Buffer<T>, offsets: Buffer<i32>) -> Self {
        List { data, offsets }
    }

    /// Get the length of the List (number of array slots)
    pub fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    /// Get a reference to the raw data in the list
    pub fn data(&self) -> &Buffer<T> {
        &self.data
    }

    /// Get a reference to the offsets in the list
    pub fn offsets(&self) -> &Buffer<i32> {
        &self.offsets
    }

    /// Get the contents of a single array slot
    pub fn get(&self, index: usize) -> &[T] {
        let start = *self.offsets.get(index) as usize;
        let end = *self.offsets.get(index + 1) as usize;
        self.data.slice(start, end)
    }
}

/// Create a List<u8> from a Vec<String>
impl From<Vec<String>> for List<u8> {
    fn from(v: Vec<String>) -> Self {
        let mut b: ListBuilder<u8> = ListBuilder::with_capacity(v.len());
        v.iter().for_each(|s| {
            b.push(s.as_bytes());
        });
        b.finish()
    }
}

/// Create a List<u8> from a Vec<&str>
impl From<Vec<&'static str>> for List<u8> {
    fn from(v: Vec<&'static str>) -> Self {
        List::from(v.iter().map(|s| s.to_string()).collect::<Vec<String>>())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_utf8_slices() {
        let list = List::from(vec!["this", "is", "a", "test"]);
        assert_eq!(4, list.len());
        assert_eq!("this", str::from_utf8(list.get(0)).unwrap());
        assert_eq!("is", str::from_utf8(list.get(1)).unwrap());
        assert_eq!("a", str::from_utf8(list.get(2)).unwrap());
        assert_eq!("test", str::from_utf8(list.get(3)).unwrap());
    }

    #[test]
    fn test_utf8_empty_strings() {
        let list = List::from(vec!["", "", "", ""]);
        assert_eq!(4, list.len());
        assert_eq!("", str::from_utf8(list.get(0)).unwrap());
        assert_eq!("", str::from_utf8(list.get(1)).unwrap());
        assert_eq!("", str::from_utf8(list.get(2)).unwrap());
        assert_eq!("", str::from_utf8(list.get(3)).unwrap());
    }

}
