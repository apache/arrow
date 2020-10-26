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

use std::cmp;
use std::io::{self, Read};
use std::rc::Rc;

/// This is object to use if your file is already in memory.
/// The sliceable cursor is similar to std::io::Cursor, except that it makes it easy to create "cursor slices".
/// To achieve this, it uses Rc instead of shared references. Indeed reference fields are painfull
/// because the lack of Generic Associated Type implies that you would require complex lifetime propagation when
/// returning such a cursor.
pub struct SliceableCursor {
    inner: Rc<Vec<u8>>,
    start: u64,
    length: usize,
    pos: u64,
}

impl SliceableCursor {
    pub fn new(content: Vec<u8>) -> Self {
        let size = content.len();
        SliceableCursor {
            inner: Rc::new(content),
            start: 0,
            pos: 0,
            length: size,
        }
    }

    /// Create a slice cursor backed by the same data as a current one.
    /// If the slice length is larger than the remaining bytes in the source, the slice is clamped.
    /// Panics if start is larger than the vector size.
    pub fn slice(&self, start: u64, length: usize) -> Self {
        if start > self.length as u64 {
            panic!("Slice start larger than cursor");
        }
        let absolute_start = self.start + start;
        let clamped_length = std::cmp::min(length, self.length - start as usize);
        SliceableCursor {
            inner: Rc::clone(&self.inner),
            start: absolute_start,
            pos: absolute_start,
            length: clamped_length,
        }
    }

    fn remaining_slice(&self) -> &[u8] {
        let end = self.start as usize + self.length;
        let offset = cmp::min(self.pos, end as u64) as usize;
        &self.inner[offset..end]
    }

    /// Get the length of the current cursor slice
    pub fn len(&self) -> u64 {
        self.length as u64
    }
}

/// Implementation inspired by std::io::Cursor
impl Read for SliceableCursor {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = Read::read(&mut self.remaining_slice(), buf)?;
        self.pos += n as u64;
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Create a SliceableCursor of all u8 values in ascending order
    fn get_u8_range() -> SliceableCursor {
        let data: Vec<u8> = (0u8..=255).collect();
        SliceableCursor::new(data)
    }

    /// Reads all the bytes in the slice and checks that it matches the u8 range from start to end_included
    fn check_read_all(mut cursor: SliceableCursor, start: u8, end_included: u8) {
        let mut target = vec![];
        let cursor_res = cursor.read_to_end(&mut target);
        println!("{:?}", cursor_res);
        assert!(!cursor_res.is_err(), "reading error");
        assert_eq!((end_included - start) as usize + 1, cursor_res.unwrap());
        assert_eq!((start..=end_included).collect::<Vec<_>>(), target);
    }

    #[test]
    fn read_all_whole() {
        let cursor = get_u8_range();
        check_read_all(cursor, 0, 255);
    }

    #[test]
    fn read_all_slice() {
        let cursor = get_u8_range().slice(10, 10);
        check_read_all(cursor, 10, 19);
    }

    #[test]
    fn read_all_clipped_slice() {
        let cursor = get_u8_range().slice(250, 10);
        check_read_all(cursor, 250, 255);
    }

    #[test]
    fn chaining_slices() {
        let cursor = get_u8_range().slice(200, 50).slice(10, 10);
        check_read_all(cursor, 210, 219);
    }
}
