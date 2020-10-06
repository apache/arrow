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
use std::io::{self, Error, ErrorKind, Read, Seek, SeekFrom};
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

  /// Create a slice cursor using the same data as a current one.
  pub fn slice(&self, start: u64, length: usize) -> io::Result<Self> {
    let new_start = self.start + start;
    if new_start >= self.inner.len() as u64
      || new_start as usize + length > self.inner.len()
    {
      return Err(Error::new(ErrorKind::InvalidInput, "out of bound"));
    }
    Ok(SliceableCursor {
      inner: Rc::clone(&self.inner),
      start: new_start,
      pos: new_start,
      length: length,
    })
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
impl Seek for SliceableCursor {
  fn seek(&mut self, style: SeekFrom) -> io::Result<u64> {
    // base_pos the pos in the original Vec
    let (base_pos, offset) = match style {
      SeekFrom::Start(n) => {
        self.pos = self.start + n;
        return Ok(n);
      }
      SeekFrom::End(n) => (self.start + self.length as u64, n),
      SeekFrom::Current(n) => (self.pos, n),
    };
    let new_pos = if offset >= 0 {
      base_pos.checked_add(offset as u64)
    } else {
      base_pos.checked_sub((offset.wrapping_neg()) as u64)
    };
    match new_pos {
      Some(n) => {
        self.pos = n;
        Ok(self.pos - self.start)
      }
      None => Err(Error::new(
        ErrorKind::InvalidInput,
        "invalid seek to a negative or overflowing position",
      )),
    }
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
    let cursor = get_u8_range().slice(10, 10).expect("error while slicing");
    check_read_all(cursor, 10, 19);
  }

  #[test]
  fn seek_from_start_whole() {
    let mut cursor = get_u8_range();
    let seek_result = cursor.seek(SeekFrom::Start(100));
    assert!(!seek_result.is_err(), "seek error");
    assert_eq!(100, seek_result.unwrap());
    check_read_all(cursor, 100, 255);
  }

  #[test]
  fn seek_from_end_whole() {
    let mut cursor = get_u8_range();
    let seek_result = cursor.seek(SeekFrom::End(-100));
    assert!(!seek_result.is_err(), "seek error");
    assert_eq!(156, seek_result.unwrap());
    check_read_all(cursor, 156, 255);
  }

  #[test]
  fn seek_from_current_whole() {
    let mut cursor = get_u8_range();
    let seek_result = cursor.seek(SeekFrom::Current(110));
    assert!(!seek_result.is_err(), "seek error");
    assert_eq!(110, seek_result.unwrap());
    let seek_result = cursor.seek(SeekFrom::Current(-10));
    assert!(!seek_result.is_err(), "seek error");
    assert_eq!(100, seek_result.unwrap());
    check_read_all(cursor, 100, 255);
  }

  #[test]
  fn seek_from_start_slice() {
    let mut cursor = get_u8_range().slice(100, 100).expect("error while slicing");
    let seek_result = cursor.seek(SeekFrom::Start(25));
    assert!(!seek_result.is_err(), "seek error");
    assert_eq!(25, seek_result.unwrap());
    check_read_all(cursor, 125, 199);
  }

  #[test]
  fn seek_from_end_slice() {
    let mut cursor = get_u8_range().slice(100, 100).expect("error while slicing");
    let seek_result = cursor.seek(SeekFrom::End(-25));
    assert!(!seek_result.is_err(), "seek error");
    assert_eq!(75, seek_result.unwrap());
    check_read_all(cursor, 175, 199);
  }

  #[test]
  fn seek_from_current_slice() {
    let mut cursor = get_u8_range().slice(100, 100).expect("error while slicing");
    let seek_result = cursor.seek(SeekFrom::Current(80));
    assert!(!seek_result.is_err(), "seek error");
    assert_eq!(80, seek_result.unwrap());
    let seek_result = cursor.seek(SeekFrom::Current(-20));
    assert!(!seek_result.is_err(), "seek error");
    assert_eq!(60, seek_result.unwrap());
    check_read_all(cursor, 160, 199);
  }
}
