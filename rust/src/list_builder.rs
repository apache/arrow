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

use super::builder::*;
use super::list::List;

/// Builder for List<T>
pub struct ListBuilder<T> {
    data: Builder<T>,
    offsets: Builder<i32>,
}

impl<T> ListBuilder<T> {
    /// Create a ListBuilder with a default capacity
    pub fn new() -> Self {
        ListBuilder::with_capacity(64)
    }

    /// Create a ListBuilder with the specified capacity
    pub fn with_capacity(n: usize) -> Self {
        let data = Builder::with_capacity(n);
        let mut offsets = Builder::with_capacity(n);
        offsets.push(0_i32);
        ListBuilder { data, offsets }
    }

    /// Push one array slot to the builder
    pub fn push(&mut self, slice: &[T]) {
        self.data.push_slice(slice);
        self.offsets.push(self.data.len() as i32);
    }

    /// Create an immutable List<T> from the builder
    pub fn finish(&mut self) -> List<T> {
        List::from_raw_parts(self.data.finish(), self.offsets.finish())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_list_u8() {
        let mut b: ListBuilder<u8> = ListBuilder::new();
        b.push("Hello, ".as_bytes());
        b.push("World!".as_bytes());
        let buffer = b.finish();

        assert_eq!(2, buffer.len());
        assert_eq!("Hello, ".as_bytes(), buffer.slice(0));
        assert_eq!("World!".as_bytes(), buffer.slice(1));
    }
}
