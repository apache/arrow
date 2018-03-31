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

use super::buffer::Buffer;

pub struct Bitmap {
    bits: Buffer<u8>
}

impl Bitmap {

    pub fn new(num_bits: usize) -> Self {
        let num_bytes = num_bits/8 + if num_bits%8 > 0 { 1 } else { 0 };
        let r = num_bytes % 64;
        let len = if r==0 { num_bytes } else { num_bytes + 64-r };
        let mut v = Vec::with_capacity(len);
        for _ in 0 .. len {
            v.push(255); // 1 is not null
        }
        Bitmap { bits: Buffer::from(v) }
    }

    pub fn len(&self) -> i32 {
        self.bits.len()
    }

    pub fn is_set(&self, i: usize) -> bool {
        let byte_offset = i / 8;
        self.bits.get(byte_offset) & (1_u8 << ((i % 8) as u8)) > 0
    }

    pub fn set(&mut self, i: usize) {
        let byte_offset = i / 8;
        let v : u8 = {
            self.bits.get(byte_offset) | (1_u8 << ((i % 8) as u8))
        };
        self.bits.set(byte_offset, v);
    }

    pub fn clear(&mut self, i: usize) {
        let byte_offset = i / 8;
        let v : u8 = self.bits.get(byte_offset) ^ (1_u8 << ((i % 8) as u8));
        self.bits.set(byte_offset, v);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bitmap_length() {
        assert_eq!(64, Bitmap::new(63*8).len());
        assert_eq!(64, Bitmap::new(64*8).len());
        assert_eq!(128, Bitmap::new(65*8).len());
    }

    #[test]
    fn test_set_clear_bit() {
        let mut b = Bitmap::new(64*8);
        assert_eq!(true, b.is_set(12));
        b.clear(12);
        assert_eq!(false, b.is_set(12));
        b.set(12);
        assert_eq!(true, b.is_set(12));
    }

}



