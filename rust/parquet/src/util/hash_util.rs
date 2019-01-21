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

use std::ptr;

use crate::data_type::AsBytes;

/// Computes hash value for `data`, with a seed value `seed`.
/// The data type `T` must implement the `AsBytes` trait.
pub fn hash<T: AsBytes>(data: &T, seed: u32) -> u32 {
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    {
        if is_x86_feature_detected!("sse4.2") {
            unsafe { crc32_hash(data, seed) }
        } else {
            murmur_hash2_64a(data, seed as u64) as u32
        }
    }
}

const MURMUR_PRIME: u64 = 0xc6a4a7935bd1e995;
const MURMUR_R: i32 = 47;

/// Rust implementation of MurmurHash2, 64-bit version for 64-bit platforms
fn murmur_hash2_64a<T: AsBytes>(data: &T, seed: u64) -> u64 {
    let data_bytes = data.as_bytes();
    let len = data_bytes.len();
    let len_64 = (len / 8) * 8;
    #[allow(clippy::cast_ptr_alignment)]
    let data_bytes_64 = data_bytes.as_ptr() as *const u64;

    let mut h = seed ^ (MURMUR_PRIME.wrapping_mul(data_bytes.len() as u64));
    for offset in (0..len_64).step_by(8) {
        let mut k = unsafe { ptr::read_unaligned(data_bytes_64.add(offset / 8)) };
        k = k.wrapping_mul(MURMUR_PRIME);
        k ^= k >> MURMUR_R;
        k = k.wrapping_mul(MURMUR_PRIME);
        h ^= k;
        h = h.wrapping_mul(MURMUR_PRIME);
    }

    let data2 = &data_bytes[len_64..];

    let v = len & 7;
    if v == 7 {
        h ^= (data2[6] as u64) << 48;
    }
    if v >= 6 {
        h ^= (data2[5] as u64) << 40;
    }
    if v >= 5 {
        h ^= (data2[4] as u64) << 32;
    }
    if v >= 4 {
        h ^= (data2[3] as u64) << 24;
    }
    if v >= 3 {
        h ^= (data2[2] as u64) << 16;
    }
    if v >= 2 {
        h ^= (data2[1] as u64) << 8;
    }
    if v >= 1 {
        h ^= data2[0] as u64;
    }
    if v > 0 {
        h = h.wrapping_mul(MURMUR_PRIME);
    }

    h ^= h >> MURMUR_R;
    h = h.wrapping_mul(MURMUR_PRIME);
    h ^= h >> MURMUR_R;
    h
}

/// CRC32 hash implementation using SSE4 instructions. Borrowed from Impala.
#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "sse4.2")]
unsafe fn crc32_hash<T: AsBytes>(data: &T, seed: u32) -> u32 {
    #[cfg(target_arch = "x86")]
    use std::arch::x86::*;
    #[cfg(target_arch = "x86_64")]
    use std::arch::x86_64::*;

    let bytes: &[u8] = data.as_bytes();
    let u32_num_bytes = ::std::mem::size_of::<u32>();
    let mut num_bytes = bytes.len();
    let num_words = num_bytes / u32_num_bytes;
    num_bytes %= u32_num_bytes;

    #[allow(clippy::cast_ptr_alignment)]
    let bytes_u32 = bytes.as_ptr() as *const u32;

    let mut offset = 0;
    let mut hash = seed;
    while offset < num_words * u32_num_bytes {
        hash = _mm_crc32_u32(
            hash,
            ptr::read_unaligned(bytes_u32.add(offset / u32_num_bytes)),
        );
        offset += u32_num_bytes;
    }

    while offset < num_bytes {
        hash = _mm_crc32_u8(hash, bytes[offset]);
        offset += 1;
    }

    // The lower half of the CRC hash has poor uniformity, so swap the halves
    // for anyone who only uses the first several bits of the hash.
    hash = (hash << 16) | (hash >> 16);
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_murmur2_64a() {
        let result = murmur_hash2_64a(&"hello", 123);
        assert_eq!(result, 2597646618390559622);

        let result = murmur_hash2_64a(&"helloworld", 123);
        assert_eq!(result, 4934371746140206573);

        let result = murmur_hash2_64a(&"helloworldparquet", 123);
        assert_eq!(result, 2392198230801491746);
    }

    #[test]
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    fn test_crc32() {
        if is_x86_feature_detected!("sse4.2") {
            unsafe {
                let result = crc32_hash(&"hello", 123);
                assert_eq!(result, 2927487359);

                let result = crc32_hash(&"helloworld", 123);
                assert_eq!(result, 314229527);

                let result = crc32_hash(&"helloworldparquet", 123);
                assert_eq!(result, 667078870);
            }
        }
    }
}
