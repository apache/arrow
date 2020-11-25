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

pub(crate) const AVX512_U8X64_LANES: usize = 64;

#[target_feature(enable = "avx512f")]
pub(crate) unsafe fn avx512_bin_and(left: &[u8], right: &[u8], res: &mut [u8]) {
    use core::arch::x86_64::{__m512i, _mm512_and_si512, _mm512_loadu_epi64};

    let l: __m512i = _mm512_loadu_epi64(left.as_ptr() as *const _);
    let r: __m512i = _mm512_loadu_epi64(right.as_ptr() as *const _);
    let f = _mm512_and_si512(l, r);
    let s = &f as *const __m512i as *const u8;
    let d = res.get_unchecked_mut(0) as *mut _ as *mut u8;
    std::ptr::copy_nonoverlapping(s, d, std::mem::size_of::<__m512i>());
}

#[target_feature(enable = "avx512f")]
pub(crate) unsafe fn avx512_bin_or(left: &[u8], right: &[u8], res: &mut [u8]) {
    use core::arch::x86_64::{__m512i, _mm512_loadu_epi64, _mm512_or_si512};

    let l: __m512i = _mm512_loadu_epi64(left.as_ptr() as *const _);
    let r: __m512i = _mm512_loadu_epi64(right.as_ptr() as *const _);
    let f = _mm512_or_si512(l, r);
    let s = &f as *const __m512i as *const u8;
    let d = res.get_unchecked_mut(0) as *mut _ as *mut u8;
    std::ptr::copy_nonoverlapping(s, d, std::mem::size_of::<__m512i>());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bitwise_and_avx512() {
        let buf1 = [0b00110011u8; 64];
        let buf2 = [0b11110000u8; 64];
        let mut buf3 = [0b00000000; 64];
        unsafe {
            avx512_bin_and(&buf1, &buf2, &mut buf3);
        };
        for i in buf3.iter() {
            assert_eq!(&0b00110000u8, i);
        }
    }

    #[test]
    fn test_bitwise_or_avx512() {
        let buf1 = [0b00010011u8; 64];
        let buf2 = [0b11100000u8; 64];
        let mut buf3 = [0b00000000; 64];
        unsafe {
            avx512_bin_or(&buf1, &buf2, &mut buf3);
        };
        for i in buf3.iter() {
            assert_eq!(&0b11110011u8, i);
        }
    }
}
