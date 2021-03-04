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

use super::bit_util;
use crate::{
    buffer::{Buffer, MutableBuffer},
    datatypes::ArrowNativeType,
};

/// Creates two [`Buffer`]s from an iterator of `Option`.
/// The first buffer corresponds to a bitmap buffer, the second one
/// corresponds to a values buffer.
/// # Safety
/// The caller must ensure that `iterator` is `TrustedLen`.
#[inline]
pub(crate) unsafe fn trusted_len_unzip<I, P, T>(iterator: I) -> (Buffer, Buffer)
where
    T: ArrowNativeType,
    P: std::borrow::Borrow<Option<T>>,
    I: Iterator<Item = P>,
{
    let (_, upper) = iterator.size_hint();
    let upper = upper.expect("trusted_len_unzip requires an upper limit");
    let len = upper * std::mem::size_of::<T>();

    let mut null = MutableBuffer::from_len_zeroed(upper.saturating_add(7) / 8);
    let mut buffer = MutableBuffer::new(len);

    let dst_null = null.as_mut_ptr();
    let mut dst = buffer.as_mut_ptr() as *mut T;
    for (i, item) in iterator.enumerate() {
        let item = item.borrow();
        if let Some(item) = item {
            std::ptr::write(dst, *item);
            bit_util::set_bit_raw(dst_null, i);
        } else {
            std::ptr::write(dst, T::default());
        }
        dst = dst.add(1);
    }
    assert_eq!(
        dst.offset_from(buffer.as_ptr() as *mut T) as usize,
        upper,
        "Trusted iterator length was not accurately reported"
    );
    buffer.set_len(len);
    (null.into(), buffer.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trusted_len_unzip_good() {
        let vec = vec![Some(1u32), None];
        let (null, buffer) = unsafe { trusted_len_unzip(vec.iter()) };
        assert_eq!(null.as_slice(), &[0b00000001]);
        assert_eq!(buffer.as_slice(), &[1u8, 0, 0, 0, 0, 0, 0, 0]);
    }

    #[test]
    #[should_panic(expected = "trusted_len_unzip requires an upper limit")]
    fn trusted_len_unzip_panic() {
        let iter = std::iter::repeat(Some(4i32));
        unsafe { trusted_len_unzip(iter) };
    }
}
