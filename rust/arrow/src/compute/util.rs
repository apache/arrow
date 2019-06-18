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

//! Common utilities for computation kernels.

use crate::bitmap::Bitmap;
use crate::buffer::Buffer;
use crate::error::Result;

/// Applies a given binary operation, `op`, to two references to `Option<Bitmap>`'s.
///
/// This function is useful when implementing operations on higher level arrays.
pub(crate) fn apply_bin_op_to_option_bitmap<F>(
    left: &Option<Bitmap>,
    right: &Option<Bitmap>,
    op: F,
) -> Result<Option<Buffer>>
where
    F: Fn(&Buffer, &Buffer) -> Result<Buffer>,
{
    match *left {
        None => match *right {
            None => Ok(None),
            Some(ref r) => Ok(Some(r.bits.clone())),
        },
        Some(ref l) => match *right {
            None => Ok(Some(l.bits.clone())),
            Some(ref r) => Ok(Some(op(&l.bits, &r.bits)?)),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_apply_bin_op_to_option_bitmap() {
        assert_eq!(
            Ok(None),
            apply_bin_op_to_option_bitmap(&None, &None, |a, b| a & b)
        );
        assert_eq!(
            Ok(Some(Buffer::from([0b01101010]))),
            apply_bin_op_to_option_bitmap(
                &Some(Bitmap::from(Buffer::from([0b01101010]))),
                &None,
                |a, b| a & b
            )
        );
        assert_eq!(
            Ok(Some(Buffer::from([0b01001110]))),
            apply_bin_op_to_option_bitmap(
                &None,
                &Some(Bitmap::from(Buffer::from([0b01001110]))),
                |a, b| a & b
            )
        );
        assert_eq!(
            Ok(Some(Buffer::from([0b01001010]))),
            apply_bin_op_to_option_bitmap(
                &Some(Bitmap::from(Buffer::from([0b01101010]))),
                &Some(Bitmap::from(Buffer::from([0b01001110]))),
                |a, b| a & b
            )
        );
    }

}
