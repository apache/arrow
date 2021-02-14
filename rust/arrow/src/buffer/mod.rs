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

//! This module contains two main structs: [Buffer] and [MutableBuffer]. A buffer represents
//! a contiguous memory region that can be shared via `offsets`.

mod immutable;
pub use immutable::*;
mod mutable;
pub use mutable::*;
mod ops;
pub(super) use ops::*;

use crate::error::{ArrowError, Result};
use std::ops::{BitAnd, BitOr, Not};

impl<'a, 'b> BitAnd<&'b Buffer> for &'a Buffer {
    type Output = Result<Buffer>;

    fn bitand(self, rhs: &'b Buffer) -> Result<Buffer> {
        if self.len() != rhs.len() {
            return Err(ArrowError::ComputeError(
                "Buffers must be the same size to apply Bitwise AND.".to_string(),
            ));
        }

        let len_in_bits = self.len() * 8;
        Ok(buffer_bin_and(&self, 0, &rhs, 0, len_in_bits))
    }
}

impl<'a, 'b> BitOr<&'b Buffer> for &'a Buffer {
    type Output = Result<Buffer>;

    fn bitor(self, rhs: &'b Buffer) -> Result<Buffer> {
        if self.len() != rhs.len() {
            return Err(ArrowError::ComputeError(
                "Buffers must be the same size to apply Bitwise OR.".to_string(),
            ));
        }

        let len_in_bits = self.len() * 8;

        Ok(buffer_bin_or(&self, 0, &rhs, 0, len_in_bits))
    }
}

impl Not for &Buffer {
    type Output = Buffer;

    fn not(self) -> Buffer {
        let len_in_bits = self.len() * 8;
        buffer_unary_not(&self, 0, len_in_bits)
    }
}
