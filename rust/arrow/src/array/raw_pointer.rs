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

use crate::memory;

pub(super) struct RawPtrBox<T> {
    inner: *const T,
}

impl<T> RawPtrBox<T> {
    pub(super) fn new(inner: *const T) -> Self {
        Self { inner }
    }

    pub(super) fn get(&self) -> *const T {
        self.inner
    }
}

unsafe impl<T> Send for RawPtrBox<T> {}
unsafe impl<T> Sync for RawPtrBox<T> {}

pub(super) fn as_aligned_pointer<T>(p: *const u8) -> *const T {
    assert!(
        memory::is_aligned(p, std::mem::align_of::<T>()),
        "memory is not aligned"
    );
    p as *const T
}
