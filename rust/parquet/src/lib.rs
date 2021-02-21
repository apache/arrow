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

#![allow(incomplete_features)]
#![allow(dead_code)]
#![allow(non_camel_case_types)]
#![allow(bare_trait_objects)]
#![allow(
    clippy::approx_constant,
    clippy::borrowed_box,
    clippy::cast_ptr_alignment,
    clippy::comparison_chain,
    clippy::float_cmp,
    clippy::float_equality_without_abs,
    clippy::many_single_char_names,
    clippy::needless_range_loop,
    clippy::new_without_default,
    clippy::or_fun_call,
    clippy::same_item_push,
    clippy::too_many_arguments,
    clippy::transmute_ptr_to_ptr
)]

#[macro_use]
pub mod errors;
pub mod basic;
#[macro_use]
pub mod data_type;

// Exported for external use, such as benchmarks
pub use self::encodings::{decoding, encoding};
pub use self::util::memory;

#[macro_use]
mod util;
#[cfg(any(feature = "arrow", test))]
pub mod arrow;
pub mod column;
pub mod compression;
mod encodings;
pub mod file;
pub mod record;
pub mod schema;
