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

#![feature(test)]
extern crate parquet;
extern crate rand;
extern crate test;
use test::Bencher;

#[allow(dead_code)]
#[path = "common.rs"]
mod common;
use common::*;

use std::rc::Rc;

use parquet::{basic::*, data_type::*, encoding::*, memory::MemTracker};

macro_rules! plain {
  ($fname:ident, $batch_size:expr, $ty:ident, $pty:expr, $gen_data_fn:expr) => {
    #[bench]
    fn $fname(bench: &mut Bencher) {
      let mem_tracker = Rc::new(MemTracker::new());
      let encoder =
        PlainEncoder::<$ty>::new(Rc::new(col_desc(0, $pty)), mem_tracker, vec![]);
      let (bytes, values) = $gen_data_fn($batch_size);
      bench_encoding(bench, bytes, values, Box::new(encoder));
    }
  };
}

macro_rules! dict {
  ($fname:ident, $batch_size:expr, $ty:ident, $pty:expr, $gen_data_fn:expr) => {
    #[bench]
    fn $fname(bench: &mut Bencher) {
      let mem_tracker = Rc::new(MemTracker::new());
      let encoder = DictEncoder::<$ty>::new(Rc::new(col_desc(0, $pty)), mem_tracker);
      let (bytes, values) = $gen_data_fn($batch_size);
      bench_encoding(bench, bytes, values, Box::new(encoder));
    }
  };
}

macro_rules! delta_bit_pack {
  ($fname:ident, $batch_size:expr, $ty:ident, $gen_data_fn:expr) => {
    #[bench]
    fn $fname(bench: &mut Bencher) {
      let encoder = DeltaBitPackEncoder::<$ty>::new();
      let (bytes, values) = $gen_data_fn($batch_size);
      bench_encoding(bench, bytes, values, Box::new(encoder));
    }
  };
}

fn bench_encoding<T: DataType>(
  bench: &mut Bencher,
  bytes: usize,
  values: Vec<T::T>,
  mut encoder: Box<Encoder<T>>,
)
{
  bench.bytes = bytes as u64;
  bench.iter(|| {
    encoder.put(&values[..]).expect("put() should be OK");
    encoder.flush_buffer().expect("flush_buffer() should be OK");
  })
}

plain!(plain_i32_1k_10, 1024, Int32Type, Type::INT32, gen_10);
plain!(plain_i32_1k_100, 1024, Int32Type, Type::INT32, gen_100);
plain!(plain_i32_1k_1000, 1024, Int32Type, Type::INT32, gen_1000);
plain!(plain_i32_1m_10, 1024 * 1024, Int32Type, Type::INT32, gen_10);
plain!(
  plain_i32_1m_100,
  1024 * 1024,
  Int32Type,
  Type::INT32,
  gen_100
);
plain!(
  plain_i32_1m_1000,
  1024 * 1024,
  Int32Type,
  Type::INT32,
  gen_1000
);
plain!(
  plain_str_1m,
  1024 * 1024,
  ByteArrayType,
  Type::BYTE_ARRAY,
  gen_test_strs
);

dict!(dict_i32_1k_10, 1024, Int32Type, Type::INT32, gen_10);
dict!(dict_i32_1k_100, 1024, Int32Type, Type::INT32, gen_100);
dict!(dict_i32_1k_1000, 1024, Int32Type, Type::INT32, gen_1000);
dict!(dict_i32_1m_10, 1024 * 1024, Int32Type, Type::INT32, gen_10);
dict!(
  dict_i32_1m_100,
  1024 * 1024,
  Int32Type,
  Type::INT32,
  gen_100
);
dict!(
  dict_i32_1m_1000,
  1024 * 1024,
  Int32Type,
  Type::INT32,
  gen_1000
);
plain!(
  dict_str_1m,
  1024 * 1024,
  ByteArrayType,
  Type::BYTE_ARRAY,
  gen_test_strs
);

delta_bit_pack!(delta_bit_pack_i32_1k_10, 1024, Int32Type, gen_10);
delta_bit_pack!(delta_bit_pack_i32_1k_100, 1024, Int32Type, gen_100);
delta_bit_pack!(delta_bit_pack_i32_1k_1000, 1024, Int32Type, gen_1000);
delta_bit_pack!(delta_bit_pack_i32_1m_10, 1024 * 1024, Int32Type, gen_10);
delta_bit_pack!(delta_bit_pack_i32_1m_100, 1024 * 1024, Int32Type, gen_100);
delta_bit_pack!(delta_bit_pack_i32_1m_1000, 1024 * 1024, Int32Type, gen_1000);
