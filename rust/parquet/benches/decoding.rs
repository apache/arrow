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

use parquet::{
  basic::*,
  data_type::*,
  decoding::*,
  encoding::*,
  memory::{ByteBufferPtr, MemTracker},
};

macro_rules! plain {
  ($fname:ident, $num_values:expr, $batch_size:expr, $ty:ident, $pty:expr,
   $gen_data_fn:expr) => {
    #[bench]
    fn $fname(bench: &mut Bencher) {
      let mem_tracker = Rc::new(MemTracker::new());
      let mut encoder =
        PlainEncoder::<$ty>::new(Rc::new(col_desc(0, $pty)), mem_tracker, vec![]);

      let (_, values) = $gen_data_fn($num_values);
      encoder.put(&values[..]).expect("put() should be OK");
      let buffer = encoder.flush_buffer().expect("flush_buffer() should be OK");

      let decoder = PlainDecoder::<$ty>::new(0);
      bench_decoding(bench, $num_values, $batch_size, buffer, Box::new(decoder));
    }
  };
}

macro_rules! dict {
  ($fname:ident, $num_values:expr, $batch_size:expr, $ty:ident, $pty:expr,
   $gen_data_fn:expr) => {
    #[bench]
    fn $fname(bench: &mut Bencher) {
      let mem_tracker = Rc::new(MemTracker::new());
      let mut encoder = DictEncoder::<$ty>::new(Rc::new(col_desc(0, $pty)), mem_tracker);

      let (_, values) = $gen_data_fn($num_values);
      encoder.put(&values[..]).expect("put() should be OK");
      let mut dict_decoder = PlainDecoder::<$ty>::new(0);
      dict_decoder
        .set_data(
          encoder.write_dict().expect("write_dict() should be OK"),
          encoder.num_entries(),
        )
        .expect("set_data() should be OK");

      let buffer = encoder.flush_buffer().expect("flush_buffer() should be OK");
      let mut decoder = DictDecoder::<$ty>::new();
      decoder
        .set_dict(Box::new(dict_decoder))
        .expect("set_dict() should be OK");

      bench_decoding(bench, $num_values, $batch_size, buffer, Box::new(decoder));
    }
  };
}

macro_rules! delta_bit_pack {
  ($fname:ident, $num_values:expr, $batch_size:expr, $ty:ident, $gen_data_fn:expr) => {
    #[bench]
    fn $fname(bench: &mut Bencher) {
      let mut encoder = DeltaBitPackEncoder::<$ty>::new();

      let (_, values) = $gen_data_fn($num_values);
      encoder.put(&values[..]).expect("put() should be OK");
      let buffer = encoder.flush_buffer().expect("flush_buffer() should be OK");

      let decoder = DeltaBitPackDecoder::<$ty>::new();
      bench_decoding(bench, $num_values, $batch_size, buffer, Box::new(decoder));
    }
  };
}

fn bench_decoding<T: DataType>(
  bench: &mut Bencher,
  num_values: usize,
  batch_size: usize,
  buffer: ByteBufferPtr,
  mut decoder: Box<Decoder<T>>,
)
{
  bench.bytes = buffer.len() as u64;
  bench.iter(|| {
    decoder
      .set_data(buffer.clone(), num_values)
      .expect("set_data() should be OK");
    let mut values = vec![T::T::default(); batch_size];
    loop {
      if decoder.get(&mut values[..]).expect("get() should be OK") < batch_size {
        break;
      }
    }
  })
}

plain!(plain_i32_1k_32, 1024, 32, Int32Type, Type::INT32, gen_1000);
plain!(plain_i32_1k_64, 1024, 64, Int32Type, Type::INT32, gen_1000);
plain!(
  plain_i32_1k_128,
  1024,
  128,
  Int32Type,
  Type::INT32,
  gen_1000
);
plain!(plain_i32_1m_32, 1024, 32, Int32Type, Type::INT32, gen_1000);
plain!(plain_i32_1m_64, 1024, 64, Int32Type, Type::INT32, gen_1000);
plain!(
  plain_i32_1m_128,
  1024,
  128,
  Int32Type,
  Type::INT32,
  gen_1000
);
plain!(
  plain_str_1m_128,
  1024,
  128,
  ByteArrayType,
  Type::BYTE_ARRAY,
  gen_test_strs
);

dict!(dict_i32_1k_32, 1024, 32, Int32Type, Type::INT32, gen_1000);
dict!(dict_i32_1k_64, 1024, 64, Int32Type, Type::INT32, gen_1000);
dict!(dict_i32_1k_128, 1024, 128, Int32Type, Type::INT32, gen_1000);
dict!(
  dict_i32_1m_32,
  1024 * 1024,
  32,
  Int32Type,
  Type::INT32,
  gen_1000
);
dict!(
  dict_i32_1m_64,
  1024 * 1024,
  64,
  Int32Type,
  Type::INT32,
  gen_1000
);
dict!(
  dict_i32_1m_128,
  1024 * 1024,
  128,
  Int32Type,
  Type::INT32,
  gen_1000
);
dict!(
  dict_str_1m_128,
  1024 * 1024,
  128,
  ByteArrayType,
  Type::BYTE_ARRAY,
  gen_test_strs
);

delta_bit_pack!(delta_bit_pack_i32_1k_32, 1024, 32, Int32Type, gen_1000);
delta_bit_pack!(delta_bit_pack_i32_1k_64, 1024, 64, Int32Type, gen_1000);
delta_bit_pack!(delta_bit_pack_i32_1k_128, 1024, 128, Int32Type, gen_1000);
delta_bit_pack!(
  delta_bit_pack_i32_1m_32,
  1024 * 1024,
  32,
  Int32Type,
  gen_1000
);
delta_bit_pack!(
  delta_bit_pack_i32_1m_64,
  1024 * 1024,
  64,
  Int32Type,
  gen_1000
);
delta_bit_pack!(
  delta_bit_pack_i32_1m_128,
  1024 * 1024,
  128,
  Int32Type,
  gen_1000
);
