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
#[macro_use]
extern crate lazy_static;
extern crate test;
use test::Bencher;

use std::{env, fs::File};

use parquet::{basic::Compression, compression::*, file::reader::*};

// 10k rows written in page v2 with type:
//
//   message test {
//     required binary binary_field,
//     required int32 int32_field,
//     required int64 int64_field,
//     required boolean boolean_field,
//     required float float_field,
//     required double double_field,
//     required fixed_len_byte_array(1024) flba_field,
//     required int96 int96_field
//   }
//
// filled with random values.
const TEST_FILE: &str = "10k-v2.parquet";

fn get_rg_reader() -> Box<RowGroupReader> {
  let mut path_buf = env::current_dir().unwrap();
  path_buf.push("data");
  path_buf.push(TEST_FILE);
  let file = File::open(path_buf.as_path()).unwrap();
  let f_reader = SerializedFileReader::new(file).unwrap();
  f_reader.get_row_group(0).unwrap()
}

fn get_pages_bytes(col_idx: usize) -> Vec<u8> {
  let mut data: Vec<u8> = Vec::new();
  let rg_reader = get_rg_reader();
  let mut pg_reader = rg_reader.get_column_page_reader(col_idx).unwrap();
  loop {
    if let Some(p) = pg_reader.get_next_page().unwrap() {
      data.extend_from_slice(p.buffer().data());
    } else {
      break;
    }
  }
  data
}

macro_rules! compress {
  ($fname:ident, $codec:expr, $col_idx:expr) => {
    #[bench]
    fn $fname(bench: &mut Bencher) {
      lazy_static! {
        static ref DATA: Vec<u8> = { get_pages_bytes($col_idx) };
      }

      let mut codec = create_codec($codec).unwrap().unwrap();
      let mut v = vec![];
      bench.bytes = DATA.len() as u64;
      bench.iter(|| {
        codec.compress(&DATA[..], &mut v).unwrap();
      })
    }
  };
}

macro_rules! decompress {
  ($fname:ident, $codec:expr, $col_idx:expr) => {
    #[bench]
    fn $fname(bench: &mut Bencher) {
      lazy_static! {
        static ref COMPRESSED_PAGES: Vec<u8> = {
          let mut codec = create_codec($codec).unwrap().unwrap();
          let raw_data = get_pages_bytes($col_idx);
          let mut v = vec![];
          codec.compress(&raw_data[..], &mut v).unwrap();
          v
        };
      }

      let mut codec = create_codec($codec).unwrap().unwrap();
      let rg_reader = get_rg_reader();
      bench.bytes = rg_reader.metadata().total_byte_size() as u64;
      bench.iter(|| {
        let mut v = Vec::new();
        let _ = codec.decompress(&COMPRESSED_PAGES[..], &mut v).unwrap();
      })
    }
  };
}

compress!(compress_brotli_binary, Compression::BROTLI, 0);
compress!(compress_brotli_int32, Compression::BROTLI, 1);
compress!(compress_brotli_int64, Compression::BROTLI, 2);
compress!(compress_brotli_boolean, Compression::BROTLI, 3);
compress!(compress_brotli_float, Compression::BROTLI, 4);
compress!(compress_brotli_double, Compression::BROTLI, 5);
compress!(compress_brotli_fixed, Compression::BROTLI, 6);
compress!(compress_brotli_int96, Compression::BROTLI, 7);

compress!(compress_gzip_binary, Compression::GZIP, 0);
compress!(compress_gzip_int32, Compression::GZIP, 1);
compress!(compress_gzip_int64, Compression::GZIP, 2);
compress!(compress_gzip_boolean, Compression::GZIP, 3);
compress!(compress_gzip_float, Compression::GZIP, 4);
compress!(compress_gzip_double, Compression::GZIP, 5);
compress!(compress_gzip_fixed, Compression::GZIP, 6);
compress!(compress_gzip_int96, Compression::GZIP, 7);

compress!(compress_snappy_binary, Compression::SNAPPY, 0);
compress!(compress_snappy_int32, Compression::SNAPPY, 1);
compress!(compress_snappy_int64, Compression::SNAPPY, 2);
compress!(compress_snappy_boolean, Compression::SNAPPY, 3);
compress!(compress_snappy_float, Compression::SNAPPY, 4);
compress!(compress_snappy_double, Compression::SNAPPY, 5);
compress!(compress_snappy_fixed, Compression::SNAPPY, 6);
compress!(compress_snappy_int96, Compression::SNAPPY, 7);

compress!(compress_lz4_binary, Compression::LZ4, 0);
compress!(compress_lz4_int32, Compression::LZ4, 1);
compress!(compress_lz4_int64, Compression::LZ4, 2);
compress!(compress_lz4_boolean, Compression::LZ4, 3);
compress!(compress_lz4_float, Compression::LZ4, 4);
compress!(compress_lz4_double, Compression::LZ4, 5);
compress!(compress_lz4_fixed, Compression::LZ4, 6);
compress!(compress_lz4_int96, Compression::LZ4, 7);

compress!(compress_zstd_binary, Compression::ZSTD, 0);
compress!(compress_zstd_int32, Compression::ZSTD, 1);
compress!(compress_zstd_int64, Compression::ZSTD, 2);
compress!(compress_zstd_boolean, Compression::ZSTD, 3);
compress!(compress_zstd_float, Compression::ZSTD, 4);
compress!(compress_zstd_double, Compression::ZSTD, 5);
compress!(compress_zstd_fixed, Compression::ZSTD, 6);
compress!(compress_zstd_int96, Compression::ZSTD, 7);

decompress!(decompress_brotli_binary, Compression::BROTLI, 0);
decompress!(decompress_brotli_int32, Compression::BROTLI, 1);
decompress!(decompress_brotli_int64, Compression::BROTLI, 2);
decompress!(decompress_brotli_boolean, Compression::BROTLI, 3);
decompress!(decompress_brotli_float, Compression::BROTLI, 4);
decompress!(decompress_brotli_double, Compression::BROTLI, 5);
decompress!(decompress_brotli_fixed, Compression::BROTLI, 6);
decompress!(decompress_brotli_int96, Compression::BROTLI, 7);

decompress!(decompress_gzip_binary, Compression::GZIP, 0);
decompress!(decompress_gzip_int32, Compression::GZIP, 1);
decompress!(decompress_gzip_int64, Compression::GZIP, 2);
decompress!(decompress_gzip_boolean, Compression::GZIP, 3);
decompress!(decompress_gzip_float, Compression::GZIP, 4);
decompress!(decompress_gzip_double, Compression::GZIP, 5);
decompress!(decompress_gzip_fixed, Compression::GZIP, 6);
decompress!(decompress_gzip_int96, Compression::GZIP, 7);

decompress!(decompress_snappy_binary, Compression::SNAPPY, 0);
decompress!(decompress_snappy_int32, Compression::SNAPPY, 1);
decompress!(decompress_snappy_int64, Compression::SNAPPY, 2);
decompress!(decompress_snappy_boolean, Compression::SNAPPY, 3);
decompress!(decompress_snappy_float, Compression::SNAPPY, 4);
decompress!(decompress_snappy_double, Compression::SNAPPY, 5);
decompress!(decompress_snappy_fixed, Compression::SNAPPY, 6);
decompress!(decompress_snappy_int96, Compression::SNAPPY, 7);

decompress!(decompress_lz4_binary, Compression::LZ4, 0);
decompress!(decompress_lz4_int32, Compression::LZ4, 1);
decompress!(decompress_lz4_int64, Compression::LZ4, 2);
decompress!(decompress_lz4_boolean, Compression::LZ4, 3);
decompress!(decompress_lz4_float, Compression::LZ4, 4);
decompress!(decompress_lz4_double, Compression::LZ4, 5);
decompress!(decompress_lz4_fixed, Compression::LZ4, 6);
decompress!(decompress_lz4_int96, Compression::LZ4, 7);

decompress!(decompress_zstd_binary, Compression::ZSTD, 0);
decompress!(decompress_zstd_int32, Compression::ZSTD, 1);
decompress!(decompress_zstd_int64, Compression::ZSTD, 2);
decompress!(decompress_zstd_boolean, Compression::ZSTD, 3);
decompress!(decompress_zstd_float, Compression::ZSTD, 4);
decompress!(decompress_zstd_double, Compression::ZSTD, 5);
decompress!(decompress_zstd_fixed, Compression::ZSTD, 6);
decompress!(decompress_zstd_int96, Compression::ZSTD, 7);
