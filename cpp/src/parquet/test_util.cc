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

// This module defines an abstract interface for iterating through pages in a
// Parquet column chunk within a row group. It could be extended in the future
// to iterate through all data pages in all chunks in a file.

#include "parquet/test_util.h"

#include <algorithm>
#include <chrono>
#include <limits>
#include <memory>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include "arrow/testing/uniform_real.h"
#include "parquet/column_page.h"
#include "parquet/column_reader.h"
#include "parquet/column_writer.h"
#include "parquet/encoding.h"
#include "parquet/platform.h"

namespace parquet {
namespace test {

const char* get_data_dir() {
  const auto result = std::getenv("PARQUET_TEST_DATA");
  if (!result || !result[0]) {
    throw ParquetTestException(
        "Please point the PARQUET_TEST_DATA environment "
        "variable to the test data directory");
  }
  return result;
}

std::string get_bad_data_dir() {
  // PARQUET_TEST_DATA should point to ARROW_HOME/cpp/submodules/parquet-testing/data
  // so need to reach one folder up to access the "bad_data" folder.
  std::string data_dir(get_data_dir());
  std::stringstream ss;
  ss << data_dir << "/../bad_data";
  return ss.str();
}

std::string get_data_file(const std::string& filename, bool is_good) {
  std::stringstream ss;

  if (is_good) {
    ss << get_data_dir();
  } else {
    ss << get_bad_data_dir();
  }

  ss << "/" << filename;
  return ss.str();
}

void random_bytes(int n, uint32_t seed, std::vector<uint8_t>* out) {
  std::default_random_engine gen(seed);
  std::uniform_int_distribution<int> d(0, 255);

  out->resize(n);
  for (int i = 0; i < n; ++i) {
    (*out)[i] = static_cast<uint8_t>(d(gen));
  }
}

void random_bools(int n, double p, uint32_t seed, bool* out) {
  std::default_random_engine gen(seed);
  std::bernoulli_distribution d(p);
  for (int i = 0; i < n; ++i) {
    out[i] = d(gen);
  }
}

void random_Int96_numbers(int n, uint32_t seed, int32_t min_value, int32_t max_value,
                          Int96* out) {
  std::default_random_engine gen(seed);
  std::uniform_int_distribution<int32_t> d(min_value, max_value);
  for (int i = 0; i < n; ++i) {
    out[i].value[0] = d(gen);
    out[i].value[1] = d(gen);
    out[i].value[2] = d(gen);
  }
}

void random_float16_numbers(int n, uint32_t seed, ::arrow::util::Float16 min_value,
                            ::arrow::util::Float16 max_value, uint16_t* out) {
  std::vector<float> values(n);
  random_numbers(n, seed, static_cast<float>(min_value), static_cast<float>(max_value),
                 values.data());
  for (int i = 0; i < n; ++i) {
    out[i] = ::arrow::util::Float16(values[i]).bits();
  }
}

void random_fixed_byte_array(int n, uint32_t seed, uint8_t* buf, int len, FLBA* out) {
  std::default_random_engine gen(seed);
  std::uniform_int_distribution<int> d(0, 255);
  for (int i = 0; i < n; ++i) {
    out[i].ptr = buf;
    for (int j = 0; j < len; ++j) {
      buf[j] = static_cast<uint8_t>(d(gen));
    }
    buf += len;
  }
}

void random_byte_array(int n, uint32_t seed, uint8_t* buf, ByteArray* out, int min_size,
                       int max_size) {
  std::default_random_engine gen(seed);
  std::uniform_int_distribution<int> d1(min_size, max_size);
  std::uniform_int_distribution<int> d2(0, 255);
  for (int i = 0; i < n; ++i) {
    int len = d1(gen);
    out[i].len = len;
    out[i].ptr = buf;
    for (int j = 0; j < len; ++j) {
      buf[j] = static_cast<uint8_t>(d2(gen));
    }
    buf += len;
  }
}

void random_byte_array(int n, uint32_t seed, uint8_t* buf, ByteArray* out, int max_size) {
  random_byte_array(n, seed, buf, out, 0, max_size);
}

void prefixed_random_byte_array(int n, uint32_t seed, uint8_t* buf, ByteArray* out,
                                int min_size, int max_size, double prefixed_probability) {
  std::default_random_engine gen(seed);
  std::uniform_int_distribution<int> dist_size(min_size, max_size);
  std::uniform_int_distribution<int> dist_byte(0, 255);
  std::bernoulli_distribution dist_has_prefix(prefixed_probability);
  std::uniform_real_distribution<double> dist_prefix_length(0, 1);

  for (int i = 0; i < n; ++i) {
    int len = dist_size(gen);
    out[i].len = len;
    out[i].ptr = buf;

    bool do_prefix = dist_has_prefix(gen) && i > 0;
    int prefix_len = 0;
    if (do_prefix) {
      int max_prefix_len = std::min(len, static_cast<int>(out[i - 1].len));
      prefix_len = static_cast<int>(std::ceil(max_prefix_len * dist_prefix_length(gen)));
    }
    for (int j = 0; j < prefix_len; ++j) {
      buf[j] = out[i - 1].ptr[j];
    }
    for (int j = prefix_len; j < len; ++j) {
      buf[j] = static_cast<uint8_t>(dist_byte(gen));
    }
    buf += len;
  }
}

void prefixed_random_byte_array(int n, uint32_t seed, uint8_t* buf, int len, FLBA* out,
                                double prefixed_probability) {
  std::default_random_engine gen(seed);
  std::uniform_int_distribution<int> dist_byte(0, 255);
  std::bernoulli_distribution dist_has_prefix(prefixed_probability);
  std::uniform_int_distribution<int> dist_size(0, len);

  for (int i = 0; i < n; ++i) {
    out[i].ptr = buf;

    bool do_prefix = dist_has_prefix(gen) && i > 0;
    int prefix_len = do_prefix ? dist_size(gen) : 0;
    for (int j = 0; j < prefix_len; ++j) {
      buf[j] = out[i - 1].ptr[j];
    }
    for (int j = prefix_len; j < len; ++j) {
      buf[j] = static_cast<uint8_t>(dist_byte(gen));
    }
    buf += len;
  }
}

}  // namespace test
}  // namespace parquet
