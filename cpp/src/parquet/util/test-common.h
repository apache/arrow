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

#ifndef PARQUET_UTIL_TEST_COMMON_H
#define PARQUET_UTIL_TEST_COMMON_H

#include <chrono>
#include <iostream>
#include <limits>
#include <random>
#include <vector>

#include "parquet/exception.h"
#include "parquet/types.h"

namespace parquet {

namespace test {

typedef ::testing::Types<BooleanType, Int32Type, Int64Type, Int96Type, FloatType,
                         DoubleType, ByteArrayType, FLBAType>
    ParquetTypes;

class ParquetTestException : public parquet::ParquetException {
  using ParquetException::ParquetException;
};

const char* get_data_dir() {
  const auto result = std::getenv("PARQUET_TEST_DATA");
  if (!result || !result[0]) {
    throw ParquetTestException(
        "Please point the PARQUET_TEST_DATA environment "
        "variable to the test data directory");
  }
  return result;
}

template <typename T>
static inline void assert_vector_equal(const std::vector<T>& left,
                                       const std::vector<T>& right) {
  ASSERT_EQ(left.size(), right.size());

  for (size_t i = 0; i < left.size(); ++i) {
    ASSERT_EQ(left[i], right[i]) << i;
  }
}

template <typename T>
static inline bool vector_equal(const std::vector<T>& left, const std::vector<T>& right) {
  if (left.size() != right.size()) {
    return false;
  }

  for (size_t i = 0; i < left.size(); ++i) {
    if (left[i] != right[i]) {
      std::cerr << "index " << i << " left was " << left[i] << " right was " << right[i]
                << std::endl;
      return false;
    }
  }

  return true;
}

template <typename T>
static std::vector<T> slice(const std::vector<T>& values, int start, int end) {
  if (end < start) {
    return std::vector<T>(0);
  }

  std::vector<T> out(end - start);
  for (int i = start; i < end; ++i) {
    out[i - start] = values[i];
  }
  return out;
}

static inline std::vector<bool> flip_coins_seed(int n, double p, uint32_t seed) {
  std::default_random_engine gen(seed);
  std::bernoulli_distribution d(p);

  std::vector<bool> draws(n);
  for (int i = 0; i < n; ++i) {
    draws[i] = d(gen);
  }
  return draws;
}

static inline std::vector<bool> flip_coins(int n, double p) {
  uint64_t seed = std::chrono::high_resolution_clock::now().time_since_epoch().count();
  return flip_coins_seed(n, p, static_cast<uint32_t>(seed));
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

template <typename T>
void random_numbers(int n, uint32_t seed, T min_value, T max_value, T* out) {
  std::default_random_engine gen(seed);
  std::uniform_int_distribution<T> d(min_value, max_value);
  for (int i = 0; i < n; ++i) {
    out[i] = d(gen);
  }
}

template <>
void random_numbers(int n, uint32_t seed, float min_value, float max_value, float* out) {
  std::default_random_engine gen(seed);
  std::uniform_real_distribution<float> d(min_value, max_value);
  for (int i = 0; i < n; ++i) {
    out[i] = d(gen);
  }
}

template <>
void random_numbers(int n, uint32_t seed, double min_value, double max_value,
                    double* out) {
  std::default_random_engine gen(seed);
  std::uniform_real_distribution<double> d(min_value, max_value);
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

}  // namespace test
}  // namespace parquet

#endif  // PARQUET_UTIL_TEST_COMMON_H
