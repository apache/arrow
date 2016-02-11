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

#include <iostream>
#include <random>
#include <vector>

using std::vector;

namespace parquet_cpp {

namespace test {

template <typename T>
static inline void assert_vector_equal(const vector<T>& left,
    const vector<T>& right) {
  ASSERT_EQ(left.size(), right.size());

  for (size_t i = 0; i < left.size(); ++i) {
    ASSERT_EQ(left[i], right[i]) << i;
  }
}

template <typename T>
static inline bool vector_equal(const vector<T>& left, const vector<T>& right) {
  if (left.size() != right.size()) {
    return false;
  }

  for (size_t i = 0; i < left.size(); ++i) {
    if (left[i] != right[i]) {
      std::cerr << "index " << i
                << " left was " << left[i]
                << " right was " << right[i]
                << std::endl;
      return false;
    }
  }

  return true;
}

template <typename T>
static vector<T> slice(const vector<T>& values, size_t start, size_t end) {
  if (end < start) {
    return vector<T>(0);
  }

  vector<T> out(end - start);
  for (size_t i = start; i < end; ++i) {
    out[i - start] = values[i];
  }
  return out;
}

static inline vector<bool> flip_coins_seed(size_t n, double p, uint32_t seed) {
  std::mt19937 gen(seed);
  std::bernoulli_distribution d(p);

  vector<bool> draws;
  for (size_t i = 0; i < n; ++i) {
    draws.push_back(d(gen));
  }
  return draws;
}


static inline vector<bool> flip_coins(size_t n, double p) {
  std::random_device rd;
  std::mt19937 gen(rd());

  std::bernoulli_distribution d(p);

  vector<bool> draws;
  for (size_t i = 0; i < n; ++i) {
    draws.push_back(d(gen));
  }
  return draws;
}


} // namespace test

} // namespace parquet_cpp

#endif // PARQUET_UTIL_TEST_COMMON_H
