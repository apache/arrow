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

#pragma once

#include <random>
#include <string>

#include "parquet/encoding.h"

namespace parquet::benchmark {

void GenerateRandomString(uint32_t length, uint32_t seed, std::vector<uint8_t>* heap) {
  // Character set used to generate random string
  const std::string charset =
      "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

  std::default_random_engine gen(seed);
  std::uniform_int_distribution<uint32_t> dist(0, static_cast<int>(charset.size() - 1));

  for (uint32_t i = 0; i < length; i++) {
    heap->push_back(charset[dist(gen)]);
  }
}

constexpr uint32_t kDefaultDataStringLength = 8;

template <typename T>
void GenerateBenchmarkData(uint32_t size, uint32_t seed, T* data,
                           [[maybe_unused]] std::vector<uint8_t>* heap,
                           uint32_t data_string_length = kDefaultDataStringLength) {
  if constexpr (std::is_integral_v<T>) {
    std::default_random_engine gen(seed);
    std::uniform_int_distribution<T> d(std::numeric_limits<T>::min(),
                                       std::numeric_limits<T>::max());
    for (uint32_t i = 0; i < size; ++i) {
      data[i] = d(gen);
    }
  } else if constexpr (std::is_floating_point_v<T>) {
    std::default_random_engine gen(seed);
    std::uniform_real_distribution<T> d(std::numeric_limits<T>::lowest(),
                                        std::numeric_limits<T>::max());
    for (uint32_t i = 0; i < size; ++i) {
      data[i] = d(gen);
    }
  } else if constexpr (std::is_same_v<FLBA, T>) {
    GenerateRandomString(data_string_length * size, seed, heap);
    for (uint32_t i = 0; i < size; ++i) {
      data[i].ptr = heap->data() + i * data_string_length;
    }
  } else if constexpr (std::is_same_v<ByteArray, T>) {
    GenerateRandomString(data_string_length * size, seed, heap);
    for (uint32_t i = 0; i < size; ++i) {
      data[i].ptr = heap->data() + i * data_string_length;
      data[i].len = data_string_length;
    }
  } else if constexpr (std::is_same_v<Int96, T>) {
    std::default_random_engine gen(seed);
    std::uniform_int_distribution<int> d(std::numeric_limits<int>::min(),
                                         std::numeric_limits<int>::max());
    for (uint32_t i = 0; i < size; ++i) {
      data[i].value[0] = d(gen);
      data[i].value[1] = d(gen);
      data[i].value[2] = d(gen);
    }
  }
}

}  // namespace parquet::benchmark
