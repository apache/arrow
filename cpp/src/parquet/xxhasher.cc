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

#include "parquet/xxhasher.h"

#define XXH_INLINE_ALL
#include "arrow/vendored/xxhash/xxhash.h"

namespace parquet {

namespace {
template <typename T>
uint64_t XxHashHelper(T value, uint32_t seed) {
  return XXH64(reinterpret_cast<const void*>(&value), sizeof(T), seed);
}
}  // namespace

uint64_t XxHasher::Hash(int32_t value) const {
  return XxHashHelper(value, kParquetBloomXxHashSeed);
}

uint64_t XxHasher::Hash(int64_t value) const {
  return XxHashHelper(value, kParquetBloomXxHashSeed);
}

uint64_t XxHasher::Hash(float value) const {
  return XxHashHelper(value, kParquetBloomXxHashSeed);
}

uint64_t XxHasher::Hash(double value) const {
  return XxHashHelper(value, kParquetBloomXxHashSeed);
}

uint64_t XxHasher::Hash(const FLBA* value, uint32_t len) const {
  return XXH64(reinterpret_cast<const void*>(value->ptr), len, kParquetBloomXxHashSeed);
}

uint64_t XxHasher::Hash(const Int96* value) const {
  return XXH64(reinterpret_cast<const void*>(value->value), sizeof(value->value),
               kParquetBloomXxHashSeed);
}

uint64_t XxHasher::Hash(const ByteArray* value) const {
  return XXH64(reinterpret_cast<const void*>(value->ptr), value->len,
               kParquetBloomXxHashSeed);
}

}  // namespace parquet
