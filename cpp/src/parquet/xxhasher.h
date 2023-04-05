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

#include <cstdint>

#include "parquet/hasher.h"
#include "parquet/platform.h"
#include "parquet/types.h"

namespace parquet {

class PARQUET_EXPORT XxHasher : public Hasher {
 public:
  uint64_t Hash(int32_t value) const override;
  uint64_t Hash(int64_t value) const override;
  uint64_t Hash(float value) const override;
  uint64_t Hash(double value) const override;
  uint64_t Hash(const Int96* value) const override;
  uint64_t Hash(const ByteArray* value) const override;
  uint64_t Hash(const FLBA* val, uint32_t len) const override;

  static constexpr int kParquetBloomXxHashSeed = 0;
};

}  // namespace parquet
