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

namespace parquet::variant {

enum class VariantBasicType : uint8_t {
  kPrimitive = 0,
  kShortString = 1,
  kObject = 2,
  kArray = 3,
};

enum class VariantPrimitiveType : uint8_t {
  kNull = 0,
  kBooleanTrue = 1,
  kBooleanFalse = 2,
  kInt8 = 3,
  kInt16 = 4,
  kInt32 = 5,
  kInt64 = 6,
  kDouble = 7,
  kDecimal4 = 8,
  kDecimal8 = 9,
  kDecimal16 = 10,
  kDate = 11,
  kTimestampMicros = 12,
  kTimestampNTZMicros = 13,
  kFloat = 14,
  kBinary = 15,
  kString = 16,
  kTimeNTZMicros = 17,
  kTimestampNanos = 18,
  kTimestampNTZNanos = 19,
  kUuid = 20,
};

}  // namespace parquet::variant
