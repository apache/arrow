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

#include <memory>

#include "arrow/type.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/time.h"

namespace arrow {

using internal::checked_cast;

namespace util {

// TimestampType -> TimestampType
static const std::pair<DivideOrMultiply, int64_t> kTimestampConversionTable[4][4] = {
    // TimestampType::SECOND
    {{MULTIPLY, 1}, {MULTIPLY, 1000}, {MULTIPLY, 1000000}, {MULTIPLY, 1000000000}},
    // TimestampType::MILLI
    {{DIVIDE, 1000}, {MULTIPLY, 1}, {MULTIPLY, 1000}, {MULTIPLY, 1000000}},
    // TimestampType::MICRO
    {{DIVIDE, 1000000}, {DIVIDE, 1000}, {MULTIPLY, 1}, {MULTIPLY, 1000}},
    // TimestampType::NANO
    {{DIVIDE, 1000000000}, {DIVIDE, 1000000}, {DIVIDE, 1000}, {MULTIPLY, 1}},
};

std::pair<DivideOrMultiply, int64_t> GetTimestampConversion(TimeUnit::type in_unit,
                                                            TimeUnit::type out_unit) {
  return kTimestampConversionTable[static_cast<int>(in_unit)][static_cast<int>(out_unit)];
}

Result<int64_t> ConvertTimestampValue(const std::shared_ptr<DataType>& in,
                                      const std::shared_ptr<DataType>& out,
                                      int64_t value) {
  auto op_factor =
      GetTimestampConversion(checked_cast<const TimestampType&>(*in).unit(),
                             checked_cast<const TimestampType&>(*out).unit());

  auto op = op_factor.first;
  auto factor = op_factor.second;
  switch (op) {
    case MULTIPLY:
      return value * factor;
    case DIVIDE:
      return value / factor;
  }

  // unreachable...
  return 0;
}

}  // namespace util
}  // namespace arrow
