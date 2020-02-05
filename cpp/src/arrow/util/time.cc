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

#include "arrow/util/time.h"
#include "arrow/util/checked_cast.h"

namespace arrow {
namespace util {

Result<int64_t> ConvertTimestampValue(const std::shared_ptr<DataType>& in,
                                      const std::shared_ptr<DataType>& out,
                                      int64_t value) {
  auto from = internal::checked_pointer_cast<TimestampType>(in)->unit();
  auto to = internal::checked_pointer_cast<TimestampType>(out)->unit();

  auto op_factor =
      util::kTimestampConversionTable[static_cast<int>(from)][static_cast<int>(to)];

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
