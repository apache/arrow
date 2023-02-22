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

#include <algorithm>
#include <cstdint>

#include "arrow/builder.h"
#include "arrow/util/logging.h"
#include "arrow/util/ree_util.h"

namespace arrow {
namespace ree_util {

int64_t FindPhysicalIndex(const ArraySpan& span, int64_t i, int64_t absolute_offset) {
  const auto type_id = RunEndsArray(span).type->id();
  if (type_id == Type::INT16) {
    return internal::FindPhysicalIndex<int16_t>(span, i, absolute_offset);
  }
  if (type_id == Type::INT32) {
    return internal::FindPhysicalIndex<int32_t>(span, i, absolute_offset);
  }
  DCHECK_EQ(type_id, Type::INT64);
  return internal::FindPhysicalIndex<int64_t>(span, i, absolute_offset);
}

int64_t FindPhysicalLength(const ArraySpan& span) {
  auto type_id = RunEndsArray(span).type->id();
  if (type_id == Type::INT16) {
    return internal::FindPhysicalLength<int16_t>(span);
  }
  if (type_id == Type::INT32) {
    return internal::FindPhysicalLength<int32_t>(span);
  }
  DCHECK_EQ(type_id, Type::INT64);
  return internal::FindPhysicalLength<int64_t>(span);
}

}  // namespace ree_util
}  // namespace arrow
