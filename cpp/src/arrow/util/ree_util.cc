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

#include "arrow/builder.h"
#include "arrow/util/ree_util.h"

namespace arrow {
namespace ree_util {

template <typename RunEndsType>
int64_t FindPhysicalOffset(const RunEndsType* run_ends, int64_t num_run_ends,
                           int64_t logical_offset) {
  auto it = std::upper_bound(run_ends, run_ends + num_run_ends, logical_offset);
  int64_t result = std::distance(run_ends, it);
  ARROW_DCHECK_LE(result, num_run_ends);
  return result;
}

void AddArtificialOffsetInChildArray(ArrayData* array, int64_t offset) {
  auto& child = array->child_data[1];
  auto builder = MakeBuilder(child->type).ValueOrDie();
  ARROW_CHECK_OK(builder->AppendNulls(offset));
  ARROW_CHECK_OK(builder->AppendArraySlice(ArraySpan(*child), 0, child->length));
  array->child_data[1] = builder->Finish().ValueOrDie()->Slice(offset)->data();
}

int64_t GetPhysicalOffset(const ArraySpan& span) {
  // TODO: caching
  auto type_id = RunEndsArray(span).type->id();
  if (type_id == Type::INT16) {
    return FindPhysicalOffset(RunEnds<int16_t>(span), RunEndsArray(span).length,
                              span.offset);
  } else if (type_id == Type::INT32) {
    return FindPhysicalOffset(RunEnds<int32_t>(span), RunEndsArray(span).length,
                              span.offset);
  } else {
    ARROW_CHECK(type_id == Type::INT64);
    return FindPhysicalOffset(RunEnds<int64_t>(span), RunEndsArray(span).length,
                              span.offset);
  }
}

template <typename RunEndsType>
static int64_t GetPhysicalLengthInternal(const ArraySpan& span) {
  // find the offset of the last element and add 1
  int64_t physical_offset = GetPhysicalOffset(span);
  return FindPhysicalOffset(RunEnds<RunEndsType>(span) + physical_offset,
                            RunEndsArray(span).length - physical_offset,
                            span.offset + span.length - 1) +
         1;
}

int64_t GetPhysicalLength(const ArraySpan& span) {
  // TODO: caching
  if (span.length == 0) {
    return 0;
  } else {
    auto type_id = RunEndsArray(span).type->id();
    if (type_id == Type::INT16) {
      return GetPhysicalLengthInternal<int16_t>(span);
    } else if (type_id == Type::INT32) {
      return GetPhysicalLengthInternal<int32_t>(span);
    } else {
      ARROW_CHECK_EQ(type_id, Type::INT64);
      return GetPhysicalLengthInternal<int64_t>(span);
    }
  }
}

}  // namespace ree_util
}  // namespace arrow
