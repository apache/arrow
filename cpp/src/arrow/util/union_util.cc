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

#include "arrow/util/union_util.h"

#include <cstdint>

#include "arrow/array/data.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging.h"

namespace arrow::union_util {

int64_t LogicalSparseUnionNullCount(const ArraySpan& span) {
  const auto* sparse_union_type =
      internal::checked_cast<const SparseUnionType*>(span.type);
  DCHECK_LE(span.child_data.size(), 128);

  const int8_t* types = span.GetValues<int8_t>(1);  // NOLINT
  int64_t null_count = 0;
  for (int64_t i = 0; i < span.length; i++) {
    const int8_t child_id = sparse_union_type->child_ids()[types[span.offset + i]];

    null_count += span.child_data[child_id].IsNull(i);
  }
  return null_count;
}

int64_t LogicalDenseUnionNullCount(const ArraySpan& span) {
  const auto* dense_union_type = internal::checked_cast<const DenseUnionType*>(span.type);
  DCHECK_LE(span.child_data.size(), 128);

  const int8_t* types = span.GetValues<int8_t>(1);      // NOLINT
  const int32_t* offsets = span.GetValues<int32_t>(2);  // NOLINT
  int64_t null_count = 0;
  for (int64_t i = 0; i < span.length; i++) {
    const int8_t child_id = dense_union_type->child_ids()[types[span.offset + i]];
    const int32_t offset = offsets[span.offset + i];
    null_count += span.child_data[child_id].IsNull(offset);
  }
  return null_count;
}

}  // namespace arrow::union_util
