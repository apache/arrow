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

#include "arrow/util/dict_util_internal.h"

#include "arrow/array/array_dict.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/checked_cast.h"

namespace arrow {
namespace dict_util {

namespace {

template <typename IndexArrowType>
int64_t LogicalNullCount(const ArraySpan& span) {
  const auto* indices_null_bit_map = span.buffers[0].data;
  const auto& dictionary_span = span.dictionary();
  const auto* dictionary_null_bit_map = dictionary_span.buffers[0].data;

  using CType = typename IndexArrowType::c_type;
  const CType* indices_data = span.GetValues<CType>(1);
  int64_t null_count = 0;
  for (int64_t i = 0; i < span.length; i++) {
    if (indices_null_bit_map != nullptr &&
        !bit_util::GetBit(indices_null_bit_map, i + span.offset)) {
      null_count++;
      continue;
    }

    CType current_index = indices_data[i];
    if (!bit_util::GetBit(dictionary_null_bit_map,
                          current_index + dictionary_span.offset)) {
      null_count++;
    }
  }
  return null_count;
}

}  // namespace

int64_t LogicalNullCount(const ArraySpan& span) {
  if (span.dictionary().GetNullCount() == 0 || span.length == 0) {
    return span.GetNullCount();
  }

  const auto& dict_array_type = internal::checked_cast<const DictionaryType&>(*span.type);
  switch (dict_array_type.index_type()->id()) {
    case Type::UINT8:
      return LogicalNullCount<UInt8Type>(span);
    case Type::INT8:
      return LogicalNullCount<Int8Type>(span);
    case Type::UINT16:
      return LogicalNullCount<UInt16Type>(span);
    case Type::INT16:
      return LogicalNullCount<Int16Type>(span);
    case Type::UINT32:
      return LogicalNullCount<UInt32Type>(span);
    case Type::INT32:
      return LogicalNullCount<Int32Type>(span);
    case Type::UINT64:
      return LogicalNullCount<UInt64Type>(span);
    default:
      return LogicalNullCount<Int64Type>(span);
  }
}
}  // namespace dict_util
}  // namespace arrow
