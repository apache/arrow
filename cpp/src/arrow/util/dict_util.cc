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
#include "arrow/util/bit_run_reader.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging_internal.h"

namespace arrow {
namespace dict_util {

namespace {

template <typename IndexArrowType>
int64_t LogicalNullCount(const ArraySpan& span) {
  const auto* indices_valid_bitmap = span.buffers[0].data;
  const auto& dictionary_span = span.dictionary();
  const auto* dictionary_valid_bitmap = dictionary_span.buffers[0].data;

  using CType = typename IndexArrowType::c_type;
  const CType* indices_data = span.GetValues<CType>(1);
  int64_t null_count = 0;
  DCHECK_OK(internal::VisitBitRuns(
      indices_valid_bitmap, span.offset, span.length,
      [=, &null_count](int64_t position, int64_t length, bool valid) {
        if (valid) {
          // This is a run of valid indicies, so we need to look each of them up
          // in the dictionary array to see if they are null
          for (int64_t i = position; i < position + length; i++) {
            CType index = indices_data[i];
            if (!bit_util::GetBit(dictionary_valid_bitmap,
                                  index + dictionary_span.offset)) {
              null_count++;
            }
          }
        } else {
          // This is a run of null indices
          null_count += length;
        }
        return Status::OK();
      }));
  return null_count;
}

template <typename IndexArrowType>
void SetLogicalNullBits(const ArraySpan& span, uint8_t* out_bitmap, int64_t out_offset,
                        bool set_on_null) {
  const auto* indices_valid_bitmap = span.buffers[0].data;
  const auto& dictionary_span = span.dictionary();
  const auto* dictionary_valid_bitmap = dictionary_span.buffers[0].data;

  using CType = typename IndexArrowType::c_type;
  const CType* indices_data = span.GetValues<CType>(1);
  DCHECK_OK(internal::VisitBitRuns(
      indices_valid_bitmap, span.offset, span.length,
      [=](int64_t position, int64_t length, bool valid) {
        if (valid) {
          // This is a run of valid indicies, so we need to look each of them up
          // in the dictionary array to see if they are null
          for (int64_t i = position; i < position + length; i++) {
            CType index = indices_data[i];
            bool is_null = !bit_util::GetBit(dictionary_valid_bitmap,
                                             index + dictionary_span.offset);
            bit_util::SetBitTo(out_bitmap, out_offset + i, is_null == set_on_null);
          }
        } else {
          // This is a run of null indices
          bit_util::SetBitsTo(out_bitmap, out_offset + position, length, set_on_null);
        }
        return Status::OK();
      }));
}

}  // namespace

int64_t LogicalNullCount(const ArraySpan& span) {
  if (span.dictionary().GetNullCount() == 0) {
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

void SetLogicalNullBits(const ArraySpan& span, uint8_t* out_bitmap, int64_t out_offset,
                        bool set_on_null) {
  if (span.dictionary().GetNullCount() == 0) {
    // No nulls in dictionary, so a value is a logical null if and only if
    // its index is null
    if (span.GetNullCount() == 0) {
      // No null indices either (and possibly no validity bitmap at all), so
      // there are no logical nulls
      bit_util::SetBitsTo(out_bitmap, out_offset, span.length, !set_on_null);
    } else if (set_on_null) {
      internal::InvertBitmap(span.buffers[0].data, span.offset, span.length, out_bitmap,
                             out_offset);
    } else {
      internal::CopyBitmap(span.buffers[0].data, span.offset, span.length, out_bitmap,
                           out_offset);
    }
    return;
  }

  const auto& dict_array_type = internal::checked_cast<const DictionaryType&>(*span.type);
  switch (dict_array_type.index_type()->id()) {
    case Type::UINT8:
      SetLogicalNullBits<UInt8Type>(span, out_bitmap, out_offset, set_on_null);
      break;
    case Type::INT8:
      SetLogicalNullBits<Int8Type>(span, out_bitmap, out_offset, set_on_null);
      break;
    case Type::UINT16:
      SetLogicalNullBits<UInt16Type>(span, out_bitmap, out_offset, set_on_null);
      break;
    case Type::INT16:
      SetLogicalNullBits<Int16Type>(span, out_bitmap, out_offset, set_on_null);
      break;
    case Type::UINT32:
      SetLogicalNullBits<UInt32Type>(span, out_bitmap, out_offset, set_on_null);
      break;
    case Type::INT32:
      SetLogicalNullBits<Int32Type>(span, out_bitmap, out_offset, set_on_null);
      break;
    case Type::UINT64:
      SetLogicalNullBits<UInt64Type>(span, out_bitmap, out_offset, set_on_null);
      break;
    default:
      SetLogicalNullBits<Int64Type>(span, out_bitmap, out_offset, set_on_null);
      break;
  }
}

}  // namespace dict_util
}  // namespace arrow
