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

#include "arrow/util/byte_size.h"

#include <cstdint>
#include <unordered_set>

#include "arrow/array.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/buffer.h"
#include "arrow/util/logging.h"
#include "arrow/visitor_inline.h"

namespace arrow {

using internal::checked_cast;
using internal::checked_pointer_cast;

namespace util {

namespace {

int64_t DoTotalBufferSize(const ArrayData& array_data,
                          std::unordered_set<const uint8_t*>* seen_buffers) {
  int64_t sum = 0;
  for (const auto& buffer : array_data.buffers) {
    if (buffer && seen_buffers->insert(buffer->data()).second) {
      sum += buffer->size();
    }
  }
  for (const auto& child : array_data.child_data) {
    sum += DoTotalBufferSize(*child, seen_buffers);
  }
  if (array_data.dictionary) {
    sum += DoTotalBufferSize(*array_data.dictionary, seen_buffers);
  }
  return sum;
}

int64_t DoTotalBufferSize(const Array& array,
                          std::unordered_set<const uint8_t*>* seen_buffers) {
  return DoTotalBufferSize(*array.data(), seen_buffers);
}

int64_t DoTotalBufferSize(const ChunkedArray& chunked_array,
                          std::unordered_set<const uint8_t*>* seen_buffers) {
  int64_t sum = 0;
  for (const auto& chunk : chunked_array.chunks()) {
    sum += DoTotalBufferSize(*chunk, seen_buffers);
  }
  return sum;
}

int64_t DoTotalBufferSize(const RecordBatch& record_batch,
                          std::unordered_set<const uint8_t*>* seen_buffers) {
  int64_t sum = 0;
  for (const auto& column : record_batch.columns()) {
    sum += DoTotalBufferSize(*column, seen_buffers);
  }
  return sum;
}

int64_t DoTotalBufferSize(const Table& table,
                          std::unordered_set<const uint8_t*>* seen_buffers) {
  int64_t sum = 0;
  for (const auto& column : table.columns()) {
    sum += DoTotalBufferSize(*column, seen_buffers);
  }
  return sum;
}

}  // namespace

int64_t TotalBufferSize(const ArrayData& array_data) {
  std::unordered_set<const uint8_t*> seen_buffers;
  return DoTotalBufferSize(array_data, &seen_buffers);
}

int64_t TotalBufferSize(const Array& array) { return TotalBufferSize(*array.data()); }

int64_t TotalBufferSize(const ChunkedArray& chunked_array) {
  std::unordered_set<const uint8_t*> seen_buffers;
  return DoTotalBufferSize(chunked_array, &seen_buffers);
}

int64_t TotalBufferSize(const RecordBatch& record_batch) {
  std::unordered_set<const uint8_t*> seen_buffers;
  return DoTotalBufferSize(record_batch, &seen_buffers);
}

int64_t TotalBufferSize(const Table& table) {
  std::unordered_set<const uint8_t*> seen_buffers;
  return DoTotalBufferSize(table, &seen_buffers);
}

namespace {

struct GetByteRangesArray {
  const ArrayData& input;
  int64_t offset;
  int64_t length;
  UInt64Builder* range_starts;
  UInt64Builder* range_offsets;
  UInt64Builder* range_lengths;

  Status VisitBitmap(const std::shared_ptr<Buffer>& buffer) const {
    if (buffer) {
      uint64_t data_start = reinterpret_cast<uint64_t>(buffer->data());
      RETURN_NOT_OK(range_starts->Append(data_start));
      RETURN_NOT_OK(range_offsets->Append(BitUtil::RoundDown(offset, 8) / 8));
      RETURN_NOT_OK(range_lengths->Append(BitUtil::CoveringBytes(offset, length)));
    }
    return Status::OK();
  }

  Status VisitFixedWidthArray(const Buffer& buffer, const FixedWidthType& type) const {
    uint64_t data_start = reinterpret_cast<uint64_t>(buffer.data());
    uint64_t offset_bits = offset * type.bit_width();
    uint64_t offset_bytes = BitUtil::RoundDown(static_cast<int64_t>(offset_bits), 8) / 8;
    uint64_t end_byte =
        BitUtil::RoundUp(static_cast<int64_t>(offset_bits + (length * type.bit_width())),
                         8) /
        8;
    uint64_t length_bytes = (end_byte - offset_bytes);
    RETURN_NOT_OK(range_starts->Append(data_start));
    RETURN_NOT_OK(range_offsets->Append(offset_bytes));
    return range_lengths->Append(length_bytes);
  }

  Status Visit(const FixedWidthType& type) const {
    static_assert(sizeof(uint8_t*) <= sizeof(uint64_t),
                  "Undefined behavior if pointer larger than uint64_t");
    RETURN_NOT_OK(VisitBitmap(input.buffers[0]));
    RETURN_NOT_OK(VisitFixedWidthArray(*input.buffers[1], type));
    if (input.dictionary) {
      // This is slightly imprecise because we always assume the entire dictionary is
      // referenced.  If this array has an offset it may only be referencing a portion of
      // the dictionary
      GetByteRangesArray dict_visitor{*input.dictionary,
                                      input.dictionary->offset,
                                      input.dictionary->length,
                                      range_starts,
                                      range_offsets,
                                      range_lengths};
      return VisitTypeInline(*input.dictionary->type, &dict_visitor);
    }
    return Status::OK();
  }

  Status Visit(const NullType& type) const { return Status::OK(); }

  template <typename BaseBinaryType>
  Status VisitBaseBinary(const BaseBinaryType& type) const {
    using offset_type = typename BaseBinaryType::offset_type;
    RETURN_NOT_OK(VisitBitmap(input.buffers[0]));

    const Buffer& offsets_buffer = *input.buffers[1];
    RETURN_NOT_OK(
        range_starts->Append(reinterpret_cast<uint64_t>(offsets_buffer.data())));
    RETURN_NOT_OK(range_offsets->Append(sizeof(offset_type) * offset));
    RETURN_NOT_OK(range_lengths->Append(sizeof(offset_type) * length));

    const offset_type* offsets = input.GetValues<offset_type>(1, offset);
    const Buffer& values = *input.buffers[2];
    offset_type start = offsets[0];
    offset_type end = offsets[length];
    RETURN_NOT_OK(range_starts->Append(reinterpret_cast<uint64_t>(values.data())));
    RETURN_NOT_OK(range_offsets->Append(static_cast<uint64_t>(start)));
    return range_lengths->Append(static_cast<uint64_t>(end - start));
  }

  Status Visit(const BinaryType& type) const { return VisitBaseBinary(type); }

  Status Visit(const LargeBinaryType& type) const { return VisitBaseBinary(type); }

  template <typename BaseListType>
  Status VisitBaseList(const BaseListType& type) const {
    using offset_type = typename BaseListType::offset_type;
    RETURN_NOT_OK(VisitBitmap(input.buffers[0]));

    const Buffer& offsets_buffer = *input.buffers[1];
    RETURN_NOT_OK(
        range_starts->Append(reinterpret_cast<uint64_t>(offsets_buffer.data())));
    RETURN_NOT_OK(range_offsets->Append(sizeof(offset_type) * offset));
    RETURN_NOT_OK(range_lengths->Append(sizeof(offset_type) * length));

    const offset_type* offsets = input.GetValues<offset_type>(1, offset);
    int64_t start = static_cast<int64_t>(offsets[0]);
    int64_t end = static_cast<int64_t>(offsets[length]);
    GetByteRangesArray child{*input.child_data[0], start,         end - start,
                             range_starts,         range_offsets, range_lengths};
    return VisitTypeInline(*type.value_type(), &child);
  }

  Status Visit(const ListType& type) const { return VisitBaseList(type); }

  Status Visit(const LargeListType& type) const { return VisitBaseList(type); }

  Status Visit(const FixedSizeListType& type) const {
    RETURN_NOT_OK(VisitBitmap(input.buffers[0]));
    GetByteRangesArray child{*input.child_data[0],
                             offset * type.list_size(),
                             length * type.list_size(),
                             range_starts,
                             range_offsets,
                             range_lengths};
    return VisitTypeInline(*type.value_type(), &child);
  }

  Status Visit(const StructType& type) const {
    for (int i = 0; i < type.num_fields(); i++) {
      GetByteRangesArray child{*input.child_data[i],
                               offset + input.child_data[i]->offset,
                               length,
                               range_starts,
                               range_offsets,
                               range_lengths};
      RETURN_NOT_OK(VisitTypeInline(*type.field(i)->type(), &child));
    }
    return Status::OK();
  }

  Status Visit(const DenseUnionType& type) const {
    // Skip validity map for DenseUnionType
    // Types buffer is always int8
    RETURN_NOT_OK(VisitFixedWidthArray(
        *input.buffers[1], *std::dynamic_pointer_cast<FixedWidthType>(int8())));
    // Offsets buffer is always int32
    RETURN_NOT_OK(VisitFixedWidthArray(
        *input.buffers[2], *std::dynamic_pointer_cast<FixedWidthType>(int32())));

    // We have to loop through the types buffer to figure out the correct
    // offset / length being referenced in the child arrays
    std::vector<int64_t> lengths_per_type(type.type_codes().size());
    std::vector<int64_t> offsets_per_type(type.type_codes().size());
    const int8_t* type_codes = input.GetValues<int8_t>(1, 0);
    for (const int8_t* it = type_codes; it != type_codes + offset; it++) {
      DCHECK_NE(type.child_ids()[static_cast<std::size_t>(*it)],
                UnionType::kInvalidChildId);
      offsets_per_type[type.child_ids()[static_cast<std::size_t>(*it)]]++;
    }
    for (const int8_t* it = type_codes + offset; it != type_codes + offset + length;
         it++) {
      DCHECK_NE(type.child_ids()[static_cast<std::size_t>(*it)],
                UnionType::kInvalidChildId);
      lengths_per_type[type.child_ids()[static_cast<std::size_t>(*it)]]++;
    }

    for (int i = 0; i < type.num_fields(); i++) {
      GetByteRangesArray child{
          *input.child_data[i], offsets_per_type[i] + input.child_data[i]->offset,
          lengths_per_type[i],  range_starts,
          range_offsets,        range_lengths};
      RETURN_NOT_OK(VisitTypeInline(*type.field(i)->type(), &child));
    }

    return Status::OK();
  }

  Status Visit(const SparseUnionType& type) const {
    // Skip validity map for SparseUnionType
    // Types buffer is always int8
    RETURN_NOT_OK(VisitFixedWidthArray(
        *input.buffers[1], *std::dynamic_pointer_cast<FixedWidthType>(int8())));

    for (int i = 0; i < type.num_fields(); i++) {
      GetByteRangesArray child{*input.child_data[i],
                               offset + input.child_data[i]->offset,
                               length,
                               range_starts,
                               range_offsets,
                               range_lengths};
      RETURN_NOT_OK(VisitTypeInline(*type.field(i)->type(), &child));
    }

    return Status::OK();
  }

  Status Visit(const ExtensionType& extension_type) const {
    GetByteRangesArray storage{input,        offset,        length,
                               range_starts, range_offsets, range_lengths};
    return VisitTypeInline(*extension_type.storage_type(), &storage);
  }

  Status Visit(const DataType& type) const {
    return Status::TypeError("Extracting byte ranges not supported for type ",
                             type.ToString());
  }

  static std::shared_ptr<DataType> RangesType() {
    return struct_(
        {field("start", uint64()), field("offset", uint64()), field("length", uint64())});
  }

  Result<std::shared_ptr<Array>> MakeRanges() const {
    std::shared_ptr<Array> range_starts_arr, range_offsets_arr, range_lengths_arr;
    RETURN_NOT_OK(range_starts->Finish(&range_starts_arr));
    RETURN_NOT_OK(range_offsets->Finish(&range_offsets_arr));
    RETURN_NOT_OK(range_lengths->Finish(&range_lengths_arr));
    return StructArray::Make(
        {range_starts_arr, range_offsets_arr, range_lengths_arr},
        {field("start", uint64()), field("offset", uint64()), field("length", uint64())});
  }

  static Result<std::shared_ptr<Array>> Exec(const ArrayData& input) {
    UInt64Builder range_starts, range_offsets, range_lengths;
    GetByteRangesArray self{input,         input.offset,   input.length,
                            &range_starts, &range_offsets, &range_lengths};
    RETURN_NOT_OK(VisitTypeInline(*input.type, &self));
    return self.MakeRanges();
  }
};

int64_t RangesToLengthSum(const Array& ranges) {
  int64_t sum = 0;
  const StructArray& ranges_struct = checked_cast<const StructArray&>(ranges);
  std::shared_ptr<UInt64Array> lengths =
      checked_pointer_cast<UInt64Array>(ranges_struct.field(2));
  for (auto length : *lengths) {
    sum += static_cast<int64_t>(*length);
  }
  return sum;
}

}  // namespace

Result<std::shared_ptr<Array>> ReferencedRanges(const ArrayData& array_data) {
  return GetByteRangesArray::Exec(array_data);
}

Result<int64_t> ReferencedBufferSize(const ArrayData& array_data) {
  ARROW_ASSIGN_OR_RAISE(const std::shared_ptr<Array> ranges,
                        GetByteRangesArray::Exec(array_data));
  return RangesToLengthSum(*ranges);
}

Result<int64_t> ReferencedBufferSize(const Array& array) {
  ARROW_ASSIGN_OR_RAISE(const std::shared_ptr<Array> ranges,
                        GetByteRangesArray::Exec(*array.data()));
  return RangesToLengthSum(*ranges);
}

Result<int64_t> ReferencedBufferSize(const ChunkedArray& array) {
  int64_t sum = 0;
  for (const auto& chunk : array.chunks()) {
    ARROW_ASSIGN_OR_RAISE(int64_t chunk_sum, ReferencedBufferSize(*chunk));
    sum += chunk_sum;
  }
  return sum;
}

Result<int64_t> ReferencedBufferSize(const RecordBatch& record_batch) {
  int64_t sum = 0;
  for (const auto& column : record_batch.columns()) {
    ARROW_ASSIGN_OR_RAISE(int64_t column_sum, ReferencedBufferSize(*column));
    sum += column_sum;
  }
  return sum;
}

Result<int64_t> ReferencedBufferSize(const Table& table) {
  int64_t sum = 0;
  for (const auto& column : table.columns()) {
    for (const auto& chunk : column->chunks()) {
      ARROW_ASSIGN_OR_RAISE(int64_t chunk_sum, ReferencedBufferSize(*chunk));
      sum += chunk_sum;
    }
  }
  return sum;
}

}  // namespace util

}  // namespace arrow
