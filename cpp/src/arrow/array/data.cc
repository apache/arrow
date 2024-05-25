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

#include "arrow/array/data.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/array/util.h"
#include "arrow/buffer.h"
#include "arrow/device.h"
#include "arrow/scalar.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/binary_view_util.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/dict_util.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"
#include "arrow/util/range.h"
#include "arrow/util/ree_util.h"
#include "arrow/util/slice_util_internal.h"
#include "arrow/util/union_util.h"

namespace arrow {

using internal::checked_cast;
using internal::CountSetBits;

static inline void AdjustNonNullable(Type::type type_id, int64_t length,
                                     std::vector<std::shared_ptr<Buffer>>* buffers,
                                     int64_t* null_count) {
  if (type_id == Type::NA) {
    *null_count = length;
    (*buffers)[0] = nullptr;
  } else if (internal::may_have_validity_bitmap(type_id)) {
    if (*null_count == 0) {
      // In case there are no nulls, don't keep an allocated null bitmap around
      (*buffers)[0] = nullptr;
    } else if (*null_count == kUnknownNullCount && buffers->at(0) == nullptr) {
      // Conversely, if no null bitmap is provided, set the null count to 0
      *null_count = 0;
    }
  } else {
    *null_count = 0;
  }
}

namespace internal {

bool IsNullSparseUnion(const ArrayData& data, int64_t i) {
  auto* union_type = checked_cast<const SparseUnionType*>(data.type.get());
  const auto* types = reinterpret_cast<const int8_t*>(data.buffers[1]->data());
  const int child_id = union_type->child_ids()[types[data.offset + i]];
  return data.child_data[child_id]->IsNull(i);
}

bool IsNullDenseUnion(const ArrayData& data, int64_t i) {
  auto* union_type = checked_cast<const DenseUnionType*>(data.type.get());
  const auto* types = reinterpret_cast<const int8_t*>(data.buffers[1]->data());
  const int child_id = union_type->child_ids()[types[data.offset + i]];
  const auto* offsets = reinterpret_cast<const int32_t*>(data.buffers[2]->data());
  const int64_t child_offset = offsets[data.offset + i];
  return data.child_data[child_id]->IsNull(child_offset);
}

bool IsNullRunEndEncoded(const ArrayData& data, int64_t i) {
  return ArraySpan(data).IsNullRunEndEncoded(i);
}

bool UnionMayHaveLogicalNulls(const ArrayData& data) {
  return ArraySpan(data).MayHaveLogicalNulls();
}

bool RunEndEncodedMayHaveLogicalNulls(const ArrayData& data) {
  return ArraySpan(data).MayHaveLogicalNulls();
}

bool DictionaryMayHaveLogicalNulls(const ArrayData& data) {
  return ArraySpan(data).MayHaveLogicalNulls();
}

BufferSpan PackVariadicBuffers(util::span<const std::shared_ptr<Buffer>> buffers) {
  return {const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(buffers.data())),
          static_cast<int64_t>(buffers.size() * sizeof(std::shared_ptr<Buffer>))};
}

}  // namespace internal

std::shared_ptr<ArrayData> ArrayData::Make(std::shared_ptr<DataType> type, int64_t length,
                                           std::vector<std::shared_ptr<Buffer>> buffers,
                                           int64_t null_count, int64_t offset) {
  AdjustNonNullable(type->id(), length, &buffers, &null_count);
  return std::make_shared<ArrayData>(std::move(type), length, std::move(buffers),
                                     null_count, offset);
}

std::shared_ptr<ArrayData> ArrayData::Make(
    std::shared_ptr<DataType> type, int64_t length,
    std::vector<std::shared_ptr<Buffer>> buffers,
    std::vector<std::shared_ptr<ArrayData>> child_data, int64_t null_count,
    int64_t offset) {
  AdjustNonNullable(type->id(), length, &buffers, &null_count);
  return std::make_shared<ArrayData>(std::move(type), length, std::move(buffers),
                                     std::move(child_data), null_count, offset);
}

std::shared_ptr<ArrayData> ArrayData::Make(
    std::shared_ptr<DataType> type, int64_t length,
    std::vector<std::shared_ptr<Buffer>> buffers,
    std::vector<std::shared_ptr<ArrayData>> child_data,
    std::shared_ptr<ArrayData> dictionary, int64_t null_count, int64_t offset) {
  AdjustNonNullable(type->id(), length, &buffers, &null_count);
  auto data = std::make_shared<ArrayData>(std::move(type), length, std::move(buffers),
                                          std::move(child_data), null_count, offset);
  data->dictionary = std::move(dictionary);
  return data;
}

std::shared_ptr<ArrayData> ArrayData::Make(std::shared_ptr<DataType> type, int64_t length,
                                           int64_t null_count, int64_t offset) {
  return std::make_shared<ArrayData>(std::move(type), length, null_count, offset);
}

namespace {
template <typename Fn>
Result<std::shared_ptr<ArrayData>> CopyToImpl(const ArrayData& data,
                                              const std::shared_ptr<MemoryManager>& to,
                                              Fn&& copy_fn) {
  auto output = ArrayData::Make(data.type, data.length, data.null_count, data.offset);
  output->buffers.resize(data.buffers.size());
  for (auto&& [buf, out_buf] : internal::Zip(data.buffers, output->buffers)) {
    if (buf) {
      ARROW_ASSIGN_OR_RAISE(out_buf, copy_fn(buf, to));
    }
  }

  output->child_data.reserve(data.child_data.size());
  for (const auto& child : data.child_data) {
    ARROW_ASSIGN_OR_RAISE(auto copied, CopyToImpl(*child, to, copy_fn));
    output->child_data.push_back(std::move(copied));
  }

  if (data.dictionary) {
    ARROW_ASSIGN_OR_RAISE(output->dictionary, CopyToImpl(*data.dictionary, to, copy_fn));
  }

  return output;
}
}  // namespace

Result<std::shared_ptr<ArrayData>> ArrayData::CopyTo(
    const std::shared_ptr<MemoryManager>& to) const {
  return CopyToImpl(*this, to, MemoryManager::CopyBuffer);
}

Result<std::shared_ptr<ArrayData>> ArrayData::ViewOrCopyTo(
    const std::shared_ptr<MemoryManager>& to) const {
  return CopyToImpl(*this, to, Buffer::ViewOrCopy);
}

std::shared_ptr<ArrayData> ArrayData::Slice(int64_t off, int64_t len) const {
  ARROW_CHECK_LE(off, length) << "Slice offset (" << off
                              << ") greater than array length (" << length << ")";
  len = std::min(length - off, len);
  off += offset;

  auto copy = this->Copy();
  copy->length = len;
  copy->offset = off;
  if (null_count == length) {
    copy->null_count = len;
  } else if (off == offset && len == length) {  // A copy of current.
    copy->null_count = null_count.load();
  } else {
    copy->null_count = null_count != 0 ? kUnknownNullCount : 0;
  }
  return copy;
}

Result<std::shared_ptr<ArrayData>> ArrayData::SliceSafe(int64_t off, int64_t len) const {
  RETURN_NOT_OK(internal::CheckSliceParams(length, off, len, "array"));
  return Slice(off, len);
}

int64_t ArrayData::GetNullCount() const {
  int64_t precomputed = this->null_count.load();
  if (ARROW_PREDICT_FALSE(precomputed == kUnknownNullCount)) {
    if (this->buffers[0]) {
      precomputed = this->length -
                    CountSetBits(this->buffers[0]->data(), this->offset, this->length);
    } else {
      precomputed = 0;
    }
    this->null_count.store(precomputed);
  }
  return precomputed;
}

int64_t ArrayData::ComputeLogicalNullCount() const {
  if (this->buffers[0] && this->type->id() != Type::DICTIONARY) {
    return GetNullCount();
  }
  return ArraySpan(*this).ComputeLogicalNullCount();
}

DeviceAllocationType ArrayData::device_type() const {
  // we're using 0 as a sentinel value for NOT YET ASSIGNED
  // there is explicitly no constant DeviceAllocationType to represent
  // the "UNASSIGNED" case as it is invalid for data to not have an
  // assigned device type. If it's still 0 at the end, then we return
  // CPU as the allocation device type
  int type = 0;
  for (const auto& buf : buffers) {
    if (!buf) continue;
    if (type == 0) {
      type = static_cast<int>(buf->device_type());
    } else {
      DCHECK_EQ(type, static_cast<int>(buf->device_type()));
    }
  }

  for (const auto& child : child_data) {
    if (!child) continue;
    if (type == 0) {
      type = static_cast<int>(child->device_type());
    } else {
      DCHECK_EQ(type, static_cast<int>(child->device_type()));
    }
  }

  if (dictionary) {
    if (type == 0) {
      type = static_cast<int>(dictionary->device_type());
    } else {
      DCHECK_EQ(type, static_cast<int>(dictionary->device_type()));
    }
  }

  return type == 0 ? DeviceAllocationType::kCPU : static_cast<DeviceAllocationType>(type);
}

// ----------------------------------------------------------------------
// Methods for ArraySpan

void ArraySpan::SetMembers(const ArrayData& data) {
  this->type = data.type.get();
  this->length = data.length;
  if (this->type->id() == Type::NA) {
    this->null_count = this->length;
  } else {
    this->null_count = data.null_count.load();
  }
  this->offset = data.offset;

  for (int i = 0; i < std::min(static_cast<int>(data.buffers.size()), 3); ++i) {
    const std::shared_ptr<Buffer>& buffer = data.buffers[i];
    // It is the invoker-of-kernels's responsibility to ensure that
    // const buffers are not written to accidentally.
    if (buffer) {
      SetBuffer(i, buffer);
    } else {
      this->buffers[i] = {};
    }
  }

  Type::type type_id = this->type->id();
  if (type_id == Type::EXTENSION) {
    auto* ext_type = checked_cast<const ExtensionType*>(this->type);
    type_id = ext_type->storage_type()->id();
  }

  if ((data.buffers.size() == 0 || data.buffers[0] == nullptr) && type_id != Type::NA &&
      type_id != Type::SPARSE_UNION && type_id != Type::DENSE_UNION) {
    // This should already be zero but we make for sure
    this->null_count = 0;
  }

  // Makes sure any other buffers are seen as null / nonexistent
  for (int i = static_cast<int>(data.buffers.size()); i < 3; ++i) {
    this->buffers[i] = {};
  }

  if (type_id == Type::STRING_VIEW || type_id == Type::BINARY_VIEW) {
    // store the span of data buffers in the third buffer
    this->buffers[2] = internal::PackVariadicBuffers(util::span(data.buffers).subspan(2));
  }

  if (type_id == Type::DICTIONARY) {
    this->child_data.resize(1);
    this->child_data[0].SetMembers(*data.dictionary);
  } else {
    this->child_data.resize(data.child_data.size());
    for (size_t child_index = 0; child_index < data.child_data.size(); ++child_index) {
      this->child_data[child_index].SetMembers(*data.child_data[child_index]);
    }
  }
}

namespace {

BufferSpan OffsetsForScalar(uint8_t* scratch_space, int64_t offset_width) {
  return {scratch_space, offset_width * 2};
}

std::pair<BufferSpan, BufferSpan> OffsetsAndSizesForScalar(uint8_t* scratch_space,
                                                           int64_t offset_width) {
  auto* offsets = scratch_space;
  auto* sizes = scratch_space + offset_width;
  return {BufferSpan{offsets, offset_width}, BufferSpan{sizes, offset_width}};
}

int GetNumBuffers(const DataType& type) {
  switch (type.id()) {
    case Type::NA:
    case Type::STRUCT:
    case Type::FIXED_SIZE_LIST:
    case Type::RUN_END_ENCODED:
      return 1;
    case Type::BINARY:
    case Type::LARGE_BINARY:
    case Type::STRING:
    case Type::LARGE_STRING:
    case Type::STRING_VIEW:
    case Type::BINARY_VIEW:
    case Type::DENSE_UNION:
    case Type::LIST_VIEW:
    case Type::LARGE_LIST_VIEW:
      return 3;
    case Type::EXTENSION:
      // The number of buffers depends on the storage type
      return GetNumBuffers(
          *internal::checked_cast<const ExtensionType&>(type).storage_type());
    default:
      // Everything else has 2 buffers
      return 2;
  }
}

}  // namespace

namespace internal {

void FillZeroLengthArray(const DataType* type, ArraySpan* span) {
  span->type = type;
  span->length = 0;
  int num_buffers = GetNumBuffers(*type);
  for (int i = 0; i < num_buffers; ++i) {
    alignas(int64_t) static std::array<uint8_t, sizeof(int64_t) * 2> kZeros{0};
    span->buffers[i].data = kZeros.data();
    span->buffers[i].size = 0;
  }

  if (!may_have_validity_bitmap(type->id())) {
    span->buffers[0] = {};
  }

  for (int i = num_buffers; i < 3; ++i) {
    span->buffers[i] = {};
  }

  if (type->id() == Type::DICTIONARY) {
    span->child_data.resize(1);
    const std::shared_ptr<DataType>& value_type =
        checked_cast<const DictionaryType*>(type)->value_type();
    FillZeroLengthArray(value_type.get(), &span->child_data[0]);
  } else {
    // Fill children
    span->child_data.resize(type->num_fields());
    for (int i = 0; i < type->num_fields(); ++i) {
      FillZeroLengthArray(type->field(i)->type().get(), &span->child_data[i]);
    }
  }
}

}  // namespace internal

void ArraySpan::FillFromScalar(const Scalar& value) {
  static uint8_t kTrueBit = 0x01;
  static uint8_t kFalseBit = 0x00;

  this->type = value.type.get();
  this->length = 1;

  Type::type type_id = value.type->id();

  if (type_id == Type::NA) {
    this->null_count = 1;
  } else if (!internal::may_have_validity_bitmap(type_id)) {
    this->null_count = 0;
  } else {
    // Populate null count and validity bitmap
    this->null_count = value.is_valid ? 0 : 1;
    this->buffers[0].data = value.is_valid ? &kTrueBit : &kFalseBit;
    this->buffers[0].size = 1;
  }

  if (type_id == Type::BOOL) {
    const auto& scalar = checked_cast<const BooleanScalar&>(value);
    this->buffers[1].data = scalar.value ? &kTrueBit : &kFalseBit;
    this->buffers[1].size = 1;
  } else if (is_primitive(type_id) || is_decimal(type_id) ||
             type_id == Type::DICTIONARY) {
    const auto& scalar = checked_cast<const internal::PrimitiveScalarBase&>(value);
    const uint8_t* scalar_data = reinterpret_cast<const uint8_t*>(scalar.view().data());
    this->buffers[1].data = const_cast<uint8_t*>(scalar_data);
    this->buffers[1].size = scalar.type->byte_width();
    if (type_id == Type::DICTIONARY) {
      // Populate dictionary data
      const auto& dict_scalar = checked_cast<const DictionaryScalar&>(value);
      this->child_data.resize(1);
      this->child_data[0].SetMembers(*dict_scalar.value.dictionary->data());
    }
  } else if (is_base_binary_like(type_id)) {
    const auto& scalar = checked_cast<const BaseBinaryScalar&>(value);

    const uint8_t* data_buffer = nullptr;
    int64_t data_size = 0;
    if (scalar.is_valid) {
      data_buffer = scalar.value->data();
      data_size = scalar.value->size();
    }
    if (is_binary_like(type_id)) {
      const auto& binary_scalar = checked_cast<const BinaryScalar&>(value);
      this->buffers[1] = OffsetsForScalar(binary_scalar.scratch_space_, sizeof(int32_t));
    } else {
      // is_large_binary_like
      const auto& large_binary_scalar = checked_cast<const LargeBinaryScalar&>(value);
      this->buffers[1] =
          OffsetsForScalar(large_binary_scalar.scratch_space_, sizeof(int64_t));
    }
    this->buffers[2].data = const_cast<uint8_t*>(data_buffer);
    this->buffers[2].size = data_size;
  } else if (type_id == Type::BINARY_VIEW || type_id == Type::STRING_VIEW) {
    const auto& scalar = checked_cast<const BinaryViewScalar&>(value);

    this->buffers[1].size = BinaryViewType::kSize;
    this->buffers[1].data = scalar.scratch_space_;
    if (scalar.is_valid) {
      this->buffers[2] = internal::PackVariadicBuffers({&scalar.value, 1});
    }
  } else if (type_id == Type::FIXED_SIZE_BINARY) {
    const auto& scalar = checked_cast<const BaseBinaryScalar&>(value);
    this->buffers[1].data = const_cast<uint8_t*>(scalar.value->data());
    this->buffers[1].size = scalar.value->size();
  } else if (is_var_length_list_like(type_id) || type_id == Type::FIXED_SIZE_LIST) {
    const auto& scalar = checked_cast<const BaseListScalar&>(value);

    this->child_data.resize(1);
    if (scalar.value != nullptr) {
      // When the scalar is null, scalar.value can also be null
      this->child_data[0].SetMembers(*scalar.value->data());
    } else {
      // Even when the value is null, we still must populate the
      // child_data to yield a valid array. Tedious
      internal::FillZeroLengthArray(this->type->field(0)->type().get(),
                                    &this->child_data[0]);
    }

    if (type_id == Type::LIST) {
      const auto& list_scalar = checked_cast<const ListScalar&>(value);
      this->buffers[1] = OffsetsForScalar(list_scalar.scratch_space_, sizeof(int32_t));
    } else if (type_id == Type::MAP) {
      const auto& map_scalar = checked_cast<const MapScalar&>(value);
      this->buffers[1] = OffsetsForScalar(map_scalar.scratch_space_, sizeof(int32_t));
    } else if (type_id == Type::LARGE_LIST) {
      const auto& large_list_scalar = checked_cast<const LargeListScalar&>(value);
      this->buffers[1] =
          OffsetsForScalar(large_list_scalar.scratch_space_, sizeof(int64_t));
    } else if (type_id == Type::LIST_VIEW) {
      const auto& list_view_scalar = checked_cast<const ListViewScalar&>(value);
      std::tie(this->buffers[1], this->buffers[2]) =
          OffsetsAndSizesForScalar(list_view_scalar.scratch_space_, sizeof(int32_t));
    } else if (type_id == Type::LARGE_LIST_VIEW) {
      const auto& large_list_view_scalar =
          checked_cast<const LargeListViewScalar&>(value);
      std::tie(this->buffers[1], this->buffers[2]) = OffsetsAndSizesForScalar(
          large_list_view_scalar.scratch_space_, sizeof(int64_t));
    } else {
      DCHECK_EQ(type_id, Type::FIXED_SIZE_LIST);
      // FIXED_SIZE_LIST: does not have a second buffer
      this->buffers[1] = {};
    }
  } else if (type_id == Type::STRUCT) {
    const auto& scalar = checked_cast<const StructScalar&>(value);
    this->child_data.resize(this->type->num_fields());
    DCHECK_EQ(this->type->num_fields(), static_cast<int>(scalar.value.size()));
    for (size_t i = 0; i < scalar.value.size(); ++i) {
      this->child_data[i].FillFromScalar(*scalar.value[i]);
    }
  } else if (is_union(type_id)) {
    // First buffer is kept null since unions have no validity vector
    this->buffers[0] = {};

    this->child_data.resize(this->type->num_fields());
    if (type_id == Type::DENSE_UNION) {
      const auto& scalar = checked_cast<const DenseUnionScalar&>(value);
      auto* union_scratch_space =
          reinterpret_cast<UnionScalar::UnionScratchSpace*>(&scalar.scratch_space_);

      this->buffers[1].data = reinterpret_cast<uint8_t*>(&union_scratch_space->type_code);
      this->buffers[1].size = 1;

      this->buffers[2] = OffsetsForScalar(union_scratch_space->offsets, sizeof(int32_t));
      // We can't "see" the other arrays in the union, but we put the "active"
      // union array in the right place and fill zero-length arrays for the
      // others
      const auto& child_ids = checked_cast<const UnionType*>(this->type)->child_ids();
      DCHECK_GE(scalar.type_code, 0);
      DCHECK_LT(scalar.type_code, static_cast<int>(child_ids.size()));
      for (int i = 0; i < static_cast<int>(this->child_data.size()); ++i) {
        if (i == child_ids[scalar.type_code]) {
          this->child_data[i].FillFromScalar(*scalar.value);
        } else {
          internal::FillZeroLengthArray(this->type->field(i)->type().get(),
                                        &this->child_data[i]);
        }
      }
    } else {
      const auto& scalar = checked_cast<const SparseUnionScalar&>(value);
      auto* union_scratch_space =
          reinterpret_cast<UnionScalar::UnionScratchSpace*>(&scalar.scratch_space_);

      this->buffers[1].data = reinterpret_cast<uint8_t*>(&union_scratch_space->type_code);
      this->buffers[1].size = 1;

      // Sparse union scalars have a full complement of child values even
      // though only one of them is relevant, so we just fill them in here
      for (int i = 0; i < static_cast<int>(this->child_data.size()); ++i) {
        this->child_data[i].FillFromScalar(*scalar.value[i]);
      }
    }
  } else if (type_id == Type::EXTENSION) {
    // Pass through storage
    const auto& scalar = checked_cast<const ExtensionScalar&>(value);
    FillFromScalar(*scalar.value);

    // Restore the extension type
    this->type = value.type.get();
  } else if (type_id == Type::RUN_END_ENCODED) {
    const auto& scalar = checked_cast<const RunEndEncodedScalar&>(value);
    this->child_data.resize(2);

    auto set_run_end = [&](auto run_end) {
      auto& e = this->child_data[0];
      e.type = scalar.run_end_type().get();
      e.length = 1;
      e.null_count = 0;
      e.buffers[1].data = scalar.scratch_space_;
      e.buffers[1].size = sizeof(run_end);
    };

    switch (scalar.run_end_type()->id()) {
      case Type::INT16:
        set_run_end(static_cast<int16_t>(1));
        break;
      case Type::INT32:
        set_run_end(static_cast<int32_t>(1));
        break;
      default:
        DCHECK_EQ(scalar.run_end_type()->id(), Type::INT64);
        set_run_end(static_cast<int64_t>(1));
    }
    this->child_data[1].FillFromScalar(*scalar.value);
  } else {
    DCHECK_EQ(Type::NA, type_id) << "should be unreachable: " << *value.type;
  }
}

int64_t ArraySpan::GetNullCount() const {
  int64_t precomputed = this->null_count;
  if (ARROW_PREDICT_FALSE(precomputed == kUnknownNullCount)) {
    if (this->buffers[0].data != nullptr) {
      precomputed =
          this->length - CountSetBits(this->buffers[0].data, this->offset, this->length);
    } else {
      precomputed = 0;
    }
    this->null_count = precomputed;
  }
  return precomputed;
}

int64_t ArraySpan::ComputeLogicalNullCount() const {
  const auto t = this->type->id();
  if (t == Type::SPARSE_UNION) {
    return union_util::LogicalSparseUnionNullCount(*this);
  }
  if (t == Type::DENSE_UNION) {
    return union_util::LogicalDenseUnionNullCount(*this);
  }
  if (t == Type::RUN_END_ENCODED) {
    return ree_util::LogicalNullCount(*this);
  }
  if (t == Type::DICTIONARY) {
    return dict_util::LogicalNullCount(*this);
  }
  return GetNullCount();
}

int ArraySpan::num_buffers() const { return GetNumBuffers(*this->type); }

std::shared_ptr<ArrayData> ArraySpan::ToArrayData() const {
  auto result = std::make_shared<ArrayData>(this->type->GetSharedPtr(), this->length,
                                            this->null_count, this->offset);

  for (int i = 0; i < this->num_buffers(); ++i) {
    result->buffers.emplace_back(this->GetBuffer(i));
  }

  Type::type type_id = this->type->id();
  if (type_id == Type::EXTENSION) {
    const ExtensionType* ext_type = checked_cast<const ExtensionType*>(this->type);
    type_id = ext_type->storage_type()->id();
  }

  if (HasVariadicBuffers()) {
    DCHECK_EQ(result->buffers.size(), 3);
    result->buffers.pop_back();
    for (const auto& data_buffer : GetVariadicBuffers()) {
      result->buffers.push_back(data_buffer);
    }
  }

  if (type_id == Type::NA) {
    result->null_count = this->length;
  } else if (this->buffers[0].data == nullptr) {
    // No validity bitmap, so the null count is 0
    result->null_count = 0;
  }

  if (type_id == Type::DICTIONARY) {
    result->dictionary = this->dictionary().ToArrayData();
  } else {
    // Emit children, too
    for (size_t i = 0; i < this->child_data.size(); ++i) {
      result->child_data.push_back(this->child_data[i].ToArrayData());
    }
  }
  return result;
}

util::span<const std::shared_ptr<Buffer>> ArraySpan::GetVariadicBuffers() const {
  DCHECK(HasVariadicBuffers());
  return {buffers[2].data_as<std::shared_ptr<Buffer>>(),
          static_cast<size_t>(buffers[2].size) / sizeof(std::shared_ptr<Buffer>)};
}

bool ArraySpan::HasVariadicBuffers() const {
  return type->id() == Type::BINARY_VIEW || type->id() == Type::STRING_VIEW;
}

std::shared_ptr<Array> ArraySpan::ToArray() const {
  return MakeArray(this->ToArrayData());
}

bool ArraySpan::IsNullSparseUnion(int64_t i) const {
  auto* union_type = checked_cast<const SparseUnionType*>(this->type);
  const auto* types = reinterpret_cast<const int8_t*>(this->buffers[1].data);
  const int child_id = union_type->child_ids()[types[this->offset + i]];
  return this->child_data[child_id].IsNull(i);
}

bool ArraySpan::IsNullDenseUnion(int64_t i) const {
  auto* union_type = checked_cast<const DenseUnionType*>(this->type);
  const auto* types = reinterpret_cast<const int8_t*>(this->buffers[1].data);
  const auto* offsets = reinterpret_cast<const int32_t*>(this->buffers[2].data);
  const int64_t child_id = union_type->child_ids()[types[this->offset + i]];
  const int64_t child_offset = offsets[this->offset + i];
  return this->child_data[child_id].IsNull(child_offset);
}

bool ArraySpan::IsNullRunEndEncoded(int64_t i) const {
  const auto& values = ree_util::ValuesArray(*this);
  if (values.MayHaveLogicalNulls()) {
    const int64_t physical_offset = ree_util::FindPhysicalIndex(*this, i, this->offset);
    return ree_util::ValuesArray(*this).IsNull(physical_offset);
  }
  return false;
}

bool ArraySpan::UnionMayHaveLogicalNulls() const {
  for (auto& child : this->child_data) {
    if (child.MayHaveLogicalNulls()) {
      return true;
    }
  }
  return false;
}

bool ArraySpan::RunEndEncodedMayHaveLogicalNulls() const {
  return ree_util::ValuesArray(*this).MayHaveLogicalNulls();
}

bool ArraySpan::DictionaryMayHaveLogicalNulls() const {
  return this->GetNullCount() != 0 || this->dictionary().GetNullCount() != 0;
}

// ----------------------------------------------------------------------
// Implement internal::GetArrayView

namespace {

void AccumulateLayouts(const std::shared_ptr<DataType>& type,
                       std::vector<DataTypeLayout>* layouts) {
  layouts->push_back(type->layout());
  for (const auto& child : type->fields()) {
    AccumulateLayouts(child->type(), layouts);
  }
}

void AccumulateArrayData(const std::shared_ptr<ArrayData>& data,
                         std::vector<std::shared_ptr<ArrayData>>* out) {
  out->push_back(data);
  for (const auto& child : data->child_data) {
    AccumulateArrayData(child, out);
  }
}

struct ViewDataImpl {
  std::shared_ptr<DataType> root_in_type;
  std::shared_ptr<DataType> root_out_type;
  std::vector<DataTypeLayout> in_layouts;
  std::vector<std::shared_ptr<ArrayData>> in_data;
  int64_t in_data_length;
  size_t in_layout_idx = 0;
  size_t in_buffer_idx = 0;
  bool input_exhausted = false;

  Status InvalidView(const std::string& msg) {
    return Status::Invalid("Can't view array of type ", root_in_type->ToString(), " as ",
                           root_out_type->ToString(), ": ", msg);
  }

  void AdjustInputPointer() {
    if (input_exhausted) {
      return;
    }
    while (true) {
      // Skip exhausted layout (might be empty layout)
      while (in_buffer_idx >= in_layouts[in_layout_idx].buffers.size()) {
        in_buffer_idx = 0;
        ++in_layout_idx;
        if (in_layout_idx >= in_layouts.size()) {
          input_exhausted = true;
          return;
        }
      }
      const auto& in_spec = in_layouts[in_layout_idx].buffers[in_buffer_idx];
      if (in_spec.kind != DataTypeLayout::ALWAYS_NULL) {
        return;
      }
      // Skip always-null input buffers
      // (e.g. buffer 0 of a null type or buffer 2 of a sparse union)
      ++in_buffer_idx;
    }
  }

  Status CheckInputAvailable() {
    if (input_exhausted) {
      return InvalidView("not enough buffers for view type");
    }
    return Status::OK();
  }

  Status CheckInputExhausted() {
    if (!input_exhausted) {
      return InvalidView("too many buffers for view type");
    }
    return Status::OK();
  }

  Result<std::shared_ptr<ArrayData>> GetDictionaryView(const DataType& out_type) {
    if (in_data[in_layout_idx]->type->id() != Type::DICTIONARY) {
      return InvalidView("Cannot get view as dictionary type");
    }
    const auto& dict_out_type = static_cast<const DictionaryType&>(out_type);
    return internal::GetArrayView(in_data[in_layout_idx]->dictionary,
                                  dict_out_type.value_type());
  }

  Status MakeDataView(const std::shared_ptr<Field>& out_field,
                      std::shared_ptr<ArrayData>* out) {
    const auto& out_type = out_field->type();
    const auto out_layout = out_type->layout();

    AdjustInputPointer();
    int64_t out_length = in_data_length;
    int64_t out_offset = 0;
    int64_t out_null_count;

    std::shared_ptr<ArrayData> dictionary;
    if (out_type->id() == Type::DICTIONARY) {
      ARROW_ASSIGN_OR_RAISE(dictionary, GetDictionaryView(*out_type));
    }

    // No type has a purely empty layout
    DCHECK_GT(out_layout.buffers.size(), 0);

    std::vector<std::shared_ptr<Buffer>> out_buffers;

    // Process null bitmap
    if (in_buffer_idx == 0 && out_layout.buffers[0].kind == DataTypeLayout::BITMAP) {
      // Copy input null bitmap
      RETURN_NOT_OK(CheckInputAvailable());
      const auto& in_data_item = in_data[in_layout_idx];
      if (!out_field->nullable() && in_data_item->GetNullCount() != 0) {
        return InvalidView("nulls in input cannot be viewed as non-nullable");
      }
      DCHECK_GT(in_data_item->buffers.size(), in_buffer_idx);
      out_buffers.push_back(in_data_item->buffers[in_buffer_idx]);
      out_length = in_data_item->length;
      out_offset = in_data_item->offset;
      out_null_count = in_data_item->null_count;
      ++in_buffer_idx;
      AdjustInputPointer();
    } else {
      // No null bitmap in input, append no-nulls bitmap
      out_buffers.push_back(nullptr);
      if (out_type->id() == Type::NA) {
        out_null_count = out_length;
      } else {
        out_null_count = 0;
      }
    }

    // Process other buffers in output layout
    for (size_t out_buffer_idx = 1; out_buffer_idx < out_layout.buffers.size();
         ++out_buffer_idx) {
      const auto& out_spec = out_layout.buffers[out_buffer_idx];
      // If always-null buffer is expected, just construct it
      if (out_spec.kind == DataTypeLayout::ALWAYS_NULL) {
        out_buffers.push_back(nullptr);
        continue;
      }

      // If input buffer is null bitmap, try to ignore it
      while (in_buffer_idx == 0) {
        RETURN_NOT_OK(CheckInputAvailable());
        if (in_data[in_layout_idx]->GetNullCount() != 0) {
          return InvalidView("cannot represent nested nulls");
        }
        ++in_buffer_idx;
        AdjustInputPointer();
      }

      RETURN_NOT_OK(CheckInputAvailable());
      const auto& in_layout = in_layouts[in_layout_idx];
      const auto& in_spec = in_layout.buffers[in_buffer_idx];
      if (out_spec != in_spec) {
        return InvalidView("incompatible layouts");
      }
      // Copy input buffer
      const auto& in_data_item = in_data[in_layout_idx];
      out_length = in_data_item->length;
      out_offset = in_data_item->offset;
      DCHECK_GT(in_data_item->buffers.size(), in_buffer_idx);
      out_buffers.push_back(in_data_item->buffers[in_buffer_idx]);
      ++in_buffer_idx;

      if (in_buffer_idx == in_layout.buffers.size()) {
        if (out_layout.variadic_spec != in_layout.variadic_spec) {
          return InvalidView("incompatible layouts");
        }

        if (in_layout.variadic_spec) {
          for (; in_buffer_idx < in_data_item->buffers.size(); ++in_buffer_idx) {
            out_buffers.push_back(in_data_item->buffers[in_buffer_idx]);
          }
        }
      }
      AdjustInputPointer();
    }

    std::shared_ptr<ArrayData> out_data = ArrayData::Make(
        out_type, out_length, std::move(out_buffers), out_null_count, out_offset);
    out_data->dictionary = dictionary;

    // Process children recursively, depth-first
    for (const auto& child_field : out_type->fields()) {
      std::shared_ptr<ArrayData> child_data;
      RETURN_NOT_OK(MakeDataView(child_field, &child_data));
      out_data->child_data.push_back(std::move(child_data));
    }
    *out = std::move(out_data);
    return Status::OK();
  }
};

}  // namespace

namespace internal {

Result<std::shared_ptr<ArrayData>> GetArrayView(
    const std::shared_ptr<ArrayData>& data, const std::shared_ptr<DataType>& out_type) {
  ViewDataImpl impl;
  impl.root_in_type = data->type;
  impl.root_out_type = out_type;
  AccumulateLayouts(impl.root_in_type, &impl.in_layouts);
  AccumulateArrayData(data, &impl.in_data);
  impl.in_data_length = data->length;

  std::shared_ptr<ArrayData> out_data;
  // Dummy field for output type
  auto out_field = field("", out_type);
  RETURN_NOT_OK(impl.MakeDataView(out_field, &out_data));
  RETURN_NOT_OK(impl.CheckInputExhausted());
  return out_data;
}

}  // namespace internal
}  // namespace arrow
