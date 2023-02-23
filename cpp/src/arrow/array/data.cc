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
#include "arrow/scalar.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"
#include "arrow/util/slice_util_internal.h"

namespace arrow {

using internal::checked_cast;
using internal::CountSetBits;

static inline void AdjustNonNullable(Type::type type_id, int64_t length,
                                     std::vector<std::shared_ptr<Buffer>>* buffers,
                                     int64_t* null_count) {
  if (type_id == Type::NA) {
    *null_count = length;
    (*buffers)[0] = nullptr;
  } else if (internal::HasValidityBitmap(type_id)) {
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

std::shared_ptr<ArrayData> ArrayData::Slice(int64_t off, int64_t len) const {
  ARROW_CHECK_LE(off, length) << "Slice offset greater than array length";
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

  for (int i = 0; i < static_cast<int>(data.buffers.size()); ++i) {
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
  if (data.buffers[0] == nullptr && type_id != Type::NA &&
      type_id != Type::SPARSE_UNION && type_id != Type::DENSE_UNION) {
    // This should already be zero but we make for sure
    this->null_count = 0;
  }

  // Makes sure any other buffers are seen as null / non-existent
  for (int i = static_cast<int>(data.buffers.size()); i < 3; ++i) {
    this->buffers[i] = {};
  }

  if (this->type->id() == Type::DICTIONARY) {
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

template <typename offset_type>
void SetOffsetsForScalar(ArraySpan* span, offset_type* buffer, int64_t value_size,
                         int buffer_index = 1) {
  buffer[0] = 0;
  buffer[1] = static_cast<offset_type>(value_size);
  span->buffers[buffer_index].data = reinterpret_cast<uint8_t*>(buffer);
  span->buffers[buffer_index].size = 2 * sizeof(offset_type);
}

int GetNumBuffers(const DataType& type) {
  switch (type.id()) {
    case Type::NA:
    case Type::STRUCT:
    case Type::FIXED_SIZE_LIST:
      return 1;
    case Type::RUN_END_ENCODED:
      return 0;
    case Type::BINARY:
    case Type::LARGE_BINARY:
    case Type::STRING:
    case Type::LARGE_STRING:
    case Type::DENSE_UNION:
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
  memset(span->scratch_space, 0x00, sizeof(span->scratch_space));

  span->type = type;
  span->length = 0;
  int num_buffers = GetNumBuffers(*type);
  for (int i = 0; i < num_buffers; ++i) {
    span->buffers[i].data = reinterpret_cast<uint8_t*>(span->scratch_space);
    span->buffers[i].size = 0;
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

  // Populate null count and validity bitmap (only for non-union/null types)
  this->null_count = value.is_valid ? 0 : 1;
  if (!is_union(type_id) && type_id != Type::NA) {
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
    this->buffers[1].data = reinterpret_cast<uint8_t*>(this->scratch_space);
    const uint8_t* data_buffer = nullptr;
    int64_t data_size = 0;
    if (scalar.is_valid) {
      data_buffer = scalar.value->data();
      data_size = scalar.value->size();
    }
    if (is_binary_like(type_id)) {
      SetOffsetsForScalar<int32_t>(this, reinterpret_cast<int32_t*>(this->scratch_space),
                                   data_size);
    } else {
      // is_large_binary_like
      SetOffsetsForScalar<int64_t>(this, reinterpret_cast<int64_t*>(this->scratch_space),
                                   data_size);
    }
    this->buffers[2].data = const_cast<uint8_t*>(data_buffer);
    this->buffers[2].size = data_size;
  } else if (type_id == Type::FIXED_SIZE_BINARY) {
    const auto& scalar = checked_cast<const BaseBinaryScalar&>(value);
    this->buffers[1].data = const_cast<uint8_t*>(scalar.value->data());
    this->buffers[1].size = scalar.value->size();
  } else if (is_list_like(type_id)) {
    const auto& scalar = checked_cast<const BaseListScalar&>(value);

    int64_t value_length = 0;
    this->child_data.resize(1);
    if (scalar.value != nullptr) {
      // When the scalar is null, scalar.value can also be null
      this->child_data[0].SetMembers(*scalar.value->data());
      value_length = scalar.value->length();
    } else {
      // Even when the value is null, we still must populate the
      // child_data to yield a valid array. Tedious
      internal::FillZeroLengthArray(this->type->field(0)->type().get(),
                                    &this->child_data[0]);
    }

    if (type_id == Type::LIST || type_id == Type::MAP) {
      SetOffsetsForScalar<int32_t>(this, reinterpret_cast<int32_t*>(this->scratch_space),
                                   value_length);
    } else if (type_id == Type::LARGE_LIST) {
      SetOffsetsForScalar<int64_t>(this, reinterpret_cast<int64_t*>(this->scratch_space),
                                   value_length);
    } else {
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

    this->buffers[1].data = reinterpret_cast<uint8_t*>(this->scratch_space);
    this->buffers[1].size = 1;
    int8_t* type_codes = reinterpret_cast<int8_t*>(this->scratch_space);
    type_codes[0] = checked_cast<const UnionScalar&>(value).type_code;

    this->child_data.resize(this->type->num_fields());
    if (type_id == Type::DENSE_UNION) {
      const auto& scalar = checked_cast<const DenseUnionScalar&>(value);
      // Has offset; start 4 bytes in so it's aligned to a 32-bit boundaries
      SetOffsetsForScalar<int32_t>(this,
                                   reinterpret_cast<int32_t*>(this->scratch_space) + 1, 1,
                                   /*buffer_index=*/2);
      // We can't "see" the other arrays in the union, but we put the "active"
      // union array in the right place and fill zero-length arrays for the
      // others
      const std::vector<int>& child_ids =
          checked_cast<const UnionType*>(this->type)->child_ids();
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

int ArraySpan::num_buffers() const { return GetNumBuffers(*this->type); }

std::shared_ptr<ArrayData> ArraySpan::ToArrayData() const {
  auto result = std::make_shared<ArrayData>(this->type->GetSharedPtr(), this->length,
                                            this->null_count, this->offset);

  for (int i = 0; i < this->num_buffers(); ++i) {
    result->buffers.emplace_back(this->GetBuffer(i));
  }

  if (this->type->id() == Type::NA) {
    result->null_count = this->length;
  } else if (this->buffers[0].data == nullptr) {
    // No validity bitmap, so the null count is 0
    result->null_count = 0;
  }

  // TODO(wesm): what about extension arrays?

  if (this->type->id() == Type::DICTIONARY) {
    result->dictionary = this->dictionary().ToArrayData();
  } else {
    // Emit children, too
    for (size_t i = 0; i < this->child_data.size(); ++i) {
      result->child_data.push_back(this->child_data[i].ToArrayData());
    }
  }
  return result;
}

std::shared_ptr<Array> ArraySpan::ToArray() const {
  return MakeArray(this->ToArrayData());
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
      const auto& in_spec = in_layouts[in_layout_idx].buffers[in_buffer_idx];
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
