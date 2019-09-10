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

#include "arrow/array/dict_internal.h"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <type_traits>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/hashing.h"
#include "arrow/util/int_util.h"
#include "arrow/util/logging.h"
#include "arrow/visitor_inline.h"

namespace arrow {

using internal::checked_cast;
using internal::CopyBitmap;

// ----------------------------------------------------------------------
// DictionaryType unification

struct UnifyDictionaryValues {
  MemoryPool* pool_;
  std::shared_ptr<DataType> value_type_;
  const std::vector<const DictionaryType*>& types_;
  const std::vector<const Array*>& dictionaries_;
  std::shared_ptr<Array>* out_values_;
  std::vector<std::vector<int32_t>>* out_transpose_maps_;

  template <typename T>
  enable_if_no_memoize<T, Status> Visit(const T&) {
    // Default implementation for non-dictionary-supported datatypes
    return Status::NotImplemented("Unification of ", value_type_,
                                  " dictionaries is not implemented");
  }

  template <typename T>
  enable_if_memoize<T, Status> Visit(const T&) {
    using ArrayType = typename TypeTraits<T>::ArrayType;
    using DictTraits = typename internal::DictionaryTraits<T>;
    using MemoTableType = typename DictTraits::MemoTableType;

    MemoTableType memo_table(pool_);
    if (out_transpose_maps_ != nullptr) {
      out_transpose_maps_->clear();
      out_transpose_maps_->reserve(types_.size());
    }
    // Build up the unified dictionary values and the transpose maps
    for (size_t i = 0; i < types_.size(); ++i) {
      const ArrayType& values = checked_cast<const ArrayType&>(*dictionaries_[i]);
      if (out_transpose_maps_ != nullptr) {
        std::vector<int32_t> transpose_map;
        transpose_map.reserve(values.length());
        for (int64_t i = 0; i < values.length(); ++i) {
          int32_t dict_index = memo_table.GetOrInsert(values.GetView(i));
          transpose_map.push_back(dict_index);
        }
        out_transpose_maps_->push_back(std::move(transpose_map));
      } else {
        for (int64_t i = 0; i < values.length(); ++i) {
          memo_table.GetOrInsert(values.GetView(i));
        }
      }
    }
    // Build unified dictionary array
    std::shared_ptr<ArrayData> data;
    RETURN_NOT_OK(DictTraits::GetDictionaryArrayData(pool_, value_type_, memo_table,
                                                     0 /* start_offset */, &data));
    *out_values_ = MakeArray(data);
    return Status::OK();
  }
};

Status DictionaryType::Unify(MemoryPool* pool, const std::vector<const DataType*>& types,
                             const std::vector<const Array*>& dictionaries,
                             std::shared_ptr<DataType>* out_type,
                             std::shared_ptr<Array>* out_dictionary,
                             std::vector<std::vector<int32_t>>* out_transpose_maps) {
  if (types.size() == 0) {
    return Status::Invalid("need at least one input type");
  }

  if (types.size() != dictionaries.size()) {
    return Status::Invalid("expecting the same number of types and dictionaries");
  }

  std::vector<const DictionaryType*> dict_types;
  dict_types.reserve(types.size());
  for (const auto& type : types) {
    if (type->id() != Type::DICTIONARY) {
      return Status::TypeError("input types must be dictionary types");
    }
    dict_types.push_back(checked_cast<const DictionaryType*>(type));
  }

  // XXX Should we check the ordered flag?
  auto value_type = dict_types[0]->value_type();
  for (size_t i = 0; i < types.size(); ++i) {
    if (!(dictionaries[i]->type()->Equals(*value_type) &&
          dict_types[i]->value_type()->Equals(*value_type))) {
      return Status::TypeError("dictionary value types were not all consistent");
    }
    if (dictionaries[i]->null_count() != 0) {
      return Status::TypeError("input types have null values");
    }
  }

  std::shared_ptr<Array> values;
  {
    UnifyDictionaryValues visitor{pool,         value_type, dict_types,
                                  dictionaries, &values,    out_transpose_maps};
    RETURN_NOT_OK(VisitTypeInline(*value_type, &visitor));
  }

  // Build unified dictionary type with the right index type
  std::shared_ptr<DataType> index_type;
  if (values->length() <= std::numeric_limits<int8_t>::max()) {
    index_type = int8();
  } else if (values->length() <= std::numeric_limits<int16_t>::max()) {
    index_type = int16();
  } else if (values->length() <= std::numeric_limits<int32_t>::max()) {
    index_type = int32();
  } else {
    index_type = int64();
  }
  *out_type = arrow::dictionary(index_type, values->type());
  *out_dictionary = values;
  return Status::OK();
}

// ----------------------------------------------------------------------
// DictionaryArray transposition

static bool IsTrivialTransposition(const std::vector<int32_t>& transpose_map,
                                   int64_t input_dict_size) {
  for (int64_t i = 0; i < input_dict_size; ++i) {
    if (transpose_map[i] != i) {
      return false;
    }
  }
  return true;
}

template <typename InType, typename OutType>
static Status TransposeDictIndices(MemoryPool* pool, const ArrayData& in_data,
                                   const std::vector<int32_t>& transpose_map,
                                   const std::shared_ptr<ArrayData>& out_data,
                                   std::shared_ptr<Array>* out) {
  using in_c_type = typename InType::c_type;
  using out_c_type = typename OutType::c_type;
  internal::TransposeInts(in_data.GetValues<in_c_type>(1),
                          out_data->GetMutableValues<out_c_type>(1), in_data.length,
                          transpose_map.data());
  *out = MakeArray(out_data);
  return Status::OK();
}

Status DictionaryArray::Transpose(MemoryPool* pool, const std::shared_ptr<DataType>& type,
                                  const std::shared_ptr<Array>& dictionary,
                                  const std::vector<int32_t>& transpose_map,
                                  std::shared_ptr<Array>* out) const {
  if (type->id() != Type::DICTIONARY) {
    return Status::TypeError("Expected dictionary type");
  }
  const int64_t in_dict_len = data_->dictionary->length();
  if (in_dict_len > static_cast<int64_t>(transpose_map.size())) {
    return Status::Invalid(
        "Transpose map too small for dictionary array "
        "(has ",
        transpose_map.size(), " values, need at least ", in_dict_len, ")");
  }

  const auto& out_dict_type = checked_cast<const DictionaryType&>(*type);

  const auto& out_index_type =
      static_cast<const FixedWidthType&>(*out_dict_type.index_type());

  auto in_type_id = dict_type_->index_type()->id();
  auto out_type_id = out_index_type.id();

  if (in_type_id == out_type_id && IsTrivialTransposition(transpose_map, in_dict_len)) {
    // Index type and values will be identical => we can simply reuse
    // the existing buffers.
    auto out_data =
        ArrayData::Make(type, data_->length, {data_->buffers[0], data_->buffers[1]},
                        data_->null_count, data_->offset);
    out_data->dictionary = dictionary;
    *out = MakeArray(out_data);
    return Status::OK();
  }

  // Default path: compute a buffer of transposed indices.
  std::shared_ptr<Buffer> out_buffer;
  RETURN_NOT_OK(AllocateBuffer(
      pool, data_->length * out_index_type.bit_width() * CHAR_BIT, &out_buffer));

  // Shift null buffer if the original offset is non-zero
  std::shared_ptr<Buffer> null_bitmap;
  if (data_->offset != 0) {
    RETURN_NOT_OK(
        CopyBitmap(pool, null_bitmap_data_, data_->offset, data_->length, &null_bitmap));
  } else {
    null_bitmap = data_->buffers[0];
  }

  auto out_data =
      ArrayData::Make(type, data_->length, {null_bitmap, out_buffer}, data_->null_count);
  out_data->dictionary = dictionary;

#define TRANSPOSE_IN_OUT_CASE(IN_INDEX_TYPE, OUT_INDEX_TYPE)    \
  case OUT_INDEX_TYPE::type_id:                                 \
    return TransposeDictIndices<IN_INDEX_TYPE, OUT_INDEX_TYPE>( \
        pool, *data_, transpose_map, out_data, out);

#define TRANSPOSE_IN_CASE(IN_INDEX_TYPE)                        \
  case IN_INDEX_TYPE::type_id:                                  \
    switch (out_type_id) {                                      \
      TRANSPOSE_IN_OUT_CASE(IN_INDEX_TYPE, Int8Type)            \
      TRANSPOSE_IN_OUT_CASE(IN_INDEX_TYPE, Int16Type)           \
      TRANSPOSE_IN_OUT_CASE(IN_INDEX_TYPE, Int32Type)           \
      TRANSPOSE_IN_OUT_CASE(IN_INDEX_TYPE, Int64Type)           \
      default:                                                  \
        return Status::NotImplemented("unexpected index type"); \
    }

  switch (in_type_id) {
    TRANSPOSE_IN_CASE(Int8Type)
    TRANSPOSE_IN_CASE(Int16Type)
    TRANSPOSE_IN_CASE(Int32Type)
    TRANSPOSE_IN_CASE(Int64Type)
    default:
      return Status::NotImplemented("unexpected index type");
  }
#undef TRANSPOSE_IN_CASE
#undef TRANSPOSE_IN_OUT_CASE
}

}  // namespace arrow
