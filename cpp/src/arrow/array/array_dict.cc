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

#include "arrow/array/array_dict.h"

#include <algorithm>
#include <climits>
#include <cstdint>
#include <limits>
#include <utility>
#include <vector>

#include "arrow/array/array_primitive.h"
#include "arrow/array/data.h"
#include "arrow/array/dict_internal.h"
#include "arrow/array/util.h"
#include "arrow/buffer.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/int_util.h"
#include "arrow/util/logging.h"
#include "arrow/visitor_inline.h"

namespace arrow {

using internal::checked_cast;
using internal::CopyBitmap;

// ----------------------------------------------------------------------
// DictionaryArray

/// \brief Perform validation check to determine if all dictionary indices
/// are within valid range (0 <= index < upper_bound)
///
/// \param[in] indices array of dictionary indices
/// \param[in] upper_bound upper bound of valid range for indices
/// \return Status
template <typename ArrowType>
Status ValidateDictionaryIndices(const std::shared_ptr<Array>& indices,
                                 const int64_t upper_bound) {
  using ArrayType = typename TypeTraits<ArrowType>::ArrayType;
  const auto& array = checked_cast<const ArrayType&>(*indices);
  const typename ArrowType::c_type* data = array.raw_values();
  const int64_t size = array.length();

  if (array.null_count() == 0) {
    for (int64_t idx = 0; idx < size; ++idx) {
      if (data[idx] < 0 || data[idx] >= upper_bound) {
        return Status::Invalid("Dictionary has out-of-bound index [0, dict.length)");
      }
    }
  } else {
    for (int64_t idx = 0; idx < size; ++idx) {
      if (!array.IsNull(idx)) {
        if (data[idx] < 0 || data[idx] >= upper_bound) {
          return Status::Invalid("Dictionary has out-of-bound index [0, dict.length)");
        }
      }
    }
  }

  return Status::OK();
}

std::shared_ptr<Array> DictionaryArray::indices() const { return indices_; }

int64_t DictionaryArray::GetValueIndex(int64_t i) const {
  switch (indices_->type_id()) {
    case Type::INT8:
      return checked_cast<const Int8Array&>(*indices_).Value(i);
    case Type::INT16:
      return checked_cast<const Int16Array&>(*indices_).Value(i);
    case Type::INT32:
      return checked_cast<const Int32Array&>(*indices_).Value(i);
    case Type::INT64:
      return checked_cast<const Int64Array&>(*indices_).Value(i);
    default:
      break;
  }
  ARROW_CHECK(false) << "unreachable";
  return -1;
}

DictionaryArray::DictionaryArray(const std::shared_ptr<ArrayData>& data)
    : dict_type_(checked_cast<const DictionaryType*>(data->type.get())) {
  ARROW_CHECK_EQ(data->type->id(), Type::DICTIONARY);
  ARROW_CHECK_NE(data->dictionary, nullptr);
  SetData(data);
}

void DictionaryArray::SetData(const std::shared_ptr<ArrayData>& data) {
  this->Array::SetData(data);
  auto indices_data = data_->Copy();
  indices_data->type = dict_type_->index_type();
  indices_data->dictionary = nullptr;
  indices_ = MakeArray(indices_data);
}

DictionaryArray::DictionaryArray(const std::shared_ptr<DataType>& type,
                                 const std::shared_ptr<Array>& indices,
                                 const std::shared_ptr<Array>& dictionary)
    : dict_type_(checked_cast<const DictionaryType*>(type.get())) {
  ARROW_CHECK_EQ(type->id(), Type::DICTIONARY);
  ARROW_CHECK_EQ(indices->type_id(), dict_type_->index_type()->id());
  ARROW_CHECK_EQ(dict_type_->value_type()->id(), dictionary->type()->id());
  DCHECK(dict_type_->value_type()->Equals(*dictionary->type()));
  auto data = indices->data()->Copy();
  data->type = type;
  data->dictionary = dictionary->data();
  SetData(data);
}

std::shared_ptr<Array> DictionaryArray::dictionary() const {
  if (!dictionary_) {
    dictionary_ = MakeArray(data_->dictionary);
  }
  return dictionary_;
}

Result<std::shared_ptr<Array>> DictionaryArray::FromArrays(
    const std::shared_ptr<DataType>& type, const std::shared_ptr<Array>& indices,
    const std::shared_ptr<Array>& dictionary) {
  if (type->id() != Type::DICTIONARY) {
    return Status::TypeError("Expected a dictionary type");
  }
  const auto& dict = checked_cast<const DictionaryType&>(*type);
  ARROW_CHECK_EQ(indices->type_id(), dict.index_type()->id());

  int64_t upper_bound = dictionary->length();
  Status is_valid;

  switch (indices->type_id()) {
    case Type::INT8:
      is_valid = ValidateDictionaryIndices<Int8Type>(indices, upper_bound);
      break;
    case Type::INT16:
      is_valid = ValidateDictionaryIndices<Int16Type>(indices, upper_bound);
      break;
    case Type::INT32:
      is_valid = ValidateDictionaryIndices<Int32Type>(indices, upper_bound);
      break;
    case Type::INT64:
      is_valid = ValidateDictionaryIndices<Int64Type>(indices, upper_bound);
      break;
    default:
      return Status::NotImplemented("Dictionary index type not supported: ",
                                    indices->type()->ToString());
  }
  RETURN_NOT_OK(is_valid);
  return std::make_shared<DictionaryArray>(type, indices, dictionary);
}

bool DictionaryArray::CanCompareIndices(const DictionaryArray& other) const {
  DCHECK(dictionary()->type()->Equals(other.dictionary()->type()))
      << "dictionaries have differing type " << *dictionary()->type() << " vs "
      << *other.dictionary()->type();

  if (!indices()->type()->Equals(other.indices()->type())) {
    return false;
  }

  auto min_length = std::min(dictionary()->length(), other.dictionary()->length());
  return dictionary()->RangeEquals(other.dictionary(), 0, min_length, 0);
}

// ----------------------------------------------------------------------
// DictionaryType unification

template <typename T>
class DictionaryUnifierImpl : public DictionaryUnifier {
 public:
  using ArrayType = typename TypeTraits<T>::ArrayType;
  using DictTraits = typename internal::DictionaryTraits<T>;
  using MemoTableType = typename DictTraits::MemoTableType;

  DictionaryUnifierImpl(MemoryPool* pool, std::shared_ptr<DataType> value_type)
      : pool_(pool), value_type_(value_type), memo_table_(pool) {}

  Status Unify(const Array& dictionary, std::shared_ptr<Buffer>* out) override {
    if (dictionary.null_count() > 0) {
      return Status::Invalid("Cannot yet unify dictionaries with nulls");
    }
    if (!dictionary.type()->Equals(*value_type_)) {
      return Status::Invalid("Dictionary type different from unifier: ",
                             dictionary.type()->ToString());
    }
    const ArrayType& values = checked_cast<const ArrayType&>(dictionary);
    if (out != nullptr) {
      ARROW_ASSIGN_OR_RAISE(auto result,
                            AllocateBuffer(dictionary.length() * sizeof(int32_t), pool_));
      auto result_raw = reinterpret_cast<int32_t*>(result->mutable_data());
      for (int64_t i = 0; i < values.length(); ++i) {
        RETURN_NOT_OK(memo_table_.GetOrInsert(values.GetView(i), &result_raw[i]));
      }
      *out = std::move(result);
    } else {
      for (int64_t i = 0; i < values.length(); ++i) {
        int32_t unused_memo_index;
        RETURN_NOT_OK(memo_table_.GetOrInsert(values.GetView(i), &unused_memo_index));
      }
    }
    return Status::OK();
  }

  Status Unify(const Array& dictionary) override { return Unify(dictionary, nullptr); }

  Status GetResult(std::shared_ptr<DataType>* out_type,
                   std::shared_ptr<Array>* out_dict) override {
    int64_t dict_length = memo_table_.size();
    std::shared_ptr<DataType> index_type;
    if (dict_length <= std::numeric_limits<int8_t>::max()) {
      index_type = int8();
    } else if (dict_length <= std::numeric_limits<int16_t>::max()) {
      index_type = int16();
    } else if (dict_length <= std::numeric_limits<int32_t>::max()) {
      index_type = int32();
    } else {
      index_type = int64();
    }
    // Build unified dictionary type with the right index type
    *out_type = arrow::dictionary(index_type, value_type_);

    // Build unified dictionary array
    std::shared_ptr<ArrayData> data;
    RETURN_NOT_OK(DictTraits::GetDictionaryArrayData(pool_, value_type_, memo_table_,
                                                     0 /* start_offset */, &data));
    *out_dict = MakeArray(data);
    return Status::OK();
  }

 private:
  MemoryPool* pool_;
  std::shared_ptr<DataType> value_type_;
  MemoTableType memo_table_;
};

struct MakeUnifier {
  MemoryPool* pool;
  std::shared_ptr<DataType> value_type;
  std::unique_ptr<DictionaryUnifier> result;

  MakeUnifier(MemoryPool* pool, std::shared_ptr<DataType> value_type)
      : pool(pool), value_type(value_type) {}

  template <typename T>
  enable_if_no_memoize<T, Status> Visit(const T&) {
    // Default implementation for non-dictionary-supported datatypes
    return Status::NotImplemented("Unification of ", value_type,
                                  " dictionaries is not implemented");
  }

  template <typename T>
  enable_if_memoize<T, Status> Visit(const T&) {
    result.reset(new DictionaryUnifierImpl<T>(pool, value_type));
    return Status::OK();
  }
};

Result<std::unique_ptr<DictionaryUnifier>> DictionaryUnifier::Make(
    std::shared_ptr<DataType> value_type, MemoryPool* pool) {
  MakeUnifier maker(pool, value_type);
  RETURN_NOT_OK(VisitTypeInline(*value_type, &maker));
  return std::move(maker.result);
}

// ----------------------------------------------------------------------
// DictionaryArray transposition

namespace {

inline bool IsTrivialTransposition(const int32_t* transpose_map,
                                   int64_t input_dict_size) {
  for (int64_t i = 0; i < input_dict_size; ++i) {
    if (transpose_map[i] != i) {
      return false;
    }
  }
  return true;
}

template <typename InType, typename OutType>
Result<std::shared_ptr<Array>> TransposeDictIndices(
    MemoryPool* pool, const ArrayData& in_data, const int32_t* transpose_map,
    const std::shared_ptr<ArrayData>& out_data) {
  using in_c_type = typename InType::c_type;
  using out_c_type = typename OutType::c_type;
  internal::TransposeInts(in_data.GetValues<in_c_type>(1),
                          out_data->GetMutableValues<out_c_type>(1), in_data.length,
                          transpose_map);
  return MakeArray(out_data);
}

}  // namespace

Result<std::shared_ptr<Array>> DictionaryArray::Transpose(
    const std::shared_ptr<DataType>& type, const std::shared_ptr<Array>& dictionary,
    const int32_t* transpose_map, MemoryPool* pool) const {
  if (type->id() != Type::DICTIONARY) {
    return Status::TypeError("Expected dictionary type");
  }
  const int64_t in_dict_len = data_->dictionary->length;
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
    out_data->dictionary = dictionary->data();
    return MakeArray(out_data);
  }

  // Default path: compute a buffer of transposed indices.
  ARROW_ASSIGN_OR_RAISE(
      auto out_buffer,
      AllocateBuffer(data_->length * out_index_type.bit_width() * CHAR_BIT, pool));

  // Shift null buffer if the original offset is non-zero
  std::shared_ptr<Buffer> null_bitmap;
  if (data_->offset != 0 && data_->null_count != 0) {
    ARROW_ASSIGN_OR_RAISE(
        null_bitmap, CopyBitmap(pool, null_bitmap_data_, data_->offset, data_->length));
  } else {
    null_bitmap = data_->buffers[0];
  }

  auto out_data = ArrayData::Make(
      type, data_->length, {null_bitmap, std::move(out_buffer)}, data_->null_count);
  out_data->dictionary = dictionary->data();

#define TRANSPOSE_IN_OUT_CASE(IN_INDEX_TYPE, OUT_INDEX_TYPE)                 \
  case OUT_INDEX_TYPE::type_id:                                              \
    return TransposeDictIndices<IN_INDEX_TYPE, OUT_INDEX_TYPE>(pool, *data_, \
                                                               transpose_map, out_data);

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
