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
#include "arrow/chunked_array.h"
#include "arrow/compute/api.h"
#include "arrow/datum.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/int_util.h"
#include "arrow/util/logging.h"
#include "arrow/visit_type_inline.h"

namespace arrow {

using internal::checked_cast;
using internal::CopyBitmap;

// ----------------------------------------------------------------------
// DictionaryArray

const std::shared_ptr<Array>& DictionaryArray::indices() const { return indices_; }

int64_t DictionaryArray::GetValueIndex(int64_t i) const {
  const uint8_t* indices_data = data_->buffers[1]->data();
  // If the value is non-negative then we can use the unsigned path
  switch (indices_->type_id()) {
    case Type::UINT8:
    case Type::INT8:
      return static_cast<int64_t>(indices_data[data_->offset + i]);
    case Type::UINT16:
    case Type::INT16:
      return static_cast<int64_t>(
          reinterpret_cast<const uint16_t*>(indices_data)[data_->offset + i]);
    case Type::UINT32:
    case Type::INT32:
      return static_cast<int64_t>(
          reinterpret_cast<const uint32_t*>(indices_data)[data_->offset + i]);
    case Type::UINT64:
    case Type::INT64:
      return static_cast<int64_t>(
          reinterpret_cast<const uint64_t*>(indices_data)[data_->offset + i]);
    default:
      ARROW_CHECK(false) << "unreachable";
      return -1;
  }
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

const std::shared_ptr<Array>& DictionaryArray::dictionary() const {
  if (!dictionary_) {
    // TODO(GH-36503) this isn't thread safe
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
  if (indices->type_id() != dict.index_type()->id()) {
    return Status::TypeError(
        "Dictionary type's index type does not match "
        "indices array's type");
  }
  RETURN_NOT_OK(internal::CheckIndexBounds(*indices->data(),
                                           static_cast<uint64_t>(dictionary->length())));
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
// Dictionary transposition

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

Result<std::shared_ptr<ArrayData>> TransposeDictIndices(
    const std::shared_ptr<ArrayData>& data, const std::shared_ptr<DataType>& in_type,
    const std::shared_ptr<DataType>& out_type,
    const std::shared_ptr<ArrayData>& dictionary, const int32_t* transpose_map,
    MemoryPool* pool) {
  // Note that in_type may be different from data->type if data is of type ExtensionType
  if (in_type->id() != Type::DICTIONARY || out_type->id() != Type::DICTIONARY) {
    return Status::TypeError("Expected dictionary type");
  }
  const int64_t in_dict_len = data->dictionary->length;
  const auto& in_dict_type = checked_cast<const DictionaryType&>(*in_type);
  const auto& out_dict_type = checked_cast<const DictionaryType&>(*out_type);

  const auto& in_index_type = *in_dict_type.index_type();
  const auto& out_index_type =
      checked_cast<const FixedWidthType&>(*out_dict_type.index_type());

  if (in_index_type.id() == out_index_type.id() &&
      IsTrivialTransposition(transpose_map, in_dict_len)) {
    // Index type and values will be identical => we can simply reuse
    // the existing buffers.
    auto out_data =
        ArrayData::Make(out_type, data->length, {data->buffers[0], data->buffers[1]},
                        data->null_count, data->offset);
    out_data->dictionary = dictionary;
    return out_data;
  }

  // Default path: compute a buffer of transposed indices.
  ARROW_ASSIGN_OR_RAISE(
      auto out_buffer,
      AllocateBuffer(data->length * (out_index_type.bit_width() / CHAR_BIT), pool));

  // Shift null buffer if the original offset is non-zero
  std::shared_ptr<Buffer> null_bitmap;
  if (data->offset != 0 && data->null_count != 0) {
    ARROW_ASSIGN_OR_RAISE(null_bitmap, CopyBitmap(pool, data->buffers[0]->data(),
                                                  data->offset, data->length));
  } else {
    null_bitmap = data->buffers[0];
  }

  auto out_data = ArrayData::Make(out_type, data->length,
                                  {null_bitmap, std::move(out_buffer)}, data->null_count);
  out_data->dictionary = dictionary;
  RETURN_NOT_OK(internal::TransposeInts(
      in_index_type, out_index_type, data->GetValues<uint8_t>(1, 0),
      out_data->GetMutableValues<uint8_t>(1, 0), data->offset, out_data->offset,
      data->length, transpose_map));
  return out_data;
}

struct CompactTransposeMapVisitor {
  const std::shared_ptr<ArrayData>& data;
  arrow::MemoryPool* pool;
  std::unique_ptr<Buffer> output_map;
  std::shared_ptr<Array> out_compact_dictionary;

  template <typename IndexArrowType>
  Status CompactTransposeMapImpl() {
    int64_t index_length = data->length;
    int64_t dict_length = data->dictionary->length;
    if (dict_length == 0) {
      output_map = nullptr;
      out_compact_dictionary = nullptr;
      return Status::OK();
    } else if (index_length == 0) {
      ARROW_ASSIGN_OR_RAISE(out_compact_dictionary,
                            MakeEmptyArray(data->dictionary->type, pool));
      ARROW_ASSIGN_OR_RAISE(output_map, AllocateBuffer(0, pool))
      return Status::OK();
    }

    using CType = typename IndexArrowType::c_type;
    const CType* indices_data = data->GetValues<CType>(1);
    std::vector<bool> dict_used(dict_length, false);
    CType dict_len = static_cast<CType>(dict_length);
    int64_t dict_used_count = 0;
    for (int64_t i = 0; i < index_length; i++) {
      if (data->IsNull(i)) {
        continue;
      }

      CType current_index = indices_data[i];
      if (current_index < 0 || current_index >= dict_len) {
        return Status::IndexError(
            "Index out of bounds while compacting dictionary array: ", current_index,
            "(dictionary is ", dict_length, " long) at position ", i);
      }
      if (dict_used[current_index]) continue;
      dict_used[current_index] = true;
      dict_used_count++;

      if (dict_used_count == dict_length) {
        // The dictionary is already compact, so just return here
        output_map = nullptr;
        out_compact_dictionary = nullptr;
        return Status::OK();
      }
    }

    using BuilderType = NumericBuilder<IndexArrowType>;
    using arrow::compute::Take;
    using arrow::compute::TakeOptions;
    BuilderType dict_indices_builder(pool);
    ARROW_RETURN_NOT_OK(dict_indices_builder.Reserve(dict_used_count));
    ARROW_ASSIGN_OR_RAISE(output_map,
                          AllocateBuffer(dict_length * sizeof(int32_t), pool));
    auto* output_map_raw = output_map->mutable_data_as<int32_t>();
    int32_t current_index = 0;
    for (CType i = 0; i < dict_len; i++) {
      if (dict_used[i]) {
        dict_indices_builder.UnsafeAppend(i);
        output_map_raw[i] = current_index;
        current_index++;
      } else {
        output_map_raw[i] = -1;
      }
    }
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> compacted_dict_indices,
                          dict_indices_builder.Finish());
    ARROW_ASSIGN_OR_RAISE(auto compacted_dict_res,
                          Take(Datum(data->dictionary), compacted_dict_indices,
                               TakeOptions::NoBoundsCheck()));
    out_compact_dictionary = compacted_dict_res.make_array();
    return Status::OK();
  }

  template <typename Type>
  enable_if_integer<Type, Status> Visit(const Type&) {
    return CompactTransposeMapImpl<Type>();
  }

  Status Visit(const DataType& type) {
    return Status::TypeError("Expected an Index Type of Int or UInt");
  }
};

Result<std::unique_ptr<Buffer>> CompactTransposeMap(
    const std::shared_ptr<ArrayData>& data, MemoryPool* pool,
    std::shared_ptr<Array>& out_compact_dictionary) {
  if (data->type->id() != Type::DICTIONARY) {
    return Status::TypeError("Expected dictionary type");
  }

  const auto& dict_type = checked_cast<const DictionaryType&>(*data->type);
  CompactTransposeMapVisitor visitor{data, pool, nullptr, nullptr};
  RETURN_NOT_OK(VisitTypeInline(*dict_type.index_type(), &visitor));

  out_compact_dictionary = visitor.out_compact_dictionary;
  return std::move(visitor.output_map);
}
}  // namespace

Result<std::shared_ptr<Array>> DictionaryArray::Transpose(
    const std::shared_ptr<DataType>& type, const std::shared_ptr<Array>& dictionary,
    const int32_t* transpose_map, MemoryPool* pool) const {
  ARROW_ASSIGN_OR_RAISE(auto transposed,
                        TransposeDictIndices(data_, data_->type, type, dictionary->data(),
                                             transpose_map, pool));
  return MakeArray(std::move(transposed));
}

Result<std::shared_ptr<Array>> DictionaryArray::Compact(MemoryPool* pool) const {
  std::shared_ptr<Array> compact_dictionary;
  ARROW_ASSIGN_OR_RAISE(std::unique_ptr<Buffer> transpose_map,
                        CompactTransposeMap(this->data_, pool, compact_dictionary));

  if (transpose_map == nullptr) {
    return std::make_shared<DictionaryArray>(this->data_);
  } else {
    return this->Transpose(this->type(), compact_dictionary,
                           transpose_map->data_as<int32_t>(), pool);
  }
}

// ----------------------------------------------------------------------
// Dictionary unification

namespace {

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
    ARROW_ASSIGN_OR_RAISE(
        auto data, DictTraits::GetDictionaryArrayData(pool_, value_type_, memo_table_,
                                                      0 /* start_offset */));
    *out_dict = MakeArray(data);
    return Status::OK();
  }

  Status GetResultWithIndexType(const std::shared_ptr<DataType>& index_type,
                                std::shared_ptr<Array>* out_dict) override {
    Int64Scalar dict_length(memo_table_.size());
    if (!internal::IntegersCanFit(dict_length, *index_type).ok()) {
      return Status::Invalid(
          "These dictionaries cannot be combined.  The unified dictionary requires a "
          "larger index type.");
    }

    // Build unified dictionary array
    ARROW_ASSIGN_OR_RAISE(
        auto data, DictTraits::GetDictionaryArrayData(pool_, value_type_, memo_table_,
                                                      0 /* start_offset */));
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
    return Status::NotImplemented("Unification of ", *value_type,
                                  " dictionaries is not implemented");
  }

  template <typename T>
  enable_if_memoize<T, Status> Visit(const T&) {
    result.reset(new DictionaryUnifierImpl<T>(pool, value_type));
    return Status::OK();
  }
};

struct RecursiveUnifier {
  MemoryPool* pool;

  // Return true if any of the arrays was changed (including descendents)
  Result<bool> Unify(std::shared_ptr<DataType> type, ArrayDataVector* chunks) {
    DCHECK(!chunks->empty());
    bool changed = false;
    std::shared_ptr<DataType> ext_type = nullptr;

    if (type->id() == Type::EXTENSION) {
      ext_type = std::move(type);
      type = checked_cast<const ExtensionType&>(*ext_type).storage_type();
    }

    // Unify all child dictionaries (if any)
    if (type->num_fields() > 0) {
      ArrayDataVector children(chunks->size());
      for (int i = 0; i < type->num_fields(); ++i) {
        std::transform(chunks->begin(), chunks->end(), children.begin(),
                       [i](const std::shared_ptr<ArrayData>& array) {
                         return array->child_data[i];
                       });
        ARROW_ASSIGN_OR_RAISE(bool child_changed,
                              Unify(type->field(i)->type(), &children));
        if (child_changed) {
          // Only do this when unification actually occurred
          for (size_t j = 0; j < chunks->size(); ++j) {
            (*chunks)[j]->child_data[i] = std::move(children[j]);
          }
          changed = true;
        }
      }
    }

    // Unify this dictionary
    if (type->id() == Type::DICTIONARY) {
      const auto& dict_type = checked_cast<const DictionaryType&>(*type);
      // XXX Ideally, we should unify dictionaries nested in value_type first,
      // but DictionaryUnifier doesn't supported nested dictionaries anyway,
      // so this will fail.
      ARROW_ASSIGN_OR_RAISE(auto unifier,
                            DictionaryUnifier::Make(dict_type.value_type(), this->pool));
      // Unify all dictionary array chunks
      BufferVector transpose_maps(chunks->size());
      for (size_t j = 0; j < chunks->size(); ++j) {
        DCHECK_NE((*chunks)[j]->dictionary, nullptr);
        RETURN_NOT_OK(
            unifier->Unify(*MakeArray((*chunks)[j]->dictionary), &transpose_maps[j]));
      }
      std::shared_ptr<Array> dictionary;
      RETURN_NOT_OK(unifier->GetResultWithIndexType(dict_type.index_type(), &dictionary));
      for (size_t j = 0; j < chunks->size(); ++j) {
        ARROW_ASSIGN_OR_RAISE(
            (*chunks)[j],
            TransposeDictIndices(
                (*chunks)[j], type, type, dictionary->data(),
                reinterpret_cast<const int32_t*>(transpose_maps[j]->data()), this->pool));
        if (ext_type) {
          (*chunks)[j]->type = ext_type;
        }
      }
      changed = true;
    }

    return changed;
  }
};

}  // namespace

Result<std::unique_ptr<DictionaryUnifier>> DictionaryUnifier::Make(
    std::shared_ptr<DataType> value_type, MemoryPool* pool) {
  MakeUnifier maker(pool, value_type);
  RETURN_NOT_OK(VisitTypeInline(*value_type, &maker));
  return std::move(maker.result);
}

Result<std::shared_ptr<ChunkedArray>> DictionaryUnifier::UnifyChunkedArray(
    const std::shared_ptr<ChunkedArray>& array, MemoryPool* pool) {
  if (array->num_chunks() <= 1) {
    return array;
  }

  ArrayDataVector data_chunks(array->num_chunks());
  std::transform(array->chunks().begin(), array->chunks().end(), data_chunks.begin(),
                 [](const std::shared_ptr<Array>& array) { return array->data(); });
  ARROW_ASSIGN_OR_RAISE(bool changed,
                        RecursiveUnifier{pool}.Unify(array->type(), &data_chunks));
  if (!changed) {
    return array;
  }
  ArrayVector chunks(array->num_chunks());
  std::transform(data_chunks.begin(), data_chunks.end(), chunks.begin(),
                 [](const std::shared_ptr<ArrayData>& data) { return MakeArray(data); });
  return std::make_shared<ChunkedArray>(std::move(chunks), array->type());
}

Result<std::shared_ptr<Table>> DictionaryUnifier::UnifyTable(const Table& table,
                                                             MemoryPool* pool) {
  ChunkedArrayVector columns = table.columns();
  for (auto& col : columns) {
    ARROW_ASSIGN_OR_RAISE(col, DictionaryUnifier::UnifyChunkedArray(col, pool));
  }
  return Table::Make(table.schema(), std::move(columns), table.num_rows());
}

}  // namespace arrow
