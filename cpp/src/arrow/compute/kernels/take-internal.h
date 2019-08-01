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

#pragma once

#include <algorithm>
#include <limits>
#include <memory>
#include <type_traits>
#include <utility>
#include <vector>

#include "arrow/builder.h"
#include "arrow/compute/context.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging.h"
#include "arrow/util/stl.h"
#include "arrow/visitor_inline.h"

namespace arrow {
namespace compute {

using internal::checked_cast;
using internal::checked_pointer_cast;

// For non-binary builders, use regular value append
template <typename Builder, typename Scalar>
static typename std::enable_if<
    !std::is_base_of<BaseBinaryType, typename Builder::TypeClass>::value, Status>::type
UnsafeAppend(Builder* builder, Scalar&& value) {
  builder->UnsafeAppend(std::forward<Scalar>(value));
  return Status::OK();
}

// For binary builders, need to reserve byte storage first
template <typename Builder>
static enable_if_base_binary<typename Builder::TypeClass, Status> UnsafeAppend(
    Builder* builder, util::string_view value) {
  RETURN_NOT_OK(builder->ReserveData(static_cast<int64_t>(value.size())));
  builder->UnsafeAppend(value);
  return Status::OK();
}

/// \brief visit indices from an IndexSequence while bounds checking
///
/// \param[in] indices IndexSequence to visit
/// \param[in] values array to bounds check against, if necessary
/// \param[in] vis index visitor, signature must be Status(int64_t index, bool is_valid)
template <bool SomeIndicesNull, bool SomeValuesNull, bool NeverOutOfBounds,
          typename IndexSequence, typename Visitor>
Status VisitIndices(IndexSequence indices, const Array& values, Visitor&& vis) {
  for (int64_t i = 0; i < indices.length(); ++i) {
    auto index_valid = indices.Next();
    if (SomeIndicesNull && !index_valid.second) {
      RETURN_NOT_OK(vis(0, false));
      continue;
    }

    auto index = index_valid.first;
    if (!NeverOutOfBounds) {
      if (index < 0 || index >= values.length()) {
        return Status::IndexError("take index out of bounds");
      }
    } else {
      DCHECK_GE(index, 0);
      DCHECK_LT(index, values.length());
    }

    bool is_valid = !SomeValuesNull || values.IsValid(index);
    RETURN_NOT_OK(vis(index, is_valid));
  }
  return Status::OK();
}

template <bool SomeIndicesNull, bool SomeValuesNull, typename IndexSequence,
          typename Visitor>
Status VisitIndices(IndexSequence indices, const Array& values, Visitor&& vis) {
  if (indices.never_out_of_bounds()) {
    return VisitIndices<SomeIndicesNull, SomeValuesNull, true>(
        indices, values, std::forward<Visitor>(vis));
  }
  return VisitIndices<SomeIndicesNull, SomeValuesNull, false>(indices, values,
                                                              std::forward<Visitor>(vis));
}

template <bool SomeIndicesNull, typename IndexSequence, typename Visitor>
Status VisitIndices(IndexSequence indices, const Array& values, Visitor&& vis) {
  if (values.null_count() == 0) {
    return VisitIndices<SomeIndicesNull, false>(indices, values,
                                                std::forward<Visitor>(vis));
  }
  return VisitIndices<SomeIndicesNull, true>(indices, values, std::forward<Visitor>(vis));
}

template <typename IndexSequence, typename Visitor>
Status VisitIndices(IndexSequence indices, const Array& values, Visitor&& vis) {
  if (indices.null_count() == 0) {
    return VisitIndices<false>(indices, values, std::forward<Visitor>(vis));
  }
  return VisitIndices<true>(indices, values, std::forward<Visitor>(vis));
}

// Helper class for gathering values from an array
template <typename IndexSequence>
class Taker {
 public:
  explicit Taker(const std::shared_ptr<DataType>& type) : type_(type) {}

  virtual ~Taker() = default;

  // initialize this taker including constructing any children,
  // must be called once after construction before any other methods are called
  virtual Status Init() { return Status::OK(); }

  // reset this Taker and set FunctionContext for taking an array
  // must be called each time the FunctionContext may have changed
  virtual Status SetContext(FunctionContext* ctx) = 0;

  // gather elements from an array at the provided indices
  virtual Status Take(const Array& values, IndexSequence indices) = 0;

  // assemble an array of all gathered values
  virtual Status Finish(std::shared_ptr<Array>*) = 0;

  // factory; the output Taker will support gathering values of the given type
  static Status Make(const std::shared_ptr<DataType>& type, std::unique_ptr<Taker>* out);

  static_assert(std::is_literal_type<IndexSequence>::value,
                "Index sequences must be literal type");

  static_assert(std::is_copy_constructible<IndexSequence>::value,
                "Index sequences must be copy constructible");

  static_assert(std::is_same<decltype(std::declval<IndexSequence>().Next()),
                             std::pair<int64_t, bool>>::value,
                "An index sequence must yield pairs of indices:int64_t, validity:bool.");

  static_assert(std::is_same<decltype(std::declval<const IndexSequence>().length()),
                             int64_t>::value,
                "An index sequence must provide its length.");

  static_assert(std::is_same<decltype(std::declval<const IndexSequence>().null_count()),
                             int64_t>::value,
                "An index sequence must provide the number of nulls it will take.");

  static_assert(
      std::is_same<decltype(std::declval<const IndexSequence>().never_out_of_bounds()),
                   bool>::value,
      "Index sequences must declare whether bounds checking is necessary");

  static_assert(
      std::is_same<decltype(std::declval<IndexSequence>().set_never_out_of_bounds()),
                   void>::value,
      "An index sequence must support ignoring bounds checking.");

 protected:
  template <typename Builder>
  Status MakeBuilder(MemoryPool* pool, std::unique_ptr<Builder>* out) {
    std::unique_ptr<ArrayBuilder> builder;
    RETURN_NOT_OK(arrow::MakeBuilder(pool, type_, &builder));
    out->reset(checked_cast<Builder*>(builder.release()));
    return Status::OK();
  }

  std::shared_ptr<DataType> type_;
};

// an IndexSequence which yields indices from a specified range
// or yields null for the length of that range
class RangeIndexSequence {
 public:
  constexpr bool never_out_of_bounds() const { return true; }
  void set_never_out_of_bounds() {}

  constexpr RangeIndexSequence() = default;

  RangeIndexSequence(bool is_valid, int64_t offset, int64_t length)
      : is_valid_(is_valid), index_(offset), length_(length) {}

  std::pair<int64_t, bool> Next() { return std::make_pair(index_++, is_valid_); }

  int64_t length() const { return length_; }

  int64_t null_count() const { return is_valid_ ? 0 : length_; }

 private:
  bool is_valid_ = true;
  int64_t index_ = 0, length_ = -1;
};

// an IndexSequence which yields the values of an Array of integers
template <typename IndexType>
class ArrayIndexSequence {
 public:
  bool never_out_of_bounds() const { return never_out_of_bounds_; }
  void set_never_out_of_bounds() { never_out_of_bounds_ = true; }

  constexpr ArrayIndexSequence() = default;

  explicit ArrayIndexSequence(const Array& indices)
      : indices_(&checked_cast<const NumericArray<IndexType>&>(indices)) {}

  std::pair<int64_t, bool> Next() {
    if (indices_->IsNull(index_)) {
      ++index_;
      return std::make_pair(-1, false);
    }
    return std::make_pair(indices_->Value(index_++), true);
  }

  int64_t length() const { return indices_->length(); }

  int64_t null_count() const { return indices_->null_count(); }

 private:
  const NumericArray<IndexType>* indices_ = nullptr;
  int64_t index_ = 0;
  bool never_out_of_bounds_ = false;
};

// Default implementation: taking from a simple array into a builder requires only that
// the array supports array.GetView() and the corresponding builder supports
// builder.UnsafeAppend(array.GetView())
template <typename IndexSequence, typename T>
class TakerImpl : public Taker<IndexSequence> {
 public:
  using ArrayType = typename TypeTraits<T>::ArrayType;
  using BuilderType = typename TypeTraits<T>::BuilderType;

  using Taker<IndexSequence>::Taker;

  Status SetContext(FunctionContext* ctx) override {
    return this->MakeBuilder(ctx->memory_pool(), &builder_);
  }

  Status Take(const Array& values, IndexSequence indices) override {
    DCHECK(this->type_->Equals(values.type()));
    RETURN_NOT_OK(builder_->Reserve(indices.length()));
    return VisitIndices(indices, values, [&](int64_t index, bool is_valid) {
      if (!is_valid) {
        builder_->UnsafeAppendNull();
        return Status::OK();
      }
      auto value = checked_cast<const ArrayType&>(values).GetView(index);
      return UnsafeAppend(builder_.get(), value);
    });
  }

  Status Finish(std::shared_ptr<Array>* out) override { return builder_->Finish(out); }

 private:
  std::unique_ptr<BuilderType> builder_;
};

// Gathering from NullArrays is trivial; skip the builder and just
// do bounds checking
template <typename IndexSequence>
class TakerImpl<IndexSequence, NullType> : public Taker<IndexSequence> {
 public:
  using Taker<IndexSequence>::Taker;

  Status SetContext(FunctionContext*) override { return Status::OK(); }

  Status Take(const Array& values, IndexSequence indices) override {
    DCHECK(this->type_->Equals(values.type()));

    length_ += indices.length();

    if (indices.never_out_of_bounds()) {
      return Status::OK();
    }

    return VisitIndices(indices, values, [](int64_t, bool) { return Status::OK(); });
  }

  Status Finish(std::shared_ptr<Array>* out) override {
    out->reset(new NullArray(length_));
    return Status::OK();
  }

 private:
  int64_t length_ = 0;
};

template <typename IndexSequence, typename TypeClass>
class ListTakerImpl : public Taker<IndexSequence> {
 public:
  using offset_type = typename TypeClass::offset_type;
  using ArrayType = typename TypeTraits<TypeClass>::ArrayType;

  using Taker<IndexSequence>::Taker;

  Status Init() override {
    const auto& list_type = checked_cast<const TypeClass&>(*this->type_);
    return Taker<RangeIndexSequence>::Make(list_type.value_type(), &value_taker_);
  }

  Status SetContext(FunctionContext* ctx) override {
    auto pool = ctx->memory_pool();
    null_bitmap_builder_.reset(new TypedBufferBuilder<bool>(pool));
    offset_builder_.reset(new TypedBufferBuilder<offset_type>(pool));
    RETURN_NOT_OK(offset_builder_->Append(0));
    return value_taker_->SetContext(ctx);
  }

  Status Take(const Array& values, IndexSequence indices) override {
    DCHECK(this->type_->Equals(values.type()));

    const auto& list_array = checked_cast<const ArrayType&>(values);

    RETURN_NOT_OK(null_bitmap_builder_->Reserve(indices.length()));
    RETURN_NOT_OK(offset_builder_->Reserve(indices.length()));

    offset_type offset = offset_builder_->data()[offset_builder_->length() - 1];
    return VisitIndices(indices, values, [&](int64_t index, bool is_valid) {
      null_bitmap_builder_->UnsafeAppend(is_valid);

      if (is_valid) {
        offset += list_array.value_length(index);
        RangeIndexSequence value_indices(true, list_array.value_offset(index),
                                         list_array.value_length(index));
        RETURN_NOT_OK(value_taker_->Take(*list_array.values(), value_indices));
      }

      offset_builder_->UnsafeAppend(offset);
      return Status::OK();
    });
  }

  Status Finish(std::shared_ptr<Array>* out) override {
    auto null_count = null_bitmap_builder_->false_count();
    auto length = null_bitmap_builder_->length();

    std::shared_ptr<Buffer> offsets, null_bitmap;
    RETURN_NOT_OK(null_bitmap_builder_->Finish(&null_bitmap));
    RETURN_NOT_OK(offset_builder_->Finish(&offsets));

    std::shared_ptr<Array> taken_values;
    RETURN_NOT_OK(value_taker_->Finish(&taken_values));

    out->reset(new ArrayType(this->type_, length, offsets, taken_values, null_bitmap,
                             null_count));
    return Status::OK();
  }

  std::unique_ptr<TypedBufferBuilder<bool>> null_bitmap_builder_;
  std::unique_ptr<TypedBufferBuilder<offset_type>> offset_builder_;
  std::unique_ptr<Taker<RangeIndexSequence>> value_taker_;
};

template <typename IndexSequence>
class TakerImpl<IndexSequence, ListType> : public ListTakerImpl<IndexSequence, ListType> {
  using ListTakerImpl<IndexSequence, ListType>::ListTakerImpl;
};

template <typename IndexSequence>
class TakerImpl<IndexSequence, LargeListType>
    : public ListTakerImpl<IndexSequence, LargeListType> {
  using ListTakerImpl<IndexSequence, LargeListType>::ListTakerImpl;
};

template <typename IndexSequence>
class TakerImpl<IndexSequence, MapType> : public ListTakerImpl<IndexSequence, MapType> {
  using ListTakerImpl<IndexSequence, MapType>::ListTakerImpl;
};

template <typename IndexSequence>
class TakerImpl<IndexSequence, FixedSizeListType> : public Taker<IndexSequence> {
 public:
  using Taker<IndexSequence>::Taker;

  Status Init() override {
    const auto& list_type = checked_cast<const FixedSizeListType&>(*this->type_);
    return Taker<RangeIndexSequence>::Make(list_type.value_type(), &value_taker_);
  }

  Status SetContext(FunctionContext* ctx) override {
    auto pool = ctx->memory_pool();
    null_bitmap_builder_.reset(new TypedBufferBuilder<bool>(pool));
    return value_taker_->SetContext(ctx);
  }

  Status Take(const Array& values, IndexSequence indices) override {
    DCHECK(this->type_->Equals(values.type()));

    const auto& list_array = checked_cast<const FixedSizeListArray&>(values);
    auto list_size = list_array.list_type()->list_size();

    RETURN_NOT_OK(null_bitmap_builder_->Reserve(indices.length()));
    return VisitIndices(indices, values, [&](int64_t index, bool is_valid) {
      null_bitmap_builder_->UnsafeAppend(is_valid);

      // for FixedSizeList, null lists are not empty (they also span a segment of
      // list_size in the child data), so we must append to value_taker_ even if !is_valid
      RangeIndexSequence value_indices(is_valid, list_array.value_offset(index),
                                       list_size);
      return value_taker_->Take(*list_array.values(), value_indices);
    });
  }

  Status Finish(std::shared_ptr<Array>* out) override {
    auto null_count = null_bitmap_builder_->false_count();
    auto length = null_bitmap_builder_->length();

    std::shared_ptr<Buffer> null_bitmap;
    RETURN_NOT_OK(null_bitmap_builder_->Finish(&null_bitmap));

    std::shared_ptr<Array> taken_values;
    RETURN_NOT_OK(value_taker_->Finish(&taken_values));

    out->reset(new FixedSizeListArray(this->type_, length, taken_values, null_bitmap,
                                      null_count));
    return Status::OK();
  }

 protected:
  std::unique_ptr<TypedBufferBuilder<bool>> null_bitmap_builder_;
  std::unique_ptr<Taker<RangeIndexSequence>> value_taker_;
};

template <typename IndexSequence>
class TakerImpl<IndexSequence, StructType> : public Taker<IndexSequence> {
 public:
  using Taker<IndexSequence>::Taker;

  Status Init() override {
    children_.resize(this->type_->num_children());
    for (int i = 0; i < this->type_->num_children(); ++i) {
      RETURN_NOT_OK(
          Taker<IndexSequence>::Make(this->type_->child(i)->type(), &children_[i]));
    }
    return Status::OK();
  }

  Status SetContext(FunctionContext* ctx) override {
    null_bitmap_builder_.reset(new TypedBufferBuilder<bool>(ctx->memory_pool()));
    for (int i = 0; i < this->type_->num_children(); ++i) {
      RETURN_NOT_OK(children_[i]->SetContext(ctx));
    }
    return Status::OK();
  }

  Status Take(const Array& values, IndexSequence indices) override {
    DCHECK(this->type_->Equals(values.type()));

    RETURN_NOT_OK(null_bitmap_builder_->Reserve(indices.length()));
    RETURN_NOT_OK(VisitIndices(indices, values, [&](int64_t, bool is_valid) {
      null_bitmap_builder_->UnsafeAppend(is_valid);
      return Status::OK();
    }));

    // bounds checking was done while appending to the null bitmap
    indices.set_never_out_of_bounds();

    const auto& struct_array = checked_cast<const StructArray&>(values);
    for (int i = 0; i < this->type_->num_children(); ++i) {
      RETURN_NOT_OK(children_[i]->Take(*struct_array.field(i), indices));
    }
    return Status::OK();
  }

  Status Finish(std::shared_ptr<Array>* out) override {
    auto null_count = null_bitmap_builder_->false_count();
    auto length = null_bitmap_builder_->length();
    std::shared_ptr<Buffer> null_bitmap;
    RETURN_NOT_OK(null_bitmap_builder_->Finish(&null_bitmap));

    ArrayVector fields(this->type_->num_children());
    for (int i = 0; i < this->type_->num_children(); ++i) {
      RETURN_NOT_OK(children_[i]->Finish(&fields[i]));
    }

    out->reset(
        new StructArray(this->type_, length, std::move(fields), null_bitmap, null_count));
    return Status::OK();
  }

 protected:
  std::unique_ptr<TypedBufferBuilder<bool>> null_bitmap_builder_;
  std::vector<std::unique_ptr<Taker<IndexSequence>>> children_;
};

template <typename IndexSequence>
class TakerImpl<IndexSequence, UnionType> : public Taker<IndexSequence> {
 public:
  using Taker<IndexSequence>::Taker;

  Status Init() override {
    union_type_ = checked_cast<const UnionType*>(this->type_.get());

    if (union_type_->mode() == UnionMode::SPARSE) {
      sparse_children_.resize(this->type_->num_children());
    } else {
      dense_children_.resize(this->type_->num_children());
      child_length_.resize(union_type_->max_type_code() + 1);
    }

    for (int i = 0; i < this->type_->num_children(); ++i) {
      if (union_type_->mode() == UnionMode::SPARSE) {
        RETURN_NOT_OK(Taker<IndexSequence>::Make(this->type_->child(i)->type(),
                                                 &sparse_children_[i]));
      } else {
        RETURN_NOT_OK(Taker<ArrayIndexSequence<Int32Type>>::Make(
            this->type_->child(i)->type(), &dense_children_[i]));
      }
    }

    return Status::OK();
  }

  Status SetContext(FunctionContext* ctx) override {
    pool_ = ctx->memory_pool();
    null_bitmap_builder_.reset(new TypedBufferBuilder<bool>(pool_));
    type_id_builder_.reset(new TypedBufferBuilder<int8_t>(pool_));

    if (union_type_->mode() == UnionMode::DENSE) {
      offset_builder_.reset(new TypedBufferBuilder<int32_t>(pool_));
      std::fill(child_length_.begin(), child_length_.end(), 0);
    }

    for (int i = 0; i < this->type_->num_children(); ++i) {
      if (union_type_->mode() == UnionMode::SPARSE) {
        RETURN_NOT_OK(sparse_children_[i]->SetContext(ctx));
      } else {
        RETURN_NOT_OK(dense_children_[i]->SetContext(ctx));
      }
    }

    return Status::OK();
  }

  Status Take(const Array& values, IndexSequence indices) override {
    DCHECK(this->type_->Equals(values.type()));
    const auto& union_array = checked_cast<const UnionArray&>(values);
    auto type_ids = union_array.raw_type_ids();

    if (union_type_->mode() == UnionMode::SPARSE) {
      RETURN_NOT_OK(null_bitmap_builder_->Reserve(indices.length()));
      RETURN_NOT_OK(type_id_builder_->Reserve(indices.length()));
      RETURN_NOT_OK(VisitIndices(indices, values, [&](int64_t index, bool is_valid) {
        null_bitmap_builder_->UnsafeAppend(is_valid);
        type_id_builder_->UnsafeAppend(type_ids[index]);
        return Status::OK();
      }));

      // bounds checking was done while appending to the null bitmap
      indices.set_never_out_of_bounds();

      for (int i = 0; i < this->type_->num_children(); ++i) {
        RETURN_NOT_OK(sparse_children_[i]->Take(*union_array.child(i), indices));
      }
    } else {
      // Gathering from the offsets into child arrays is a bit tricky.
      std::vector<uint32_t> child_counts(union_type_->max_type_code() + 1);
      RETURN_NOT_OK(null_bitmap_builder_->Reserve(indices.length()));
      RETURN_NOT_OK(type_id_builder_->Reserve(indices.length()));
      RETURN_NOT_OK(VisitIndices(indices, values, [&](int64_t index, bool is_valid) {
        null_bitmap_builder_->UnsafeAppend(is_valid);
        type_id_builder_->UnsafeAppend(type_ids[index]);
        child_counts[type_ids[index]] += is_valid;
        return Status::OK();
      }));

      // bounds checking was done while appending to the null bitmap
      indices.set_never_out_of_bounds();

      // Allocate temporary storage for the offsets of all valid slots
      // NB: Overestimates required space when indices and union_array are
      //     not null at identical positions.
      auto child_offsets_storage_size =
          indices.length() - std::max(union_array.null_count(), indices.null_count());
      std::shared_ptr<Buffer> child_offsets_storage;
      RETURN_NOT_OK(AllocateBuffer(pool_, child_offsets_storage_size * sizeof(int32_t),
                                   &child_offsets_storage));

      // Partition offsets by type_id: child_offset_partitions[type_id] will
      // point to storage for child_counts[type_id] offsets
      std::vector<int32_t*> child_offset_partitions(child_counts.size());
      auto child_offsets_storage_data = GetInt32(child_offsets_storage);
      for (auto type_id : union_type_->type_codes()) {
        child_offset_partitions[type_id] = child_offsets_storage_data;
        child_offsets_storage_data += child_counts[type_id];
      }

      // Fill child_offsets_storage with the taken offsets
      RETURN_NOT_OK(offset_builder_->Reserve(indices.length()));
      RETURN_NOT_OK(VisitIndices(indices, values, [&](int64_t index, bool is_valid) {
        auto type_id = type_ids[index];
        if (is_valid) {
          offset_builder_->UnsafeAppend(child_length_[type_id]++);
          *child_offset_partitions[type_id] = union_array.value_offset(index);
          ++child_offset_partitions[type_id];
        } else {
          offset_builder_->UnsafeAppend(0);
        }
        return Status::OK();
      }));

      // Take from each child at those offsets
      int64_t taken_offset_begin = 0;
      for (int i = 0; i < this->type_->num_children(); ++i) {
        auto type_id = union_type_->type_codes()[i];
        auto length = child_counts[type_id];
        Int32Array taken_offsets(length, SliceBuffer(child_offsets_storage,
                                                     sizeof(int32_t) * taken_offset_begin,
                                                     sizeof(int32_t) * length));
        ArrayIndexSequence<Int32Type> child_indices(taken_offsets);
        child_indices.set_never_out_of_bounds();
        RETURN_NOT_OK(dense_children_[i]->Take(*union_array.child(i), child_indices));
        taken_offset_begin += length;
      }
    }

    return Status::OK();
  }

  Status Finish(std::shared_ptr<Array>* out) override {
    auto null_count = null_bitmap_builder_->false_count();
    auto length = null_bitmap_builder_->length();
    std::shared_ptr<Buffer> null_bitmap, type_ids;
    RETURN_NOT_OK(null_bitmap_builder_->Finish(&null_bitmap));
    RETURN_NOT_OK(type_id_builder_->Finish(&type_ids));

    std::shared_ptr<Buffer> offsets;
    if (union_type_->mode() == UnionMode::DENSE) {
      RETURN_NOT_OK(offset_builder_->Finish(&offsets));
    }

    ArrayVector fields(this->type_->num_children());
    for (int i = 0; i < this->type_->num_children(); ++i) {
      if (union_type_->mode() == UnionMode::SPARSE) {
        RETURN_NOT_OK(sparse_children_[i]->Finish(&fields[i]));
      } else {
        RETURN_NOT_OK(dense_children_[i]->Finish(&fields[i]));
      }
    }

    out->reset(new UnionArray(this->type_, length, std::move(fields), type_ids, offsets,
                              null_bitmap, null_count));
    return Status::OK();
  }

 protected:
  int32_t* GetInt32(const std::shared_ptr<Buffer>& b) const {
    return reinterpret_cast<int32_t*>(b->mutable_data());
  }

  const UnionType* union_type_ = nullptr;
  MemoryPool* pool_ = nullptr;
  std::unique_ptr<TypedBufferBuilder<bool>> null_bitmap_builder_;
  std::unique_ptr<TypedBufferBuilder<int8_t>> type_id_builder_;
  std::unique_ptr<TypedBufferBuilder<int32_t>> offset_builder_;
  std::vector<std::unique_ptr<Taker<IndexSequence>>> sparse_children_;
  std::vector<std::unique_ptr<Taker<ArrayIndexSequence<Int32Type>>>> dense_children_;
  std::vector<int32_t> child_length_;
};

// taking from a DictionaryArray is accomplished by taking from its indices
template <typename IndexSequence>
class TakerImpl<IndexSequence, DictionaryType> : public Taker<IndexSequence> {
 public:
  using Taker<IndexSequence>::Taker;

  Status Init() override {
    const auto& dict_type = checked_cast<const DictionaryType&>(*this->type_);
    return Taker<IndexSequence>::Make(dict_type.index_type(), &index_taker_);
  }

  Status SetContext(FunctionContext* ctx) override {
    dictionary_ = nullptr;
    return index_taker_->SetContext(ctx);
  }

  Status Take(const Array& values, IndexSequence indices) override {
    DCHECK(this->type_->Equals(values.type()));
    const auto& dict_array = checked_cast<const DictionaryArray&>(values);

    if (dictionary_ != nullptr && dictionary_ != dict_array.dictionary()) {
      return Status::NotImplemented(
          "taking from DictionaryArrays with different dictionaries");
    } else {
      dictionary_ = dict_array.dictionary();
    }
    return index_taker_->Take(*dict_array.indices(), indices);
  }

  Status Finish(std::shared_ptr<Array>* out) override {
    std::shared_ptr<Array> taken_indices;
    RETURN_NOT_OK(index_taker_->Finish(&taken_indices));
    out->reset(new DictionaryArray(this->type_, taken_indices, dictionary_));
    return Status::OK();
  }

 protected:
  std::shared_ptr<Array> dictionary_;
  std::unique_ptr<Taker<IndexSequence>> index_taker_;
};

// taking from an ExtensionArray is accomplished by taking from its storage
template <typename IndexSequence>
class TakerImpl<IndexSequence, ExtensionType> : public Taker<IndexSequence> {
 public:
  using Taker<IndexSequence>::Taker;

  Status Init() override {
    const auto& ext_type = checked_cast<const ExtensionType&>(*this->type_);
    return Taker<IndexSequence>::Make(ext_type.storage_type(), &storage_taker_);
  }

  Status SetContext(FunctionContext* ctx) override {
    return storage_taker_->SetContext(ctx);
  }

  Status Take(const Array& values, IndexSequence indices) override {
    DCHECK(this->type_->Equals(values.type()));
    const auto& ext_array = checked_cast<const ExtensionArray&>(values);
    return storage_taker_->Take(*ext_array.storage(), indices);
  }

  Status Finish(std::shared_ptr<Array>* out) override {
    std::shared_ptr<Array> taken_storage;
    RETURN_NOT_OK(storage_taker_->Finish(&taken_storage));
    out->reset(new ExtensionArray(this->type_, taken_storage));
    return Status::OK();
  }

 protected:
  std::unique_ptr<Taker<IndexSequence>> storage_taker_;
};

template <typename IndexSequence>
struct TakerMakeImpl {
  template <typename T>
  Status Visit(const T&) {
    out_->reset(new TakerImpl<IndexSequence, T>(type_));
    return (*out_)->Init();
  }

  std::shared_ptr<DataType> type_;
  std::unique_ptr<Taker<IndexSequence>>* out_;
};

template <typename IndexSequence>
Status Taker<IndexSequence>::Make(const std::shared_ptr<DataType>& type,
                                  std::unique_ptr<Taker>* out) {
  TakerMakeImpl<IndexSequence> visitor{type, out};
  return VisitTypeInline(*type, &visitor);
}

}  // namespace compute
}  // namespace arrow
