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
#include <memory>

#include "arrow/array/builder_adaptive.h"   // IWYU pragma: export
#include "arrow/array/builder_base.h"       // IWYU pragma: export
#include "arrow/array/builder_primitive.h"  // IWYU pragma: export

#include "arrow/array.h"

namespace arrow {

// ----------------------------------------------------------------------
// Dictionary builder

namespace internal {

template <typename T>
struct DictionaryScalar {
  using type = typename T::c_type;
};

template <>
struct DictionaryScalar<BinaryType> {
  using type = util::string_view;
};

template <>
struct DictionaryScalar<StringType> {
  using type = util::string_view;
};

template <>
struct DictionaryScalar<FixedSizeBinaryType> {
  using type = util::string_view;
};

class ARROW_EXPORT DictionaryMemoTable {
 public:
  explicit DictionaryMemoTable(const std::shared_ptr<DataType>& type);
  explicit DictionaryMemoTable(const std::shared_ptr<Array>& dictionary);
  ~DictionaryMemoTable();

  int32_t GetOrInsert(const bool& value);
  int32_t GetOrInsert(const int8_t& value);
  int32_t GetOrInsert(const int16_t& value);
  int32_t GetOrInsert(const int32_t& value);
  int32_t GetOrInsert(const int64_t& value);
  int32_t GetOrInsert(const uint8_t& value);
  int32_t GetOrInsert(const uint16_t& value);
  int32_t GetOrInsert(const uint32_t& value);
  int32_t GetOrInsert(const uint64_t& value);
  int32_t GetOrInsert(const float& value);
  int32_t GetOrInsert(const double& value);
  int32_t GetOrInsert(const util::string_view& value);

  Status GetArrayData(MemoryPool* pool, int64_t start_offset,
                      std::shared_ptr<ArrayData>* out);

  /// \brief Insert new memo values
  Status InsertValues(const Array& values);

  int32_t size() const;

 private:
  class DictionaryMemoTableImpl;
  std::unique_ptr<DictionaryMemoTableImpl> impl_;
};

/// \brief Array builder for created encoded DictionaryArray from
/// dense array
///
/// Unlike other builders, dictionary builder does not completely
/// reset the state on Finish calls. The arrays built after the
/// initial Finish call will reuse the previously created encoding and
/// build a delta dictionary when new terms occur.
///
/// data
template <typename BuilderType, typename T>
class DictionaryBuilderBase : public ArrayBuilder {
 public:
  using Scalar = typename DictionaryScalar<T>::type;

  // WARNING: the type given below is the value type, not the DictionaryType.
  // The DictionaryType is instantiated on the Finish() call.
  template <typename T1 = T>
  DictionaryBuilderBase(
      typename std::enable_if<!std::is_base_of<FixedSizeBinaryType, T1>::value,
                              const std::shared_ptr<DataType>&>::type type,
      MemoryPool* pool = default_memory_pool())
      : ArrayBuilder(type, pool),
        memo_table_(new DictionaryMemoTable(type)),
        delta_offset_(0),
        byte_width_(-1),
        values_builder_(pool) {}

  template <typename T1 = T>
  explicit DictionaryBuilderBase(
      typename std::enable_if<std::is_base_of<FixedSizeBinaryType, T1>::value,
                              const std::shared_ptr<DataType>&>::type type,
      MemoryPool* pool = default_memory_pool())
      : ArrayBuilder(type, pool),
        memo_table_(new DictionaryMemoTable(type)),
        delta_offset_(0),
        byte_width_(static_cast<const T1&>(*type).byte_width()),
        values_builder_(pool) {}

  template <typename T1 = T>
  explicit DictionaryBuilderBase(
      typename std::enable_if<TypeTraits<T1>::is_parameter_free, MemoryPool*>::type pool =
          default_memory_pool())
      : DictionaryBuilderBase<BuilderType, T1>(TypeTraits<T1>::type_singleton(), pool) {}

  DictionaryBuilderBase(const std::shared_ptr<Array>& dictionary,
                        MemoryPool* pool = default_memory_pool())
      : ArrayBuilder(dictionary->type(), pool),
        memo_table_(new DictionaryMemoTable(dictionary)),
        delta_offset_(0),
        byte_width_(-1),
        values_builder_(pool) {}

  ~DictionaryBuilderBase() override = default;

  /// \brief Append a scalar value
  Status Append(const Scalar& value) {
    ARROW_RETURN_NOT_OK(Reserve(1));

    auto memo_index = memo_table_->GetOrInsert(value);
    ARROW_RETURN_NOT_OK(values_builder_.Append(memo_index));
    length_ += 1;

    return Status::OK();
  }

  /// \brief Append a fixed-width string (only for FixedSizeBinaryType)
  template <typename T1 = T>
  Status Append(typename std::enable_if<std::is_base_of<FixedSizeBinaryType, T1>::value,
                                        const uint8_t*>::type value) {
    return Append(util::string_view(reinterpret_cast<const char*>(value), byte_width_));
  }

  /// \brief Append a fixed-width string (only for FixedSizeBinaryType)
  template <typename T1 = T>
  Status Append(typename std::enable_if<std::is_base_of<FixedSizeBinaryType, T1>::value,
                                        const char*>::type value) {
    return Append(util::string_view(value, byte_width_));
  }

  /// \brief Append a scalar null value
  Status AppendNull() final {
    length_ += 1;
    null_count_ += 1;

    return values_builder_.AppendNull();
  }

  Status AppendNulls(int64_t length) final {
    length_ += length;
    null_count_ += length;

    return values_builder_.AppendNulls(length);
  }

  /// \brief Insert values into the dictionary's memo, but do not append any
  /// indices. Can be used to initialize a new builder with known dictionary
  /// values
  /// \param[in] values dictionary values to add to memo. Type must match
  /// builder type
  Status InsertMemoValues(const Array& values) {
    return memo_table_->InsertValues(values);
  }

  /// \brief Append a whole dense array to the builder
  template <typename T1 = T>
  Status AppendArray(
      typename std::enable_if<!std::is_base_of<FixedSizeBinaryType, T1>::value,
                              const Array&>::type array) {
    using ArrayType = typename TypeTraits<T>::ArrayType;

    const auto& concrete_array = static_cast<const ArrayType&>(array);
    for (int64_t i = 0; i < array.length(); i++) {
      if (array.IsNull(i)) {
        ARROW_RETURN_NOT_OK(AppendNull());
      } else {
        ARROW_RETURN_NOT_OK(Append(concrete_array.GetView(i)));
      }
    }
    return Status::OK();
  }

  template <typename T1 = T>
  Status AppendArray(
      typename std::enable_if<std::is_base_of<FixedSizeBinaryType, T1>::value,
                              const Array&>::type array) {
    if (!type_->Equals(*array.type())) {
      return Status::Invalid(
          "Cannot append FixedSizeBinary array with non-matching type");
    }

    const auto& concrete_array = static_cast<const FixedSizeBinaryArray&>(array);
    for (int64_t i = 0; i < array.length(); i++) {
      if (array.IsNull(i)) {
        ARROW_RETURN_NOT_OK(AppendNull());
      } else {
        ARROW_RETURN_NOT_OK(Append(concrete_array.GetValue(i)));
      }
    }
    return Status::OK();
  }

  void Reset() override {
    ArrayBuilder::Reset();
    values_builder_.Reset();
    memo_table_.reset(new DictionaryMemoTable(type_));
    delta_offset_ = 0;
  }

  Status Resize(int64_t capacity) override {
    ARROW_RETURN_NOT_OK(CheckCapacity(capacity, capacity_));
    capacity = std::max(capacity, kMinBuilderCapacity);

    if (capacity_ == 0) {
      // Initialize hash table
      // XXX should we let the user pass additional size heuristics?
      delta_offset_ = 0;
    }
    ARROW_RETURN_NOT_OK(values_builder_.Resize(capacity));
    capacity_ = values_builder_.capacity();
    return Status::OK();
  }

  Status FinishInternal(std::shared_ptr<ArrayData>* out) override {
    // Finalize indices array
    ARROW_RETURN_NOT_OK(values_builder_.FinishInternal(out));

    // Generate dictionary array from hash table contents
    std::shared_ptr<ArrayData> dictionary_data;

    ARROW_RETURN_NOT_OK(
        memo_table_->GetArrayData(pool_, delta_offset_, &dictionary_data));

    // Set type of array data to the right dictionary type
    (*out)->type = dictionary((*out)->type, type_);
    (*out)->dictionary = MakeArray(dictionary_data);

    // Update internals for further uses of this DictionaryBuilder
    delta_offset_ = memo_table_->size();
    values_builder_.Reset();

    return Status::OK();
  }

  /// \cond FALSE
  using ArrayBuilder::Finish;
  /// \endcond

  Status Finish(std::shared_ptr<DictionaryArray>* out) { return FinishTyped(out); }

  /// is the dictionary builder in the delta building mode
  bool is_building_delta() { return delta_offset_ > 0; }

 protected:
  std::unique_ptr<DictionaryMemoTable> memo_table_;

  int32_t delta_offset_;
  // Only used for FixedSizeBinaryType
  int32_t byte_width_;

  BuilderType values_builder_;
};

template <typename BuilderType>
class DictionaryBuilderBase<BuilderType, NullType> : public ArrayBuilder {
 public:
  DictionaryBuilderBase(const std::shared_ptr<DataType>& type,
                        MemoryPool* pool = default_memory_pool())
      : ArrayBuilder(type, pool), values_builder_(pool) {}

  explicit DictionaryBuilderBase(MemoryPool* pool = default_memory_pool())
      : ArrayBuilder(null(), pool), values_builder_(pool) {}

  DictionaryBuilderBase(const std::shared_ptr<Array>& dictionary,
                        MemoryPool* pool = default_memory_pool())
      : ArrayBuilder(dictionary->type(), pool), values_builder_(pool) {}

  /// \brief Append a scalar null value
  Status AppendNull() final {
    length_ += 1;
    null_count_ += 1;

    return values_builder_.AppendNull();
  }

  Status AppendNulls(int64_t length) final {
    length_ += length;
    null_count_ += length;

    return values_builder_.AppendNulls(length);
  }

  /// \brief Append a whole dense array to the builder
  Status AppendArray(const Array& array) {
    for (int64_t i = 0; i < array.length(); i++) {
      ARROW_RETURN_NOT_OK(AppendNull());
    }
    return Status::OK();
  }

  Status Resize(int64_t capacity) override {
    ARROW_RETURN_NOT_OK(CheckCapacity(capacity, capacity_));
    capacity = std::max(capacity, kMinBuilderCapacity);

    ARROW_RETURN_NOT_OK(values_builder_.Resize(capacity));
    capacity_ = values_builder_.capacity();
    return Status::OK();
  }

  Status FinishInternal(std::shared_ptr<ArrayData>* out) override {
    std::shared_ptr<Array> dictionary = std::make_shared<NullArray>(0);

    ARROW_RETURN_NOT_OK(values_builder_.FinishInternal(out));
    (*out)->type = std::make_shared<DictionaryType>((*out)->type, type_);
    (*out)->dictionary = dictionary;

    return Status::OK();
  }

  /// \cond FALSE
  using ArrayBuilder::Finish;
  /// \endcond

  Status Finish(std::shared_ptr<DictionaryArray>* out) { return FinishTyped(out); }

 protected:
  BuilderType values_builder_;
};

}  // namespace internal

/// \brief A DictionaryArray builder that uses AdaptiveIntBuilder to return the
/// smallest index size that can accommodate the dictionary indices
template <typename T>
class DictionaryBuilder : public internal::DictionaryBuilderBase<AdaptiveIntBuilder, T> {
 public:
  using BASE = internal::DictionaryBuilderBase<AdaptiveIntBuilder, T>;
  using BASE::BASE;

  /// \brief Append dictionary indices directly without modifying memo
  ///
  /// NOTE: Experimental API
  Status AppendIndices(const int64_t* values, int64_t length,
                       const uint8_t* valid_bytes = NULLPTR) {
    int64_t null_count_before = this->values_builder_.null_count();
    ARROW_RETURN_NOT_OK(this->values_builder_.AppendValues(values, length, valid_bytes));
    this->length_ += length;
    this->null_count_ += this->values_builder_.null_count() - null_count_before;
    return Status::OK();
  }
};

/// \brief A DictionaryArray builder that always returns int32 dictionary
/// indices so that data cast to dictionary form will have a consistent index
/// type, e.g. for creating a ChunkedArray
template <typename T>
class Dictionary32Builder : public internal::DictionaryBuilderBase<Int32Builder, T> {
 public:
  using BASE = internal::DictionaryBuilderBase<Int32Builder, T>;
  using BASE::BASE;

  /// \brief Append dictionary indices directly without modifying memo
  ///
  /// NOTE: Experimental API
  Status AppendIndices(const int32_t* values, int64_t length,
                       const uint8_t* valid_bytes = NULLPTR) {
    int64_t null_count_before = this->values_builder_.null_count();
    ARROW_RETURN_NOT_OK(this->values_builder_.AppendValues(values, length, valid_bytes));
    this->length_ += length;
    this->null_count_ += this->values_builder_.null_count() - null_count_before;
    return Status::OK();
  }
};

// ----------------------------------------------------------------------
// Binary / Unicode builders with slightly expanded APIs

namespace internal {

template <typename T>
class BinaryDictionaryBuilderImpl : public DictionaryBuilder<T> {
 public:
  using BASE = DictionaryBuilder<T>;
  using BASE::Append;
  using BASE::AppendIndices;
  using BASE::BASE;

  BinaryDictionaryBuilderImpl() : BinaryDictionaryBuilderImpl(default_memory_pool()) {}

  Status Append(const uint8_t* value, int32_t length) {
    return Append(reinterpret_cast<const char*>(value), length);
  }

  Status Append(const char* value, int32_t length) {
    return Append(util::string_view(value, length));
  }
};

template <typename T>
class BinaryDictionary32BuilderImpl : public Dictionary32Builder<T> {
 public:
  using BASE = Dictionary32Builder<T>;
  using BASE::Append;
  using BASE::AppendIndices;
  using BASE::BASE;

  BinaryDictionary32BuilderImpl()
      : BinaryDictionary32BuilderImpl(default_memory_pool()) {}

  Status Append(const uint8_t* value, int32_t length) {
    return Append(reinterpret_cast<const char*>(value), length);
  }

  Status Append(const char* value, int32_t length) {
    return Append(util::string_view(value, length));
  }
};

}  // namespace internal

class BinaryDictionaryBuilder : public internal::BinaryDictionaryBuilderImpl<BinaryType> {
  using BASE = internal::BinaryDictionaryBuilderImpl<BinaryType>;
  using BASE::BASE;
};

class StringDictionaryBuilder : public internal::BinaryDictionaryBuilderImpl<StringType> {
  using BASE = BinaryDictionaryBuilderImpl<StringType>;
  using BASE::BASE;
};

class BinaryDictionary32Builder
    : public internal::BinaryDictionary32BuilderImpl<BinaryType> {
  using BASE = internal::BinaryDictionary32BuilderImpl<BinaryType>;
  using BASE::BASE;
};

class StringDictionary32Builder
    : public internal::BinaryDictionary32BuilderImpl<StringType> {
  using BASE = internal::BinaryDictionary32BuilderImpl<StringType>;
  using BASE::BASE;
};

}  // namespace arrow
