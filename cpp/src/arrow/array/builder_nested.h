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

#include <limits>
#include <memory>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/array/builder_base.h"
#include "arrow/buffer_builder.h"

namespace arrow {

// ----------------------------------------------------------------------
// List builder

template <typename TYPE>
class BaseListBuilder : public ArrayBuilder {
 public:
  using TypeClass = TYPE;
  using offset_type = typename TypeClass::offset_type;

  /// Use this constructor to incrementally build the value array along with offsets and
  /// null bitmap.
  BaseListBuilder(MemoryPool* pool, std::shared_ptr<ArrayBuilder> const& value_builder,
                  const std::shared_ptr<DataType>& type = NULLPTR)
      : ArrayBuilder(type ? type
                          : std::static_pointer_cast<DataType>(
                                std::make_shared<TypeClass>(value_builder->type())),
                     pool),
        offsets_builder_(pool),
        value_builder_(value_builder) {}

  Status Resize(int64_t capacity) override {
    if (capacity > maximum_elements()) {
      return Status::CapacityError("List array cannot reserve space for more than ",
                                   maximum_elements(), " got ", capacity);
    }
    ARROW_RETURN_NOT_OK(CheckCapacity(capacity, capacity_));

    // one more then requested for offsets
    ARROW_RETURN_NOT_OK(offsets_builder_.Resize(capacity + 1));
    return ArrayBuilder::Resize(capacity);
  }

  void Reset() override {
    ArrayBuilder::Reset();
    values_.reset();
    offsets_builder_.Reset();
    value_builder_->Reset();
  }

  /// \brief Vector append
  ///
  /// If passed, valid_bytes is of equal length to values, and any zero byte
  /// will be considered as a null for that slot
  Status AppendValues(const offset_type* offsets, int64_t length,
                      const uint8_t* valid_bytes = NULLPTR) {
    ARROW_RETURN_NOT_OK(Reserve(length));
    UnsafeAppendToBitmap(valid_bytes, length);
    offsets_builder_.UnsafeAppend(offsets, length);
    return Status::OK();
  }

  /// \brief Start a new variable-length list slot
  ///
  /// This function should be called before beginning to append elements to the
  /// value builder
  Status Append(bool is_valid = true) {
    ARROW_RETURN_NOT_OK(Reserve(1));
    UnsafeAppendToBitmap(is_valid);
    return AppendNextOffset();
  }

  Status AppendNull() final { return Append(false); }

  Status AppendNulls(int64_t length) final {
    ARROW_RETURN_NOT_OK(Reserve(length));
    ARROW_RETURN_NOT_OK(CheckNextOffset());
    UnsafeAppendToBitmap(length, false);
    const int64_t num_values = value_builder_->length();
    for (int64_t i = 0; i < length; ++i) {
      offsets_builder_.UnsafeAppend(static_cast<offset_type>(num_values));
    }
    return Status::OK();
  }

  Status FinishInternal(std::shared_ptr<ArrayData>* out) override {
    ARROW_RETURN_NOT_OK(AppendNextOffset());

    // Offset padding zeroed by BufferBuilder
    std::shared_ptr<Buffer> offsets;
    ARROW_RETURN_NOT_OK(offsets_builder_.Finish(&offsets));

    std::shared_ptr<ArrayData> items;
    if (values_) {
      items = values_->data();
    } else {
      if (value_builder_->length() == 0) {
        // Try to make sure we get a non-null values buffer (ARROW-2744)
        ARROW_RETURN_NOT_OK(value_builder_->Resize(0));
      }
      ARROW_RETURN_NOT_OK(value_builder_->FinishInternal(&items));
    }

    // If the type has not been specified in the constructor, infer it
    // This is the case if the value_builder contains a DenseUnionBuilder
    if (!arrow::internal::checked_cast<TypeClass&>(*type_).value_type()) {
      type_ = std::static_pointer_cast<DataType>(
          std::make_shared<TypeClass>(value_builder_->type()));
    }
    std::shared_ptr<Buffer> null_bitmap;
    ARROW_RETURN_NOT_OK(null_bitmap_builder_.Finish(&null_bitmap));
    *out = ArrayData::Make(type_, length_, {null_bitmap, offsets}, null_count_);
    (*out)->child_data.emplace_back(std::move(items));
    Reset();
    return Status::OK();
  }

  ArrayBuilder* value_builder() const { return value_builder_.get(); }

  // Cannot make this a static attribute because of linking issues
  static constexpr int64_t maximum_elements() {
    return std::numeric_limits<offset_type>::max() - 1;
  }

 protected:
  TypedBufferBuilder<offset_type> offsets_builder_;
  std::shared_ptr<ArrayBuilder> value_builder_;
  std::shared_ptr<Array> values_;

  Status CheckNextOffset() const {
    const int64_t num_values = value_builder_->length();
    ARROW_RETURN_IF(
        num_values > maximum_elements(),
        Status::CapacityError("List array cannot contain more than ", maximum_elements(),
                              " child elements,", " have ", num_values));
    return Status::OK();
  }

  Status AppendNextOffset() {
    ARROW_RETURN_NOT_OK(CheckNextOffset());
    const int64_t num_values = value_builder_->length();
    return offsets_builder_.Append(static_cast<offset_type>(num_values));
  }
};

/// \class ListBuilder
/// \brief Builder class for variable-length list array value types
///
/// To use this class, you must append values to the child array builder and use
/// the Append function to delimit each distinct list value (once the values
/// have been appended to the child array) or use the bulk API to append
/// a sequence of offests and null values.
///
/// A note on types.  Per arrow/type.h all types in the c++ implementation are
/// logical so even though this class always builds list array, this can
/// represent multiple different logical types.  If no logical type is provided
/// at construction time, the class defaults to List<T> where t is taken from the
/// value_builder/values that the object is constructed with.
class ARROW_EXPORT ListBuilder : public BaseListBuilder<ListType> {
 public:
  using BaseListBuilder::BaseListBuilder;

  /// \cond FALSE
  using ArrayBuilder::Finish;
  /// \endcond

  Status Finish(std::shared_ptr<ListArray>* out) { return FinishTyped(out); }
};

/// \class LargeListBuilder
/// \brief Builder class for large variable-length list array value types
///
/// Like ListBuilder, but to create large list arrays (with 64-bit offsets).
class ARROW_EXPORT LargeListBuilder : public BaseListBuilder<LargeListType> {
 public:
  using BaseListBuilder::BaseListBuilder;

  /// \cond FALSE
  using ArrayBuilder::Finish;
  /// \endcond

  Status Finish(std::shared_ptr<LargeListArray>* out) { return FinishTyped(out); }
};

// ----------------------------------------------------------------------
// Map builder

/// \class MapBuilder
/// \brief Builder class for arrays of variable-size maps
///
/// To use this class, you must append values to the key and item array builders
/// and use the Append function to delimit each distinct map (once the keys and items
/// have been appended) or use the bulk API to append a sequence of offests and null
/// maps.
///
/// Key uniqueness and ordering are not validated.
class ARROW_EXPORT MapBuilder : public ArrayBuilder {
 public:
  /// Use this constructor to incrementally build the key and item arrays along with
  /// offsets and null bitmap.
  MapBuilder(MemoryPool* pool, const std::shared_ptr<ArrayBuilder>& key_builder,
             const std::shared_ptr<ArrayBuilder>& item_builder,
             const std::shared_ptr<DataType>& type);

  /// Derive built type from key and item builders' types
  MapBuilder(MemoryPool* pool, const std::shared_ptr<ArrayBuilder>& key_builder,
             const std::shared_ptr<ArrayBuilder>& item_builder, bool keys_sorted = false);

  Status Resize(int64_t capacity) override;
  void Reset() override;
  Status FinishInternal(std::shared_ptr<ArrayData>* out) override;

  /// \cond FALSE
  using ArrayBuilder::Finish;
  /// \endcond

  Status Finish(std::shared_ptr<MapArray>* out) { return FinishTyped(out); }

  /// \brief Vector append
  ///
  /// If passed, valid_bytes is of equal length to values, and any zero byte
  /// will be considered as a null for that slot
  Status AppendValues(const int32_t* offsets, int64_t length,
                      const uint8_t* valid_bytes = NULLPTR);

  /// \brief Start a new variable-length map slot
  ///
  /// This function should be called before beginning to append elements to the
  /// key and value builders
  Status Append();

  Status AppendNull() final;

  Status AppendNulls(int64_t length) final;

  ArrayBuilder* key_builder() const { return key_builder_.get(); }
  ArrayBuilder* item_builder() const { return item_builder_.get(); }

 protected:
  std::shared_ptr<ListBuilder> list_builder_;
  std::shared_ptr<ArrayBuilder> key_builder_;
  std::shared_ptr<ArrayBuilder> item_builder_;
};

// ----------------------------------------------------------------------
// FixedSizeList builder

/// \class FixedSizeListBuilder
/// \brief Builder class for fixed-length list array value types
class ARROW_EXPORT FixedSizeListBuilder : public ArrayBuilder {
 public:
  FixedSizeListBuilder(MemoryPool* pool,
                       std::shared_ptr<ArrayBuilder> const& value_builder,
                       int32_t list_size);

  FixedSizeListBuilder(MemoryPool* pool,
                       std::shared_ptr<ArrayBuilder> const& value_builder,
                       const std::shared_ptr<DataType>& type);

  Status Resize(int64_t capacity) override;
  void Reset() override;
  Status FinishInternal(std::shared_ptr<ArrayData>* out) override;

  /// \cond FALSE
  using ArrayBuilder::Finish;
  /// \endcond

  Status Finish(std::shared_ptr<FixedSizeListArray>* out) { return FinishTyped(out); }

  /// \brief Append a valid fixed length list.
  ///
  /// This function affects only the validity bitmap; the child values must be appended
  /// using the child array builder.
  Status Append();

  /// \brief Vector append
  ///
  /// If passed, valid_bytes wil be read and any zero byte
  /// will cause the corresponding slot to be null
  ///
  /// This function affects only the validity bitmap; the child values must be appended
  /// using the child array builder. This includes appending nulls for null lists.
  /// XXX this restriction is confusing, should this method be omitted?
  Status AppendValues(int64_t length, const uint8_t* valid_bytes = NULLPTR);

  /// \brief Append a null fixed length list.
  ///
  /// The child array builder will have the approriate number of nulls appended
  /// automatically.
  Status AppendNull() final;

  /// \brief Append length null fixed length lists.
  ///
  /// The child array builder will have the approriate number of nulls appended
  /// automatically.
  Status AppendNulls(int64_t length) final;

  ArrayBuilder* value_builder() const { return value_builder_.get(); }

 protected:
  const int32_t list_size_;
  std::shared_ptr<ArrayBuilder> value_builder_;
};

// ----------------------------------------------------------------------
// Struct

// ---------------------------------------------------------------------------------
// StructArray builder
/// Append, Resize and Reserve methods are acting on StructBuilder.
/// Please make sure all these methods of all child-builders' are consistently
/// called to maintain data-structure consistency.
class ARROW_EXPORT StructBuilder : public ArrayBuilder {
 public:
  StructBuilder(const std::shared_ptr<DataType>& type, MemoryPool* pool,
                std::vector<std::shared_ptr<ArrayBuilder>> field_builders);

  Status FinishInternal(std::shared_ptr<ArrayData>* out) override;

  /// \cond FALSE
  using ArrayBuilder::Finish;
  /// \endcond

  Status Finish(std::shared_ptr<StructArray>* out) { return FinishTyped(out); }

  /// Null bitmap is of equal length to every child field, and any zero byte
  /// will be considered as a null for that field, but users must using app-
  /// end methods or advance methods of the child builders' independently to
  /// insert data.
  Status AppendValues(int64_t length, const uint8_t* valid_bytes) {
    ARROW_RETURN_NOT_OK(Reserve(length));
    UnsafeAppendToBitmap(valid_bytes, length);
    return Status::OK();
  }

  /// Append an element to the Struct. All child-builders' Append method must
  /// be called independently to maintain data-structure consistency.
  Status Append(bool is_valid = true) {
    ARROW_RETURN_NOT_OK(Reserve(1));
    UnsafeAppendToBitmap(is_valid);
    return Status::OK();
  }

  Status AppendNull() final { return Append(false); }

  Status AppendNulls(int64_t length) final;

  void Reset() override;

  ArrayBuilder* field_builder(int i) const { return children_[i].get(); }

  int num_fields() const { return static_cast<int>(children_.size()); }
};

}  // namespace arrow
