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

#ifndef ARROW_BUILDER_H
#define ARROW_BUILDER_H

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "arrow/buffer.h"
#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {

class Array;

static constexpr int64_t kMinBuilderCapacity = 1 << 5;

/// Base class for all data array builders.
//
/// This class provides a facilities for incrementally building the null bitmap
/// (see Append methods) and as a side effect the current number of slots and
/// the null count.
class ARROW_EXPORT ArrayBuilder {
 public:
  explicit ArrayBuilder(MemoryPool* pool, const TypePtr& type)
      : pool_(pool),
        type_(type),
        null_bitmap_(nullptr),
        null_count_(0),
        null_bitmap_data_(nullptr),
        length_(0),
        capacity_(0) {}

  virtual ~ArrayBuilder() = default;

  /// For nested types. Since the objects are owned by this class instance, we
  /// skip shared pointers and just return a raw pointer
  ArrayBuilder* child(int i) { return children_[i].get(); }

  int num_children() const { return static_cast<int>(children_.size()); }

  int64_t length() const { return length_; }
  int64_t null_count() const { return null_count_; }
  int64_t capacity() const { return capacity_; }

  /// Append to null bitmap
  Status AppendToBitmap(bool is_valid);
  /// Vector append. Treat each zero byte as a null.   If valid_bytes is null
  /// assume all of length bits are valid.
  Status AppendToBitmap(const uint8_t* valid_bytes, int64_t length);
  /// Set the next length bits to not null (i.e. valid).
  Status SetNotNull(int64_t length);

  /// Allocates initial capacity requirements for the builder.  In most
  /// cases subclasses should override and call there parent classes
  /// method as well.
  virtual Status Init(int64_t capacity);

  /// Resizes the null_bitmap array.  In most
  /// cases subclasses should override and call there parent classes
  /// method as well.
  virtual Status Resize(int64_t new_bits);

  /// Ensures there is enough space for adding the number of elements by checking
  /// capacity and calling Resize if necessary.
  Status Reserve(int64_t elements);

  /// For cases where raw data was memcpy'd into the internal buffers, allows us
  /// to advance the length of the builder. It is your responsibility to use
  /// this function responsibly.
  Status Advance(int64_t elements);

  std::shared_ptr<PoolBuffer> null_bitmap() const { return null_bitmap_; }

  /// Creates new Array object to hold the contents of the builder and transfers
  /// ownership of the data.  This resets all variables on the builder.
  virtual Status Finish(std::shared_ptr<Array>* out) = 0;

  std::shared_ptr<DataType> type() const { return type_; }

 protected:
  MemoryPool* pool_;

  std::shared_ptr<DataType> type_;

  // When null_bitmap are first appended to the builder, the null bitmap is allocated
  std::shared_ptr<PoolBuffer> null_bitmap_;
  int64_t null_count_;
  uint8_t* null_bitmap_data_;

  // Array length, so far. Also, the index of the next element to be added
  int64_t length_;
  int64_t capacity_;

  // Child value array builders. These are owned by this class
  std::vector<std::unique_ptr<ArrayBuilder>> children_;

  //
  // Unsafe operations (don't check capacity/don't resize)
  //

  // Append to null bitmap.
  void UnsafeAppendToBitmap(bool is_valid);
  // Vector append. Treat each zero byte as a nullzero. If valid_bytes is null
  // assume all of length bits are valid.
  void UnsafeAppendToBitmap(const uint8_t* valid_bytes, int64_t length);
  // Set the next length bits to not null (i.e. valid).
  void UnsafeSetNotNull(int64_t length);

 private:
  DISALLOW_COPY_AND_ASSIGN(ArrayBuilder);
};

template <typename Type>
class ARROW_EXPORT PrimitiveBuilder : public ArrayBuilder {
 public:
  using value_type = typename Type::c_type;

  explicit PrimitiveBuilder(MemoryPool* pool, const TypePtr& type)
      : ArrayBuilder(pool, type), data_(nullptr), raw_data_(nullptr) {}

  using ArrayBuilder::Advance;

  /// Write nulls as uint8_t* (0 value indicates null) into pre-allocated memory
  Status AppendNulls(const uint8_t* valid_bytes, int64_t length) {
    RETURN_NOT_OK(Reserve(length));
    UnsafeAppendToBitmap(valid_bytes, length);
    return Status::OK();
  }

  Status AppendNull() {
    RETURN_NOT_OK(Reserve(1));
    UnsafeAppendToBitmap(false);
    return Status::OK();
  }

  std::shared_ptr<Buffer> data() const { return data_; }

  /// Vector append
  ///
  /// If passed, valid_bytes is of equal length to values, and any zero byte
  /// will be considered as a null for that slot
  Status Append(
      const value_type* values, int64_t length, const uint8_t* valid_bytes = nullptr);

  Status Finish(std::shared_ptr<Array>* out) override;
  Status Init(int64_t capacity) override;

  /// Increase the capacity of the builder to accommodate at least the indicated
  /// number of elements
  Status Resize(int64_t capacity) override;

 protected:
  std::shared_ptr<PoolBuffer> data_;
  value_type* raw_data_;
};

/// Base class for all Builders that emit an Array of a scalar numerical type.
template <typename T>
class ARROW_EXPORT NumericBuilder : public PrimitiveBuilder<T> {
 public:
  using typename PrimitiveBuilder<T>::value_type;
  using PrimitiveBuilder<T>::PrimitiveBuilder;

  template <typename T1 = T>
  explicit NumericBuilder(
      typename std::enable_if<TypeTraits<T1>::is_parameter_free, MemoryPool*>::type pool)
      : PrimitiveBuilder<T1>(pool, TypeTraits<T1>::type_singleton()) {}

  using PrimitiveBuilder<T>::Append;
  using PrimitiveBuilder<T>::Init;
  using PrimitiveBuilder<T>::Resize;
  using PrimitiveBuilder<T>::Reserve;

  /// Append a single scalar and increase the size if necessary.
  Status Append(value_type val) {
    RETURN_NOT_OK(ArrayBuilder::Reserve(1));
    UnsafeAppend(val);
    return Status::OK();
  }

  /// Append a single scalar under the assumption that the underlying Buffer is
  /// large enough.
  ///
  /// This method does not capacity-check; make sure to call Reserve
  /// beforehand.
  void UnsafeAppend(value_type val) {
    BitUtil::SetBit(null_bitmap_data_, length_);
    raw_data_[length_++] = val;
  }

 protected:
  using PrimitiveBuilder<T>::length_;
  using PrimitiveBuilder<T>::null_bitmap_data_;
  using PrimitiveBuilder<T>::raw_data_;
};

// Builders

using UInt8Builder = NumericBuilder<UInt8Type>;
using UInt16Builder = NumericBuilder<UInt16Type>;
using UInt32Builder = NumericBuilder<UInt32Type>;
using UInt64Builder = NumericBuilder<UInt64Type>;

using Int8Builder = NumericBuilder<Int8Type>;
using Int16Builder = NumericBuilder<Int16Type>;
using Int32Builder = NumericBuilder<Int32Type>;
using Int64Builder = NumericBuilder<Int64Type>;
using TimestampBuilder = NumericBuilder<TimestampType>;
using TimeBuilder = NumericBuilder<TimeType>;
using DateBuilder = NumericBuilder<DateType>;

using HalfFloatBuilder = NumericBuilder<HalfFloatType>;
using FloatBuilder = NumericBuilder<FloatType>;
using DoubleBuilder = NumericBuilder<DoubleType>;

class ARROW_EXPORT BooleanBuilder : public ArrayBuilder {
 public:
  explicit BooleanBuilder(MemoryPool* pool);
  explicit BooleanBuilder(MemoryPool* pool, const std::shared_ptr<DataType>& type);

  using ArrayBuilder::Advance;

  /// Write nulls as uint8_t* (0 value indicates null) into pre-allocated memory
  Status AppendNulls(const uint8_t* valid_bytes, int64_t length) {
    RETURN_NOT_OK(Reserve(length));
    UnsafeAppendToBitmap(valid_bytes, length);
    return Status::OK();
  }

  Status AppendNull() {
    RETURN_NOT_OK(Reserve(1));
    UnsafeAppendToBitmap(false);
    return Status::OK();
  }

  std::shared_ptr<Buffer> data() const { return data_; }

  /// Scalar append
  Status Append(bool val) {
    Reserve(1);
    BitUtil::SetBit(null_bitmap_data_, length_);
    if (val) {
      BitUtil::SetBit(raw_data_, length_);
    } else {
      BitUtil::ClearBit(raw_data_, length_);
    }
    ++length_;
    return Status::OK();
  }

  /// Vector append
  ///
  /// If passed, valid_bytes is of equal length to values, and any zero byte
  /// will be considered as a null for that slot
  Status Append(
      const uint8_t* values, int64_t length, const uint8_t* valid_bytes = nullptr);

  Status Finish(std::shared_ptr<Array>* out) override;
  Status Init(int64_t capacity) override;

  /// Increase the capacity of the builder to accommodate at least the indicated
  /// number of elements
  Status Resize(int64_t capacity) override;

 protected:
  std::shared_ptr<PoolBuffer> data_;
  uint8_t* raw_data_;
};

// ----------------------------------------------------------------------
// List builder

/// Builder class for variable-length list array value types
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
class ARROW_EXPORT ListBuilder : public ArrayBuilder {
 public:
  /// Use this constructor to incrementally build the value array along with offsets and
  /// null bitmap.
  ListBuilder(MemoryPool* pool, std::shared_ptr<ArrayBuilder> value_builder,
      const TypePtr& type = nullptr);

  /// Use this constructor to build the list with a pre-existing values array
  ListBuilder(
      MemoryPool* pool, std::shared_ptr<Array> values, const TypePtr& type = nullptr);

  Status Init(int64_t elements) override;
  Status Resize(int64_t capacity) override;
  Status Finish(std::shared_ptr<Array>* out) override;

  /// Vector append
  ///
  /// If passed, valid_bytes is of equal length to values, and any zero byte
  /// will be considered as a null for that slot
  Status Append(
      const int32_t* offsets, int64_t length, const uint8_t* valid_bytes = nullptr) {
    RETURN_NOT_OK(Reserve(length));
    UnsafeAppendToBitmap(valid_bytes, length);
    offset_builder_.UnsafeAppend<int32_t>(offsets, length);
    return Status::OK();
  }

  /// Start a new variable-length list slot
  ///
  /// This function should be called before beginning to append elements to the
  /// value builder
  Status Append(bool is_valid = true) {
    RETURN_NOT_OK(Reserve(1));
    UnsafeAppendToBitmap(is_valid);
    RETURN_NOT_OK(
        offset_builder_.Append<int32_t>(static_cast<int32_t>(value_builder_->length())));
    return Status::OK();
  }

  Status AppendNull() { return Append(false); }

  std::shared_ptr<ArrayBuilder> value_builder() const;

 protected:
  BufferBuilder offset_builder_;
  std::shared_ptr<ArrayBuilder> value_builder_;
  std::shared_ptr<Array> values_;

  void Reset();
};

// ----------------------------------------------------------------------
// Binary and String

// BinaryBuilder : public ListBuilder
class ARROW_EXPORT BinaryBuilder : public ListBuilder {
 public:
  explicit BinaryBuilder(MemoryPool* pool);
  explicit BinaryBuilder(MemoryPool* pool, const TypePtr& type);

  Status Append(const uint8_t* value, int32_t length) {
    RETURN_NOT_OK(ListBuilder::Append());
    return byte_builder_->Append(value, length);
  }

  Status Append(const char* value, int32_t length) {
    return Append(reinterpret_cast<const uint8_t*>(value), length);
  }

  Status Append(const std::string& value) {
    return Append(value.c_str(), static_cast<int32_t>(value.size()));
  }

  Status Finish(std::shared_ptr<Array>* out) override;

 protected:
  UInt8Builder* byte_builder_;
};

// String builder
class ARROW_EXPORT StringBuilder : public BinaryBuilder {
 public:
  explicit StringBuilder(MemoryPool* pool);

  using BinaryBuilder::Append;

  Status Finish(std::shared_ptr<Array>* out) override;

  Status Append(const std::vector<std::string>& values, uint8_t* null_bytes);
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
  StructBuilder(MemoryPool* pool, const std::shared_ptr<DataType>& type,
      const std::vector<std::shared_ptr<ArrayBuilder>>& field_builders)
      : ArrayBuilder(pool, type) {
    field_builders_ = field_builders;
  }

  Status Finish(std::shared_ptr<Array>* out) override;

  /// Null bitmap is of equal length to every child field, and any zero byte
  /// will be considered as a null for that field, but users must using app-
  /// end methods or advance methods of the child builders' independently to
  /// insert data.
  Status Append(int64_t length, const uint8_t* valid_bytes) {
    RETURN_NOT_OK(Reserve(length));
    UnsafeAppendToBitmap(valid_bytes, length);
    return Status::OK();
  }

  /// Append an element to the Struct. All child-builders' Append method must
  /// be called independently to maintain data-structure consistency.
  Status Append(bool is_valid = true) {
    RETURN_NOT_OK(Reserve(1));
    UnsafeAppendToBitmap(is_valid);
    return Status::OK();
  }

  Status AppendNull() { return Append(false); }

  std::shared_ptr<ArrayBuilder> field_builder(int pos) const;

  const std::vector<std::shared_ptr<ArrayBuilder>>& field_builders() const {
    return field_builders_;
  }

 protected:
  std::vector<std::shared_ptr<ArrayBuilder>> field_builders_;
};

// ----------------------------------------------------------------------
// Helper functions

Status ARROW_EXPORT MakeBuilder(MemoryPool* pool, const std::shared_ptr<DataType>& type,
    std::shared_ptr<ArrayBuilder>* out);

}  // namespace arrow

#endif  // ARROW_BUILDER_H_
