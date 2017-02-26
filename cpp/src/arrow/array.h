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

#ifndef ARROW_ARRAY_H
#define ARROW_ARRAY_H

#include <cmath>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "arrow/buffer.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {

class MemoryPool;
class MutableBuffer;
class Status;

class ARROW_EXPORT ArrayVisitor {
 public:
  virtual ~ArrayVisitor() = default;

  virtual Status Visit(const NullArray& array);
  virtual Status Visit(const BooleanArray& array);
  virtual Status Visit(const Int8Array& array);
  virtual Status Visit(const Int16Array& array);
  virtual Status Visit(const Int32Array& array);
  virtual Status Visit(const Int64Array& array);
  virtual Status Visit(const UInt8Array& array);
  virtual Status Visit(const UInt16Array& array);
  virtual Status Visit(const UInt32Array& array);
  virtual Status Visit(const UInt64Array& array);
  virtual Status Visit(const HalfFloatArray& array);
  virtual Status Visit(const FloatArray& array);
  virtual Status Visit(const DoubleArray& array);
  virtual Status Visit(const StringArray& array);
  virtual Status Visit(const BinaryArray& array);
  virtual Status Visit(const DateArray& array);
  virtual Status Visit(const TimeArray& array);
  virtual Status Visit(const TimestampArray& array);
  virtual Status Visit(const IntervalArray& array);
  virtual Status Visit(const DecimalArray& array);
  virtual Status Visit(const ListArray& array);
  virtual Status Visit(const StructArray& array);
  virtual Status Visit(const UnionArray& array);
  virtual Status Visit(const DictionaryArray& type);
};

/// Immutable data array with some logical type and some length.
///
/// Any memory is owned by the respective Buffer instance (or its parents).
///
/// The base class is only required to have a null bitmap buffer if the null
/// count is greater than 0
///
/// If known, the null count can be provided in the base Array constructor. If
/// the null count is not known, pass -1 to indicate that the null count is to
/// be computed on the first call to null_count()
class ARROW_EXPORT Array {
 public:
  Array(const std::shared_ptr<DataType>& type, int32_t length,
      const std::shared_ptr<Buffer>& null_bitmap = nullptr, int32_t null_count = 0,
      int32_t offset = 0);

  virtual ~Array() = default;

  /// Determine if a slot is null. For inner loops. Does *not* boundscheck
  bool IsNull(int i) const {
    return null_bitmap_data_ != nullptr &&
           BitUtil::BitNotSet(null_bitmap_data_, i + offset_);
  }

  /// Size in the number of elements this array contains.
  int32_t length() const { return length_; }

  /// A relative position into another array's data, to enable zero-copy
  /// slicing. This value defaults to zero
  int32_t offset() const { return offset_; }

  /// The number of null entries in the array. If the null count was not known
  /// at time of construction (and set to a negative value), then the null
  /// count will be computed and cached on the first invocation of this
  /// function
  int32_t null_count() const;

  std::shared_ptr<DataType> type() const { return type_; }
  Type::type type_enum() const { return type_->type; }

  /// Buffer for the null bitmap.
  ///
  /// Note that for `null_count == 0`, this can be a `nullptr`.
  /// This buffer does not account for any slice offset
  std::shared_ptr<Buffer> null_bitmap() const { return null_bitmap_; }

  /// Raw pointer to the null bitmap.
  ///
  /// Note that for `null_count == 0`, this can be a `nullptr`.
  /// This buffer does not account for any slice offset
  const uint8_t* null_bitmap_data() const { return null_bitmap_data_; }

  bool Equals(const Array& arr) const;
  bool Equals(const std::shared_ptr<Array>& arr) const;

  bool ApproxEquals(const std::shared_ptr<Array>& arr) const;
  bool ApproxEquals(const Array& arr) const;

  /// Compare if the range of slots specified are equal for the given array and
  /// this array.  end_idx exclusive.  This methods does not bounds check.
  bool RangeEquals(int32_t start_idx, int32_t end_idx, int32_t other_start_idx,
      const std::shared_ptr<Array>& other) const;

  bool RangeEquals(const Array& other, int32_t start_idx, int32_t end_idx,
      int32_t other_start_idx) const;

  /// Determines if the array is internally consistent.
  ///
  /// Defaults to always returning Status::OK. This can be an expensive check.
  virtual Status Validate() const;

  virtual Status Accept(ArrayVisitor* visitor) const = 0;

  /// Construct a zero-copy slice of the array with the indicated offset and
  /// length
  ///
  /// \param[in] offset the position of the first element in the constructed slice
  /// \param[in] length the length of the slice. If there are not enough elements in the
  /// array,
  ///     the length will be adjusted accordingly
  ///
  /// \return a new object wrapped in std::shared_ptr<Array>
  virtual std::shared_ptr<Array> Slice(int32_t offset, int32_t length) const = 0;

  /// Slice from offset until end of the array
  std::shared_ptr<Array> Slice(int32_t offset) const;

 protected:
  std::shared_ptr<DataType> type_;
  int32_t length_;
  int32_t offset_;

  // This member is marked mutable so that it can be modified when null_count()
  // is called from a const context and the null count has to be computed (if
  // it is not already known)
  mutable int32_t null_count_;

  std::shared_ptr<Buffer> null_bitmap_;
  const uint8_t* null_bitmap_data_;

 private:
  Array() {}
  DISALLOW_COPY_AND_ASSIGN(Array);
};

/// Degenerate null type Array
class ARROW_EXPORT NullArray : public Array {
 public:
  using TypeClass = NullType;

  explicit NullArray(int32_t length);

  Status Accept(ArrayVisitor* visitor) const override;

  std::shared_ptr<Array> Slice(int32_t offset, int32_t length) const override;
};

/// Base class for fixed-size logical types
class ARROW_EXPORT PrimitiveArray : public Array {
 public:
  PrimitiveArray(const std::shared_ptr<DataType>& type, int32_t length,
      const std::shared_ptr<Buffer>& data,
      const std::shared_ptr<Buffer>& null_bitmap = nullptr, int32_t null_count = 0,
      int32_t offset = 0);

  /// The memory containing this array's data
  /// This buffer does not account for any slice offset
  std::shared_ptr<Buffer> data() const { return data_; }

 protected:
  std::shared_ptr<Buffer> data_;
  const uint8_t* raw_data_;
};

template <typename TYPE>
class ARROW_EXPORT NumericArray : public PrimitiveArray {
 public:
  using TypeClass = TYPE;
  using value_type = typename TypeClass::c_type;

  using PrimitiveArray::PrimitiveArray;

  // Only enable this constructor without a type argument for types without additional
  // metadata
  template <typename T1 = TYPE>
  NumericArray(
      typename std::enable_if<TypeTraits<T1>::is_parameter_free, int32_t>::type length,
      const std::shared_ptr<Buffer>& data,
      const std::shared_ptr<Buffer>& null_bitmap = nullptr, int32_t null_count = 0,
      int32_t offset = 0)
      : PrimitiveArray(TypeTraits<T1>::type_singleton(), length, data, null_bitmap,
            null_count, offset) {}

  const value_type* raw_data() const {
    return reinterpret_cast<const value_type*>(raw_data_) + offset_;
  }

  Status Accept(ArrayVisitor* visitor) const override;

  std::shared_ptr<Array> Slice(int32_t offset, int32_t length) const override;

  value_type Value(int i) const { return raw_data()[i]; }
};

class ARROW_EXPORT BooleanArray : public PrimitiveArray {
 public:
  using TypeClass = BooleanType;

  using PrimitiveArray::PrimitiveArray;

  BooleanArray(int32_t length, const std::shared_ptr<Buffer>& data,
      const std::shared_ptr<Buffer>& null_bitmap = nullptr, int32_t null_count = 0,
      int32_t offset = 0);

  Status Accept(ArrayVisitor* visitor) const override;

  std::shared_ptr<Array> Slice(int32_t offset, int32_t length) const override;

  bool Value(int i) const {
    return BitUtil::GetBit(reinterpret_cast<const uint8_t*>(raw_data_), i + offset_);
  }
};

// ----------------------------------------------------------------------
// ListArray

class ARROW_EXPORT ListArray : public Array {
 public:
  using TypeClass = ListType;

  ListArray(const std::shared_ptr<DataType>& type, int32_t length,
      const std::shared_ptr<Buffer>& value_offsets, const std::shared_ptr<Array>& values,
      const std::shared_ptr<Buffer>& null_bitmap = nullptr, int32_t null_count = 0,
      int32_t offset = 0)
      : Array(type, length, null_bitmap, null_count, offset) {
    value_offsets_ = value_offsets;
    raw_value_offsets_ = value_offsets == nullptr
                             ? nullptr
                             : reinterpret_cast<const int32_t*>(value_offsets_->data());
    values_ = values;
  }

  Status Validate() const override;

  // Return a shared pointer in case the requestor desires to share ownership
  // with this array.
  std::shared_ptr<Array> values() const { return values_; }

  /// Note that this buffer does not account for any slice offset
  std::shared_ptr<Buffer> value_offsets() const { return value_offsets_; }

  std::shared_ptr<DataType> value_type() const { return values_->type(); }

  /// Return pointer to raw value offsets accounting for any slice offset
  const int32_t* raw_value_offsets() const { return raw_value_offsets_ + offset_; }

  // Neither of these functions will perform boundschecking
  int32_t value_offset(int i) const { return raw_value_offsets_[i + offset_]; }
  int32_t value_length(int i) const {
    i += offset_;
    return raw_value_offsets_[i + 1] - raw_value_offsets_[i];
  }

  Status Accept(ArrayVisitor* visitor) const override;

  std::shared_ptr<Array> Slice(int32_t offset, int32_t length) const override;

 protected:
  std::shared_ptr<Buffer> value_offsets_;
  const int32_t* raw_value_offsets_;
  std::shared_ptr<Array> values_;
};

// ----------------------------------------------------------------------
// Binary and String

class ARROW_EXPORT BinaryArray : public Array {
 public:
  using TypeClass = BinaryType;

  BinaryArray(int32_t length, const std::shared_ptr<Buffer>& value_offsets,
      const std::shared_ptr<Buffer>& data,
      const std::shared_ptr<Buffer>& null_bitmap = nullptr, int32_t null_count = 0,
      int32_t offset = 0);

  // Return the pointer to the given elements bytes
  // TODO(emkornfield) introduce a StringPiece or something similar to capture zero-copy
  // pointer + offset
  const uint8_t* GetValue(int i, int32_t* out_length) const {
    // Account for base offset
    i += offset_;

    const int32_t pos = raw_value_offsets_[i + offset_];
    *out_length = raw_value_offsets_[i + offset_ + 1] - pos;
    return raw_data_ + pos;
  }

  /// Note that this buffer does not account for any slice offset
  std::shared_ptr<Buffer> data() const { return data_; }

  /// Note that this buffer does not account for any slice offset
  std::shared_ptr<Buffer> value_offsets() const { return value_offsets_; }

  const int32_t* raw_value_offsets() const { return raw_value_offsets_ + offset_; }

  // Neither of these functions will perform boundschecking
  int32_t value_offset(int i) const { return raw_value_offsets_[i + offset_]; }
  int32_t value_length(int i) const {
    i += offset_;
    return raw_value_offsets_[i + 1] - raw_value_offsets_[i];
  }

  Status Validate() const override;

  Status Accept(ArrayVisitor* visitor) const override;

  std::shared_ptr<Array> Slice(int32_t offset, int32_t length) const override;

 protected:
  // Constructor that allows sub-classes/builders to propagate there logical type up the
  // class hierarchy.
  BinaryArray(const std::shared_ptr<DataType>& type, int32_t length,
      const std::shared_ptr<Buffer>& value_offsets, const std::shared_ptr<Buffer>& data,
      const std::shared_ptr<Buffer>& null_bitmap = nullptr, int32_t null_count = 0,
      int32_t offset = 0);

  std::shared_ptr<Buffer> value_offsets_;
  const int32_t* raw_value_offsets_;

  std::shared_ptr<Buffer> data_;
  const uint8_t* raw_data_;
};

class ARROW_EXPORT StringArray : public BinaryArray {
 public:
  using TypeClass = StringType;

  StringArray(int32_t length, const std::shared_ptr<Buffer>& value_offsets,
      const std::shared_ptr<Buffer>& data,
      const std::shared_ptr<Buffer>& null_bitmap = nullptr, int32_t null_count = 0,
      int32_t offset = 0);

  // Construct a std::string
  // TODO: std::bad_alloc possibility
  std::string GetString(int i) const {
    int32_t nchars;
    const uint8_t* str = GetValue(i, &nchars);
    return std::string(reinterpret_cast<const char*>(str), nchars);
  }

  Status Validate() const override;

  Status Accept(ArrayVisitor* visitor) const override;

  std::shared_ptr<Array> Slice(int32_t offset, int32_t length) const override;
};

// ----------------------------------------------------------------------
// Struct

class ARROW_EXPORT StructArray : public Array {
 public:
  using TypeClass = StructType;

  StructArray(const std::shared_ptr<DataType>& type, int32_t length,
      const std::vector<std::shared_ptr<Array>>& children,
      std::shared_ptr<Buffer> null_bitmap = nullptr, int32_t null_count = 0,
      int32_t offset = 0);

  Status Validate() const override;

  // Return a shared pointer in case the requestor desires to share ownership
  // with this array.
  std::shared_ptr<Array> field(int32_t pos) const;

  const std::vector<std::shared_ptr<Array>>& fields() const { return children_; }

  Status Accept(ArrayVisitor* visitor) const override;

  std::shared_ptr<Array> Slice(int32_t offset, int32_t length) const override;

 protected:
  // The child arrays corresponding to each field of the struct data type.
  std::vector<std::shared_ptr<Array>> children_;
};

// ----------------------------------------------------------------------
// Union

class ARROW_EXPORT UnionArray : public Array {
 public:
  using TypeClass = UnionType;
  using type_id_t = uint8_t;

  UnionArray(const std::shared_ptr<DataType>& type, int32_t length,
      const std::vector<std::shared_ptr<Array>>& children,
      const std::shared_ptr<Buffer>& type_ids,
      const std::shared_ptr<Buffer>& value_offsets = nullptr,
      const std::shared_ptr<Buffer>& null_bitmap = nullptr, int32_t null_count = 0,
      int32_t offset = 0);

  Status Validate() const override;

  /// Note that this buffer does not account for any slice offset
  std::shared_ptr<Buffer> type_ids() const { return type_ids_; }

  /// Note that this buffer does not account for any slice offset
  std::shared_ptr<Buffer> value_offsets() const { return value_offsets_; }

  const type_id_t* raw_type_ids() const { return raw_type_ids_ + offset_; }
  const int32_t* raw_value_offsets() const { return raw_value_offsets_ + offset_; }

  UnionMode mode() const { return static_cast<const UnionType&>(*type_.get()).mode; }

  std::shared_ptr<Array> child(int32_t pos) const;

  const std::vector<std::shared_ptr<Array>>& children() const { return children_; }

  Status Accept(ArrayVisitor* visitor) const override;

  std::shared_ptr<Array> Slice(int32_t offset, int32_t length) const override;

 protected:
  std::vector<std::shared_ptr<Array>> children_;

  std::shared_ptr<Buffer> type_ids_;
  const type_id_t* raw_type_ids_;

  std::shared_ptr<Buffer> value_offsets_;
  const int32_t* raw_value_offsets_;
};

// ----------------------------------------------------------------------
// DictionaryArray (categorical and dictionary-encoded in memory)

// A dictionary array contains an array of non-negative integers (the
// "dictionary indices") along with a data type containing a "dictionary"
// corresponding to the distinct values represented in the data.
//
// For example, the array
//
//   ["foo", "bar", "foo", "bar", "foo", "bar"]
//
// with dictionary ["bar", "foo"], would have dictionary array representation
//
//   indices: [1, 0, 1, 0, 1, 0]
//   dictionary: ["bar", "foo"]
//
// The indices in principle may have any integer type (signed or unsigned),
// though presently data in IPC exchanges must be signed int32.
class ARROW_EXPORT DictionaryArray : public Array {
 public:
  using TypeClass = DictionaryType;

  DictionaryArray(
      const std::shared_ptr<DataType>& type, const std::shared_ptr<Array>& indices);

  // Alternate ctor; other attributes (like null count) are inherited from the
  // passed indices array
  static Status FromBuffer(const std::shared_ptr<DataType>& type, int32_t length,
      const std::shared_ptr<Buffer>& indices, const std::shared_ptr<Buffer>& null_bitmap,
      int32_t null_count, int32_t offset, std::shared_ptr<DictionaryArray>* out);

  Status Validate() const override;

  std::shared_ptr<Array> indices() const { return indices_; }
  std::shared_ptr<Array> dictionary() const;

  const DictionaryType* dict_type() { return dict_type_; }

  Status Accept(ArrayVisitor* visitor) const override;

  std::shared_ptr<Array> Slice(int32_t offset, int32_t length) const override;

 protected:
  const DictionaryType* dict_type_;
  std::shared_ptr<Array> indices_;
};

// ----------------------------------------------------------------------
// extern templates and other details

// gcc and clang disagree about how to handle template visibility when you have
// explicit specializations https://llvm.org/bugs/show_bug.cgi?id=24815
#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wattributes"
#endif

// Only instantiate these templates once
extern template class ARROW_EXPORT NumericArray<Int8Type>;
extern template class ARROW_EXPORT NumericArray<UInt8Type>;
extern template class ARROW_EXPORT NumericArray<Int16Type>;
extern template class ARROW_EXPORT NumericArray<UInt16Type>;
extern template class ARROW_EXPORT NumericArray<Int32Type>;
extern template class ARROW_EXPORT NumericArray<UInt32Type>;
extern template class ARROW_EXPORT NumericArray<Int64Type>;
extern template class ARROW_EXPORT NumericArray<UInt64Type>;
extern template class ARROW_EXPORT NumericArray<HalfFloatType>;
extern template class ARROW_EXPORT NumericArray<FloatType>;
extern template class ARROW_EXPORT NumericArray<DoubleType>;
extern template class ARROW_EXPORT NumericArray<TimestampType>;
extern template class ARROW_EXPORT NumericArray<DateType>;
extern template class ARROW_EXPORT NumericArray<TimeType>;

#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC diagnostic pop
#endif

// ----------------------------------------------------------------------
// Helper functions

// Create new arrays for logical types that are backed by primitive arrays.
Status ARROW_EXPORT MakePrimitiveArray(const std::shared_ptr<DataType>& type,
    int32_t length, const std::shared_ptr<Buffer>& data,
    const std::shared_ptr<Buffer>& null_bitmap, int32_t null_count, int32_t offset,
    std::shared_ptr<Array>* out);

}  // namespace arrow

#endif
