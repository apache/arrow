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
#include "arrow/visitor.h"

namespace arrow {

using BufferVector = std::vector<std::shared_ptr<Buffer>>;

// When slicing, we do not know the null count of the sliced range without
// doing some computation. To avoid doing this eagerly, we set the null count
// to -1 (any negative number will do). When Array::null_count is called the
// first time, the null count will be computed. See ARROW-33
constexpr int64_t kUnknownNullCount = -1;

class MemoryPool;
class Status;

template <typename T>
struct Decimal;

/// \brief Container for generic array data
struct ARROW_EXPORT ArrayData {
  ArrayData(const std::shared_ptr<DataType>& type, int64_t length,
      const std::vector<std::shared_ptr<Buffer>>& buffers,
      int64_t null_count = kUnknownNullCount, int64_t offset = 0)
      : type(type),
        length(length),
        buffers(buffers),
        null_count(null_count),
        offset(offset) {}

  ArrayData(const std::shared_ptr<DataType>& type, int64_t length,
      std::vector<std::shared_ptr<Buffer>>&& buffers,
      int64_t null_count = kUnknownNullCount, int64_t offset = 0)
      : type(type),
        length(length),
        buffers(std::move(buffers)),
        null_count(null_count),
        offset(offset) {}

  std::shared_ptr<ArrayData> ShallowCopy() const {
    auto result = std::make_shared<ArrayData>(type, length, buffers, null_count, offset);
    result->child_data = this->child_data;
    return result;
  }

  std::shared_ptr<DataType> type;
  int64_t length;
  std::vector<std::shared_ptr<Buffer>> buffers;
  int64_t null_count;
  int64_t offset;
  std::vector<std::shared_ptr<ArrayData>> child_data;
};

/// \brief Array base type
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
  virtual ~Array() = default;

  /// Determine if a slot is null. For inner loops. Does *not* boundscheck
  bool IsNull(int64_t i) const {
    return null_bitmap_data_ != nullptr &&
           BitUtil::BitNotSet(null_bitmap_data_, i + offset_);
  }

  /// Size in the number of elements this array contains.
  int64_t length() const { return length_; }

  /// A relative position into another array's data, to enable zero-copy
  /// slicing. This value defaults to zero
  int64_t offset() const { return offset_; }

  /// The number of null entries in the array. If the null count was not known
  /// at time of construction (and set to a negative value), then the null
  /// count will be computed and cached on the first invocation of this
  /// function
  int64_t null_count() const;

  std::shared_ptr<DataType> type() const { return data_->type; }
  Type::type type_id() const { return data_->type->id(); }

  /// Buffer for the null bitmap.
  ///
  /// Note that for `null_count == 0`, this can be a `nullptr`.
  /// This buffer does not account for any slice offset
  std::shared_ptr<Buffer> null_bitmap() const { return data_->buffers[0]; }

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
  bool RangeEquals(int64_t start_idx, int64_t end_idx, int64_t other_start_idx,
      const std::shared_ptr<Array>& other) const;

  bool RangeEquals(const Array& other, int64_t start_idx, int64_t end_idx,
      int64_t other_start_idx) const;

  Status Accept(ArrayVisitor* visitor) const;

  /// Construct a zero-copy slice of the array with the indicated offset and
  /// length
  ///
  /// \param[in] offset the position of the first element in the constructed slice
  /// \param[in] length the length of the slice. If there are not enough elements in the
  /// array,
  ///     the length will be adjusted accordingly
  ///
  /// \return a new object wrapped in std::shared_ptr<Array>
  virtual std::shared_ptr<Array> Slice(int64_t offset, int64_t length) const = 0;

  /// Slice from offset until end of the array
  std::shared_ptr<Array> Slice(int64_t offset) const;

  std::shared_ptr<ArrayData> data() const { return data_; }

 protected:
  Array() {}

  std::shared_ptr<ArrayData> data_;
  int64_t length_;
  int64_t offset_;
  const uint8_t* null_bitmap_data_;

  /// Protected method for constructors
  inline void SetData(const std::shared_ptr<ArrayData>& data) {
    data_ = data;
    length_ = data->length;
    offset_ = data->offset;
    if (data->buffers.size() > 0 && data->buffers[0]) {
      null_bitmap_data_ = data->buffers[0]->data();
    } else {
      null_bitmap_data_ = nullptr;
    }
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(Array);
};

/// Degenerate null type Array
class ARROW_EXPORT NullArray : public Array {
 public:
  using TypeClass = NullType;

  explicit NullArray(int64_t length);

  std::shared_ptr<Array> Slice(int64_t offset, int64_t length) const override;
};

/// Base class for fixed-size logical types
class ARROW_EXPORT PrimitiveArray : public Array {
 public:
  PrimitiveArray(const std::shared_ptr<DataType>& type, int64_t length,
      const std::shared_ptr<Buffer>& data,
      const std::shared_ptr<Buffer>& null_bitmap = nullptr, int64_t null_count = 0,
      int64_t offset = 0);

  /// Does not account for any slice offset
  std::shared_ptr<Buffer> values() const { return data_->buffers[1]; }

  /// Does not account for any slice offset
  const uint8_t* raw_values() const { return raw_values_; }

 protected:
  PrimitiveArray() {}

  inline PrimitiveArray(const std::shared_ptr<ArrayData>& data) { SetData(data); }

  inline void SetData(const std::shared_ptr<ArrayData>& data) {
    this->Array::SetData(data);
    raw_values_ = data == nullptr ? nullptr : data->buffers[1]->data();
  }

  const uint8_t* raw_values_;
};

template <typename TYPE>
class ARROW_EXPORT NumericArray : public PrimitiveArray {
 public:
  using TypeClass = TYPE;
  using value_type = typename TypeClass::c_type;

  NumericArray(const std::shared_ptr<ArrayData>& data) : PrimitiveArray(data) {}

  // Only enable this constructor without a type argument for types without additional
  // metadata
  template <typename T1 = TYPE>
  NumericArray(
      typename std::enable_if<TypeTraits<T1>::is_parameter_free, int64_t>::type length,
      const std::shared_ptr<Buffer>& data,
      const std::shared_ptr<Buffer>& null_bitmap = nullptr, int64_t null_count = 0,
      int64_t offset = 0)
      : PrimitiveArray(TypeTraits<T1>::type_singleton(), length, data, null_bitmap,
            null_count, offset) {}

  const value_type* raw_values() const {
    return reinterpret_cast<const value_type*>(raw_values_) + offset_;
  }

  std::shared_ptr<Array> Slice(int64_t offset, int64_t length) const override;

  value_type Value(int64_t i) const { return raw_values()[i]; }

 protected:
  using PrimitiveArray::PrimitiveArray;
};

class ARROW_EXPORT BooleanArray : public PrimitiveArray {
 public:
  using TypeClass = BooleanType;

  inline BooleanArray(const std::shared_ptr<ArrayData>& data) : PrimitiveArray(data) {}

  BooleanArray(int64_t length, const std::shared_ptr<Buffer>& data,
      const std::shared_ptr<Buffer>& null_bitmap = nullptr, int64_t null_count = 0,
      int64_t offset = 0);

  std::shared_ptr<Array> Slice(int64_t offset, int64_t length) const override;

  bool Value(int64_t i) const {
    return BitUtil::GetBit(reinterpret_cast<const uint8_t*>(raw_values_), i + offset_);
  }

 protected:
  using PrimitiveArray::PrimitiveArray;
};

// ----------------------------------------------------------------------
// ListArray

class ARROW_EXPORT ListArray : public Array {
 public:
  using TypeClass = ListType;

  ListArray(const std::shared_ptr<DataType>& type, int64_t length,
      const std::shared_ptr<Buffer>& value_offsets, const std::shared_ptr<Array>& values,
      const std::shared_ptr<Buffer>& null_bitmap = nullptr, int64_t null_count = 0,
      int64_t offset = 0);

  // Return a shared pointer in case the requestor desires to share ownership
  // with this array.
  std::shared_ptr<Array> values() const { return values_; }

  /// Note that this buffer does not account for any slice offset
  std::shared_ptr<Buffer> value_offsets() const { return data_->buffers[1]; }

  std::shared_ptr<DataType> value_type() const { return values_->type(); }

  /// Return pointer to raw value offsets accounting for any slice offset
  const int32_t* raw_value_offsets() const { return raw_value_offsets_ + offset_; }

  // Neither of these functions will perform boundschecking
  int32_t value_offset(int64_t i) const { return raw_value_offsets_[i + offset_]; }
  int32_t value_length(int64_t i) const {
    i += offset_;
    return raw_value_offsets_[i + 1] - raw_value_offsets_[i];
  }

  std::shared_ptr<Array> Slice(int64_t offset, int64_t length) const override;

 protected:
  const int32_t* raw_value_offsets_;
  std::shared_ptr<Array> values_;
};

// ----------------------------------------------------------------------
// Binary and String

class ARROW_EXPORT BinaryArray : public Array {
 public:
  using TypeClass = BinaryType;

  BinaryArray(const std::shared_ptr<ArrayData>& data) { SetData(data); }

  BinaryArray(int64_t length, const std::shared_ptr<Buffer>& value_offsets,
      const std::shared_ptr<Buffer>& data,
      const std::shared_ptr<Buffer>& null_bitmap = nullptr, int64_t null_count = 0,
      int64_t offset = 0);

  // Return the pointer to the given elements bytes
  // TODO(emkornfield) introduce a StringPiece or something similar to capture zero-copy
  // pointer + offset
  const uint8_t* GetValue(int64_t i, int32_t* out_length) const {
    // Account for base offset
    i += offset_;

    const int32_t pos = raw_value_offsets_[i];
    *out_length = raw_value_offsets_[i + 1] - pos;
    return raw_data_ + pos;
  }

  /// Note that this buffer does not account for any slice offset
  std::shared_ptr<Buffer> value_offsets() const { return data_->buffers[1]; }

  /// Note that this buffer does not account for any slice offset
  std::shared_ptr<Buffer> value_data() const { return data_->buffers[2]; }

  const int32_t* raw_value_offsets() const { return raw_value_offsets_ + offset_; }

  // Neither of these functions will perform boundschecking
  int32_t value_offset(int64_t i) const { return raw_value_offsets_[i + offset_]; }
  int32_t value_length(int64_t i) const {
    i += offset_;
    return raw_value_offsets_[i + 1] - raw_value_offsets_[i];
  }

  std::shared_ptr<Array> Slice(int64_t offset, int64_t length) const override;

 protected:
  /// Protected method for constructors
  void SetData(const std::shared_ptr<ArrayData>& data);

  // Constructor that allows sub-classes/builders to propagate there logical type up the
  // class hierarchy.
  BinaryArray(const std::shared_ptr<DataType>& type, int64_t length,
      const std::shared_ptr<Buffer>& value_offsets, const std::shared_ptr<Buffer>& data,
      const std::shared_ptr<Buffer>& null_bitmap = nullptr, int64_t null_count = 0,
      int64_t offset = 0);

  const int32_t* raw_value_offsets_;
  const uint8_t* raw_data_;
};

class ARROW_EXPORT StringArray : public BinaryArray {
 public:
  using TypeClass = StringType;

  StringArray(const std::shared_ptr<ArrayData>& data) : BinaryArray(data) {}

  StringArray(int64_t length, const std::shared_ptr<Buffer>& value_offsets,
      const std::shared_ptr<Buffer>& data,
      const std::shared_ptr<Buffer>& null_bitmap = nullptr, int64_t null_count = 0,
      int64_t offset = 0);

  // Construct a std::string
  // TODO: std::bad_alloc possibility
  std::string GetString(int64_t i) const {
    int32_t nchars;
    const uint8_t* str = GetValue(i, &nchars);
    return std::string(reinterpret_cast<const char*>(str), nchars);
  }

  std::shared_ptr<Array> Slice(int64_t offset, int64_t length) const override;
};

// ----------------------------------------------------------------------
// Fixed width binary

class ARROW_EXPORT FixedSizeBinaryArray : public PrimitiveArray {
 public:
  using TypeClass = FixedSizeBinaryType;

  FixedSizeBinaryArray(const std::shared_ptr<ArrayData>& data) { SetData(data); }

  FixedSizeBinaryArray(const std::shared_ptr<DataType>& type, int64_t length,
      const std::shared_ptr<Buffer>& data,
      const std::shared_ptr<Buffer>& null_bitmap = nullptr, int64_t null_count = 0,
      int64_t offset = 0);

  const uint8_t* GetValue(int64_t i) const;

  int32_t byte_width() const { return byte_width_; }

  const uint8_t* raw_values() const { return raw_values_; }

  std::shared_ptr<Array> Slice(int64_t offset, int64_t length) const override;

 protected:
  inline void SetData(const std::shared_ptr<ArrayData>& data) {
    this->PrimitiveArray::SetData(data);
    byte_width_ = static_cast<const FixedSizeBinaryType&>(*type()).byte_width();
  }

  int32_t byte_width_;
};

// ----------------------------------------------------------------------
// DecimalArray
class ARROW_EXPORT DecimalArray : public Array {
 public:
  using TypeClass = Type;

  DecimalArray(const std::shared_ptr<ArrayData>& data) { SetData(data); }

  DecimalArray(const std::shared_ptr<DataType>& type, int64_t length,
      const std::shared_ptr<Buffer>& data,
      const std::shared_ptr<Buffer>& null_bitmap = nullptr, int64_t null_count = 0,
      int64_t offset = 0, const std::shared_ptr<Buffer>& sign_bitmap = nullptr);

  bool IsNegative(int64_t i) const;

  const uint8_t* GetValue(int64_t i) const;

  std::string FormatValue(int64_t i) const;

  std::shared_ptr<Array> Slice(int64_t offset, int64_t length) const override;

  /// \brief The main decimal data
  /// For 32/64-bit decimal this is everything
  std::shared_ptr<Buffer> values() const { return data_->buffers[1]; }

  /// Only needed for 128 bit Decimals
  std::shared_ptr<Buffer> sign_bitmap() const { return data_->buffers[2]; }

  int32_t byte_width() const {
    return static_cast<const DecimalType&>(*type()).byte_width();
  }
  const uint8_t* raw_values() const { return raw_values_; }

 private:
  void SetData(const std::shared_ptr<ArrayData>& data);
  const uint8_t* raw_values_;
  const uint8_t* sign_bitmap_data_;
};

// ----------------------------------------------------------------------
// Struct

class ARROW_EXPORT StructArray : public Array {
 public:
  using TypeClass = StructType;

  StructArray(const std::shared_ptr<DataType>& type, int64_t length,
      const std::vector<std::shared_ptr<Array>>& children,
      std::shared_ptr<Buffer> null_bitmap = nullptr, int64_t null_count = 0,
      int64_t offset = 0);

  // Return a shared pointer in case the requestor desires to share ownership
  // with this array.
  std::shared_ptr<Array> field(int pos) const;
  const std::vector<std::shared_ptr<Array>> fields() const { return children_; }

  int num_fields() const { return static_cast<int>(children_.size()); }

  std::shared_ptr<Array> Slice(int64_t offset, int64_t length) const override;

 private:
  std::vector<std::shared_ptr<Array>> children_;
};

// ----------------------------------------------------------------------
// Union

class ARROW_EXPORT UnionArray : public Array {
 public:
  using TypeClass = UnionType;
  using type_id_t = uint8_t;

  UnionArray(const std::shared_ptr<DataType>& type, int64_t length,
      const std::vector<std::shared_ptr<Array>>& children,
      const std::shared_ptr<Buffer>& type_ids,
      const std::shared_ptr<Buffer>& value_offsets = nullptr,
      const std::shared_ptr<Buffer>& null_bitmap = nullptr, int64_t null_count = 0,
      int64_t offset = 0);

  /// Note that this buffer does not account for any slice offset
  std::shared_ptr<Buffer> type_ids() const { return data_->buffers[1]; }

  /// Note that this buffer does not account for any slice offset
  std::shared_ptr<Buffer> value_offsets() const { return data_->buffers[2]; }

  const type_id_t* raw_type_ids() const { return raw_type_ids_ + offset_; }
  const int32_t* raw_value_offsets() const { return raw_value_offsets_ + offset_; }

  UnionMode mode() const { return static_cast<const UnionType&>(*type()).mode(); }

  std::shared_ptr<Array> child(int pos) const;
  const std::vector<std::shared_ptr<Array>> children() const { return children_; }

  std::shared_ptr<Array> Slice(int64_t offset, int64_t length) const override;

 protected:
  const type_id_t* raw_type_ids_;
  const int32_t* raw_value_offsets_;
  std::vector<std::shared_ptr<Array>> children_;
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

  std::shared_ptr<Array> indices() const { return indices_; }
  std::shared_ptr<Array> dictionary() const;

  const DictionaryType* dict_type() const { return dict_type_; }

  std::shared_ptr<Array> Slice(int64_t offset, int64_t length) const override;

 protected:
  const DictionaryType* dict_type_;
  std::shared_ptr<Array> indices_;
};

// ----------------------------------------------------------------------
// extern templates and other details

// Only instantiate these templates once
ARROW_EXTERN_TEMPLATE NumericArray<Int8Type>;
ARROW_EXTERN_TEMPLATE NumericArray<UInt8Type>;
ARROW_EXTERN_TEMPLATE NumericArray<Int16Type>;
ARROW_EXTERN_TEMPLATE NumericArray<UInt16Type>;
ARROW_EXTERN_TEMPLATE NumericArray<Int32Type>;
ARROW_EXTERN_TEMPLATE NumericArray<UInt32Type>;
ARROW_EXTERN_TEMPLATE NumericArray<Int64Type>;
ARROW_EXTERN_TEMPLATE NumericArray<UInt64Type>;
ARROW_EXTERN_TEMPLATE NumericArray<HalfFloatType>;
ARROW_EXTERN_TEMPLATE NumericArray<FloatType>;
ARROW_EXTERN_TEMPLATE NumericArray<DoubleType>;
ARROW_EXTERN_TEMPLATE NumericArray<Date32Type>;
ARROW_EXTERN_TEMPLATE NumericArray<Date64Type>;
ARROW_EXTERN_TEMPLATE NumericArray<Time32Type>;
ARROW_EXTERN_TEMPLATE NumericArray<Time64Type>;
ARROW_EXTERN_TEMPLATE NumericArray<TimestampType>;

/// \brief Perform any validation checks to determine obvious inconsistencies
/// with the array's internal data
///
/// This can be an expensive check.
///
/// \param array an Array instance
/// \return Status
Status ARROW_EXPORT ValidateArray(const Array& array);

}  // namespace arrow

#endif
