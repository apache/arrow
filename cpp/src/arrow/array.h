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

// ----------------------------------------------------------------------
// Generic array data container

/// \class ArrayData
/// \brief Mutable container for generic Arrow array data
///
/// This data structure is a self-contained representation of the memory and
/// metadata inside an Arrow array data structure (called vectors in Java). The
/// classes arrow::Array and its subclasses provide strongly-typed accessors
/// with support for the visitor pattern and other affordances.
///
/// This class is designed for easy internal data manipulation, analytical data
/// processing, and data transport to and from IPC messages. For example, we
/// could cast from int64 to float64 like so:
///
/// Int64Array arr = GetMyData();
/// auto new_data = arr->data()->ShallowCopy();
/// new_data->type = arrow::float64();
/// Float64Array double_arr(new_data);
///
/// This object is also useful in an analytics setting where memory may be
/// reused. For example, if we had a group of operations all returning doubles,
/// say:
///
/// Log(Sqrt(Expr(arr))
///
/// Then the low-level implementations of each of these functions could have
/// the signatures
///
/// void Log(const ArrayData& values, ArrayData* out);
///
/// As another example a function may consume one or more memory buffers in an
/// input array and replace them with newly-allocated data, changing the output
/// data type as well.
struct ARROW_EXPORT ArrayData {
  ArrayData() : length(0) {}

  ArrayData(const std::shared_ptr<DataType>& type, int64_t length,
            int64_t null_count = kUnknownNullCount, int64_t offset = 0)
      : type(type), length(length), null_count(null_count), offset(offset) {}

  ArrayData(const std::shared_ptr<DataType>& type, int64_t length,
            const std::vector<std::shared_ptr<Buffer>>& buffers,
            int64_t null_count = kUnknownNullCount, int64_t offset = 0)
      : ArrayData(type, length, null_count, offset) {
    this->buffers = buffers;
  }

  ArrayData(const std::shared_ptr<DataType>& type, int64_t length,
            std::vector<std::shared_ptr<Buffer>>&& buffers,
            int64_t null_count = kUnknownNullCount, int64_t offset = 0)
      : ArrayData(type, length, null_count, offset) {
    this->buffers = std::move(buffers);
  }

  // Move constructor
  ArrayData(ArrayData&& other) noexcept
      : type(std::move(other.type)),
        length(other.length),
        null_count(other.null_count),
        offset(other.offset),
        buffers(std::move(other.buffers)),
        child_data(std::move(other.child_data)) {}

  ArrayData(const ArrayData& other) noexcept
      : type(other.type),
        length(other.length),
        null_count(other.null_count),
        offset(other.offset),
        buffers(other.buffers),
        child_data(other.child_data) {}

  // Move assignment
  ArrayData& operator=(ArrayData&& other) {
    type = std::move(other.type);
    length = other.length;
    null_count = other.null_count;
    offset = other.offset;
    buffers = std::move(other.buffers);
    child_data = std::move(other.child_data);
    return *this;
  }

  std::shared_ptr<ArrayData> ShallowCopy() const {
    return std::make_shared<ArrayData>(*this);
  }

  std::shared_ptr<DataType> type;
  int64_t length;
  int64_t null_count;
  int64_t offset;
  std::vector<std::shared_ptr<Buffer>> buffers;
  std::vector<std::shared_ptr<ArrayData>> child_data;
};

#ifndef ARROW_NO_DEPRECATED_API

/// \brief Create a strongly-typed Array instance from generic ArrayData
/// \param[in] data the array contents
/// \param[out] out the resulting Array instance
/// \return Status
///
/// \note Deprecated since 0.8.0
ARROW_EXPORT
Status MakeArray(const std::shared_ptr<ArrayData>& data, std::shared_ptr<Array>* out);

#endif

/// \brief Create a strongly-typed Array instance from generic ArrayData
/// \param[in] data the array contents
/// \return the resulting Array instance
ARROW_EXPORT
std::shared_ptr<Array> MakeArray(const std::shared_ptr<ArrayData>& data);

// ----------------------------------------------------------------------
// User array accessor types

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

  /// \brief Return true if value at index is null. Does not boundscheck
  bool IsNull(int64_t i) const {
    return null_bitmap_data_ != nullptr &&
           BitUtil::BitNotSet(null_bitmap_data_, i + data_->offset);
  }

  /// \brief Return true if value at index is valid (not null). Does not
  /// boundscheck
  bool IsValid(int64_t i) const {
    return null_bitmap_data_ != nullptr &&
           BitUtil::GetBit(null_bitmap_data_, i + data_->offset);
  }

  /// Size in the number of elements this array contains.
  int64_t length() const { return data_->length; }

  /// A relative position into another array's data, to enable zero-copy
  /// slicing. This value defaults to zero
  int64_t offset() const { return data_->offset; }

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
  /// \param[in] offset the position of the first element in the constructed
  /// slice
  /// \param[in] length the length of the slice. If there are not enough
  /// elements in the array, the length will be adjusted accordingly
  ///
  /// \return a new object wrapped in std::shared_ptr<Array>
  std::shared_ptr<Array> Slice(int64_t offset, int64_t length) const;

  /// Slice from offset until end of the array
  std::shared_ptr<Array> Slice(int64_t offset) const;

  std::shared_ptr<ArrayData> data() const { return data_; }

  int num_fields() const { return static_cast<int>(data_->child_data.size()); }

  /// \return PrettyPrint representation of array suitable for debugging
  std::string ToString() const;

 protected:
  Array() {}

  std::shared_ptr<ArrayData> data_;
  const uint8_t* null_bitmap_data_;

  /// Protected method for constructors
  inline void SetData(const std::shared_ptr<ArrayData>& data) {
    if (data->buffers.size() > 0 && data->buffers[0]) {
      null_bitmap_data_ = data->buffers[0]->data();
    } else {
      null_bitmap_data_ = nullptr;
    }
    data_ = data;
  }

 private:
  ARROW_DISALLOW_COPY_AND_ASSIGN(Array);
};

static inline std::ostream& operator<<(std::ostream& os, const Array& x) {
  os << x.ToString();
  return os;
}

class ARROW_EXPORT FlatArray : public Array {
 protected:
  using Array::Array;
};

/// Degenerate null type Array
class ARROW_EXPORT NullArray : public FlatArray {
 public:
  using TypeClass = NullType;

  explicit NullArray(const std::shared_ptr<ArrayData>& data) { SetData(data); }
  explicit NullArray(int64_t length);

 private:
  inline void SetData(const std::shared_ptr<ArrayData>& data) {
    null_bitmap_data_ = nullptr;
    data->null_count = data->length;
    data_ = data;
  }
};

/// Base class for fixed-size logical types
class ARROW_EXPORT PrimitiveArray : public FlatArray {
 public:
  PrimitiveArray(const std::shared_ptr<DataType>& type, int64_t length,
                 const std::shared_ptr<Buffer>& data,
                 const std::shared_ptr<Buffer>& null_bitmap = nullptr,
                 int64_t null_count = 0, int64_t offset = 0);

  /// Does not account for any slice offset
  std::shared_ptr<Buffer> values() const { return data_->buffers[1]; }

  /// \brief Return pointer to start of raw data
  const uint8_t* raw_values() const;

 protected:
  PrimitiveArray() {}

  inline void SetData(const std::shared_ptr<ArrayData>& data) {
    auto values = data->buffers[1];
    this->Array::SetData(data);
    raw_values_ = values == nullptr ? nullptr : values->data();
  }

  explicit inline PrimitiveArray(const std::shared_ptr<ArrayData>& data) {
    SetData(data);
  }

  const uint8_t* raw_values_;
};

template <typename TYPE>
class ARROW_EXPORT NumericArray : public PrimitiveArray {
 public:
  using TypeClass = TYPE;
  using value_type = typename TypeClass::c_type;

  explicit NumericArray(const std::shared_ptr<ArrayData>& data);

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
    return reinterpret_cast<const value_type*>(raw_values_) + data_->offset;
  }

  value_type Value(int64_t i) const { return raw_values()[i]; }

 protected:
  using PrimitiveArray::PrimitiveArray;
};

class ARROW_EXPORT BooleanArray : public PrimitiveArray {
 public:
  using TypeClass = BooleanType;

  explicit BooleanArray(const std::shared_ptr<ArrayData>& data);

  BooleanArray(int64_t length, const std::shared_ptr<Buffer>& data,
               const std::shared_ptr<Buffer>& null_bitmap = nullptr,
               int64_t null_count = 0, int64_t offset = 0);

  bool Value(int64_t i) const {
    return BitUtil::GetBit(reinterpret_cast<const uint8_t*>(raw_values_),
                           i + data_->offset);
  }

 protected:
  using PrimitiveArray::PrimitiveArray;
};

// ----------------------------------------------------------------------
// ListArray

class ARROW_EXPORT ListArray : public Array {
 public:
  using TypeClass = ListType;

  explicit ListArray(const std::shared_ptr<ArrayData>& data);

  ListArray(const std::shared_ptr<DataType>& type, int64_t length,
            const std::shared_ptr<Buffer>& value_offsets,
            const std::shared_ptr<Array>& values,
            const std::shared_ptr<Buffer>& null_bitmap = nullptr, int64_t null_count = 0,
            int64_t offset = 0);

  /// \brief Construct ListArray from array of offsets and child value array
  ///
  /// Note: does not validate input beyond sanity checks. Use
  /// arrow::ValidateArray if you need stronger validation of inputs
  ///
  /// \param[in] offsets Array containing n + 1 offsets encoding length and size
  /// \param[in] values Array containing
  /// \param[in] pool MemoryPool in case new offsets array needs to be
  /// allocated because of null values
  /// \param[out] out Will have length equal to offsets.length() - 1
  static Status FromArrays(const Array& offsets, const Array& values, MemoryPool* pool,
                           std::shared_ptr<Array>* out);

  /// \brief Return array object containing the list's values
  std::shared_ptr<Array> values() const;

  /// Note that this buffer does not account for any slice offset
  std::shared_ptr<Buffer> value_offsets() const { return data_->buffers[1]; }

  std::shared_ptr<DataType> value_type() const;

  /// Return pointer to raw value offsets accounting for any slice offset
  const int32_t* raw_value_offsets() const { return raw_value_offsets_ + data_->offset; }

  // Neither of these functions will perform boundschecking
  int32_t value_offset(int64_t i) const { return raw_value_offsets_[i + data_->offset]; }
  int32_t value_length(int64_t i) const {
    i += data_->offset;
    return raw_value_offsets_[i + 1] - raw_value_offsets_[i];
  }

 protected:
  void SetData(const std::shared_ptr<ArrayData>& data);
  const int32_t* raw_value_offsets_;

 private:
  std::shared_ptr<Array> values_;
};

// ----------------------------------------------------------------------
// Binary and String

class ARROW_EXPORT BinaryArray : public FlatArray {
 public:
  using TypeClass = BinaryType;

  explicit BinaryArray(const std::shared_ptr<ArrayData>& data);

  BinaryArray(int64_t length, const std::shared_ptr<Buffer>& value_offsets,
              const std::shared_ptr<Buffer>& data,
              const std::shared_ptr<Buffer>& null_bitmap = nullptr,
              int64_t null_count = 0, int64_t offset = 0);

  // Return the pointer to the given elements bytes
  // TODO(emkornfield) introduce a StringPiece or something similar to capture zero-copy
  // pointer + offset
  const uint8_t* GetValue(int64_t i, int32_t* out_length) const {
    // Account for base offset
    i += data_->offset;

    const int32_t pos = raw_value_offsets_[i];
    *out_length = raw_value_offsets_[i + 1] - pos;
    return raw_data_ + pos;
  }

  /// \brief Get binary value as a std::string
  ///
  /// \param i the value index
  /// \return the value copied into a std::string
  std::string GetString(int64_t i) const {
    int32_t length = 0;
    const uint8_t* bytes = GetValue(i, &length);
    return std::string(reinterpret_cast<const char*>(bytes), static_cast<size_t>(length));
  }

  /// Note that this buffer does not account for any slice offset
  std::shared_ptr<Buffer> value_offsets() const { return data_->buffers[1]; }

  /// Note that this buffer does not account for any slice offset
  std::shared_ptr<Buffer> value_data() const { return data_->buffers[2]; }

  const int32_t* raw_value_offsets() const { return raw_value_offsets_ + data_->offset; }

  // Neither of these functions will perform boundschecking
  int32_t value_offset(int64_t i) const { return raw_value_offsets_[i + data_->offset]; }
  int32_t value_length(int64_t i) const {
    i += data_->offset;
    return raw_value_offsets_[i + 1] - raw_value_offsets_[i];
  }

 protected:
  // For subclasses
  BinaryArray() {}

  /// Protected method for constructors
  void SetData(const std::shared_ptr<ArrayData>& data);

  // Constructor that allows sub-classes/builders to propagate there logical type up the
  // class hierarchy.
  BinaryArray(const std::shared_ptr<DataType>& type, int64_t length,
              const std::shared_ptr<Buffer>& value_offsets,
              const std::shared_ptr<Buffer>& data,
              const std::shared_ptr<Buffer>& null_bitmap = nullptr,
              int64_t null_count = 0, int64_t offset = 0);

  const int32_t* raw_value_offsets_;
  const uint8_t* raw_data_;
};

class ARROW_EXPORT StringArray : public BinaryArray {
 public:
  using TypeClass = StringType;

  explicit StringArray(const std::shared_ptr<ArrayData>& data);

  StringArray(int64_t length, const std::shared_ptr<Buffer>& value_offsets,
              const std::shared_ptr<Buffer>& data,
              const std::shared_ptr<Buffer>& null_bitmap = nullptr,
              int64_t null_count = 0, int64_t offset = 0);

  // Construct a std::string
  // TODO: std::bad_alloc possibility
  std::string GetString(int64_t i) const {
    int32_t nchars;
    const uint8_t* str = GetValue(i, &nchars);
    return std::string(reinterpret_cast<const char*>(str), nchars);
  }
};

// ----------------------------------------------------------------------
// Fixed width binary

class ARROW_EXPORT FixedSizeBinaryArray : public PrimitiveArray {
 public:
  using TypeClass = FixedSizeBinaryType;

  explicit FixedSizeBinaryArray(const std::shared_ptr<ArrayData>& data);

  FixedSizeBinaryArray(const std::shared_ptr<DataType>& type, int64_t length,
                       const std::shared_ptr<Buffer>& data,
                       const std::shared_ptr<Buffer>& null_bitmap = nullptr,
                       int64_t null_count = 0, int64_t offset = 0);

  const uint8_t* GetValue(int64_t i) const;
  const uint8_t* Value(int64_t i) const { return GetValue(i); }

  int32_t byte_width() const { return byte_width_; }

 protected:
  inline void SetData(const std::shared_ptr<ArrayData>& data) {
    this->PrimitiveArray::SetData(data);
    byte_width_ = static_cast<const FixedSizeBinaryType&>(*type()).byte_width();
  }

  int32_t byte_width_;
};

// ----------------------------------------------------------------------
// DecimalArray
class ARROW_EXPORT DecimalArray : public FixedSizeBinaryArray {
 public:
  using TypeClass = DecimalType;

  using FixedSizeBinaryArray::FixedSizeBinaryArray;

  /// \brief Construct DecimalArray from ArrayData instance
  explicit DecimalArray(const std::shared_ptr<ArrayData>& data);

  std::string FormatValue(int64_t i) const;
};

// ----------------------------------------------------------------------
// Struct

class ARROW_EXPORT StructArray : public Array {
 public:
  using TypeClass = StructType;

  explicit StructArray(const std::shared_ptr<ArrayData>& data);

  StructArray(const std::shared_ptr<DataType>& type, int64_t length,
              const std::vector<std::shared_ptr<Array>>& children,
              std::shared_ptr<Buffer> null_bitmap = nullptr, int64_t null_count = 0,
              int64_t offset = 0);

  // Return a shared pointer in case the requestor desires to share ownership
  // with this array.
  std::shared_ptr<Array> field(int pos) const;

 private:
  // For caching boxed child data
  mutable std::vector<std::shared_ptr<Array>> boxed_fields_;
};

// ----------------------------------------------------------------------
// Union

class ARROW_EXPORT UnionArray : public Array {
 public:
  using TypeClass = UnionType;
  using type_id_t = uint8_t;

  explicit UnionArray(const std::shared_ptr<ArrayData>& data);

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

  const type_id_t* raw_type_ids() const { return raw_type_ids_ + data_->offset; }
  const int32_t* raw_value_offsets() const { return raw_value_offsets_ + data_->offset; }

  UnionMode mode() const { return static_cast<const UnionType&>(*type()).mode(); }

  std::shared_ptr<Array> child(int pos) const;

  /// Only use this while the UnionArray is in scope
  const Array* UnsafeChild(int pos) const;

 protected:
  void SetData(const std::shared_ptr<ArrayData>& data);

  const type_id_t* raw_type_ids_;
  const int32_t* raw_value_offsets_;

  // For caching boxed child data
  mutable std::vector<std::shared_ptr<Array>> boxed_fields_;
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

  explicit DictionaryArray(const std::shared_ptr<ArrayData>& data);

  DictionaryArray(const std::shared_ptr<DataType>& type,
                  const std::shared_ptr<Array>& indices);

  std::shared_ptr<Array> indices() const;
  std::shared_ptr<Array> dictionary() const;

  const DictionaryType* dict_type() const { return dict_type_; }

 private:
  void SetData(const std::shared_ptr<ArrayData>& data);

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
ARROW_EXPORT
Status ValidateArray(const Array& array);

}  // namespace arrow

#endif  // ARROW_ARRAY_H
