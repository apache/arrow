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

#include <cstdint>
#include <iosfwd>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "arrow/buffer.h"
#include "arrow/compare.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit_util_base.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {

class Array;
class ArrayVisitor;

// When slicing, we do not know the null count of the sliced range without
// doing some computation. To avoid doing this eagerly, we set the null count
// to -1 (any negative number will do). When Array::null_count is called the
// first time, the null count will be computed. See ARROW-33
constexpr int64_t kUnknownNullCount = -1;

class MemoryPool;
class Status;

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
/// auto new_data = arr.data()->Copy();
/// new_data->type = arrow::float64();
/// DoubleArray double_arr(new_data);
///
/// This object is also useful in an analytics setting where memory may be
/// reused. For example, if we had a group of operations all returning doubles,
/// say:
///
/// Log(Sqrt(Expr(arr)))
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
  ArrayData() : length(0), null_count(0), offset(0) {}

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
            const std::vector<std::shared_ptr<Buffer>>& buffers,
            const std::vector<std::shared_ptr<ArrayData>>& child_data,
            int64_t null_count = kUnknownNullCount, int64_t offset = 0)
      : ArrayData(type, length, null_count, offset) {
    this->buffers = buffers;
    this->child_data = child_data;
  }

  ArrayData(const std::shared_ptr<DataType>& type, int64_t length,
            std::vector<std::shared_ptr<Buffer>>&& buffers,
            int64_t null_count = kUnknownNullCount, int64_t offset = 0)
      : ArrayData(type, length, null_count, offset) {
    this->buffers = std::move(buffers);
  }

  static std::shared_ptr<ArrayData> Make(const std::shared_ptr<DataType>& type,
                                         int64_t length,
                                         std::vector<std::shared_ptr<Buffer>>&& buffers,
                                         int64_t null_count = kUnknownNullCount,
                                         int64_t offset = 0);

  static std::shared_ptr<ArrayData> Make(
      const std::shared_ptr<DataType>& type, int64_t length,
      const std::vector<std::shared_ptr<Buffer>>& buffers,
      int64_t null_count = kUnknownNullCount, int64_t offset = 0);

  static std::shared_ptr<ArrayData> Make(
      const std::shared_ptr<DataType>& type, int64_t length,
      const std::vector<std::shared_ptr<Buffer>>& buffers,
      const std::vector<std::shared_ptr<ArrayData>>& child_data,
      int64_t null_count = kUnknownNullCount, int64_t offset = 0);

  static std::shared_ptr<ArrayData> Make(
      const std::shared_ptr<DataType>& type, int64_t length,
      const std::vector<std::shared_ptr<Buffer>>& buffers,
      const std::vector<std::shared_ptr<ArrayData>>& child_data,
      const std::shared_ptr<Array>& dictionary, int64_t null_count = kUnknownNullCount,
      int64_t offset = 0);

  static std::shared_ptr<ArrayData> Make(const std::shared_ptr<DataType>& type,
                                         int64_t length,
                                         int64_t null_count = kUnknownNullCount,
                                         int64_t offset = 0);

  // Move constructor
  ArrayData(ArrayData&& other) noexcept
      : type(std::move(other.type)),
        length(other.length),
        null_count(other.null_count),
        offset(other.offset),
        buffers(std::move(other.buffers)),
        child_data(std::move(other.child_data)),
        dictionary(std::move(other.dictionary)) {}

  // Copy constructor
  ArrayData(const ArrayData& other) noexcept
      : type(other.type),
        length(other.length),
        null_count(other.null_count),
        offset(other.offset),
        buffers(other.buffers),
        child_data(other.child_data),
        dictionary(other.dictionary) {}

  // Move assignment
  ArrayData& operator=(ArrayData&& other) = default;

  // Copy assignment
  ArrayData& operator=(const ArrayData& other) = default;

  std::shared_ptr<ArrayData> Copy() const { return std::make_shared<ArrayData>(*this); }

  // Access a buffer's data as a typed C pointer
  template <typename T>
  inline const T* GetValues(int i, int64_t absolute_offset) const {
    if (buffers[i]) {
      return reinterpret_cast<const T*>(buffers[i]->data()) + absolute_offset;
    } else {
      return NULLPTR;
    }
  }

  template <typename T>
  inline const T* GetValues(int i) const {
    return GetValues<T>(i, offset);
  }

  // Access a buffer's data as a typed C pointer
  template <typename T>
  inline T* GetMutableValues(int i, int64_t absolute_offset) {
    if (buffers[i]) {
      return reinterpret_cast<T*>(buffers[i]->mutable_data()) + absolute_offset;
    } else {
      return NULLPTR;
    }
  }

  template <typename T>
  inline T* GetMutableValues(int i) {
    return GetMutableValues<T>(i, offset);
  }

  // Construct a zero-copy slice of the data with the indicated offset and length
  ArrayData Slice(int64_t offset, int64_t length) const;

  /// \brief Return null count, or compute and set it if it's not known
  int64_t GetNullCount() const;

  std::shared_ptr<DataType> type;
  int64_t length;
  mutable int64_t null_count;
  // The logical start point into the physical buffers (in values, not bytes).
  // Note that, for child data, this must be *added* to the child data's own offset.
  int64_t offset;
  std::vector<std::shared_ptr<Buffer>> buffers;
  std::vector<std::shared_ptr<ArrayData>> child_data;

  // The dictionary for this Array, if any. Only used for dictionary
  // type
  std::shared_ptr<Array> dictionary;
};

/// \brief Create a strongly-typed Array instance from generic ArrayData
/// \param[in] data the array contents
/// \return the resulting Array instance
ARROW_EXPORT
std::shared_ptr<Array> MakeArray(const std::shared_ptr<ArrayData>& data);

/// \brief Create a strongly-typed Array instance with all elements null
/// \param[in] type the array type
/// \param[in] length the array length
/// \param[out] out resulting Array instance
ARROW_EXPORT
Status MakeArrayOfNull(const std::shared_ptr<DataType>& type, int64_t length,
                       std::shared_ptr<Array>* out);

/// \brief Create a strongly-typed Array instance with all elements null
/// \param[in] pool the pool from which memory for this array will be allocated
/// \param[in] type the array type
/// \param[in] length the array length
/// \param[out] out resulting Array instance
ARROW_EXPORT
Status MakeArrayOfNull(MemoryPool* pool, const std::shared_ptr<DataType>& type,
                       int64_t length, std::shared_ptr<Array>* out);

/// \brief Create an Array instance whose slots are the given scalar
/// \param[in] scalar the value with which to fill the array
/// \param[in] length the array length
/// \param[out] out resulting Array instance
ARROW_EXPORT
Status MakeArrayFromScalar(const Scalar& scalar, int64_t length,
                           std::shared_ptr<Array>* out);

/// \brief Create a strongly-typed Array instance with all elements null
/// \param[in] pool the pool from which memory for this array will be allocated
/// \param[in] scalar the value with which to fill the array
/// \param[in] length the array length
/// \param[out] out resulting Array instance
ARROW_EXPORT
Status MakeArrayFromScalar(MemoryPool* pool, const Scalar& scalar, int64_t length,
                           std::shared_ptr<Array>* out);

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
    return null_bitmap_data_ != NULLPTR &&
           !BitUtil::GetBit(null_bitmap_data_, i + data_->offset);
  }

  /// \brief Return true if value at index is valid (not null). Does not
  /// boundscheck
  bool IsValid(int64_t i) const {
    return null_bitmap_data_ == NULLPTR ||
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
  /// Note that for `null_count == 0`, this can be null.
  /// This buffer does not account for any slice offset
  std::shared_ptr<Buffer> null_bitmap() const { return data_->buffers[0]; }

  /// Raw pointer to the null bitmap.
  ///
  /// Note that for `null_count == 0`, this can be null.
  /// This buffer does not account for any slice offset
  const uint8_t* null_bitmap_data() const { return null_bitmap_data_; }

  /// Equality comparison with another array
  bool Equals(const Array& arr, const EqualOptions& = EqualOptions::Defaults()) const;
  bool Equals(const std::shared_ptr<Array>& arr,
              const EqualOptions& = EqualOptions::Defaults()) const;

  /// \brief Return the formatted unified diff of arrow::Diff between this
  /// Array and another Array
  std::string Diff(const Array& other) const;

  /// Approximate equality comparison with another array
  ///
  /// epsilon is only used if this is FloatArray or DoubleArray
  bool ApproxEquals(const std::shared_ptr<Array>& arr,
                    const EqualOptions& = EqualOptions::Defaults()) const;
  bool ApproxEquals(const Array& arr,
                    const EqualOptions& = EqualOptions::Defaults()) const;

  /// Compare if the range of slots specified are equal for the given array and
  /// this array.  end_idx exclusive.  This methods does not bounds check.
  bool RangeEquals(int64_t start_idx, int64_t end_idx, int64_t other_start_idx,
                   const Array& other) const;
  bool RangeEquals(int64_t start_idx, int64_t end_idx, int64_t other_start_idx,
                   const std::shared_ptr<Array>& other) const;
  bool RangeEquals(const Array& other, int64_t start_idx, int64_t end_idx,
                   int64_t other_start_idx) const;
  bool RangeEquals(const std::shared_ptr<Array>& other, int64_t start_idx,
                   int64_t end_idx, int64_t other_start_idx) const;

  Status Accept(ArrayVisitor* visitor) const;

  /// Construct a zero-copy view of this array with the given type.
  ///
  /// This method checks if the types are layout-compatible.
  /// Nested types are traversed in depth-first order. Data buffers must have
  /// the same item sizes, even though the logical types may be different.
  /// An error is returned if the types are not layout-compatible.
  Status View(const std::shared_ptr<DataType>& type, std::shared_ptr<Array>* out) const;

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

  /// \brief Perform cheap validation checks to determine obvious inconsistencies
  /// within the array's internal data.
  ///
  /// This is O(k) where k is the number of descendents.
  ///
  /// \return Status
  Status Validate() const;

  /// \brief Perform extensive validation checks to determine inconsistencies
  /// within the array's internal data.
  ///
  /// This is potentially O(k*n) where k is the number of descendents and n
  /// is the array length.
  ///
  /// \return Status
  Status ValidateFull() const;

 protected:
  Array() : null_bitmap_data_(NULLPTR) {}

  std::shared_ptr<ArrayData> data_;
  const uint8_t* null_bitmap_data_;

  /// Protected method for constructors
  inline void SetData(const std::shared_ptr<ArrayData>& data) {
    if (data->buffers.size() > 0 && data->buffers[0]) {
      null_bitmap_data_ = data->buffers[0]->data();
    } else {
      null_bitmap_data_ = NULLPTR;
    }
    data_ = data;
  }

 private:
  ARROW_DISALLOW_COPY_AND_ASSIGN(Array);
};

using ArrayVector = std::vector<std::shared_ptr<Array>>;

namespace internal {

/// Given a number of ArrayVectors, treat each ArrayVector as the
/// chunks of a chunked array.  Then rechunk each ArrayVector such that
/// all ArrayVectors are chunked identically.  It is mandatory that
/// all ArrayVectors contain the same total number of elements.
ARROW_EXPORT
std::vector<ArrayVector> RechunkArraysConsistently(const std::vector<ArrayVector>&);

}  // namespace internal

static inline std::ostream& operator<<(std::ostream& os, const Array& x) {
  os << x.ToString();
  return os;
}

/// Base class for non-nested arrays
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
    null_bitmap_data_ = NULLPTR;
    data->null_count = data->length;
    data_ = data;
  }
};

}  // namespace arrow
