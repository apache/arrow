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
#include <memory>
#include <vector>

#include "arrow/type.h"
#include "arrow/util/macros.h"
#include "arrow/util/status.h"

namespace arrow {

class Array;
class MemoryPool;
class PoolBuffer;

static constexpr int32_t MIN_BUILDER_CAPACITY = 1 << 5;

// Base class for all data array builders.
// This class provides a facilities for incrementally building the null bitmap
// (see Append methods) and as a side effect the current number of slots and
// the null count.
class ArrayBuilder {
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

  // For nested types. Since the objects are owned by this class instance, we
  // skip shared pointers and just return a raw pointer
  ArrayBuilder* child(int i) { return children_[i].get(); }

  int num_children() const { return children_.size(); }

  int32_t length() const { return length_; }
  int32_t null_count() const { return null_count_; }
  int32_t capacity() const { return capacity_; }

  // Append to null bitmap
  Status AppendToBitmap(bool is_valid);
  // Vector append. Treat each zero byte as a null.   If valid_bytes is null
  // assume all of length bits are valid.
  Status AppendToBitmap(const uint8_t* valid_bytes, int32_t length);
  // Set the next length bits to not null (i.e. valid).
  Status SetNotNull(int32_t length);

  // Allocates initial capacity requirements for the builder.  In most
  // cases subclasses should override and call there parent classes
  // method as well.
  virtual Status Init(int32_t capacity);

  // Resizes the null_bitmap array.  In most
  // cases subclasses should override and call there parent classes
  // method as well.
  virtual Status Resize(int32_t new_bits);

  // Ensures there is enough space for adding the number of elements by checking
  // capacity and calling Resize if necessary.
  Status Reserve(int32_t elements);

  // For cases where raw data was memcpy'd into the internal buffers, allows us
  // to advance the length of the builder. It is your responsibility to use
  // this function responsibly.
  Status Advance(int32_t elements);

  const std::shared_ptr<PoolBuffer>& null_bitmap() const { return null_bitmap_; }

  // Creates new array object to hold the contents of the builder and transfers
  // ownership of the data.  This resets all variables on the builder.
  virtual std::shared_ptr<Array> Finish() = 0;

  const std::shared_ptr<DataType>& type() const { return type_; }

 protected:
  MemoryPool* pool_;

  std::shared_ptr<DataType> type_;

  // When null_bitmap are first appended to the builder, the null bitmap is allocated
  std::shared_ptr<PoolBuffer> null_bitmap_;
  int32_t null_count_;
  uint8_t* null_bitmap_data_;

  // Array length, so far. Also, the index of the next element to be added
  int32_t length_;
  int32_t capacity_;

  // Child value array builders. These are owned by this class
  std::vector<std::unique_ptr<ArrayBuilder>> children_;

  //
  // Unsafe operations (don't check capacity/don't resize)
  //

  // Append to null bitmap.
  void UnsafeAppendToBitmap(bool is_valid);
  // Vector append. Treat each zero byte as a nullzero. If valid_bytes is null
  // assume all of length bits are valid.
  void UnsafeAppendToBitmap(const uint8_t* valid_bytes, int32_t length);
  // Set the next length bits to not null (i.e. valid).
  void UnsafeSetNotNull(int32_t length);

 private:
  DISALLOW_COPY_AND_ASSIGN(ArrayBuilder);
};

}  // namespace arrow

#endif  // ARROW_BUILDER_H_
