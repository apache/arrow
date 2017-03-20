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

// Function for constructing Array array objects from metadata and raw memory
// buffers

#ifndef ARROW_LOADER_H
#define ARROW_LOADER_H

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "arrow/status.h"
#include "arrow/util/visibility.h"

namespace arrow {

class Array;
class Buffer;
struct DataType;

// ARROW-109: We set this number arbitrarily to help catch user mistakes. For
// deeply nested schemas, it is expected the user will indicate explicitly the
// maximum allowed recursion depth
constexpr int kMaxNestingDepth = 64;

struct ARROW_EXPORT FieldMetadata {
  FieldMetadata() {}
  FieldMetadata(int64_t length, int64_t null_count, int64_t offset)
      : length(length), null_count(null_count), offset(offset) {}

  FieldMetadata(const FieldMetadata& other) {
    this->length = other.length;
    this->null_count = other.null_count;
    this->offset = other.offset;
  }

  int64_t length;
  int64_t null_count;
  int64_t offset;
};

struct ARROW_EXPORT BufferMetadata {
  BufferMetadata() {}
  BufferMetadata(int32_t page, int64_t offset, int64_t length)
      : page(page), offset(offset), length(length) {}

  /// The shared memory page id where to find this. Set to -1 if unused
  int32_t page;

  /// The relative offset into the memory page to the starting byte of the buffer
  int64_t offset;

  /// Absolute length in bytes of the buffer
  int64_t length;
};

/// Implement this to create new types of Arrow data loaders
class ARROW_EXPORT ArrayComponentSource {
 public:
  virtual ~ArrayComponentSource() = default;

  virtual Status GetBuffer(int buffer_index, std::shared_ptr<Buffer>* out) = 0;
  virtual Status GetFieldMetadata(int field_index, FieldMetadata* metadata) = 0;
};

/// Bookkeeping struct for loading array objects from their constituent pieces of raw data
///
/// The field_index and buffer_index are incremented in the ArrayLoader
/// based on how much of the batch is "consumed" (through nested data
/// reconstruction, for example)
struct ArrayLoaderContext {
  ArrayComponentSource* source;
  int buffer_index;
  int field_index;
  int max_recursion_depth;
};

/// Construct an Array container from type metadata and a collection of memory
/// buffers
///
/// \param[in] field the data type of the array being loaded
/// \param[in] source an implementation of ArrayComponentSource
/// \param[out] out the constructed array
/// \return Status indicating success or failure
Status ARROW_EXPORT LoadArray(const std::shared_ptr<DataType>& type,
    ArrayComponentSource* source, std::shared_ptr<Array>* out);

Status ARROW_EXPORT LoadArray(const std::shared_ptr<DataType>& field,
    ArrayLoaderContext* context, std::shared_ptr<Array>* out);

Status ARROW_EXPORT LoadArray(const std::shared_ptr<DataType>& type,
    const std::vector<FieldMetadata>& fields,
    const std::vector<std::shared_ptr<Buffer>>& buffers, std::shared_ptr<Array>* out);

/// Create new arrays for logical types that are backed by primitive arrays.
Status ARROW_EXPORT MakePrimitiveArray(const std::shared_ptr<DataType>& type,
    int64_t length, const std::shared_ptr<Buffer>& data,
    const std::shared_ptr<Buffer>& null_bitmap, int64_t null_count, int64_t offset,
    std::shared_ptr<Array>* out);

Status ARROW_EXPORT MakePrimitiveArray(const std::shared_ptr<DataType>& type,
    const std::vector<std::shared_ptr<Buffer>>& buffers, int64_t length,
    int64_t null_count, int64_t offset, std::shared_ptr<Array>* out);

}  // namespace arrow

#endif  // ARROW_LOADER_H
