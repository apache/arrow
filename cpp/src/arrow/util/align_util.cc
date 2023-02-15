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

#include "arrow/util/align_util.h"

#include "arrow/array.h"
#include "arrow/chunked_array.h"
#include "arrow/record_batch.h"
#include "arrow/table.h"

namespace arrow {

namespace util {

Result<std::shared_ptr<Array>> EnsureAlignment(const Array& object, int64_t alignment,
                                               MemoryPool* memory_pool) {
  std::vector<std::shared_ptr<Buffer>> buffers_ = object.data()->buffers;
  for (size_t i = 0; i < buffers_.size(); ++i) {
    if (buffers_[i]) {
      auto buffer_address = buffers_[i]->address();
      if ((buffer_address % alignment) != 0) {
        ARROW_ASSIGN_OR_RAISE(
            auto new_buffer, AllocateBuffer(buffers_[i]->size(), alignment, memory_pool));
        std::memcpy(new_buffer->mutable_data(), buffers_[i]->data(), buffers_[i]->size());
        buffers_[i] = std::move(new_buffer);
      }
    }
  }
  auto new_array_data =
      ArrayData::Make(object.data()->type, object.data()->length, std::move(buffers_),
                      object.data()->GetNullCount(), object.data()->offset);
  return MakeArray(new_array_data);
}

Result<std::shared_ptr<ChunkedArray>> EnsureAlignment(const ChunkedArray& object,
                                                      int64_t alignment,
                                                      MemoryPool* memory_pool) {
  ArrayVector chunks_ = object.chunks();
  for (int i = 0; i < object.num_chunks(); ++i) {
    ARROW_ASSIGN_OR_RAISE(chunks_[i],
                          EnsureAlignment(*object.chunk(i), alignment, memory_pool));
  }
  return ChunkedArray::Make(std::move(chunks_), object.type());
}

Result<std::shared_ptr<RecordBatch>> EnsureAlignment(const RecordBatch& object,
                                                     int64_t alignment,
                                                     MemoryPool* memory_pool) {
  ArrayVector columns_ = object.columns();
  for (int i = 0; i < object.num_columns(); ++i) {
    ARROW_ASSIGN_OR_RAISE(columns_[i],
                          EnsureAlignment(*object.column(i), alignment, memory_pool));
  }
  return RecordBatch::Make(object.schema(), object.num_rows(), std::move(columns_));
}

Result<std::shared_ptr<Table>> EnsureAlignment(const Table& object, int64_t alignment,
                                               MemoryPool* memory_pool) {
  std::vector<std::shared_ptr<ChunkedArray>> columns_ = object.columns();
  for (int i = 0; i < object.num_columns(); ++i) {
    ARROW_ASSIGN_OR_RAISE(columns_[i],
                          EnsureAlignment(*object.column(i), alignment, memory_pool));
  }
  return Table::Make(object.schema(), std::move(columns_), object.num_rows());
}

}  // namespace util
}  // namespace arrow
