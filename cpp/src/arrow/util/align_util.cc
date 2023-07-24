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
#include "arrow/type_fwd.h"
#include "arrow/type_traits.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging.h"

namespace arrow {

namespace util {

bool CheckAlignment(const Buffer& buffer, int64_t alignment) {
  if (alignment <= 0) {
    return true;
  }
  return buffer.address() % alignment == 0;
}

namespace {

// Returns the type that controls how the buffers of this ArrayData (not its children)
// should behave
Type::type GetTypeForBuffers(const ArrayData& array) {
  Type::type type_id = array.type->storage_id();
  if (type_id == Type::DICTIONARY) {
    return ::arrow::internal::checked_pointer_cast<DictionaryType>(array.type)
        ->index_type()
        ->id();
  }
  return type_id;
}

// Checks to see if an array's own buffers are aligned but doesn't check
// children
bool CheckSelfAlignment(const ArrayData& array, int64_t alignment) {
  if (alignment == kValueAlignment) {
    Type::type type_id = GetTypeForBuffers(array);
    for (std::size_t i = 0; i < array.buffers.size(); i++) {
      if (array.buffers[i]) {
        int expected_alignment =
            RequiredValueAlignmentForBuffer(type_id, static_cast<int>(i));
        if (!CheckAlignment(*array.buffers[i], expected_alignment)) {
          return false;
        }
      }
    }
  } else {
    for (const auto& buffer : array.buffers) {
      if (buffer) {
        if (!CheckAlignment(*buffer, alignment)) return false;
      }
    }
  }
  return true;
}

}  // namespace

bool CheckAlignment(const ArrayData& array, int64_t alignment) {
  if (!CheckSelfAlignment(array, alignment)) {
    return false;
  }

  if (array.dictionary) {
    if (!CheckAlignment(*array.dictionary, alignment)) return false;
  }

  for (const auto& child : array.child_data) {
    if (child) {
      if (!CheckAlignment(*child, alignment)) return false;
    }
  }
  return true;
}

bool CheckAlignment(const Array& array, int64_t alignment) {
  return CheckAlignment(*array.data(), alignment);
}

bool CheckAlignment(const ChunkedArray& array, int64_t alignment,
                    std::vector<bool>* needs_alignment, int offset) {
  bool all_aligned = true;
  needs_alignment->resize(needs_alignment->size() + array.num_chunks(), false);
  for (auto i = 0; i < array.num_chunks(); ++i) {
    if (array.chunk(i) && !CheckAlignment(*array.chunk(i), alignment)) {
      (*needs_alignment)[i + offset] = true;
      all_aligned = false;
    }
  }
  return all_aligned;
}

bool CheckAlignment(const RecordBatch& batch, int64_t alignment,
                    std::vector<bool>* needs_alignment) {
  bool all_aligned = true;
  needs_alignment->resize(batch.num_columns(), false);
  for (auto i = 0; i < batch.num_columns(); ++i) {
    if (batch.column(i) && !CheckAlignment(*batch.column(i), alignment)) {
      (*needs_alignment)[i] = true;
      all_aligned = false;
    }
  }
  return all_aligned;
}

bool CheckAlignment(const Table& table, int64_t alignment,
                    std::vector<bool>* needs_alignment) {
  bool all_aligned = true;
  needs_alignment->resize(table.num_columns(), false);
  for (auto i = 1; i <= table.num_columns(); ++i) {
    if (table.column(i - 1) &&
        !CheckAlignment(*table.column(i - 1), alignment, needs_alignment,
                        (i - 1) * (1 + table.column(i - 1)->num_chunks()))) {
      (*needs_alignment)[i * table.column(i - 1)->num_chunks() + i - 1] = true;
      all_aligned = false;
    }
  }
  return all_aligned;
}

Result<std::shared_ptr<Buffer>> EnsureAlignment(std::shared_ptr<Buffer> buffer,
                                                int64_t alignment,
                                                MemoryPool* memory_pool) {
  if (alignment == kValueAlignment) {
    return Status::Invalid(
        "The kValueAlignment option may only be used to call EnsureAlignment on arrays "
        "or tables and cannot be used with buffers");
  }
  if (alignment <= 0) {
    return Status::Invalid("Alignment must be a positive integer");
  }
  if (!CheckAlignment(*buffer, alignment)) {
    if (!buffer->is_cpu()) {
      return Status::NotImplemented("Reallocating an unaligned non-CPU buffer.");
    }
    int64_t minimum_desired_alignment = std::max(kDefaultBufferAlignment, alignment);
    ARROW_ASSIGN_OR_RAISE(
        auto new_buffer,
        AllocateBuffer(buffer->size(), minimum_desired_alignment, memory_pool));
    std::memcpy(new_buffer->mutable_data(), buffer->data(), buffer->size());
    return std::move(new_buffer);
  } else {
    return std::move(buffer);
  }
}

Result<std::shared_ptr<ArrayData>> EnsureAlignment(std::shared_ptr<ArrayData> array_data,
                                                   int64_t alignment,
                                                   MemoryPool* memory_pool) {
  if (!CheckAlignment(*array_data, alignment)) {
    std::vector<std::shared_ptr<Buffer>> buffers = array_data->buffers;
    Type::type type_id = GetTypeForBuffers(*array_data);
    for (size_t i = 0; i < buffers.size(); ++i) {
      if (buffers[i]) {
        int64_t expected_alignment = alignment;
        if (alignment == kValueAlignment) {
          expected_alignment =
              RequiredValueAlignmentForBuffer(type_id, static_cast<int>(i));
        }
        ARROW_ASSIGN_OR_RAISE(
            buffers[i],
            EnsureAlignment(std::move(buffers[i]), expected_alignment, memory_pool));
      }
    }

    for (auto& it : array_data->child_data) {
      ARROW_ASSIGN_OR_RAISE(it, EnsureAlignment(std::move(it), alignment, memory_pool));
    }

    if (array_data->type->id() == Type::DICTIONARY) {
      ARROW_ASSIGN_OR_RAISE(
          array_data->dictionary,
          EnsureAlignment(std::move(array_data->dictionary), alignment, memory_pool));
    }

    auto new_array_data = ArrayData::Make(
        array_data->type, array_data->length, std::move(buffers), array_data->child_data,
        array_data->dictionary, array_data->GetNullCount(), array_data->offset);
    return std::move(new_array_data);
  } else {
    return std::move(array_data);
  }
}

Result<std::shared_ptr<Array>> EnsureAlignment(std::shared_ptr<Array> array,
                                               int64_t alignment,
                                               MemoryPool* memory_pool) {
  ARROW_ASSIGN_OR_RAISE(auto new_array_data,
                        EnsureAlignment(array->data(), alignment, memory_pool));

  if (new_array_data.get() == array->data().get()) {
    return std::move(array);
  } else {
    return MakeArray(std::move(new_array_data));
  }
}

Result<std::shared_ptr<ChunkedArray>> EnsureAlignment(std::shared_ptr<ChunkedArray> array,
                                                      int64_t alignment,
                                                      MemoryPool* memory_pool) {
  std::vector<bool> needs_alignment;
  if (!CheckAlignment(*array, alignment, &needs_alignment)) {
    ArrayVector chunks_ = array->chunks();
    for (int i = 0; i < array->num_chunks(); ++i) {
      if (needs_alignment[i] && chunks_[i]) {
        ARROW_ASSIGN_OR_RAISE(
            chunks_[i], EnsureAlignment(std::move(chunks_[i]), alignment, memory_pool));
      }
    }
    return ChunkedArray::Make(std::move(chunks_), array->type());
  } else {
    return std::move(array);
  }
}

Result<std::shared_ptr<RecordBatch>> EnsureAlignment(std::shared_ptr<RecordBatch> batch,
                                                     int64_t alignment,
                                                     MemoryPool* memory_pool) {
  std::vector<bool> needs_alignment;
  if (!CheckAlignment(*batch, alignment, &needs_alignment)) {
    ArrayVector columns_ = batch->columns();
    for (int i = 0; i < batch->num_columns(); ++i) {
      if (needs_alignment[i] && columns_[i]) {
        ARROW_ASSIGN_OR_RAISE(
            columns_[i], EnsureAlignment(std::move(columns_[i]), alignment, memory_pool));
      }
    }
    return RecordBatch::Make(batch->schema(), batch->num_rows(), std::move(columns_));
  } else {
    return std::move(batch);
  }
}

Result<std::shared_ptr<Table>> EnsureAlignment(std::shared_ptr<Table> table,
                                               int64_t alignment,
                                               MemoryPool* memory_pool) {
  std::vector<bool> needs_alignment;
  if (!CheckAlignment(*table, alignment, &needs_alignment)) {
    std::vector<std::shared_ptr<ChunkedArray>> columns_ = table->columns();
    for (int i = 1; i <= table->num_columns(); ++i) {
      if (columns_[i - 1] && needs_alignment[i * columns_[i - 1]->num_chunks() + i - 1]) {
        ArrayVector chunks_ = columns_[i - 1]->chunks();
        for (size_t j = 0; j < chunks_.size(); ++j) {
          if (chunks_[j] &&
              needs_alignment[j + (i - 1) * (1 + columns_[i - 1]->num_chunks())]) {
            ARROW_ASSIGN_OR_RAISE(chunks_[j], EnsureAlignment(std::move(chunks_[j]),
                                                              alignment, memory_pool));
          }
        }
        ARROW_ASSIGN_OR_RAISE(
            columns_[i - 1],
            ChunkedArray::Make(std::move(chunks_), columns_[i - 1]->type()));
      }
    }
    return Table::Make(table->schema(), std::move(columns_), table->num_rows());
  } else {
    return std::move(table);
  }
}

}  // namespace util
}  // namespace arrow
