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
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging.h"

namespace arrow {

namespace util {

bool CheckAlignment(const Buffer& buffer, int64_t alignment) {
  return buffer.address() % alignment == 0;
}

namespace {

// Some buffers are frequently type-punned.  For example, in an int32 array the
// values buffer is frequently cast to int32_t*
//
// This sort of punning is only valid if the pointer is aligned to a proper width
// (e.g. 4 bytes in the case of int32).
//
// We generally assume that all buffers are at least 8-bit aligned and so we only
// need to worry about buffers that are commonly cast to wider data types.  Note that
// this alignment is something that is guaranteed by malloc (e.g. new int32_t[] will
// return a buffer that is 4 byte aligned) or common libraries (e.g. numpy) but it is
// not currently guaranteed by flight (GH-32276).
//
// By happy coincedence, for every data type, the only buffer that might need wider
// alignment is the second buffer (at index 1).  This function returns the expected
// alignment (in bits) of the second buffer for the given array to safely allow this cast.
//
// If the array's type doesn't have a second buffer or the second buffer is not expected
// to be type punned, then we return 8.
int GetMallocValuesAlignment(const ArrayData& array) {
  // Make sure to use the storage type id
  auto type_id = array.type->storage_id();
  if (type_id == Type::DICTIONARY) {
    // The values buffer is in a different ArrayData and so we only check the indices
    // buffer here.  The values array data will be checked by the calling method.
    type_id = ::arrow::internal::checked_pointer_cast<DictionaryType>(array.type)
                  ->index_type()
                  ->id();
  }
  switch (type_id) {
    case Type::NA:                 // No buffers
    case Type::FIXED_SIZE_LIST:    // No second buffer (values in child array)
    case Type::FIXED_SIZE_BINARY:  // Fixed size binary could be dangerous but the
                                   // compute kernels don't type pun this.  E.g. if
                                   // an extension type is storing some kind of struct
                                   // here then the user should do their own alignment
                                   // check before casting to an array of structs
    case Type::BOOL:               // Always treated as uint8_t*
    case Type::INT8:               // Always treated as uint8_t*
    case Type::UINT8:              // Always treated as uint8_t*
    case Type::DECIMAL128:         // Always treated as uint8_t*
    case Type::DECIMAL256:         // Always treated as uint8_t*
    case Type::SPARSE_UNION:       // Types array is uint8_t, no offsets array
    case Type::RUN_END_ENCODED:    // No buffers
    case Type::STRUCT:             // No buffers beyond validity
      // These have no buffers or all buffers need only byte alignment
      return 1;
    case Type::INT16:
    case Type::UINT16:
    case Type::HALF_FLOAT:
      return 2;
    case Type::INT32:
    case Type::UINT32:
    case Type::FLOAT:
    case Type::STRING:  // Offsets may be cast to int32_t*, data is only uint8_t*
    case Type::BINARY:  // Offsets may be cast to int32_t*, data is only uint8_t*
    case Type::DATE32:
    case Type::TIME32:
    case Type::LIST:         // Offsets may be cast to int32_t*, data is in child array
    case Type::MAP:          // This is a list array
    case Type::DENSE_UNION:  // Has an offsets buffer of int32_t*
    case Type::INTERVAL_MONTHS:  // Stored as int32_t*
      return 4;
    case Type::INT64:
    case Type::UINT64:
    case Type::DOUBLE:
    case Type::LARGE_BINARY:  // Offsets may be cast to int64_t*
    case Type::LARGE_LIST:    // Offsets may be cast to int64_t*
    case Type::LARGE_STRING:  // Offsets may be cast to int64_t*
    case Type::DATE64:
    case Type::TIME64:
    case Type::TIMESTAMP:
    case Type::DURATION:
    case Type::INTERVAL_DAY_TIME:  // Stored as two contiguous 32-bit integers but may be
                                   // cast to struct* containing both integers
      return 8;
    case Type::INTERVAL_MONTH_DAY_NANO:  // Stored as two 32-bit integers and a 64-bit
                                         // integer
      return 16;
    default:
      Status::Invalid("Could not check alignment for type id ", type_id,
                      " keeping existing alignment")
          .Warn();
      return 8;
  }
}

// Checks to see if an array's own buffers are aligned but doesn't check
// children
bool CheckSelfAlignment(const ArrayData& array, int64_t alignment) {
  if (alignment == kMallocAlignment) {
    int malloc_alignment = GetMallocValuesAlignment(array);
    if (array.buffers.size() >= 2) {
      if (!CheckAlignment(*array.buffers[1], malloc_alignment)) {
        return false;
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

  if (array.type->id() == Type::DICTIONARY) {
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
  if (!CheckAlignment(*buffer, alignment)) {
    ARROW_ASSIGN_OR_RAISE(auto new_buffer,
                          AllocateBuffer(buffer->size(), alignment, memory_pool));
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
    std::vector<std::shared_ptr<Buffer>> buffers_ = array_data->buffers;
    if (!CheckSelfAlignment(*array_data, alignment)) {
      if (alignment == kMallocAlignment) {
        DCHECK_GE(buffers_.size(), 2);
        // If we get here then we know that the values buffer is not aligned properly.
        // Since we need to copy the buffer we might as well update it to
        // kDefaultBufferAlignment. This helps to avoid cases where the minimum required
        // alignment is less than the allocator's minimum alignment (e.g. malloc requires
        // a minimum of 8 byte alignment on a 64-bit system)
        ARROW_ASSIGN_OR_RAISE(
            buffers_[1], EnsureAlignment(std::move(buffers_[1]), kDefaultBufferAlignment,
                                         memory_pool));
      } else {
        for (size_t i = 0; i < buffers_.size(); ++i) {
          if (buffers_[i]) {
            ARROW_ASSIGN_OR_RAISE(buffers_[i], EnsureAlignment(std::move(buffers_[i]),
                                                               alignment, memory_pool));
          }
        }
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
        array_data->type, array_data->length, std::move(buffers_), array_data->child_data,
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
