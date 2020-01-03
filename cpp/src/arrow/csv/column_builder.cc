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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/builder.h"
#include "arrow/csv/column_builder.h"
#include "arrow/csv/converter.h"
#include "arrow/csv/options.h"
#include "arrow/csv/parser.h"
#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/type.h"
#include "arrow/util/logging.h"
#include "arrow/util/task_group.h"

namespace arrow {
namespace csv {

class BlockParser;

using internal::TaskGroup;

void ColumnBuilder::SetTaskGroup(const std::shared_ptr<internal::TaskGroup>& task_group) {
  task_group_ = task_group;
}

void ColumnBuilder::Append(const std::shared_ptr<BlockParser>& parser) {
  Insert(static_cast<int64_t>(chunks_.size()), parser);
}

//////////////////////////////////////////////////////////////////////////
// Null column builder implementation (for a column not in the CSV file)

class NullColumnBuilder : public ColumnBuilder {
 public:
  explicit NullColumnBuilder(const std::shared_ptr<DataType>& type, MemoryPool* pool,
                             const std::shared_ptr<internal::TaskGroup>& task_group)
      : ColumnBuilder(task_group), type_(type), pool_(pool) {}

  Status Init();

  void Insert(int64_t block_index, const std::shared_ptr<BlockParser>& parser) override;
  Result<std::shared_ptr<ChunkedArray>> Finish() override;

  // While NullColumnBuilder is so cheap that it doesn't need parallelization
  // in itself, the CSV reader doesn't know this and can still call it from
  // multiple threads, so use a mutex anyway.
  std::mutex mutex_;

  std::shared_ptr<DataType> type_;
  MemoryPool* pool_;
  std::unique_ptr<ArrayBuilder> builder_;
};

Status NullColumnBuilder::Init() { return MakeBuilder(pool_, type_, &builder_); }

void NullColumnBuilder::Insert(int64_t block_index,
                               const std::shared_ptr<BlockParser>& parser) {
  // Create a null Array pointer at the back at the list.
  size_t chunk_index = static_cast<size_t>(block_index);
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (chunks_.size() <= chunk_index) {
      chunks_.resize(chunk_index + 1);
    }
  }

  // Spawn a task that will build an array of nulls with the right DataType
  const int32_t num_rows = parser->num_rows();
  DCHECK_GE(num_rows, 0);

  task_group_->Append([=]() -> Status {
    std::shared_ptr<Array> res;
    RETURN_NOT_OK(builder_->AppendNulls(num_rows));
    RETURN_NOT_OK(builder_->Finish(&res));

    std::lock_guard<std::mutex> lock(mutex_);
    // Should not insert an already built chunk
    DCHECK_EQ(chunks_[chunk_index], nullptr);
    chunks_[chunk_index] = std::move(res);
    return Status::OK();
  });
}

Result<std::shared_ptr<ChunkedArray>> NullColumnBuilder::Finish() {
  // Unnecessary iff all tasks have finished
  std::lock_guard<std::mutex> lock(mutex_);

  for (const auto& chunk : chunks_) {
    if (chunk == nullptr) {
      return Status::Invalid("a chunk failed allocating for an unknown reason");
    }
  }
  return std::make_shared<ChunkedArray>(chunks_, type_);
}

//////////////////////////////////////////////////////////////////////////
// Pre-typed column builder implementation

class TypedColumnBuilder : public ColumnBuilder {
 public:
  TypedColumnBuilder(const std::shared_ptr<DataType>& type, int32_t col_index,
                     const ConvertOptions& options, MemoryPool* pool,
                     const std::shared_ptr<internal::TaskGroup>& task_group)
      : ColumnBuilder(task_group),
        type_(type),
        col_index_(col_index),
        options_(options),
        pool_(pool) {}

  Status Init();

  void Insert(int64_t block_index, const std::shared_ptr<BlockParser>& parser) override;
  Result<std::shared_ptr<ChunkedArray>> Finish() override;

 protected:
  Status WrapConversionError(const Status& st) {
    if (st.ok()) {
      return st;
    } else {
      std::stringstream ss;
      ss << "In CSV column #" << col_index_ << ": " << st.message();
      return st.WithMessage(ss.str());
    }
  }

  std::mutex mutex_;

  std::shared_ptr<DataType> type_;
  int32_t col_index_;
  // CAUTION: ConvertOptions can grow large (if it customizes hundreds or
  // thousands of columns), so avoid copying it in each TypedColumnBuilder.
  const ConvertOptions& options_;
  MemoryPool* pool_;

  std::shared_ptr<Converter> converter_;
};

Status TypedColumnBuilder::Init() {
  ARROW_ASSIGN_OR_RAISE(converter_, Converter::Make(type_, options_, pool_));
  return Status::OK();
}

void TypedColumnBuilder::Insert(int64_t block_index,
                                const std::shared_ptr<BlockParser>& parser) {
  DCHECK_NE(converter_, nullptr);

  // Create a null Array pointer at the back at the list
  // and spawn a task to initialize it after conversion
  size_t chunk_index = static_cast<size_t>(block_index);
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (chunks_.size() <= chunk_index) {
      chunks_.resize(chunk_index + 1);
    }
  }

  // We're careful that all references in the closure outlive the Append() call
  task_group_->Append([=]() -> Status {
    auto maybe_array = converter_->Convert(*parser, col_index_);
    if (!maybe_array.ok()) {
      return WrapConversionError(maybe_array.status());
    }

    std::lock_guard<std::mutex> lock(mutex_);
    // Should not insert an already converted chunk
    DCHECK_EQ(chunks_[chunk_index], nullptr);
    chunks_[chunk_index] = *std::move(maybe_array);
    return Status::OK();
  });
}

Result<std::shared_ptr<ChunkedArray>> TypedColumnBuilder::Finish() {
  // Unnecessary iff all tasks have finished
  std::lock_guard<std::mutex> lock(mutex_);

  for (const auto& chunk : chunks_) {
    if (chunk == nullptr) {
      return Status::Invalid("a chunk failed converting for an unknown reason");
    }
  }
  return std::make_shared<ChunkedArray>(chunks_, type_);
}

//////////////////////////////////////////////////////////////////////////
// Type-inferring column builder implementation

class InferringColumnBuilder : public ColumnBuilder {
 public:
  InferringColumnBuilder(int32_t col_index, const ConvertOptions& options,
                         MemoryPool* pool,
                         const std::shared_ptr<internal::TaskGroup>& task_group)
      : ColumnBuilder(task_group),
        col_index_(col_index),
        options_(options),
        pool_(pool) {}

  Status Init();

  void Insert(int64_t block_index, const std::shared_ptr<BlockParser>& parser) override;
  Result<std::shared_ptr<ChunkedArray>> Finish() override;

 protected:
  Status LoosenType(const Status& conversion_error);
  Status UpdateType();
  Status TryConvertChunk(size_t chunk_index);
  // This must be called unlocked!
  void ScheduleConvertChunk(size_t chunk_index);

  std::mutex mutex_;

  int32_t col_index_;
  // CAUTION: ConvertOptions can grow large (if it customizes hundreds or
  // thousands of columns), so avoid copying it in each InferringColumnBuilder.
  const ConvertOptions& options_;
  MemoryPool* pool_;
  std::shared_ptr<Converter> converter_;

  // Current inference status
  enum class InferKind {
    Null,
    Integer,
    Boolean,
    Real,
    Timestamp,
    TextDict,
    BinaryDict,
    Text,
    Binary
  };

  std::shared_ptr<DataType> infer_type_;
  InferKind infer_kind_;
  bool can_loosen_type_;

  // The parsers corresponding to each chunk (for reconverting)
  std::vector<std::shared_ptr<BlockParser>> parsers_;
};

Status InferringColumnBuilder::Init() {
  infer_kind_ = InferKind::Null;
  return UpdateType();
}

Status InferringColumnBuilder::LoosenType(const Status& conversion_error) {
  // We are locked

  DCHECK(!conversion_error.ok());
  DCHECK(can_loosen_type_);
  switch (infer_kind_) {
    case InferKind::Null:
      infer_kind_ = InferKind::Integer;
      break;
    case InferKind::Integer:
      infer_kind_ = InferKind::Boolean;
      break;
    case InferKind::Boolean:
      infer_kind_ = InferKind::Timestamp;
      break;
    case InferKind::Timestamp:
      infer_kind_ = InferKind::Real;
      break;
    case InferKind::Real:
      if (options_.auto_dict_encode) {
        infer_kind_ = InferKind::TextDict;
      } else {
        infer_kind_ = InferKind::Text;
      }
      break;
    case InferKind::TextDict:
      if (conversion_error.IsIndexError()) {
        // Cardinality too large, fall back to non-dict encoding
        infer_kind_ = InferKind::Text;
      } else {
        // Assuming UTF8 validation failure
        infer_kind_ = InferKind::BinaryDict;
      }
      break;
    case InferKind::BinaryDict:
      infer_kind_ = InferKind::Binary;
      break;
    case InferKind::Text:
      infer_kind_ = InferKind::Binary;
      break;
    case InferKind::Binary:
      return Status::UnknownError("Shouldn't come here");
  }
  return UpdateType();
}

Status InferringColumnBuilder::UpdateType() {
  // We are locked

  auto make_converter = [&](std::shared_ptr<DataType> type) -> Status {
    infer_type_ = type;
    ARROW_ASSIGN_OR_RAISE(converter_, Converter::Make(type, options_, pool_));
    return Status::OK();
  };

  auto make_dict_converter = [&](std::shared_ptr<DataType> type) -> Status {
    infer_type_ = dictionary(int32(), type);
    ARROW_ASSIGN_OR_RAISE(auto dict_converter,
                          DictionaryConverter::Make(type, options_, pool_));
    dict_converter->SetMaxCardinality(options_.auto_dict_max_cardinality);
    converter_ = std::move(dict_converter);
    return Status::OK();
  };

  can_loosen_type_ = true;

  switch (infer_kind_) {
    case InferKind::Null:
      return make_converter(null());
    case InferKind::Integer:
      return make_converter(int64());
    case InferKind::Boolean:
      return make_converter(boolean());
    case InferKind::Timestamp:
      // We don't support parsing second fractions for now
      return make_converter(timestamp(TimeUnit::SECOND));
    case InferKind::Real:
      return make_converter(float64());
    case InferKind::Text:
      return make_converter(utf8());
    case InferKind::Binary:
      can_loosen_type_ = false;
      return make_converter(binary());
    case InferKind::TextDict:
      return make_dict_converter(utf8());
    case InferKind::BinaryDict:
      return make_dict_converter(binary());
  }
  return Status::UnknownError("Shouldn't come here");
}

void InferringColumnBuilder::ScheduleConvertChunk(size_t chunk_index) {
  // We're careful that all values in the closure outlive the Append() call
  task_group_->Append([=]() { return TryConvertChunk(chunk_index); });
}

Status InferringColumnBuilder::TryConvertChunk(size_t chunk_index) {
  std::unique_lock<std::mutex> lock(mutex_);
  std::shared_ptr<Converter> converter = converter_;
  std::shared_ptr<BlockParser> parser = parsers_[chunk_index];
  InferKind kind = infer_kind_;

  DCHECK_NE(parser, nullptr);

  lock.unlock();
  auto maybe_array = converter->Convert(*parser, col_index_);
  lock.lock();

  if (kind != infer_kind_) {
    // infer_kind_ was changed by another task, reconvert
    lock.unlock();
    ScheduleConvertChunk(chunk_index);
    return Status::OK();
  }

  if (maybe_array.ok()) {
    // Conversion succeeded
    chunks_[chunk_index] = std::move(*maybe_array);
    if (!can_loosen_type_) {
      // We won't try to reconvert anymore
      parsers_[chunk_index].reset();
    }
    return Status::OK();
  } else if (can_loosen_type_) {
    // Conversion failed, try another type
    RETURN_NOT_OK(LoosenType(maybe_array.status()));

    // Reconvert past finished chunks
    // (unfinished chunks will notice by themselves if they need reconverting)
    size_t nchunks = chunks_.size();
    for (size_t i = 0; i < nchunks; ++i) {
      if (i != chunk_index && chunks_[i]) {
        // We're assuming the chunk was converted using the wrong type
        // (which should be true unless the executor reorders tasks)
        chunks_[i].reset();
        lock.unlock();
        ScheduleConvertChunk(i);
        lock.lock();
      }
    }

    // Reconvert this chunk
    lock.unlock();
    ScheduleConvertChunk(chunk_index);

    return Status::OK();
  } else {
    // Conversion failed but cannot loosen more
    return maybe_array.status();
  }
}

void InferringColumnBuilder::Insert(int64_t block_index,
                                    const std::shared_ptr<BlockParser>& parser) {
  // Create a slot for the new chunk and spawn a task to convert it
  size_t chunk_index = static_cast<size_t>(block_index);
  {
    std::lock_guard<std::mutex> lock(mutex_);

    DCHECK_NE(converter_, nullptr);
    if (chunks_.size() <= chunk_index) {
      chunks_.resize(chunk_index + 1);
    }
    if (parsers_.size() <= chunk_index) {
      parsers_.resize(chunk_index + 1);
    }
    // Should not insert an already converting chunk
    DCHECK_EQ(parsers_[chunk_index], nullptr);
    parsers_[chunk_index] = parser;
  }

  ScheduleConvertChunk(chunk_index);
}

Result<std::shared_ptr<ChunkedArray>> InferringColumnBuilder::Finish() {
  // Unnecessary iff all tasks have finished
  std::lock_guard<std::mutex> lock(mutex_);

  for (const auto& chunk : chunks_) {
    if (chunk == nullptr) {
      return Status::Invalid("A chunk failed converting for an unknown reason");
    }
    // XXX Perhaps we must instead do a last equalization pass
    // in this serial step
    DCHECK_EQ(chunk->type()->id(), infer_type_->id())
        << "Inference didn't equalize types!";
  }
  auto ptr = std::make_shared<ChunkedArray>(chunks_, infer_type_);
  chunks_.clear();
  parsers_.clear();
  return ptr;
}

//////////////////////////////////////////////////////////////////////////
// Factory functions

Result<std::shared_ptr<ColumnBuilder>> ColumnBuilder::Make(
    MemoryPool* pool, const std::shared_ptr<DataType>& type, int32_t col_index,
    const ConvertOptions& options, const std::shared_ptr<TaskGroup>& task_group) {
  auto ptr =
      std::make_shared<TypedColumnBuilder>(type, col_index, options, pool, task_group);
  RETURN_NOT_OK(ptr->Init());
  return ptr;
}

Result<std::shared_ptr<ColumnBuilder>> ColumnBuilder::Make(
    MemoryPool* pool, int32_t col_index, const ConvertOptions& options,
    const std::shared_ptr<TaskGroup>& task_group) {
  auto ptr =
      std::make_shared<InferringColumnBuilder>(col_index, options, pool, task_group);
  RETURN_NOT_OK(ptr->Init());
  return ptr;
}

Result<std::shared_ptr<ColumnBuilder>> ColumnBuilder::MakeNull(
    MemoryPool* pool, const std::shared_ptr<DataType>& type,
    const std::shared_ptr<internal::TaskGroup>& task_group) {
  auto ptr = std::make_shared<NullColumnBuilder>(type, pool, task_group);
  RETURN_NOT_OK(ptr->Init());
  return ptr;
}

}  // namespace csv
}  // namespace arrow
