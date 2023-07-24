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
#include "arrow/array/builder_base.h"
#include "arrow/chunked_array.h"
#include "arrow/csv/column_builder.h"
#include "arrow/csv/converter.h"
#include "arrow/csv/inference_internal.h"
#include "arrow/csv/options.h"
#include "arrow/csv/parser.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "arrow/util/logging.h"
#include "arrow/util/task_group.h"

namespace arrow {

using internal::TaskGroup;

namespace csv {

class BlockParser;

class ConcreteColumnBuilder : public ColumnBuilder {
 public:
  explicit ConcreteColumnBuilder(MemoryPool* pool, std::shared_ptr<TaskGroup> task_group,
                                 int32_t col_index = -1)
      : ColumnBuilder(std::move(task_group)), pool_(pool), col_index_(col_index) {}

  void Append(const std::shared_ptr<BlockParser>& parser) override {
    Insert(static_cast<int64_t>(chunks_.size()), parser);
  }

  Result<std::shared_ptr<ChunkedArray>> Finish() override {
    std::lock_guard<std::mutex> lock(mutex_);

    return FinishUnlocked();
  }

 protected:
  virtual std::shared_ptr<DataType> type() const = 0;

  Result<std::shared_ptr<ChunkedArray>> FinishUnlocked() {
    auto type = this->type();
    for (const auto& chunk : chunks_) {
      if (chunk == nullptr) {
        return Status::UnknownError("a chunk failed converting for an unknown reason");
      }
      DCHECK_EQ(chunk->type()->id(), type->id()) << "Chunk types not equal!";
    }
    return std::make_shared<ChunkedArray>(chunks_, std::move(type));
  }

  void ReserveChunks(int64_t block_index) {
    // Create a null Array pointer at the back at the list.
    std::lock_guard<std::mutex> lock(mutex_);
    ReserveChunksUnlocked(block_index);
  }

  void ReserveChunksUnlocked(int64_t block_index) {
    // Create a null Array pointer at the back at the list.
    size_t chunk_index = static_cast<size_t>(block_index);
    if (chunks_.size() <= chunk_index) {
      chunks_.resize(chunk_index + 1);
    }
  }

  Status SetChunk(int64_t chunk_index, Result<std::shared_ptr<Array>> maybe_array) {
    std::lock_guard<std::mutex> lock(mutex_);
    return SetChunkUnlocked(chunk_index, std::move(maybe_array));
  }

  Status SetChunkUnlocked(int64_t chunk_index,
                          Result<std::shared_ptr<Array>> maybe_array) {
    // Should not insert an already built chunk
    DCHECK_EQ(chunks_[chunk_index], nullptr);

    if (maybe_array.ok()) {
      chunks_[chunk_index] = *std::move(maybe_array);
      return Status::OK();
    } else {
      return WrapConversionError(maybe_array.status());
    }
  }

  Status WrapConversionError(const Status& st) {
    if (ARROW_PREDICT_TRUE(st.ok())) {
      return st;
    } else {
      std::stringstream ss;
      ss << "In CSV column #" << col_index_ << ": " << st.message();
      return st.WithMessage(ss.str());
    }
  }

  MemoryPool* pool_;
  int32_t col_index_;

  ArrayVector chunks_;

  std::mutex mutex_;
};

//////////////////////////////////////////////////////////////////////////
// Null column builder implementation (for a column not in the CSV file)

class NullColumnBuilder : public ConcreteColumnBuilder {
 public:
  explicit NullColumnBuilder(const std::shared_ptr<DataType>& type, MemoryPool* pool,
                             const std::shared_ptr<TaskGroup>& task_group)
      : ConcreteColumnBuilder(pool, task_group), type_(type) {}

  void Insert(int64_t block_index, const std::shared_ptr<BlockParser>& parser) override;

 protected:
  std::shared_ptr<DataType> type() const override { return type_; }

  std::shared_ptr<DataType> type_;
};

void NullColumnBuilder::Insert(int64_t block_index,
                               const std::shared_ptr<BlockParser>& parser) {
  ReserveChunks(block_index);

  // Spawn a task that will build an array of nulls with the right DataType
  const int32_t num_rows = parser->num_rows();
  DCHECK_GE(num_rows, 0);

  task_group_->Append([this, block_index, num_rows]() -> Status {
    std::unique_ptr<ArrayBuilder> builder;
    RETURN_NOT_OK(MakeBuilder(pool_, type_, &builder));
    std::shared_ptr<Array> res;
    RETURN_NOT_OK(builder->AppendNulls(num_rows));
    RETURN_NOT_OK(builder->Finish(&res));

    return SetChunk(block_index, res);
  });
}

//////////////////////////////////////////////////////////////////////////
// Pre-typed column builder implementation

class TypedColumnBuilder : public ConcreteColumnBuilder {
 public:
  TypedColumnBuilder(const std::shared_ptr<DataType>& type, int32_t col_index,
                     const ConvertOptions& options, MemoryPool* pool,
                     const std::shared_ptr<TaskGroup>& task_group)
      : ConcreteColumnBuilder(pool, task_group, col_index),
        type_(type),
        options_(options) {}

  Status Init();

  void Insert(int64_t block_index, const std::shared_ptr<BlockParser>& parser) override;

 protected:
  std::shared_ptr<DataType> type() const override { return type_; }

  std::shared_ptr<DataType> type_;
  // CAUTION: ConvertOptions can grow large (if it customizes hundreds or
  // thousands of columns), so avoid copying it in each TypedColumnBuilder.
  const ConvertOptions& options_;

  std::shared_ptr<Converter> converter_;
};

Status TypedColumnBuilder::Init() {
  ARROW_ASSIGN_OR_RAISE(converter_, Converter::Make(type_, options_, pool_));
  return Status::OK();
}

void TypedColumnBuilder::Insert(int64_t block_index,
                                const std::shared_ptr<BlockParser>& parser) {
  DCHECK_NE(converter_, nullptr);

  ReserveChunks(block_index);

  // We're careful that all references in the closure outlive the Append() call
  task_group_->Append([this, parser, block_index]() -> Status {
    return SetChunk(block_index, converter_->Convert(*parser, col_index_));
  });
}

//////////////////////////////////////////////////////////////////////////
// Type-inferring column builder implementation

class InferringColumnBuilder : public ConcreteColumnBuilder {
 public:
  InferringColumnBuilder(int32_t col_index, const ConvertOptions& options,
                         MemoryPool* pool, const std::shared_ptr<TaskGroup>& task_group)
      : ConcreteColumnBuilder(pool, task_group, col_index),
        options_(options),
        infer_status_(options) {}

  Status Init();

  void Insert(int64_t block_index, const std::shared_ptr<BlockParser>& parser) override;
  Result<std::shared_ptr<ChunkedArray>> Finish() override;

 protected:
  std::shared_ptr<DataType> type() const override {
    DCHECK_NE(converter_, nullptr);
    return converter_->type();
  }

  Status UpdateType();
  Status TryConvertChunk(int64_t chunk_index);
  // This must be called unlocked!
  void ScheduleConvertChunk(int64_t chunk_index);

  // CAUTION: ConvertOptions can grow large (if it customizes hundreds or
  // thousands of columns), so avoid copying it in each InferringColumnBuilder.
  const ConvertOptions& options_;

  // Current inference status
  InferStatus infer_status_;
  std::shared_ptr<Converter> converter_;

  // The parsers corresponding to each chunk (for reconverting)
  std::vector<std::shared_ptr<BlockParser>> parsers_;
};

Status InferringColumnBuilder::Init() { return UpdateType(); }

Status InferringColumnBuilder::UpdateType() {
  return infer_status_.MakeConverter(pool_).Value(&converter_);
}

void InferringColumnBuilder::ScheduleConvertChunk(int64_t chunk_index) {
  task_group_->Append([this, chunk_index]() { return TryConvertChunk(chunk_index); });
}

Status InferringColumnBuilder::TryConvertChunk(int64_t chunk_index) {
  std::unique_lock<std::mutex> lock(mutex_);
  std::shared_ptr<Converter> converter = converter_;
  std::shared_ptr<BlockParser> parser = parsers_[chunk_index];
  InferKind kind = infer_status_.kind();

  DCHECK_NE(parser, nullptr);

  lock.unlock();
  auto maybe_array = converter->Convert(*parser, col_index_);
  lock.lock();

  if (kind != infer_status_.kind()) {
    // infer_kind_ was changed by another task, reconvert
    lock.unlock();
    ScheduleConvertChunk(chunk_index);
    return Status::OK();
  }

  if (maybe_array.ok() || !infer_status_.can_loosen_type()) {
    // Conversion succeeded, or failed definitively
    if (!infer_status_.can_loosen_type()) {
      // We won't try to reconvert anymore
      parsers_[chunk_index].reset();
    }
    return SetChunkUnlocked(chunk_index, maybe_array);
  }

  // Conversion failed, try another type
  infer_status_.LoosenType(maybe_array.status());
  RETURN_NOT_OK(UpdateType());

  // Reconvert past finished chunks
  // (unfinished chunks will notice by themselves if they need reconverting)
  const auto nchunks = static_cast<int64_t>(chunks_.size());
  for (int64_t i = 0; i < nchunks; ++i) {
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
}

void InferringColumnBuilder::Insert(int64_t block_index,
                                    const std::shared_ptr<BlockParser>& parser) {
  // Create a slot for the new chunk and spawn a task to convert it
  size_t chunk_index = static_cast<size_t>(block_index);
  {
    std::lock_guard<std::mutex> lock(mutex_);

    DCHECK_NE(converter_, nullptr);
    if (parsers_.size() <= chunk_index) {
      parsers_.resize(chunk_index + 1);
    }
    // Should not insert an already converting chunk
    DCHECK_EQ(parsers_[chunk_index], nullptr);
    parsers_[chunk_index] = parser;
    ReserveChunksUnlocked(block_index);
  }

  ScheduleConvertChunk(chunk_index);
}

Result<std::shared_ptr<ChunkedArray>> InferringColumnBuilder::Finish() {
  std::lock_guard<std::mutex> lock(mutex_);

  parsers_.clear();
  return FinishUnlocked();
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
    const std::shared_ptr<TaskGroup>& task_group) {
  return std::make_shared<NullColumnBuilder>(type, pool, task_group);
}

}  // namespace csv
}  // namespace arrow
