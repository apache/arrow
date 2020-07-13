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
#include "arrow/csv/column_decoder.h"
#include "arrow/csv/converter.h"
#include "arrow/csv/inference_internal.h"
#include "arrow/csv/options.h"
#include "arrow/csv/parser.h"
#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/util/future.h"
#include "arrow/util/logging.h"
#include "arrow/util/task_group.h"

namespace arrow {
namespace csv {

using internal::TaskGroup;

class ConcreteColumnDecoder : public ColumnDecoder {
 public:
  explicit ConcreteColumnDecoder(MemoryPool* pool,
                                 std::shared_ptr<internal::TaskGroup> task_group,
                                 int32_t col_index = -1)
      : ColumnDecoder(std::move(task_group)),
        pool_(pool),
        col_index_(col_index),
        num_chunks_(-1),
        next_chunk_(0) {}

  void Append(const std::shared_ptr<BlockParser>& parser) override {
    Insert(static_cast<int64_t>(chunks_.size()), parser);
  }

  void SetEOF(int64_t num_blocks) override {
    std::lock_guard<std::mutex> lock(mutex_);

    DCHECK_EQ(num_chunks_, -1) << "Cannot change EOF";
    num_chunks_ = num_blocks;

    // If further chunks have been requested in NextChunk(), arrange to return nullptr
    for (int64_t i = num_chunks_; i < static_cast<int64_t>(chunks_.size()); ++i) {
      auto* chunk = &chunks_[i];
      if (chunk->is_valid()) {
        DCHECK(!IsFutureFinished(chunk->state()));
        chunk->MarkFinished(std::shared_ptr<Array>());
      }
    }
  }

  Result<std::shared_ptr<Array>> NextChunk() override {
    std::unique_lock<std::mutex> lock(mutex_);

    if (num_chunks_ > 0 && next_chunk_ >= num_chunks_) {
      return nullptr;  // EOF
    }
    PrepareChunkUnlocked(next_chunk_);
    auto chunk_index = next_chunk_++;
    WaitForChunkUnlocked(chunk_index);
    // Move Future to avoid keeping chunk alive
    return std::move(chunks_[chunk_index]).result();
  }

 protected:
  // XXX useful?
  virtual std::shared_ptr<DataType> type() const = 0;

  void WaitForChunkUnlocked(int64_t chunk_index) {
    auto future = chunks_[chunk_index];  // Make copy because of resizes
    mutex_.unlock();
    future.Wait();
    mutex_.lock();
  }

  void PrepareChunk(int64_t block_index) {
    std::lock_guard<std::mutex> lock(mutex_);
    PrepareChunkUnlocked(block_index);
  }

  void PrepareChunkUnlocked(int64_t block_index) {
    size_t chunk_index = static_cast<size_t>(block_index);
    if (chunks_.size() <= chunk_index) {
      chunks_.resize(chunk_index + 1);
    }
    if (!chunks_[block_index].is_valid()) {
      chunks_[block_index] = Future<std::shared_ptr<Array>>::Make();
    }
  }

  void SetChunk(int64_t chunk_index, Result<std::shared_ptr<Array>> maybe_array) {
    std::lock_guard<std::mutex> lock(mutex_);
    SetChunkUnlocked(chunk_index, std::move(maybe_array));
  }

  void SetChunkUnlocked(int64_t chunk_index, Result<std::shared_ptr<Array>> maybe_array) {
    auto* chunk = &chunks_[chunk_index];
    DCHECK(chunk->is_valid());
    DCHECK(!IsFutureFinished(chunk->state()));

    if (maybe_array.ok()) {
      chunk->MarkFinished(std::move(maybe_array));
    } else {
      chunk->MarkFinished(WrapConversionError(maybe_array.status()));
    }
  }

  Status WrapConversionError(const Status& st) {
    if (st.ok()) {
      return st;
    } else {
      std::stringstream ss;
      ss << "In CSV column #" << col_index_ << ": " << st.message();
      return st.WithMessage(ss.str());
    }
  }

  MemoryPool* pool_;
  int32_t col_index_;

  std::vector<Future<std::shared_ptr<Array>>> chunks_;
  int64_t num_chunks_;
  int64_t next_chunk_;

  std::mutex mutex_;
};

//////////////////////////////////////////////////////////////////////////
// Null column decoder implementation (for a column not in the CSV file)

class NullColumnDecoder : public ConcreteColumnDecoder {
 public:
  explicit NullColumnDecoder(const std::shared_ptr<DataType>& type, MemoryPool* pool,
                             const std::shared_ptr<internal::TaskGroup>& task_group)
      : ConcreteColumnDecoder(pool, task_group), type_(type) {}

  void Insert(int64_t block_index, const std::shared_ptr<BlockParser>& parser) override;

 protected:
  std::shared_ptr<DataType> type() const override { return type_; }

  std::shared_ptr<DataType> type_;
};

void NullColumnDecoder::Insert(int64_t block_index,
                               const std::shared_ptr<BlockParser>& parser) {
  PrepareChunk(block_index);

  // Spawn a task that will build an array of nulls with the right DataType
  const int32_t num_rows = parser->num_rows();
  DCHECK_GE(num_rows, 0);

  task_group_->Append([=]() -> Status {
    std::unique_ptr<ArrayBuilder> builder;
    RETURN_NOT_OK(MakeBuilder(pool_, type_, &builder));
    std::shared_ptr<Array> array;
    RETURN_NOT_OK(builder->AppendNulls(num_rows));
    RETURN_NOT_OK(builder->Finish(&array));

    SetChunk(block_index, array);
    return Status::OK();
  });
}

//////////////////////////////////////////////////////////////////////////
// Pre-typed column decoder implementation

class TypedColumnDecoder : public ConcreteColumnDecoder {
 public:
  TypedColumnDecoder(const std::shared_ptr<DataType>& type, int32_t col_index,
                     const ConvertOptions& options, MemoryPool* pool,
                     const std::shared_ptr<internal::TaskGroup>& task_group)
      : ConcreteColumnDecoder(pool, task_group, col_index),
        type_(type),
        options_(options) {}

  Status Init();

  void Insert(int64_t block_index, const std::shared_ptr<BlockParser>& parser) override;

 protected:
  std::shared_ptr<DataType> type() const override { return type_; }

  std::shared_ptr<DataType> type_;
  // CAUTION: ConvertOptions can grow large (if it customizes hundreds or
  // thousands of columns), so avoid copying it in each TypedColumnDecoder.
  const ConvertOptions& options_;

  std::shared_ptr<Converter> converter_;
};

Status TypedColumnDecoder::Init() {
  ARROW_ASSIGN_OR_RAISE(converter_, Converter::Make(type_, options_, pool_));
  return Status::OK();
}

void TypedColumnDecoder::Insert(int64_t block_index,
                                const std::shared_ptr<BlockParser>& parser) {
  DCHECK_NE(converter_, nullptr);

  PrepareChunk(block_index);

  // We're careful that all references in the closure outlive the Append() call
  task_group_->Append([=]() -> Status {
    SetChunk(block_index, converter_->Convert(*parser, col_index_));
    return Status::OK();
  });
}

//////////////////////////////////////////////////////////////////////////
// Type-inferring column builder implementation

class InferringColumnDecoder : public ConcreteColumnDecoder {
 public:
  InferringColumnDecoder(int32_t col_index, const ConvertOptions& options,
                         MemoryPool* pool,
                         const std::shared_ptr<internal::TaskGroup>& task_group)
      : ConcreteColumnDecoder(pool, task_group, col_index),
        options_(options),
        infer_status_(options),
        type_frozen_(false) {}

  Status Init();

  void Insert(int64_t block_index, const std::shared_ptr<BlockParser>& parser) override;

 protected:
  std::shared_ptr<DataType> type() const override {
    DCHECK_NE(converter_, nullptr);
    return converter_->type();
  }

  Status UpdateType();
  Result<std::shared_ptr<Array>> RunInference(const std::shared_ptr<BlockParser>& parser);

  // CAUTION: ConvertOptions can grow large (if it customizes hundreds or
  // thousands of columns), so avoid copying it in each InferringColumnDecoder.
  const ConvertOptions& options_;

  // Current inference status
  InferStatus infer_status_;
  bool type_frozen_;
  std::shared_ptr<Converter> converter_;

  // The parsers corresponding to each chunk (for reconverting)
  std::vector<std::shared_ptr<BlockParser>> parsers_;
};

Status InferringColumnDecoder::Init() { return UpdateType(); }

Status InferringColumnDecoder::UpdateType() {
  return infer_status_.MakeConverter(pool_).Value(&converter_);
}

Result<std::shared_ptr<Array>> InferringColumnDecoder::RunInference(
    const std::shared_ptr<BlockParser>& parser) {
  while (true) {
    // (no one else should be updating converter_ concurrently)
    auto maybe_array = converter_->Convert(*parser, col_index_);

    std::unique_lock<std::mutex> lock(mutex_);
    if (maybe_array.ok() || !infer_status_.can_loosen_type()) {
      // Conversion succeeded, or failed definitively
      return maybe_array;
    }
    // Conversion failed temporarily, try another type
    infer_status_.LoosenType(maybe_array.status());
    RETURN_NOT_OK(UpdateType());
  }
}

void InferringColumnDecoder::Insert(int64_t block_index,
                                    const std::shared_ptr<BlockParser>& parser) {
  PrepareChunk(block_index);

  // First block: run inference
  if (block_index == 0) {
    task_group_->Append([=]() -> Status {
      auto maybe_array = RunInference(parser);

      std::unique_lock<std::mutex> lock(mutex_);
      DCHECK(!type_frozen_);
      type_frozen_ = true;
      SetChunkUnlocked(block_index, std::move(maybe_array));
      return Status::OK();
    });
    return;
  }

  // Non-first block: wait for inference to finish on first block now,
  // without blocking a TaskGroup thread.
  {
    std::unique_lock<std::mutex> lock(mutex_);
    PrepareChunkUnlocked(0);
    WaitForChunkUnlocked(0);
    if (!chunks_[0].status().ok()) {
      // Failed converting first chunk: bail out by marking EOF,
      // because we can't decide a type for the other chunks.
      SetChunkUnlocked(block_index, std::shared_ptr<Array>());
    }
    DCHECK(type_frozen_);
  }

  // Then use the inferred type to convert this block.
  task_group_->Append([=]() -> Status {
    auto maybe_array = converter_->Convert(*parser, col_index_);

    SetChunk(block_index, std::move(maybe_array));
    return Status::OK();
  });
}

//////////////////////////////////////////////////////////////////////////
// Factory functions

Result<std::shared_ptr<ColumnDecoder>> ColumnDecoder::Make(
    MemoryPool* pool, int32_t col_index, const ConvertOptions& options,
    std::shared_ptr<TaskGroup> task_group) {
  auto ptr = std::make_shared<InferringColumnDecoder>(col_index, options, pool,
                                                      std::move(task_group));
  RETURN_NOT_OK(ptr->Init());
  return ptr;
}

Result<std::shared_ptr<ColumnDecoder>> ColumnDecoder::Make(
    MemoryPool* pool, std::shared_ptr<DataType> type, int32_t col_index,
    const ConvertOptions& options, std::shared_ptr<TaskGroup> task_group) {
  auto ptr = std::make_shared<TypedColumnDecoder>(std::move(type), col_index, options,
                                                  pool, std::move(task_group));
  RETURN_NOT_OK(ptr->Init());
  return ptr;
}

Result<std::shared_ptr<ColumnDecoder>> ColumnDecoder::MakeNull(
    MemoryPool* pool, std::shared_ptr<DataType> type,
    std::shared_ptr<internal::TaskGroup> task_group) {
  return std::make_shared<NullColumnDecoder>(std::move(type), pool,
                                             std::move(task_group));
}

}  // namespace csv
}  // namespace arrow
