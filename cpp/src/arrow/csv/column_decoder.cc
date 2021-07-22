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

#include "arrow/csv/column_decoder.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <sstream>
#include <string>
#include <utility>

#include "arrow/array.h"
#include "arrow/array/builder_base.h"
#include "arrow/csv/converter.h"
#include "arrow/csv/inference_internal.h"
#include "arrow/csv/options.h"
#include "arrow/csv/parser.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "arrow/util/future.h"
#include "arrow/util/logging.h"
#include "arrow/util/task_group.h"

namespace arrow {
namespace csv {

using internal::TaskGroup;

class ConcreteColumnDecoder : public ColumnDecoder {
 public:
  explicit ConcreteColumnDecoder(MemoryPool* pool, int32_t col_index = -1)
      : ColumnDecoder(), pool_(pool), col_index_(col_index) {}

 protected:
  // XXX useful?
  virtual std::shared_ptr<DataType> type() const = 0;

  Result<std::shared_ptr<Array>> WrapConversionError(
      const Result<std::shared_ptr<Array>>& result) {
    if (ARROW_PREDICT_TRUE(result.ok())) {
      return result;
    } else {
      const auto& st = result.status();
      std::stringstream ss;
      ss << "In CSV column #" << col_index_ << ": " << st.message();
      return st.WithMessage(ss.str());
    }
  }

  MemoryPool* pool_;
  int32_t col_index_;
  internal::Executor* executor_;
};

//////////////////////////////////////////////////////////////////////////
// Null column decoder implementation (for a column not in the CSV file)

class NullColumnDecoder : public ConcreteColumnDecoder {
 public:
  explicit NullColumnDecoder(const std::shared_ptr<DataType>& type, MemoryPool* pool)
      : ConcreteColumnDecoder(pool), type_(type) {}

  Future<std::shared_ptr<Array>> Decode(
      const std::shared_ptr<BlockParser>& parser) override;

 protected:
  std::shared_ptr<DataType> type() const override { return type_; }

  std::shared_ptr<DataType> type_;
};

Future<std::shared_ptr<Array>> NullColumnDecoder::Decode(
    const std::shared_ptr<BlockParser>& parser) {
  DCHECK_GE(parser->num_rows(), 0);
  return WrapConversionError(MakeArrayOfNull(type_, parser->num_rows(), pool_));
}

//////////////////////////////////////////////////////////////////////////
// Pre-typed column decoder implementation

class TypedColumnDecoder : public ConcreteColumnDecoder {
 public:
  TypedColumnDecoder(const std::shared_ptr<DataType>& type, int32_t col_index,
                     const ConvertOptions& options, MemoryPool* pool)
      : ConcreteColumnDecoder(pool, col_index), type_(type), options_(options) {}

  Status Init();

  Future<std::shared_ptr<Array>> Decode(
      const std::shared_ptr<BlockParser>& parser) override;

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

Future<std::shared_ptr<Array>> TypedColumnDecoder::Decode(
    const std::shared_ptr<BlockParser>& parser) {
  DCHECK_NE(converter_, nullptr);
  return Future<std::shared_ptr<Array>>::MakeFinished(
      WrapConversionError(converter_->Convert(*parser, col_index_)));
}

//////////////////////////////////////////////////////////////////////////
// Type-inferring column builder implementation

class InferringColumnDecoder : public ConcreteColumnDecoder {
 public:
  InferringColumnDecoder(int32_t col_index, const ConvertOptions& options,
                         MemoryPool* pool)
      : ConcreteColumnDecoder(pool, col_index),
        options_(options),
        infer_status_(options),
        type_frozen_(false) {
    first_inference_run_ = Future<>::Make();
    first_inferrer_ = 0;
  }

  Status Init();

  Future<std::shared_ptr<Array>> Decode(
      const std::shared_ptr<BlockParser>& parser) override;

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
  std::atomic<int> first_inferrer_;
  Future<> first_inference_run_;
  std::shared_ptr<Converter> converter_;
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

    if (maybe_array.ok() || !infer_status_.can_loosen_type()) {
      // Conversion succeeded, or failed definitively
      DCHECK(!type_frozen_);
      type_frozen_ = true;
      return maybe_array;
    }
    // Conversion failed temporarily, try another type
    infer_status_.LoosenType(maybe_array.status());
    auto update_status = UpdateType();
    if (!update_status.ok()) {
      return update_status;
    }
  }
}

Future<std::shared_ptr<Array>> InferringColumnDecoder::Decode(
    const std::shared_ptr<BlockParser>& parser) {
  bool already_taken = first_inferrer_.fetch_or(1);
  // First block: run inference
  if (!already_taken) {
    auto maybe_array = RunInference(parser);
    first_inference_run_.MarkFinished();
    return Future<std::shared_ptr<Array>>::MakeFinished(std::move(maybe_array));
  }

  // Non-first block: wait for inference to finish on first block now,
  // without blocking a TaskGroup thread.
  return first_inference_run_.Then([this, parser] {
    DCHECK(type_frozen_);
    auto maybe_array = converter_->Convert(*parser, col_index_);
    return WrapConversionError(converter_->Convert(*parser, col_index_));
  });
}

//////////////////////////////////////////////////////////////////////////
// Factory functions

Result<std::shared_ptr<ColumnDecoder>> ColumnDecoder::Make(
    MemoryPool* pool, int32_t col_index, const ConvertOptions& options) {
  auto ptr = std::make_shared<InferringColumnDecoder>(col_index, options, pool);
  RETURN_NOT_OK(ptr->Init());
  return ptr;
}

Result<std::shared_ptr<ColumnDecoder>> ColumnDecoder::Make(
    MemoryPool* pool, std::shared_ptr<DataType> type, int32_t col_index,
    const ConvertOptions& options) {
  auto ptr =
      std::make_shared<TypedColumnDecoder>(std::move(type), col_index, options, pool);
  RETURN_NOT_OK(ptr->Init());
  return ptr;
}

Result<std::shared_ptr<ColumnDecoder>> ColumnDecoder::MakeNull(
    MemoryPool* pool, std::shared_ptr<DataType> type) {
  return std::make_shared<NullColumnDecoder>(std::move(type), pool);
}

}  // namespace csv
}  // namespace arrow
