// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <memory>
#include <string>
#include <vector>
#include "arrow/compute/type_fwd.h"
#include "arrow/engine/api.h"
#include "arrow/util/async_generator.h"
#include "arrow/util/iterator.h"
#include "arrow/util/optional.h"

namespace arrow {

namespace cp = arrow::compute;

namespace engine {

/// \brief A SinkNodeConsumer specialized to output ExecBatches via PushGenerator
class ARROW_ENGINE_EXPORT SubstraitSinkConsumer : public cp::SinkNodeConsumer {
 public:
  explicit SubstraitSinkConsumer(
      AsyncGenerator<arrow::util::optional<cp::ExecBatch>>* generator,
      arrow::util::BackpressureOptions backpressure = {})
      : producer_(MakeProducer(generator, std::move(backpressure))) {}

  Status Consume(cp::ExecBatch batch) override;

  static arrow::PushGenerator<arrow::util::optional<cp::ExecBatch>>::Producer
  MakeProducer(AsyncGenerator<arrow::util::optional<cp::ExecBatch>>* out_gen,
               arrow::util::BackpressureOptions backpressure);

  Future<> Finish() override;

 private:
  PushGenerator<arrow::util::optional<cp::ExecBatch>>::Producer producer_;
};

/// \brief An executor to run a Substrait Query
/// This interface is provided as a utility when creating language
/// bindings for consuming a Substrait plan.
class ARROW_ENGINE_EXPORT SubstraitExecutor {
 public:
  explicit SubstraitExecutor(
      std::string substrait_json,
      AsyncGenerator<arrow::util::optional<cp::ExecBatch>>* generator,
      std::shared_ptr<cp::ExecPlan> plan,
      cp::ExecContext exec_context)
      : substrait_json_(substrait_json),
        generator_(generator),
        plan_(std::move(plan)),
        exec_context_(exec_context) {}

  Result<std::shared_ptr<RecordBatchReader>> Execute();

  Status Close();

  static Result<std::shared_ptr<RecordBatchReader>> GetRecordBatchReader(
      std::string& substrait_json);

 private:
  std::string substrait_json_;
  AsyncGenerator<arrow::util::optional<cp::ExecBatch>>* generator_;
  std::vector<cp::Declaration> declarations_;
  std::shared_ptr<cp::ExecPlan> plan_;
  cp::ExecContext exec_context_;

  Status MakePlan();
};

}  // namespace engine

}  // namespace arrow
