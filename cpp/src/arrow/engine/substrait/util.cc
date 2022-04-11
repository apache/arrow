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

#include "arrow/engine/substrait/util.h"
#include "arrow/util/async_generator.h"

namespace arrow {

namespace engine {

/// \brief A SinkNodeConsumer specialized to output ExecBatches via PushGenerator
class ARROW_ENGINE_EXPORT SubstraitSinkConsumer : public compute::SinkNodeConsumer {
 public:
  explicit SubstraitSinkConsumer(
      AsyncGenerator<arrow::util::optional<compute::ExecBatch>>* generator,
      arrow::util::BackpressureOptions backpressure = {})
      : producer_(MakeProducer(generator, std::move(backpressure))) {}

  Status Consume(compute::ExecBatch batch) override {
    // Consume a batch of data
    bool did_push = producer_.Push(batch);
    if (!did_push) return Status::ExecutionError("Producer closed already");
    return Status::OK();
  }

  Status Init(const std::shared_ptr<Schema>& schema) override { return Status::OK(); }

  static arrow::PushGenerator<arrow::util::optional<compute::ExecBatch>>::Producer
  MakeProducer(AsyncGenerator<arrow::util::optional<compute::ExecBatch>>* out_gen,
               arrow::util::BackpressureOptions backpressure);

  Future<> Finish() override {
    producer_.Push(IterationEnd<arrow::util::optional<compute::ExecBatch>>());
    if (producer_.Close()) {
      return Future<>::MakeFinished();
    }
    return Future<>::MakeFinished(
        Status::ExecutionError("Error occurred in closing the batch producer"));
  }

 private:
  PushGenerator<arrow::util::optional<compute::ExecBatch>>::Producer producer_;
};

/// \brief An executor to run a Substrait Query
/// This interface is provided as a utility when creating language
/// bindings for consuming a Substrait plan.
class ARROW_ENGINE_EXPORT SubstraitExecutor {
 public:
  explicit SubstraitExecutor(
      std::shared_ptr<Buffer> substrait_buffer,
      AsyncGenerator<arrow::util::optional<compute::ExecBatch>>* generator,
      std::shared_ptr<compute::ExecPlan> plan, compute::ExecContext exec_context)
      : substrait_buffer_(substrait_buffer),
        generator_(generator),
        plan_(std::move(plan)),
        exec_context_(exec_context) {}

  Result<std::shared_ptr<RecordBatchReader>> Execute() {
    RETURN_NOT_OK(SubstraitExecutor::Init());
    for (const compute::Declaration& decl : declarations_) {
      RETURN_NOT_OK(decl.AddToPlan(plan_.get()).status());
    }
    ARROW_RETURN_NOT_OK(plan_->Validate());
    ARROW_RETURN_NOT_OK(plan_->StartProducing());
    // schema of the output can be obtained by the output_schema
    // of the input to the sink node.
    auto schema = plan_->sinks()[0]->inputs()[0]->output_schema();
    std::shared_ptr<RecordBatchReader> sink_reader = compute::MakeGeneratorReader(
        schema, std::move(*generator_), exec_context_.memory_pool());
    return sink_reader;
  }

  Status Close() { return plan_->finished().status(); }

  Status Init() {
    if (substrait_buffer_ == NULLPTR) {
      return Status::Invalid("Buffer containing Substrait plan is null.");
    }
    std::function<std::shared_ptr<compute::SinkNodeConsumer>()> consumer_factory = [&] {
      return std::make_shared<SubstraitSinkConsumer>(generator_);
    };
    ARROW_ASSIGN_OR_RAISE(declarations_,
                          engine::DeserializePlan(*substrait_buffer_, consumer_factory));
    return Status::OK();
  }

 private:
  std::shared_ptr<Buffer> substrait_buffer_;
  AsyncGenerator<arrow::util::optional<compute::ExecBatch>>* generator_;
  std::vector<compute::Declaration> declarations_;
  std::shared_ptr<compute::ExecPlan> plan_;
  compute::ExecContext exec_context_;
};

arrow::PushGenerator<arrow::util::optional<compute::ExecBatch>>::Producer
SubstraitSinkConsumer::MakeProducer(
    AsyncGenerator<arrow::util::optional<compute::ExecBatch>>* out_gen,
    arrow::util::BackpressureOptions backpressure) {
  arrow::PushGenerator<arrow::util::optional<compute::ExecBatch>> push_gen(
      std::move(backpressure));
  auto out = push_gen.producer();
  *out_gen = std::move(push_gen);
  return out;
}

Result<std::shared_ptr<RecordBatchReader>> GetRecordBatchReader(
    std::string& substrait_json) {
  ARROW_ASSIGN_OR_RAISE(auto substrait_buffer,
                        GetSubstraitBufferFromJSON(substrait_json));
  return GetRecordBatchReader(substrait_buffer);
}

Result<std::shared_ptr<RecordBatchReader>> GetRecordBatchReader(
    std::shared_ptr<Buffer> substrait_buffer) {
  arrow::AsyncGenerator<arrow::util::optional<compute::ExecBatch>> sink_gen;
  ARROW_ASSIGN_OR_RAISE(auto plan, compute::ExecPlan::Make());
  compute::ExecContext exec_context(arrow::default_memory_pool(),
                                    ::arrow::internal::GetCpuThreadPool());
  arrow::engine::SubstraitExecutor executor(substrait_buffer, &sink_gen, plan,
                                            exec_context);
  RETURN_NOT_OK(executor.Init());
  ARROW_ASSIGN_OR_RAISE(auto sink_reader, executor.Execute());
  RETURN_NOT_OK(executor.Close());
  return sink_reader;
}

Result<std::shared_ptr<Buffer>> GetSubstraitBufferFromJSON(std::string& substrait_json) {
  return engine::internal::SubstraitFromJSON("Plan", substrait_json);
}

}  // namespace engine

}  // namespace arrow
