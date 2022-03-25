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

namespace arrow {

namespace engine {

Status SubstraitSinkConsumer::Consume(cp::ExecBatch batch) {
  // Consume a batch of data
  bool did_push = producer_.Push(batch);
  if (!did_push) return Status::ExecutionError("Producer closed already");
  return Status::OK();
}

Future<> SubstraitSinkConsumer::Finish() {
  producer_.Push(IterationEnd<arrow::util::optional<cp::ExecBatch>>());
  if (producer_.Close()) {
    return Future<>::MakeFinished();
  }
  return Future<>::MakeFinished(
      Status::ExecutionError("Error occurred in closing the batch producer"));
}

Status SubstraitExecutor::MakePlan() {
  ARROW_ASSIGN_OR_RAISE(auto serialized_plan,
                        engine::internal::SubstraitFromJSON("Plan", substrait_json_));
  RETURN_NOT_OK(engine::internal::SubstraitToJSON("Plan", *serialized_plan));
  std::vector<std::shared_ptr<cp::SinkNodeConsumer>> consumers;
  std::function<std::shared_ptr<cp::SinkNodeConsumer>()> consumer_factory = [&] {
    consumers.emplace_back(new SubstraitSinkConsumer{generator_});
    return consumers.back();
  };
  ARROW_ASSIGN_OR_RAISE(declarations_,
                        engine::DeserializePlan(*serialized_plan, consumer_factory));
  return Status::OK();
}

arrow::PushGenerator<arrow::util::optional<cp::ExecBatch>>::Producer
SubstraitSinkConsumer::MakeProducer(
    AsyncGenerator<arrow::util::optional<cp::ExecBatch>>* out_gen,
    arrow::util::BackpressureOptions backpressure) {
  arrow::PushGenerator<arrow::util::optional<cp::ExecBatch>> push_gen(
      std::move(backpressure));
  auto out = push_gen.producer();
  *out_gen = std::move(push_gen);
  return out;
}

Result<std::shared_ptr<RecordBatchReader>> SubstraitExecutor::Execute() {
  RETURN_NOT_OK(SubstraitExecutor::MakePlan());
  for (const cp::Declaration& decl : declarations_) {
    RETURN_NOT_OK(decl.AddToPlan(plan_.get()).status());
  }
  ARROW_RETURN_NOT_OK(plan_->Validate());
  ARROW_RETURN_NOT_OK(plan_->StartProducing());
  std::shared_ptr<RecordBatchReader> sink_reader = cp::MakeGeneratorReader(
      schema_, std::move(*generator_), exec_context_.memory_pool());
  return sink_reader;
}

Status SubstraitExecutor::Close() {
  ARROW_RETURN_NOT_OK(plan_->finished().status());
  return Status::OK();
}

Result<std::shared_ptr<RecordBatchReader>> SubstraitExecutor::GetRecordBatchReader(
    std::string& substrait_json, std::shared_ptr<arrow::Schema> schema) {
  cp::ExecContext exec_context(arrow::default_memory_pool(),
                               ::arrow::internal::GetCpuThreadPool());

  arrow::AsyncGenerator<arrow::util::optional<cp::ExecBatch>> sink_gen;
  ARROW_ASSIGN_OR_RAISE(auto plan, cp::ExecPlan::Make());
  arrow::engine::SubstraitExecutor executor(substrait_json, &sink_gen, plan, schema,
                                            exec_context);
  RETURN_NOT_OK(executor.MakePlan());
  ARROW_ASSIGN_OR_RAISE(auto sink_reader, executor.Execute());
  RETURN_NOT_OK(executor.Close());
  return sink_reader;
}

}  // namespace engine

}  // namespace arrow
