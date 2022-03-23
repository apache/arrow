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
  ARROW_ASSIGN_OR_RAISE(auto serialized_plan, engine::internal::SubstraitFromJSON("Plan", substrait_json_));
  
  auto maybe_plan_json = engine::internal::SubstraitToJSON("Plan", *serialized_plan);
  RETURN_NOT_OK(maybe_plan_json.status());

  std::vector<std::shared_ptr<cp::SinkNodeConsumer>> consumers;
  std::function<std::shared_ptr<cp::SinkNodeConsumer>()> consumer_factory = [&] {
    // All batches produced by the plan will be fed into IgnoringConsumers:
    consumers.emplace_back(new SubstraitSinkConsumer{generator_});
    return consumers.back();
  };

  // Deserialize each relation tree in the substrait plan to an Arrow compute Declaration
  ARROW_ASSIGN_OR_RAISE(declerations_, engine::DeserializePlan(*serialized_plan, consumer_factory));
  
  // It's safe to drop the serialized plan; we don't leave references to its memory
  serialized_plan.reset();

  // Construct an empty plan (note: configure Function registry and ThreadPool here)
  return Status::OK();
}

Result<std::shared_ptr<RecordBatchReader>> SubstraitExecutor::Execute() {
  for (const cp::Declaration& decl : declerations_) {
    RETURN_NOT_OK(decl.AddToPlan(plan_.get()).status());
  }

  ARROW_RETURN_NOT_OK(plan_->Validate());

  ARROW_RETURN_NOT_OK(plan_->StartProducing());

  std::shared_ptr<RecordBatchReader> sink_reader =
      cp::MakeGeneratorReader(schema_, std::move(*generator_), exec_context_.memory_pool());
  ARROW_RETURN_NOT_OK(plan_->finished().status());
  return sink_reader;
}

}  // namespace engine

}  // namespace arrow