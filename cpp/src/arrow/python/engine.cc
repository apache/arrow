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

#include <arrow/python/engine.h>

namespace arrow {

namespace py {

namespace engine {

Status SubstraitSinkConsumer::Consume(cp::ExecBatch batch) {
  // Consume a batch of data
  bool did_push = producer_.Push(batch);
  if (!did_push) return Status::ExecutionError("Producer closed already");
  return Status::OK();
}

Future<> SubstraitSinkConsumer::Finish() {
  producer_.Push(IterationEnd<util::optional<cp::ExecBatch>>());
  if (producer_.Close()) {
    return Future<>::MakeFinished();
  }
  return Future<>::MakeFinished(
      Status::ExecutionError("Error occurred in closing the batch producer"));
}

Result<std::vector<cp::Declaration>> SubstraitHandler::BuildPlan(
    std::string substrait_json) {
  auto maybe_serialized_plan = eng::internal::SubstraitFromJSON("Plan", substrait_json);
  RETURN_NOT_OK(maybe_serialized_plan.status());
  std::shared_ptr<arrow::Buffer> serialized_plan =
      std::move(maybe_serialized_plan).ValueOrDie();

  auto maybe_plan_json = eng::internal::SubstraitToJSON("Plan", *serialized_plan);
  RETURN_NOT_OK(maybe_plan_json.status());

  std::vector<std::shared_ptr<cp::SinkNodeConsumer>> consumers;
  std::function<std::shared_ptr<cp::SinkNodeConsumer>()> consumer_factory = [&] {
    // All batches produced by the plan will be fed into IgnoringConsumers:
    consumers.emplace_back(new SubstraitSinkConsumer{generator_});
    return consumers.back();
  };

  // Deserialize each relation tree in the substrait plan to an Arrow compute Declaration
  Result<std::vector<cp::Declaration>> maybe_decls =
      eng::DeserializePlan(*serialized_plan, consumer_factory);
  RETURN_NOT_OK(maybe_decls.status());
  std::vector<cp::Declaration> decls = std::move(maybe_decls).ValueOrDie();

  // It's safe to drop the serialized plan; we don't leave references to its memory
  serialized_plan.reset();

  // Construct an empty plan (note: configure Function registry and ThreadPool here)
  return decls;
}

Result<std::shared_ptr<RecordBatchReader>> SubstraitHandler::ExecutePlan(
    cp::ExecContext exec_context, std::shared_ptr<Schema> schema,
    std::vector<cp::Declaration> declarations) {
  Result<std::shared_ptr<cp::ExecPlan>> maybe_plan = cp::ExecPlan::Make();
  RETURN_NOT_OK(maybe_plan.status());
  std::shared_ptr<cp::ExecPlan> plan = std::move(maybe_plan).ValueOrDie();

  for (const cp::Declaration& decl : declarations) {
    RETURN_NOT_OK(decl.AddToPlan(plan.get()).status());
  }

  RETURN_NOT_OK(plan->Validate());

  RETURN_NOT_OK(plan->StartProducing());

  std::shared_ptr<arrow::RecordBatchReader> sink_reader =
      cp::MakeGeneratorReader(schema, std::move(*generator_), exec_context.memory_pool());

  return sink_reader;
}


}  // namespace engine

}  // namespace py

}  // namespace arrow