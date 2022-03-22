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

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/engine/api.h>
#include "arrow/util/async_generator.h"
#include "arrow/util/iterator.h"

namespace arrow {

namespace eng = arrow::engine;
namespace cp = arrow::compute;

namespace py {

namespace engine {

class SubstraitSinkConsumer : public cp::SinkNodeConsumer {
 public:
  explicit SubstraitSinkConsumer(
      AsyncGenerator<util::optional<cp::ExecBatch>>* generator,
      util::BackpressureOptions backpressure = {})
      : producer_(MakeProducer(generator, std::move(backpressure))) {}

  Status Consume(cp::ExecBatch batch) override;

  static arrow::PushGenerator<util::optional<cp::ExecBatch>>::Producer
  MakeProducer(AsyncGenerator<util::optional<cp::ExecBatch>>* out_gen,
               util::BackpressureOptions backpressure) {
    arrow::PushGenerator<util::optional<cp::ExecBatch>> push_gen(
        std::move(backpressure));
    auto out = push_gen.producer();
    *out_gen = std::move(push_gen);
    return out;
  }

  Future<> Finish() override;

 private:
  PushGenerator<util::optional<cp::ExecBatch>>::Producer producer_;
};

class SubstraitHandler {
 public:
  explicit SubstraitHandler(AsyncGenerator<util::optional<cp::ExecBatch>>* generator)
      : generator_(generator) {}

  Result<std::vector<cp::Declaration>> BuildPlan(std::string substrait_json);

  Result<std::shared_ptr<RecordBatchReader>> ExecutePlan(
      cp::ExecContext exec_context, std::shared_ptr<Schema> schema,
      std::vector<cp::Declaration> declarations);

 private:
  AsyncGenerator<util::optional<cp::ExecBatch>>* generator_;
};

}  // namespace engine
}  // namespace py
}  // namespace arrow
