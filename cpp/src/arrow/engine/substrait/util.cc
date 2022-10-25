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

#include "arrow/compute/exec/exec_plan.h"
#include "arrow/compute/exec/options.h"
#include "arrow/util/async_generator.h"
#include "arrow/util/async_util.h"

#include "arrow/table.h"

namespace arrow {

namespace engine {

namespace {

/// \brief A SinkNodeConsumer specialized to output ExecBatches via PushGenerator
class SubstraitSinkConsumer : public compute::SinkNodeConsumer {
 public:
  explicit SubstraitSinkConsumer(
      arrow::PushGenerator<std::optional<compute::ExecBatch>>::Producer producer)
      : producer_(std::move(producer)) {}

  Status Consume(compute::ExecBatch batch) override {
    // Consume a batch of data
    bool did_push = producer_.Push(batch);
    if (!did_push) return Status::Invalid("Producer closed already");
    return Status::OK();
  }

  Status Init(const std::shared_ptr<Schema>& schema,
              compute::BackpressureControl* backpressure_control) override {
    schema_ = schema;
    return Status::OK();
  }

  Future<> Finish() override {
    ARROW_UNUSED(producer_.Close());
    return Future<>::MakeFinished();
  }

  std::shared_ptr<Schema> schema() { return schema_; }

 private:
  arrow::PushGenerator<std::optional<compute::ExecBatch>>::Producer producer_;
  std::shared_ptr<Schema> schema_;
};

/// \brief An executor to run a Substrait Query
/// This interface is provided as a utility when creating language
/// bindings for consuming a Substrait plan.
class SubstraitExecutor {
 public:
  explicit SubstraitExecutor(std::shared_ptr<compute::ExecPlan> plan,
                             compute::ExecContext* exec_context,
                             const ConversionOptions& conversion_options = {},
                             bool handle_backpressure = false)
      : plan_(std::move(plan)),
        plan_started_(false),
        exec_context_(exec_context),
        conversion_options_(conversion_options),
        handle_backpressure_(handle_backpressure) {}

  ~SubstraitExecutor() { ARROW_UNUSED(this->Close()); }

  Result<std::shared_ptr<RecordBatchReader>> Execute() {
    for (const compute::Declaration& decl : declarations_) {
      RETURN_NOT_OK(decl.AddToPlan(plan_.get()).status());
    }
    RETURN_NOT_OK(plan_->Validate());
    plan_started_ = true;
    RETURN_NOT_OK(plan_->StartProducing());
    std::shared_ptr<RecordBatchReader> sink_reader;
    if (handle_backpressure_) {
      sink_reader = compute::MakeGeneratorReader(
          std::move(output_schema_), std::move(sink_gen_), exec_context_->memory_pool());
    } else {
      output_schema_ = sink_consumer_->schema();
      sink_reader = compute::MakeGeneratorReader(
          std::move(output_schema_), std::move(generator_), exec_context_->memory_pool());
    }
    return std::move(sink_reader);
  }

  Status Close() {
    if (plan_started_) return plan_->finished().status();
    return Status::OK();
  }

  Status Init(const Buffer& substrait_buffer, const ExtensionIdRegistry* registry) {
    if (substrait_buffer.size() == 0) {
      return Status::Invalid("Empty substrait plan is passed.");
    }

    if (handle_backpressure_) {
      sink_node_options_ = std::make_shared<compute::SinkNodeOptions>(
          compute::SinkNodeOptions{&sink_gen_, {}, nullptr, &output_schema_});
      std::function<std::shared_ptr<compute::SinkNodeOptions>()> sink_factory = [&] {
        return sink_node_options_;
      };
      ARROW_ASSIGN_OR_RAISE(declarations_, engine::DeserializePlans(
                                               substrait_buffer, sink_factory, registry,
                                               nullptr, conversion_options_));
    } else {
      sink_consumer_ = std::make_shared<SubstraitSinkConsumer>(generator_.producer());
      std::function<std::shared_ptr<compute::SinkNodeConsumer>()> consumer_factory = [&] {
        return sink_consumer_;
      };
      ARROW_ASSIGN_OR_RAISE(declarations_, engine::DeserializePlans(
                                               substrait_buffer, consumer_factory,
                                               registry, nullptr, conversion_options_));
    }
    return Status::OK();
  }

 private:
  arrow::PushGenerator<std::optional<compute::ExecBatch>> generator_;
  std::shared_ptr<Schema> output_schema_;
  arrow::AsyncGenerator<std::optional<compute::ExecBatch>> sink_gen_;
  std::shared_ptr<compute::SinkNodeOptions> sink_node_options_;
  std::vector<compute::Declaration> declarations_;
  std::shared_ptr<compute::ExecPlan> plan_;
  bool plan_started_;
  compute::ExecContext* exec_context_;
  std::shared_ptr<SubstraitSinkConsumer> sink_consumer_;
  const ConversionOptions& conversion_options_;
  bool handle_backpressure_;
};

}  // namespace

Result<std::shared_ptr<RecordBatchReader>> ExecuteSerializedPlan(
    const Buffer& substrait_buffer, const bool handle_backpressure,
    std::shared_ptr<compute::ExecPlan> plan, compute::ExecContext* exec_context,
    const ExtensionIdRegistry* registry, compute::FunctionRegistry* func_registry,
    const ConversionOptions& conversion_options) {
  if (!exec_context) {
    exec_context =
        new compute::ExecContext(arrow::default_memory_pool(),
                                 ::arrow::internal::GetCpuThreadPool(), func_registry);
  }
  if (plan == nullptr) {
    ARROW_ASSIGN_OR_RAISE(plan, compute::ExecPlan::Make(exec_context));
  }
  SubstraitExecutor executor(std::move(plan), exec_context, conversion_options,
                             handle_backpressure);
  RETURN_NOT_OK(executor.Init(substrait_buffer, registry));
  ARROW_ASSIGN_OR_RAISE(auto sink_reader, executor.Execute());
  // check closing here, not in destructor, to expose error to caller
  RETURN_NOT_OK(executor.Close());
  return std::move(sink_reader);
}

Result<std::shared_ptr<Buffer>> SerializeJsonPlan(const std::string& substrait_json) {
  return engine::internal::SubstraitFromJSON("Plan", substrait_json);
}

std::shared_ptr<ExtensionIdRegistry> MakeExtensionIdRegistry() {
  return nested_extension_id_registry(default_extension_id_registry());
}

const std::string& default_extension_types_uri() {
  static std::string uri(engine::kArrowExtTypesUri);
  return uri;
}

}  // namespace engine

}  // namespace arrow
