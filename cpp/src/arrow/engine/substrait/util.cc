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
#include "arrow/util/async_generator.h"
#include "arrow/util/async_util.h"

#include "arrow/table.h"

namespace arrow {

namespace engine {

Result<std::shared_ptr<RecordBatchReader>> ExecuteSerializedPlan(
    const Buffer& substrait_buffer, const ExtensionIdRegistry* ext_id_registry,
    compute::FunctionRegistry* func_registry, const ConversionOptions& conversion_options,
    compute::BackpressureOptions backpressure_options,
    compute::BackpressureMonitor** monitor) {
  ARROW_ASSIGN_OR_RAISE(std::vector<compute::Declaration> declarations,
                        engine::DeserializePlans(substrait_buffer, ext_id_registry,
                                                 nullptr, conversion_options));
  if (declarations.size() > 1) {
    return Status::NotImplemented(
        "ExecuteSerializedPlan cannot be called on a plan with multiple top-level "
        "relations");
  }
  if (declarations.empty()) {
    return Status::Invalid("Invalid Substrait plan contained no top-level relations");
  }
  compute::Declaration declaration = declarations[0];
  return compute::DeclarationToReader(std::move(declaration), backpressure_options,
                                      monitor);
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
