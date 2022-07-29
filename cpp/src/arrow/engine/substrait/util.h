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
#include "arrow/compute/registry.h"
#include "arrow/engine/substrait/api.h"
#include "arrow/util/iterator.h"
#include "arrow/util/optional.h"

namespace arrow {

namespace engine {

namespace substrait {

/// \brief Retrieve a RecordBatchReader from a Substrait plan.
ARROW_ENGINE_EXPORT Result<std::shared_ptr<RecordBatchReader>> ExecuteSerializedPlan(
    const Buffer& substrait_buffer, const ExtensionIdRegistry* registry = NULLPTR,
    compute::FunctionRegistry* func_registry = NULLPTR);

/// \brief Get a Serialized Plan from a Substrait JSON plan.
/// This is a helper method for Python tests.
ARROW_ENGINE_EXPORT Result<std::shared_ptr<Buffer>> SerializeJsonPlan(
    const std::string& substrait_json);

/// \brief Make a nested registry with the default registry as parent.
/// See arrow::engine::nested_extension_id_registry for details.
ARROW_ENGINE_EXPORT std::shared_ptr<ExtensionIdRegistry> MakeExtensionIdRegistry();

/// \brief Register a function manually.
///
/// Register an arrow function name by an ID, defined by a URI and a name, on a given
/// extension-id-registry.
///
/// \param[in] registry an extension-id-registry to use
/// \param[in] id_uri a URI of the ID to register by
/// \param[in] id_name a name of the ID to register by
/// \param[in] arrow_function_name name of arrow function to register
ARROW_ENGINE_EXPORT Status RegisterFunction(ExtensionIdRegistry& registry,
                                            const std::string& id_uri,
                                            const std::string& id_name,
                                            const std::string& arrow_function_name);

ARROW_ENGINE_EXPORT const std::string& default_extension_types_uri();

}  // namespace substrait

}  // namespace engine

}  // namespace arrow
