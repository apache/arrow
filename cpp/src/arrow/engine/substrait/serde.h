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

// This API is EXPERIMENTAL.

#pragma once

#include <string>
#include <vector>

#include "arrow/buffer.h"
#include "arrow/compute/exec/exec_plan.h"
#include "arrow/engine/substrait/extension_types.h"
#include "arrow/engine/visibility.h"
#include "arrow/result.h"
#include "arrow/util/string_view.h"

namespace arrow {
namespace engine {

ARROW_ENGINE_EXPORT Result<std::vector<compute::Declaration>> ConvertPlan(const Buffer&);

ARROW_ENGINE_EXPORT
Result<std::shared_ptr<DataType>> DeserializeType(const Buffer&, const ExtensionSet&);

ARROW_ENGINE_EXPORT
Result<std::shared_ptr<Buffer>> SerializeType(const DataType&, ExtensionSet*);

ARROW_ENGINE_EXPORT
Result<std::shared_ptr<Schema>> DeserializeSchema(const Buffer&);

ARROW_ENGINE_EXPORT
Result<std::shared_ptr<Buffer>> SerializeSchema(const Schema&);

ARROW_ENGINE_EXPORT
Result<compute::Expression> DeserializeExpression(const Buffer&);

ARROW_ENGINE_EXPORT
Result<std::shared_ptr<Buffer>> SerializeExpression(const compute::Expression&);

namespace internal {

ARROW_ENGINE_EXPORT
Status CheckMessagesEquivalent(util::string_view message_name, const Buffer&,
                               const Buffer&);

ARROW_ENGINE_EXPORT
Result<std::shared_ptr<Buffer>> SubstraitFromJSON(util::string_view type_name,
                                                  util::string_view json);

ARROW_ENGINE_EXPORT
Result<std::string> SubstraitToJSON(util::string_view type_name, const Buffer& buf);

}  // namespace internal
}  // namespace engine
}  // namespace arrow
