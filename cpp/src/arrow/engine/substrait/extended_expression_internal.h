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

#include <memory>

#include "arrow/compute/type_fwd.h"
#include "arrow/engine/substrait/extension_set.h"
#include "arrow/engine/substrait/options.h"
#include "arrow/engine/substrait/relation.h"
#include "arrow/engine/substrait/visibility.h"
#include "arrow/result.h"
#include "arrow/status.h"

#include "substrait/extended_expression.pb.h"  // IWYU pragma: export

namespace arrow {
namespace engine {

/// Convert a Substrait ExtendedExpression to a vector of expressions and output names
ARROW_ENGINE_EXPORT
Result<BoundExpressions> FromProto(const substrait::ExtendedExpression& expression,
                                   ExtensionSet* ext_set_out,
                                   const ConversionOptions& conversion_options,
                                   const ExtensionIdRegistry* extension_id_registry);

/// Convert a vector of expressions to a Substrait ExtendedExpression
ARROW_ENGINE_EXPORT
Result<std::unique_ptr<substrait::ExtendedExpression>> ToProto(
    const BoundExpressions& bound_expressions, ExtensionSet* ext_set,
    const ConversionOptions& conversion_options);

}  // namespace engine
}  // namespace arrow
