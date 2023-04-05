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

#pragma once

#include <memory>

#include "arrow/acero/exec_plan.h"
#include "arrow/engine/substrait/visibility.h"
#include "arrow/type_fwd.h"

namespace arrow {
namespace engine {

/// Execution information resulting from converting a Substrait relation.
struct ARROW_ENGINE_EXPORT DeclarationInfo {
  /// The compute declaration produced thus far.
  acero::Declaration declaration;

  std::shared_ptr<Schema> output_schema;
};

/// Information resulting from converting a Substrait relation.
///
/// RelationInfo adds the "output indices" field for the extension to define how the
/// fields should be mapped to get the standard indices expected by Substrait.
struct ARROW_ENGINE_EXPORT RelationInfo {
  /// The execution information produced thus far.
  DeclarationInfo decl_info;
  /// A vector of indices, one per input field per input in order, each index referring
  /// to the corresponding field within the output schema, if it is in the output, or -1
  /// otherwise. Each location in this vector is a field input index. This vector is
  /// useful for translating selected field input indices (often from an output mapping in
  /// a Substrait plan) of a join-type relation to their locations in the output schema of
  /// the relation. This vector is undefined if the translation is unsupported.
  std::optional<std::vector<int>> field_output_indices;
};

/// Information resulting from converting a Substrait plan
struct ARROW_ENGINE_EXPORT PlanInfo {
  /// The root declaration.
  ///
  /// Only plans containing a single top-level relation are supported and so this will
  /// represent that relation.
  ///
  /// This should technically be a RelRoot but some producers use a simple Rel here and so
  /// Acero currently supports that case.
  DeclarationInfo root;
  /// The names of the output fields
  ///
  /// If `root` was created from a simple Rel then this will be empty
  std::vector<std::string> names;
};

}  // namespace engine
}  // namespace arrow
