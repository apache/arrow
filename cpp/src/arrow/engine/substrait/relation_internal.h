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

#include "arrow/compute/exec/exec_plan.h"
#include "arrow/engine/substrait/extension_types.h"
#include "arrow/engine/substrait/options.h"
#include "arrow/engine/substrait/serde.h"
#include "arrow/engine/substrait/visibility.h"
#include "arrow/type_fwd.h"

#include "substrait/algebra.pb.h"  // IWYU pragma: export

namespace arrow {
namespace engine {

/// Information resulting from converting a Substrait relation.
struct DeclarationInfo {
  /// The compute declaration produced thus far.
  compute::Declaration declaration;

  /// The number of columns returned by the declaration.
  int num_columns;
};

/// \brief A function to extract Acero Declaration from a Substrait Rel object
ARROW_ENGINE_EXPORT
Result<DeclarationInfo> FromProto(const substrait::Rel&, const ExtensionSet&,
                                  const ConversionOptions&);

/// \brief Convert a Declaration (and its inputs) to a Substrait Rel
///
/// A Substrait Rel is passed as a the plan and it is updated with
/// corresponding Declaration passed for serialization.
///
/// Note that this used to fuse a partially serialized plan with another plan.
/// Partially serialized plan is recursively being used to generate global plan.
/// Since kept as unique_ptr resources are relased efficiently upon releasing for
/// the global plan.
ARROW_ENGINE_EXPORT Status SerializeAndCombineRelations(const compute::Declaration&,
                                                        ExtensionSet*,
                                                        std::unique_ptr<substrait::Rel>*,
                                                        const ConversionOptions&);

/// \brief Convert an Acero Declaration to a Substrait Rel
///
/// Note that in order to provide a generic interface for ToProto for
/// declaration. The ExecNode or ExecPlan is not used in this context as Declaration
/// is preferred in the Substrait space rather than internal components of
/// Acero execution engine.
ARROW_ENGINE_EXPORT Result<std::unique_ptr<substrait::Rel>> ToProto(
    const compute::Declaration&, ExtensionSet*, const ConversionOptions&);

}  // namespace engine
}  // namespace arrow
