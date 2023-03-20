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
#include "arrow/engine/substrait/visibility.h"
#include "arrow/result.h"
#include "arrow/status.h"

#include "substrait/plan.pb.h"  // IWYU pragma: export

namespace arrow {
namespace engine {

/// \brief Replaces the extension information of a Substrait Plan message with the given
/// extension set, such that the anchors defined therein can be used in the rest of the
/// plan.
///
/// \param[in] ext_set the extension set to copy the extension information from
/// \param[in,out] plan the Substrait plan message that is to be updated
/// \return success or failure
ARROW_ENGINE_EXPORT
Status AddExtensionSetToPlan(const ExtensionSet& ext_set, substrait::Plan* plan);

/// \brief Interprets the extension information of a Substrait Plan message into an
/// ExtensionSet.
///
/// Note that the extension registry is not currently mutated, but may be in the future.
///
/// \param[in] plan the plan message to take the information from
/// \param[in,out] registry registry defining which Arrow types and compute functions
/// correspond to Substrait's URI/name pairs
ARROW_ENGINE_EXPORT
Result<ExtensionSet> GetExtensionSetFromPlan(
    const substrait::Plan& plan, const ConversionOptions& conversion_options,
    const ExtensionIdRegistry* registry = default_extension_id_registry());

/// \brief Serialize a declaration into a substrait::Plan.
///
/// Note that, this is a part of a roundtripping test API and not
/// designed for use in production
/// \param[in] declr the sequence of declarations to be serialized
/// \param[in, out] ext_set the extension set to be updated
/// \param[in] conversion_options options to control serialization behavior
/// \return the serialized plan
ARROW_ENGINE_EXPORT Result<std::unique_ptr<substrait::Plan>> PlanToProto(
    const acero::Declaration& declr, ExtensionSet* ext_set,
    const ConversionOptions& conversion_options = {});

}  // namespace engine
}  // namespace arrow
