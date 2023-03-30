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
#include <optional>

#include "arrow/compute/type_fwd.h"
#include "arrow/datum.h"
#include "arrow/engine/substrait/type_fwd.h"
#include "arrow/engine/substrait/visibility.h"
#include "arrow/result.h"

#include "substrait/algebra.pb.h"  // IWYU pragma: export

namespace arrow {
namespace engine {

class SubstraitCall;

ARROW_ENGINE_EXPORT
Result<FieldRef> DirectReferenceFromProto(const substrait::Expression::FieldReference*,
                                          const ExtensionSet&, const ConversionOptions&);

ARROW_ENGINE_EXPORT
Result<compute::Expression> FromProto(const substrait::Expression&, const ExtensionSet&,
                                      const ConversionOptions&);

ARROW_ENGINE_EXPORT
Result<std::unique_ptr<substrait::Expression>> ToProto(const compute::Expression&,
                                                       ExtensionSet*,
                                                       const ConversionOptions&);

ARROW_ENGINE_EXPORT
Result<Datum> FromProto(const substrait::Expression::Literal&, const ExtensionSet&,
                        const ConversionOptions&);

ARROW_ENGINE_EXPORT
Result<std::unique_ptr<substrait::Expression::Literal>> ToProto(const Datum&,
                                                                ExtensionSet*,
                                                                const ConversionOptions&);

ARROW_ENGINE_EXPORT
Result<SubstraitCall> FromProto(const substrait::AggregateFunction&, bool is_hash,
                                const ExtensionSet&, const ConversionOptions&);

}  // namespace engine
}  // namespace arrow
