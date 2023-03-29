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

#include "arrow/compute/api_aggregate.h"
#include "arrow/compute/type_fwd.h"
#include "arrow/engine/substrait/relation.h"
#include "arrow/engine/substrait/type_fwd.h"
#include "arrow/engine/substrait/visibility.h"
#include "arrow/result.h"

#include "substrait/algebra.pb.h"  // IWYU pragma: export

namespace arrow {
namespace engine {

/// \brief Convert a Substrait Rel object to an Acero declaration
ARROW_ENGINE_EXPORT
Result<DeclarationInfo> FromProto(const substrait::Rel&, const ExtensionSet&,
                                  const ConversionOptions&);

/// \brief Convert an Acero Declaration to a Substrait Rel
///
/// Note that, in order to provide a generic interface for ToProto,
/// the ExecNode or ExecPlan are not used in this context as Declaration
/// is preferred in the Substrait space rather than internal components of
/// Acero execution engine.
ARROW_ENGINE_EXPORT Result<std::unique_ptr<substrait::Rel>> ToProto(
    const compute::Declaration&, ExtensionSet*, const ConversionOptions&);

namespace internal {

struct ParsedMeasure {
  compute::Aggregate aggregate;
  std::vector<int> fieldset;
};

/// \brief Parse an aggregate relation's measure
///
/// \param[in] agg_measure the measure
/// \param[in] ext_set an extension mapping to use in parsing
/// \param[in] conversion_options options to control how the conversion is done
/// \param[in] input_schema the schema to which field refs apply
/// \param[in] is_hash whether the measure is a hash one (i.e., aggregation keys exist)
ARROW_ENGINE_EXPORT
Result<ParsedMeasure> ParseAggregateMeasure(
    const substrait::AggregateRel::Measure& agg_measure, const ExtensionSet& ext_set,
    const ConversionOptions& conversion_options, bool is_hash,
    const std::shared_ptr<Schema> input_schema);

/// \brief Make an aggregate declaration info
///
/// \param[in] input_decl the input declaration to use
/// \param[in] input_schema the schema to which field refs apply
/// \param[in] measure_size the number of measures to use
/// \param[in] aggregates the aggregates to use
/// \param[in] agg_src_fieldsets the field-sets per aggregate to use
/// \param[in] keys the field-refs for grouping keys to use
/// \param[in] key_field_ids the field-ids for grouping keys to use
/// \param[in] segment_keys the field-refs for segment keys to use
/// \param[in] segment_key_field_ids the field-ids for segment keys to use
/// \param[in] ext_set an extension mapping to use
/// \param[in] conversion_options options to control how the conversion is done
ARROW_ENGINE_EXPORT Result<DeclarationInfo> MakeAggregateDeclaration(
    compute::Declaration input_decl, std::shared_ptr<Schema> input_schema,
    const int measure_size, std::vector<compute::Aggregate> aggregates,
    std::vector<std::vector<int>> agg_src_fieldsets, std::vector<FieldRef> keys,
    std::vector<int> key_field_ids, std::vector<FieldRef> segment_keys,
    std::vector<int> segment_key_field_ids, const ExtensionSet& ext_set,
    const ConversionOptions& conversion_options);

}  // namespace internal

}  // namespace engine
}  // namespace arrow
