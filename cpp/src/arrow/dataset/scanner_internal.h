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
#include <utility>

#include "arrow/array/array_nested.h"
#include "arrow/array/util.h"
#include "arrow/compute/api_scalar.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/exec.h"
#include "arrow/dataset/dataset_internal.h"
#include "arrow/dataset/partition.h"
#include "arrow/dataset/scanner.h"
#include "arrow/util/algorithm.h"
#include "arrow/util/async_generator.h"
#include "arrow/util/future.h"
#include "arrow/util/logging.h"
#include "arrow/util/vector.h"

namespace arrow {

using internal::checked_cast;

namespace dataset {

inline Status NestedFieldRefsNotImplemented() {
  // TODO(ARROW-11259) Several functions (for example, IpcScanTask::Make) assume that
  // only top level fields will be materialized.
  return Status::NotImplemented("Nested field references in scans.");
}

inline Status SetProjection(ScanOptions* options, const Expression& projection) {
  ARROW_ASSIGN_OR_RAISE(options->projection, projection.Bind(*options->dataset_schema));

  if (options->projection.type()->id() != Type::STRUCT) {
    return Status::Invalid("Projection ", projection.ToString(),
                           " cannot yield record batches");
  }
  options->projected_schema = ::arrow::schema(
      checked_cast<const StructType&>(*options->projection.type()).fields(),
      options->dataset_schema->metadata());

  return Status::OK();
}

inline Status SetProjection(ScanOptions* options, std::vector<Expression> exprs,
                            std::vector<std::string> names) {
  compute::ProjectOptions project_options{std::move(names)};

  for (size_t i = 0; i < exprs.size(); ++i) {
    if (auto ref = exprs[i].field_ref()) {
      if (!ref->name()) return NestedFieldRefsNotImplemented();

      // set metadata and nullability for plain field references
      ARROW_ASSIGN_OR_RAISE(auto field, ref->GetOne(*options->dataset_schema));
      project_options.field_nullability[i] = field->nullable();
      project_options.field_metadata[i] = field->metadata();
    }
  }

  return SetProjection(options,
                       call("project", std::move(exprs), std::move(project_options)));
}

inline Status SetProjection(ScanOptions* options, std::vector<std::string> names) {
  std::vector<Expression> exprs(names.size());
  for (size_t i = 0; i < exprs.size(); ++i) {
    exprs[i] = field_ref(names[i]);
  }
  return SetProjection(options, std::move(exprs), std::move(names));
}

inline Status SetFilter(ScanOptions* options, const Expression& filter) {
  for (const auto& ref : FieldsInExpression(filter)) {
    if (!ref.name()) return NestedFieldRefsNotImplemented();

    RETURN_NOT_OK(ref.FindOne(*options->dataset_schema));
  }
  ARROW_ASSIGN_OR_RAISE(options->filter, filter.Bind(*options->dataset_schema));
  return Status::OK();
}

}  // namespace dataset
}  // namespace arrow
