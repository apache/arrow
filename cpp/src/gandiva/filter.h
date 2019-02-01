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
#include <string>
#include <utility>
#include <vector>

#include "arrow/status.h"

#include "gandiva/arrow.h"
#include "gandiva/condition.h"
#include "gandiva/configuration.h"
#include "gandiva/selection_vector.h"
#include "gandiva/visibility.h"

namespace gandiva {

class LLVMGenerator;

/// \brief filter records based on a condition.
///
/// A filter is built for a specific schema and condition. Once the filter is built, it
/// can be used to evaluate many row batches.
class GANDIVA_EXPORT Filter {
 public:
  Filter(std::unique_ptr<LLVMGenerator> llvm_generator, SchemaPtr schema,
         std::shared_ptr<Configuration> config);

  // Inline dtor will attempt to resolve the destructor for
  // LLVMGenerator on MSVC, so we compile the dtor in the object code
  ~Filter();

  /// Build a filter for the given schema and condition, with the default configuration.
  ///
  /// \param[in] schema schema for the record batches, and the condition.
  /// \param[in] condition filter condition.
  /// \param[out] filter the returned filter object
  static Status Make(SchemaPtr schema, ConditionPtr condition,
                     std::shared_ptr<Filter>* filter) {
    return Make(schema, condition, ConfigurationBuilder::DefaultConfiguration(), filter);
  }

  /// \brief Build a filter for the given schema and condition.
  /// Customize the filter with runtime configuration.
  ///
  /// \param[in] schema schema for the record batches, and the condition.
  /// \param[in] condition filter conditions.
  /// \param[in] config run time configuration.
  /// \param[out] filter the returned filter object
  static Status Make(SchemaPtr schema, ConditionPtr condition,
                     std::shared_ptr<Configuration> config,
                     std::shared_ptr<Filter>* filter);

  /// Evaluate the specified record batch, and populate output selection vector.
  ///
  /// \param[in] batch the record batch. schema should be the same as the one in 'Make'
  /// \param[in,out] out_selection the selection array with indices of rows that match
  ///                the condition.
  Status Evaluate(const arrow::RecordBatch& batch,
                  std::shared_ptr<SelectionVector> out_selection);

 private:
  const std::unique_ptr<LLVMGenerator> llvm_generator_;
  const SchemaPtr schema_;
  const std::shared_ptr<Configuration> configuration_;
};

}  // namespace gandiva
