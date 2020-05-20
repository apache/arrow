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
#include <vector>

#include "arrow/compute/exec.h"
#include "arrow/compute/function.h"
#include "arrow/compute/options.h"
#include "arrow/datum.h"
#include "arrow/result.h"

namespace arrow {
namespace compute {

class ExecContext;

class Caster {
 public:
  virtual ~Caster() = default;

  virtual Status Cast(const ArrayData& input, ArrayData* output) = 0;
};

class CastFunction : public ScalarFunction {
 public:
  CastFunction(std::string name, Type::type out_type);
  ~CastFunction();

  Type::type out_type_id() const;

  Status AddKernel(Type::type in_type_id, std::vector<InputType> in_types,
                   OutputType out_type, ArrayKernelExec exec, KernelInit init = NULLPTR);

  Status AddKernel(Type::type in_type_id, ScalarKernel kernel);

  bool CanCastTo(const DataType& out_type) const;

 private:
  struct CastFunctionImpl;
  std::unique_ptr<CastFunctionImpl> impl_;
};

ARROW_EXPORT
Result<std::shared_ptr<const CastFunction>> GetCastFunction(
    const std::shared_ptr<DataType>& from_type, const std::shared_ptr<DataType>& to_type);

/// \brief Return true if a cast function is defined
ARROW_EXPORT
bool CanCast(const DataType& from_type, const DataType& to_type);

// ----------------------------------------------------------------------
// Convenience invocation APIs for a number of kernels

/// \brief Cast from one array type to another
/// \param[in] value array to cast
/// \param[in] to_type type to cast to
/// \param[in] options casting options
/// \param[in] context the function execution context, optional
/// \return the resulting array
///
/// \since 1.0.0
/// \note API not yet finalized
ARROW_EXPORT
Result<std::shared_ptr<Array>> Cast(const Array& value, std::shared_ptr<DataType> to_type,
                                    const CastOptions& options = CastOptions::Safe(),
                                    ExecContext* context = NULLPTR);

/// \brief Cast from one value to another
/// \param[in] value datum to cast
/// \param[in] to_type type to cast to
/// \param[in] options casting options
/// \param[in] context the function execution context, optional
/// \return the resulting datum
///
/// \since 1.0.0
/// \note API not yet finalized
ARROW_EXPORT
Result<Datum> Cast(const Datum& value, std::shared_ptr<DataType> to_type,
                   const CastOptions& options = CastOptions::Safe(),
                   ExecContext* context = NULLPTR);

}  // namespace compute
}  // namespace arrow
