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

#include "arrow/python/platform.h"

#include <cstdint>
#include <memory>

#include "arrow/compute/api_scalar.h"
#include "arrow/compute/cast.h"
#include "arrow/compute/exec.h"
#include "arrow/compute/function.h"
#include "arrow/compute/registry.h"

#include "arrow/python/common.h"
#include "arrow/python/pyarrow.h"
#include "arrow/python/visibility.h"

namespace arrow {

namespace py {

/// TODO: TODO(ARROW-16041): UDF Options are not exposed to the Python
/// users. This feature will be included when extending to provide advanced
/// options for the users.
class ARROW_PYTHON_EXPORT ScalarUdfOptions {
 public:
  ScalarUdfOptions(const std::string func_name, const compute::Arity arity,
                   const compute::FunctionDoc func_doc,
                   const std::vector<compute::InputType> in_types,
                   const compute::OutputType out_type)
      : func_name_(func_name),
        kind_(compute::Function::SCALAR),
        arity_(arity),
        func_doc_(std::move(func_doc)),
        in_types_(std::move(in_types)),
        out_type_(out_type) {}

  const std::string& name() const { return func_name_; }

  compute::Function::Kind kind() { return kind_; }

  const compute::Arity& arity() const { return arity_; }

  const compute::FunctionDoc& doc() const { return func_doc_; }

  const std::vector<compute::InputType>& input_types() const { return in_types_; }

  const compute::OutputType& output_type() const { return out_type_; }

 private:
  std::string func_name_;
  compute::Function::Kind kind_;
  compute::Arity arity_;
  const compute::FunctionDoc func_doc_;
  std::vector<compute::InputType> in_types_;
  compute::OutputType out_type_;
};

/// \brief register a Scalar user-defined-function from Python
Status ARROW_PYTHON_EXPORT RegisterScalarFunction(PyObject* function,
                                                  const ScalarUdfOptions& options);

}  // namespace py

}  // namespace arrow
