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
#include "arrow/datum.h"
#include "arrow/util/cpu_info.h"
#include "arrow/util/logging.h"

#include "arrow/python/common.h"
#include "arrow/python/pyarrow.h"
#include "arrow/python/visibility.h"

namespace cp = arrow::compute;

namespace arrow {

namespace py {

#define DECLARE_CALL_UDF(TYPE_NAME, FUNCTION_SUFFIX, CONVERT_SUFFIX)               \
  ARROW_PYTHON_EXPORT Status exec_function_##FUNCTION_SUFFIX(const cp::ExecBatch&, \
                                                             PyObject*, int, Datum*);

DECLARE_CALL_UDF(Scalar, scalar, scalar)
DECLARE_CALL_UDF(Array, array, make_array)

#undef DECLARE_CALL_UDF

class ARROW_PYTHON_EXPORT UdfBuilder {
 public:
  UdfBuilder(const std::string func_name, const cp::Function::Kind kind,
             const cp::Arity arity, const cp::FunctionDoc* func_doc,
             const std::vector<cp::InputType> in_types, const cp::OutputType out_type,
             const cp::MemAllocation::type mem_allocation,
             const cp::NullHandling::type null_handling)
      : func_name_(func_name),
        kind_(kind),
        arity_(arity),
        func_doc_(func_doc),
        in_types_(in_types),
        out_type_(out_type),
        mem_allocation_(mem_allocation),
        null_handling_(null_handling) {}

  const std::string& name() const { return func_name_; }

  cp::Function::Kind kind() { return kind_; }

  const cp::Arity& arity() const { return arity_; }

  const cp::FunctionDoc& doc() const { return *func_doc_; }

  const std::vector<cp::InputType>& input_types() const { return in_types_; }

  const cp::OutputType& output_type() const { return out_type_; }

  cp::MemAllocation::type mem_allocation() { return mem_allocation_; }

  cp::NullHandling::type null_handling() { return null_handling_; }

 private:
  std::string func_name_;
  cp::Function::Kind kind_;
  cp::Arity arity_;
  const cp::FunctionDoc* func_doc_;
  std::vector<cp::InputType> in_types_;
  cp::OutputType out_type_;
  cp::MemAllocation::type mem_allocation_;
  cp::NullHandling::type null_handling_;
};

class ARROW_PYTHON_EXPORT ScalarUdfBuilder : public UdfBuilder {
 public:
  ScalarUdfBuilder(const std::string func_name, const cp::Arity arity,
                   const cp::FunctionDoc* func_doc,
                   const std::vector<cp::InputType> in_types,
                   const cp::OutputType out_type,
                   const cp::MemAllocation::type mem_allocation,
                   const cp::NullHandling::type null_handling)
      : UdfBuilder(func_name, cp::Function::SCALAR, arity, func_doc, in_types, out_type,
                   mem_allocation, null_handling) {}

  Status MakeFunction(PyObject* function);
};

}  // namespace py

}  // namespace arrow
