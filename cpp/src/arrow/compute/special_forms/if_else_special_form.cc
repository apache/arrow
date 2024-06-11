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

// NOTE: API is EXPERIMENTAL and will change without going through a
// deprecation cycle.

#include "arrow/compute/special_forms/if_else_special_form.h"
#include "arrow/compute/exec.h"
#include "arrow/compute/expression.h"
#include "arrow/compute/expression_internal.h"

namespace arrow {
namespace compute {

Result<Datum> IfElseSpecialForm::Execute(const Expression::Call& call,
                                         const ExecBatch& input,
                                         ExecContext* exec_context) {
  DCHECK(!call.kernel->selection_vector_aware);
  DCHECK(!input.selection_vector);

  std::vector<Datum> arguments(call.arguments.size());
  ARROW_ASSIGN_OR_RAISE(arguments[0],
                        ExecuteScalarExpression(call.arguments[0], input, exec_context));
  // Use cond as selection vector for IF.
  // TODO: Consider chunked array for arguments[0].
  auto if_sel = std::make_shared<SelectionVector>(arguments[0].array());
  // Duplicate and invert cond as selection vector for ELSE.
  ARROW_ASSIGN_OR_RAISE(
      auto else_sel,
      if_sel->Copy(CPUDevice::memory_manager(exec_context->memory_pool())));
  RETURN_NOT_OK(else_sel->Invert());

  ExecBatch if_input = input;
  if_input.selection_vector = std::move(if_sel);
  ARROW_ASSIGN_OR_RAISE(
      arguments[1], ExecuteScalarExpression(call.arguments[1], if_input, exec_context));
  ExecBatch else_input = input;
  else_input.selection_vector = std::move(else_sel);
  ARROW_ASSIGN_OR_RAISE(
      arguments[2], ExecuteScalarExpression(call.arguments[2], else_input, exec_context));

  return ExecuteCallNonRecursive(call, input, arguments, exec_context);
}

}  // namespace compute
}  // namespace arrow
