// Copyright (C) 2017-2018 Dremio Corporation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef GANDIVA_CONTEXT_HELPER_H
#define GANDIVA_CONTEXT_HELPER_H

#include "../execution_context.h"
#include "types.h"

void set_error_msg(int64_t context_ptr, char const* err_msg) {
  gandiva::helpers::ExecutionContext* execution_context_ptr =
      reinterpret_cast<gandiva::helpers::ExecutionContext*>(context_ptr);
  (execution_context_ptr)->set_error_msg(err_msg);
}
#endif
