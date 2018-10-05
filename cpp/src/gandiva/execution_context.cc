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

#include "gandiva/execution_context.h"

namespace gandiva {
#ifdef GDV_HELPERS
namespace helpers {
#endif

void ExecutionContext::set_error_msg(const char *error_msg) {
  if (error_msg_.empty()) {
    error_msg_ = std::string(error_msg);
  }
}

std::string ExecutionContext::get_error() const { return error_msg_; }

bool ExecutionContext::has_error() const { return !error_msg_.empty(); }

#ifdef GDV_HELPERS
}
#endif

}  // namespace gandiva
