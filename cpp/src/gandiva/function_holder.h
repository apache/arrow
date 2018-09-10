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

#ifndef GANDIVA_FUNCTION_HOLDER_H
#define GANDIVA_FUNCTION_HOLDER_H

#include <memory>

namespace gandiva {

/// Holder for a function that can be invoked from LLVM.
class FunctionHolder {
 public:
  virtual ~FunctionHolder() = default;
};

using FunctionHolderPtr = std::shared_ptr<FunctionHolder>;

}  // namespace gandiva

#endif  // GANDIVA_FUNCTION_HOLDER_H
