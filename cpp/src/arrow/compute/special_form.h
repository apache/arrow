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

#pragma once

#include "arrow/compute/expression.h"
#include "arrow/util/visibility.h"

#include <vector>

namespace arrow {
namespace compute {

class ARROW_EXPORT SpecialForm {
 public:
  static Result<std::unique_ptr<SpecialForm>> Make(const std::string& name);

  virtual ~SpecialForm() = default;

  virtual Result<Datum> Execute(const Expression::Call& call, const ExecBatch& input,
                                ExecContext* exec_context) = 0;
};

}  // namespace compute
}  // namespace arrow
