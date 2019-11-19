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

#ifndef GANDIVA_LIKE_HOLDER_H
#define GANDIVA_LIKE_HOLDER_H

#include <memory>
#include <string>

#include <re2/re2.h>

#include "arrow/status.h"

#include "gandiva/function_holder.h"
#include "gandiva/node.h"
#include "gandiva/visibility.h"

namespace gandiva {

/// Base class for Function Holder for pattern matching SQL functions like
/// 'like' and 'regexp_matches'
class GANDIVA_EXPORT LikeHolder : public FunctionHolder {
 public:
  static Status Make(const FunctionNode& node, std::string* pattern);

  virtual bool operator()(const std::string& data) = 0;
};

}  // namespace gandiva

#endif  // GANDIVA_LIKE_HOLDER_H
