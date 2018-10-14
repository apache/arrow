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

#include <string>

#include "arrow/dbi/hiveserver2/operation.h"

namespace arrow {
namespace hiveserver2 {

// Utility functions. Intended primary for testing purposes - clients should not
// rely on stability of the behavior or API of these functions.
class Util {
 public:
  // Fetches the operation's results and returns them in a nicely formatted string.
  static void PrintResults(const Operation* op, std::ostream& out);
};

}  // namespace hiveserver2
}  // namespace arrow
