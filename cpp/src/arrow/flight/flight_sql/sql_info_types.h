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

#include <unordered_map>
#include <vector>

#include "arrow/type_fwd.h"
#include "arrow/util/variant.h"

namespace arrow {
namespace flight {
namespace sql {

using SqlInfoResult =
    arrow::util::Variant<std::string, bool, int64_t, int32_t, std::vector<std::string>,
                         std::unordered_map<int32_t, std::vector<int32_t>>>;
using SqlInfoResultMap = std::unordered_map<int32_t, SqlInfoResult>;

}  // namespace sql
}  // namespace flight
}  // namespace arrow
