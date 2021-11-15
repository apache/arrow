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

// This API is EXPERIMENTAL.

#pragma once

#include <vector>

#include "arrow/buffer.h"
#include "arrow/compute/exec/exec_plan.h"
#include "arrow/engine/visibility.h"
#include "arrow/result.h"
#include "arrow/util/optional.h"

namespace arrow {
namespace engine {

ARROW_ENGINE_EXPORT
std::shared_ptr<DataType> uuid();

ARROW_ENGINE_EXPORT
std::shared_ptr<DataType> fixed_char(int32_t length);

ARROW_ENGINE_EXPORT
std::shared_ptr<DataType> varchar(int32_t length);

/// Return true if t is Uuid, otherwise false
ARROW_ENGINE_EXPORT
bool UnwrapUuid(const DataType&);

/// Return FixedChar length if t is FixedChar, otherwise nullopt
ARROW_ENGINE_EXPORT
util::optional<int32_t> UnwrapFixedChar(const DataType&);

/// Return Varchar (max) length if t is VarChar, otherwise nullopt
ARROW_ENGINE_EXPORT
util::optional<int32_t> UnwrapVarChar(const DataType& t);

}  // namespace engine
}  // namespace arrow
