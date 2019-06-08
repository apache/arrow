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

#include "arrow/error_or.h"

namespace arrow {

#ifdef NDEBUG
const char ErrorOrConstants::kValueMoveConstructorMsg[] = "";
const char ErrorOrConstants::kValueMoveAssignmentMsg[] = "";
const char ErrorOrConstants::kValueOrDieMovedMsg[] = "";
const char ErrorOrConstants::kStatusMoveConstructorMsg[] = "";
const char ErrorOrConstants::kStatusMoveAssignmentMsg[] = "";
#else
const char ErrorOrConstants::kValueMoveConstructorMsg[] =
    "Value moved by StatusOr move constructor";
const char ErrorOrConstants::kValueMoveAssignmentMsg[] =
    "Value moved by StatusOr move assignment";
const char ErrorOrConstants::kStatusMoveConstructorMsg[] =
    "Status moved by StatusOr move constructor";
const char ErrorOrConstants::kValueOrDieMovedMsg[] =
    "Value moved by StatusOr::ValueOrDie";
const char ErrorOrConstants::kStatusMoveAssignmentMsg[] =
    "Status moved by StatusOr move assignment";
#endif

}  // namespace arrow
