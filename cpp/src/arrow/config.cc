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

#include "arrow/config.h"
#include "arrow/util/config.h"

namespace arrow {

static const BuildInfo kBuildInfo = {
    // clang-format off
    ARROW_VERSION,
    ARROW_VERSION_MAJOR,
    ARROW_VERSION_MINOR,
    ARROW_VERSION_PATCH,
    ARROW_VERSION_STRING,
    ARROW_SO_VERSION,
    ARROW_FULL_SO_VERSION,
    ARROW_CXX_COMPILER_ID,
    ARROW_CXX_COMPILER_VERSION,
    ARROW_CXX_COMPILER_FLAGS,
    ARROW_GIT_ID,
    ARROW_GIT_DESCRIPTION,
    ARROW_PACKAGE_KIND,
    // clang-format on
};

const BuildInfo& GetBuildInfo() { return kBuildInfo; }

}  // namespace arrow
