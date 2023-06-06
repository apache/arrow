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


#include "arrow/status.h"
#include "libmexclass/error/Error.h"

#include <string_view>

#define MATLAB_ERROR_IF_NOT_OK(expr, id)                               \
    do {                                                               \
        arrow::Status _status = (expr);                                \
        if (!_status.ok()) {                                           \
            return libmexclass::error::Error{(id), _status.message()}; \
        }                                                              \
    } while (0)

namespace arrow::matlab::error {
    // TODO: Make Error ID Enum class to avoid defining static constexpr
    static const char* APPEND_VALUES_ERROR_ID = "arrow:matlab:proxy:make:FailedToAppendValues";
    static const char* BUILD_ARRAY_ERROR_ID = "arrow:matlab:proxy:make:FailedToAppendValues";
    static const char* BITPACK_VALIDITY_BITMAP_ERROR_ID = "arrow:matlab:proxy:make:FailedToBitPackValidityBitmap";
    static const char* UNKNOWN_PROXY_ERROR_ID = "arrow:matlab:proxy:UnknownProxy";
}
