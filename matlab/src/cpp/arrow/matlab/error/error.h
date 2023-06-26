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


//
// MATLAB_ASSIGN_OR_ERROR(lhs, rexpr, id)
//
//  --- Description ---
//
// A macro used to extract and assign an underlying value of type T
// from an expression that returns an arrow::Result<T> to a variable.
//
// If the arrow::Status associated with the arrow::Result is "OK" (i.e. no error),
// then the value of type T is assigned to the specified lhs.
//
// If the arrow::Status associated with the arrow::Result is not "OK" (i.e. error),
// then the specified error ID is returned to MATLAB.
//
// --- Arguments ---
//
// lhs - variable name to assign to (e.g. auto array)
// rexpr - expression that returns an arrow::Result<T> (e.g. MakeArray())
// id - MATLAB error ID string (const char* - "arrow:matlab:proxy:make:FailedConstruction")
//
// --- Example ---
//
// MATLAB_ASSIGN_OR_ERROR(auto array, make_array(), error::FAILED_TO_MAKE_ARRAY);
//

#define MATLAB_ASSIGN_OR_ERROR(lhs, rexpr, id)                                                    \
    MATLAB_ASSIGN_OR_ERROR_IMPL(MATLAB_ASSIGN_OR_RAISE_NAME(_matlab_error_or_value, __COUNTER__), \
                                lhs, rexpr, id);                                                  \

#define MATLAB_ASSIGN_OR_RAISE_NAME(x, y) \
    ARROW_CONCAT(x, y)                    \

#define MATLAB_ASSIGN_OR_ERROR_IMPL(result_name, lhs, rexpr, id) \
    auto&& result_name = (rexpr);                                \
    MATLAB_ERROR_IF_NOT_OK(result_name.status(), id);            \
    lhs = std::move(result_name).ValueUnsafe();                  \

namespace arrow::matlab::error {
    // TODO: Make Error ID Enum class to avoid defining static constexpr
    static const char* APPEND_VALUES_ERROR_ID = "arrow:matlab:proxy:make:FailedToAppendValues";
    static const char* BUILD_ARRAY_ERROR_ID = "arrow:matlab:proxy:make:FailedToAppendValues";
    static const char* BITPACK_VALIDITY_BITMAP_ERROR_ID = "arrow:matlab:proxy:make:FailedToBitPackValidityBitmap";
    static const char* UNKNOWN_PROXY_ERROR_ID = "arrow:matlab:proxy:UnknownProxy";
    static const char* SCHEMA_BUILDER_FINISH_ERROR_ID = "arrow:matlab:tabular:proxy:SchemaBuilderAddFields";
    static const char* SCHEMA_BUILDER_ADD_FIELDS_ERROR_ID = "arrow:matlab:tabular:proxy:SchemaBuilderFinish";
    static const char* UNICODE_CONVERSION_ERROR_ID = "arrow:matlab:unicode:UnicodeConversion";
}
