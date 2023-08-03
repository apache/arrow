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

//
// MATLAB_ERROR_IF_NOT_OK(expr, id)
//
//  --- Description ---
//
// A macro used to return an error to MATLAB if the arrow::Status returned
// by the specified expression is not "OK" (i.e. error).
//
// **NOTE**: This macro should be used inside of the static make() member function for a
//           Proxy class. Use MATLAB_ERROR_IF_NOT_OK_WITH_CONTEXT inside of a non-static
//           Proxy member function.
//
// --- Arguments ---
//
// expr - expression that returns an arrow::Status (e.g. builder.Append(...))
// id - MATLAB error ID string (const char* - "arrow:matlab:proxy:make:FailedConstruction")
//
// --- Example ---
//
// MATLAB_ERROR_IF_NOT_OK(builder.Append(...), error::BUILDER_FAILED_TO_APPEND);
//
#define MATLAB_ERROR_IF_NOT_OK(expr, id)                               \
    do {                                                               \
        arrow::Status _status = (expr);                                \
        if (!_status.ok()) {                                           \
            return libmexclass::error::Error{(id), _status.message()}; \
        }                                                              \
    } while (0)

//
// MATLAB_ERROR_IF_NOT_OK_WITH_CONTEXT(expr, context, id)
//
//  --- Description ---
//
// A macro used to return an error to MATLAB if the arrow::Status returned
// by the specified expression is not "OK" (i.e. error).
//
// **NOTE**: This macro should be used inside of a non-static member function of a
//           Proxy class which has a libmexclass::proxy::method::Context as an input argument.
//           Use MATLAB_ERROR_IF_NOT_OK inside of a Proxy static make() member function.
//
// --- Arguments ---
//
// expr - expression that returns an arrow::Status (e.g. builder.Append(...))
// context - libmexclass::proxy::method::Context context input to a Proxy method
// id - MATLAB error ID string (const char* - "arrow:matlab:proxy:make:FailedConstruction")
//
// --- Example ---
//
// MATLAB_ERROR_IF_NOT_OK_WITH_CONTEXT(builder.Append(...), context, error::BUILDER_FAILED_TO_APPEND);
//
#define MATLAB_ERROR_IF_NOT_OK_WITH_CONTEXT(expr, context, id)                \
    do {                                                                      \
        arrow::Status _status = (expr);                                       \
        if (!_status.ok()) {                                                  \
            context.error = libmexclass::error::Error{id, _status.message()}; \
            return;                                                           \
        }                                                                     \
    } while (0)

#define MATLAB_ASSIGN_OR_ERROR_NAME(x, y) \
    ARROW_CONCAT(x, y)

#define MATLAB_ASSIGN_OR_ERROR_IMPL(result_name, lhs, rexpr, id) \
    auto&& result_name = (rexpr);                                \
    MATLAB_ERROR_IF_NOT_OK(result_name.status(), id);            \
    lhs = std::move(result_name).ValueUnsafe();

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
// **NOTE**: This macro should be used inside of the static make() member function for a
//           Proxy class. Use MATLAB_ERROR_IF_NOT_OK_WITH_CONTEXT inside of a non-static
//           Proxy member function.
//
// --- Arguments ---
//
// lhs - variable name to assign to (e.g. auto array)
// rexpr - expression that returns an arrow::Result<T> (e.g. builder.Finish())
// id - MATLAB error ID string (const char* - "arrow:matlab:proxy:make:FailedConstruction")
//
// --- Example ---
//
// MATLAB_ASSIGN_OR_ERROR(auto array, builder.Finish(), error::FAILED_TO_BUILD_ARRAY);
//
#define MATLAB_ASSIGN_OR_ERROR(lhs, rexpr, id)                                                    \
    MATLAB_ASSIGN_OR_ERROR_IMPL(MATLAB_ASSIGN_OR_ERROR_NAME(_matlab_error_or_value, __COUNTER__), \
                                lhs, rexpr, id);


#define MATLAB_ASSIGN_OR_ERROR_WITH_CONTEXT_IMPL(result_name, lhs, rexpr, context, id) \
    auto&& result_name = (rexpr);                                                      \
    MATLAB_ERROR_IF_NOT_OK_WITH_CONTEXT(result_name.status(), context, id);            \
    lhs = std::move(result_name).ValueUnsafe();

//
// MATLAB_ASSIGN_OR_ERROR_WITH_CONTEXT(lhs, rexpr, context, id)
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
// **NOTE**: This macro should be used inside of a non-static member function of a
//           Proxy class which has a libmexclass::proxy::method::Context as an input argument.
//           Use MATLAB_ASSIGN_OR_ERROR inside of a Proxy static make() member function.
//
// --- Arguments ---
//
// lhs - variable name to assign to (e.g. auto array)
// rexpr - expression that returns an arrow::Result<T> (e.g. builder.Finish())
// context - libmexclass::proxy::method::Context context input to a Proxy method
// id - MATLAB error ID string (const char* - "arrow:matlab:proxy:make:FailedConstruction")
//
// --- Example ---
//
// MATLAB_ASSIGN_OR_ERROR_WITH_CONTEXT(auto array, builder.Finish(), error::FAILED_TO_BUILD_ARRAY);
//
#define MATLAB_ASSIGN_OR_ERROR_WITH_CONTEXT(lhs, rexpr, context, id)                                           \
    MATLAB_ASSIGN_OR_ERROR_WITH_CONTEXT_IMPL(MATLAB_ASSIGN_OR_ERROR_NAME(_matlab_error_or_value, __COUNTER__), \
                                             lhs, rexpr, context, id);

namespace arrow::matlab::error {
    // TODO: Make Error ID Enum class to avoid defining static constexpr
    static const char* APPEND_VALUES_ERROR_ID = "arrow:matlab:proxy:make:FailedToAppendValues";
    static const char* BUILD_ARRAY_ERROR_ID = "arrow:matlab:proxy:make:FailedToAppendValues";
    static const char* BITPACK_VALIDITY_BITMAP_ERROR_ID = "arrow:matlab:proxy:make:FailedToBitPackValidityBitmap";
    static const char* UNKNOWN_PROXY_ERROR_ID = "arrow:matlab:proxy:UnknownProxy";
    static const char* SCHEMA_BUILDER_FINISH_ERROR_ID = "arrow:matlab:tabular:proxy:SchemaBuilderAddFields";
    static const char* SCHEMA_BUILDER_ADD_FIELDS_ERROR_ID = "arrow:matlab:tabular:proxy:SchemaBuilderFinish";
    static const char* UNICODE_CONVERSION_ERROR_ID = "arrow:matlab:unicode:UnicodeConversion";
    static const char* STRING_BUILDER_APPEND_FAILED = "arrow:matlab:array:string:StringBuilderAppendFailed";
    static const char* STRING_BUILDER_FINISH_FAILED = "arrow:matlab:array:string:StringBuilderFinishFailed";
    static const char* UKNOWN_TIME_UNIT_ERROR_ID = "arrow:matlab:UnknownTimeUnit";
    static const char* FIELD_FAILED_TO_CREATE_TYPE_PROXY = "arrow:field:FailedToCreateTypeProxy";
    static const char* ARRAY_FAILED_TO_CREATE_TYPE_PROXY = "arrow:array:FailedToCreateTypeProxy";
    static const char* ARROW_TABULAR_SCHEMA_INVALID_NUMERIC_FIELD_INDEX = "arrow:tabular:schema:InvalidNumericFieldIndex";
    static const char* ARROW_TABULAR_SCHEMA_UNKNOWN_FIELD_NAME = "arrow:tabular:schema:UnknownFieldName";
    static const char* ARROW_TABULAR_SCHEMA_AMBIGUOUS_FIELD_NAME = "arrow:tabular:schema:AmbiguousFieldName";
    static const char* ARROW_TABULAR_SCHEMA_NUMERIC_FIELD_INDEX_WITH_EMPTY_SCHEMA = "arrow:tabular:schema:NumericFieldIndexWithEmptySchema";
}
