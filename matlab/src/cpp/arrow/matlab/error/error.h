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
// id - MATLAB error ID string (const char* -
// "arrow:matlab:proxy:make:FailedConstruction")
//
// --- Example ---
//
// MATLAB_ERROR_IF_NOT_OK(builder.Append(...), error::BUILDER_FAILED_TO_APPEND);
//
#define MATLAB_ERROR_IF_NOT_OK(expr, id)                         \
  do {                                                           \
    arrow::Status _status = (expr);                              \
    if (!_status.ok()) {                                         \
      return libmexclass::error::Error{(id), _status.message()}; \
    }                                                            \
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
//           Proxy class which has a libmexclass::proxy::method::Context as an input
//           argument. Use MATLAB_ERROR_IF_NOT_OK inside of a Proxy static make() member
//           function.
//
// --- Arguments ---
//
// expr - expression that returns an arrow::Status (e.g. builder.Append(...))
// context - libmexclass::proxy::method::Context context input to a Proxy method
// id - MATLAB error ID string (const char* -
// "arrow:matlab:proxy:make:FailedConstruction")
//
// --- Example ---
//
// MATLAB_ERROR_IF_NOT_OK_WITH_CONTEXT(builder.Append(...), context,
// error::BUILDER_FAILED_TO_APPEND);
//
#define MATLAB_ERROR_IF_NOT_OK_WITH_CONTEXT(expr, context, id)          \
  do {                                                                  \
    arrow::Status _status = (expr);                                     \
    if (!_status.ok()) {                                                \
      context.error = libmexclass::error::Error{id, _status.message()}; \
      return;                                                           \
    }                                                                   \
  } while (0)

#define MATLAB_ASSIGN_OR_ERROR_NAME(x, y) ARROW_CONCAT(x, y)

#define MATLAB_ASSIGN_OR_ERROR_IMPL(result_name, lhs, rexpr, id) \
  auto&& result_name = (rexpr);                                  \
  MATLAB_ERROR_IF_NOT_OK(result_name.status(), id);              \
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
// id - MATLAB error ID string (const char* -
// "arrow:matlab:proxy:make:FailedConstruction")
//
// --- Example ---
//
// MATLAB_ASSIGN_OR_ERROR(auto array, builder.Finish(), error::FAILED_TO_BUILD_ARRAY);
//
#define MATLAB_ASSIGN_OR_ERROR(lhs, rexpr, id) \
  MATLAB_ASSIGN_OR_ERROR_IMPL(                 \
      MATLAB_ASSIGN_OR_ERROR_NAME(_matlab_error_or_value, __COUNTER__), lhs, rexpr, id);

#define MATLAB_ASSIGN_OR_ERROR_WITH_CONTEXT_IMPL(result_name, lhs, rexpr, context, id) \
  auto&& result_name = (rexpr);                                                        \
  MATLAB_ERROR_IF_NOT_OK_WITH_CONTEXT(result_name.status(), context, id);              \
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
//           Proxy class which has a libmexclass::proxy::method::Context as an input
//           argument. Use MATLAB_ASSIGN_OR_ERROR inside of a Proxy static make() member
//           function.
//
// --- Arguments ---
//
// lhs - variable name to assign to (e.g. auto array)
// rexpr - expression that returns an arrow::Result<T> (e.g. builder.Finish())
// context - libmexclass::proxy::method::Context context input to a Proxy method
// id - MATLAB error ID string (const char* -
// "arrow:matlab:proxy:make:FailedConstruction")
//
// --- Example ---
//
// MATLAB_ASSIGN_OR_ERROR_WITH_CONTEXT(auto array, builder.Finish(),
// error::FAILED_TO_BUILD_ARRAY);
//
#define MATLAB_ASSIGN_OR_ERROR_WITH_CONTEXT(lhs, rexpr, context, id)                \
  MATLAB_ASSIGN_OR_ERROR_WITH_CONTEXT_IMPL(                                         \
      MATLAB_ASSIGN_OR_ERROR_NAME(_matlab_error_or_value, __COUNTER__), lhs, rexpr, \
      context, id);

namespace arrow::matlab::error {
// TODO: Make Error ID Enum class to avoid defining static constexpr
static const char* APPEND_VALUES_ERROR_ID =
    "arrow:matlab:proxy:make:FailedToAppendValues";
static const char* BUILD_ARRAY_ERROR_ID = "arrow:matlab:proxy:make:FailedToAppendValues";
static const char* BITPACK_VALIDITY_BITMAP_ERROR_ID =
    "arrow:matlab:proxy:make:FailedToBitPackValidityBitmap";
static const char* UNKNOWN_PROXY_ERROR_ID = "arrow:matlab:proxy:UnknownProxy";
static const char* SCHEMA_BUILDER_FINISH_ERROR_ID =
    "arrow:matlab:tabular:proxy:SchemaBuilderAddFields";
static const char* SCHEMA_BUILDER_ADD_FIELDS_ERROR_ID =
    "arrow:matlab:tabular:proxy:SchemaBuilderFinish";
static const char* UNICODE_CONVERSION_ERROR_ID = "arrow:matlab:unicode:UnicodeConversion";
static const char* STRING_BUILDER_APPEND_FAILED =
    "arrow:matlab:array:string:StringBuilderAppendFailed";
static const char* STRING_BUILDER_FINISH_FAILED =
    "arrow:matlab:array:string:StringBuilderFinishFailed";
static const char* UNKNOWN_TIME_UNIT_ERROR_ID = "arrow:matlab:UnknownTimeUnit";
static const char* INVALID_TIME_UNIT = "arrow:type:InvalidTimeUnit";
static const char* FIELD_FAILED_TO_CREATE_TYPE_PROXY =
    "arrow:field:FailedToCreateTypeProxy";
static const char* ARRAY_FAILED_TO_CREATE_TYPE_PROXY =
    "arrow:array:FailedToCreateTypeProxy";
static const char* LIST_TYPE_FAILED_TO_CREATE_VALUE_TYPE_PROXY =
    "arrow:type:list:FailedToCreateValueTypeProxy";
static const char* ARROW_TABULAR_SCHEMA_AMBIGUOUS_FIELD_NAME =
    "arrow:tabular:schema:AmbiguousFieldName";
static const char* UNKNOWN_PROXY_FOR_ARRAY_TYPE = "arrow:array:UnknownProxyForArrayType";
static const char* RECORD_BATCH_NUMERIC_INDEX_WITH_EMPTY_RECORD_BATCH =
    "arrow:tabular:recordbatch:NumericIndexWithEmptyRecordBatch";
static const char* RECORD_BATCH_INVALID_NUMERIC_COLUMN_INDEX =
    "arrow:tabular:recordbatch:InvalidNumericColumnIndex";
static const char* TABLE_NUMERIC_INDEX_WITH_EMPTY_TABLE =
    "arrow:tabular:table:NumericIndexWithEmptyTable";
static const char* TABLE_INVALID_NUMERIC_COLUMN_INDEX =
    "arrow:tabular:table:InvalidNumericColumnIndex";
static const char* FAILED_TO_OPEN_FILE_FOR_WRITE = "arrow:io:FailedToOpenFileForWrite";
static const char* FAILED_TO_OPEN_FILE_FOR_READ = "arrow:io:FailedToOpenFileForRead";
static const char* CSV_FAILED_TO_WRITE_TABLE = "arrow:io:csv:FailedToWriteTable";
static const char* CSV_FAILED_TO_CREATE_TABLE_READER =
    "arrow:io:csv:FailedToCreateTableReader";
static const char* CSV_FAILED_TO_READ_TABLE = "arrow:io:csv:FailedToReadTable";
static const char* FEATHER_FAILED_TO_WRITE_TABLE = "arrow:io:feather:FailedToWriteTable";
static const char* TABLE_FROM_RECORD_BATCH = "arrow:table:FromRecordBatch";
static const char* FEATHER_FAILED_TO_CREATE_READER =
    "arrow:io:feather:FailedToCreateReader";
static const char* FEATHER_VERSION_2 = "arrow:io:feather:FeatherVersion2";
static const char* FEATHER_VERSION_UNKNOWN = "arrow:io:feather:FeatherVersionUnknown";
static const char* FEATHER_FAILED_TO_READ_TABLE = "arrow:io:feather:FailedToReadTable";
static const char* FEATHER_FAILED_TO_READ_RECORD_BATCH =
    "arrow:io:feather:FailedToReadRecordBatch";
static const char* CHUNKED_ARRAY_MAKE_FAILED = "arrow:chunkedarray:MakeFailed";
static const char* CHUNKED_ARRAY_NUMERIC_INDEX_WITH_EMPTY_CHUNKED_ARRAY =
    "arrow:chunkedarray:NumericIndexWithEmptyChunkedArray";
static const char* CHUNKED_ARRAY_INVALID_NUMERIC_CHUNK_INDEX =
    "arrow:chunkedarray:InvalidNumericChunkIndex";
static const char* STRUCT_ARRAY_MAKE_FAILED = "arrow:array:StructArrayMakeFailed";
static const char* LIST_ARRAY_FROM_ARRAYS_FAILED =
    "arrow:array:ListArrayFromArraysFailed";
static const char* INDEX_EMPTY_CONTAINER = "arrow:index:EmptyContainer";
static const char* INDEX_OUT_OF_RANGE = "arrow:index:OutOfRange";
static const char* BUFFER_VIEW_OR_COPY_FAILED = "arrow:buffer:ViewOrCopyFailed";
static const char* ARRAY_PRETTY_PRINT_FAILED = "arrow:array:PrettyPrintFailed";
static const char* TABULAR_GET_ROW_AS_STRING_FAILED =
    "arrow:tabular:GetRowAsStringFailed";
static const char* ARRAY_VALIDATE_MINIMAL_FAILED = "arrow:array:ValidateMinimalFailed";
static const char* ARRAY_VALIDATE_FULL_FAILED = "arrow:array:ValidateFullFailed";
static const char* ARRAY_VALIDATE_UNSUPPORTED_ENUM =
    "arrow:array:ValidateUnsupportedEnum";
static const char* ARRAY_SLICE_NON_POSITIVE_OFFSET =
    "arrow:array:slice:NonPositiveOffset";
static const char* ARRAY_SLICE_NEGATIVE_LENGTH = "arrow:array:slice:NegativeLength";
static const char* ARRAY_SLICE_FAILED_TO_CREATE_ARRAY_PROXY =
    "arrow:array:slice:FailedToCreateArrayProxy";
static const char* C_EXPORT_FAILED = "arrow:c:export:ExportFailed";
static const char* C_IMPORT_FAILED = "arrow:c:import:ImportFailed";
static const char* IPC_RECORD_BATCH_WRITE_FAILED =
    "arrow:io:ipc:FailedToWriteRecordBatch";
static const char* IPC_RECORD_BATCH_WRITE_CLOSE_FAILED = "arrow:io:ipc:CloseFailed";
static const char* IPC_RECORD_BATCH_READER_OPEN_FAILED =
    "arrow:io:ipc:FailedToOpenRecordBatchReader";
static const char* IPC_RECORD_BATCH_READER_INVALID_CONSTRUCTION_TYPE =
    "arrow:io:ipc:InvalidConstructionType";
static const char* IPC_RECORD_BATCH_READ_INVALID_INDEX = "arrow:io:ipc:InvalidIndex";
static const char* IPC_RECORD_BATCH_READ_FAILED = "arrow:io:ipc:ReadFailed";
static const char* IPC_TABLE_READ_FAILED = "arrow:io:ipc:TableReadFailed";
static const char* IPC_END_OF_STREAM = "arrow:io:ipc:EndOfStream";
static const char* TABLE_MAKE_UNKNOWN_METHOD = "arrow:table:UnknownMakeMethod";

}  // namespace arrow::matlab::error
