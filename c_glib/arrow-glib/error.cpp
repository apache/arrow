/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <arrow-glib/error.hpp>

#include <iostream>
#include <sstream>

G_BEGIN_DECLS

/**
 * SECTION: error
 * @title: GArrowError
 * @short_description: Error code mapping between Arrow and arrow-glib
 *
 * #GArrowError provides error codes corresponding to `arrow::Status`
 * values.
 */

G_DEFINE_QUARK(garrow - error - quark, garrow_error)

G_END_DECLS

gboolean
garrow_error_check(GError **error, const arrow::Status &status, const char *context)
{
  return garrow::check(error, status, context);
}

GArrowError
garrow_error_from_status(const arrow::Status &status)
{
  switch (status.code()) {
  case arrow::StatusCode::OK:
    return GARROW_ERROR_UNKNOWN;
  case arrow::StatusCode::OutOfMemory:
    return GARROW_ERROR_OUT_OF_MEMORY;
  case arrow::StatusCode::KeyError:
    return GARROW_ERROR_KEY;
  case arrow::StatusCode::TypeError:
    return GARROW_ERROR_TYPE;
  case arrow::StatusCode::Invalid:
    return GARROW_ERROR_INVALID;
  case arrow::StatusCode::IOError:
    return GARROW_ERROR_IO;
  case arrow::StatusCode::CapacityError:
    return GARROW_ERROR_CAPACITY;
  case arrow::StatusCode::IndexError:
    return GARROW_ERROR_INDEX;
  case arrow::StatusCode::UnknownError:
    return GARROW_ERROR_UNKNOWN;
  case arrow::StatusCode::NotImplemented:
    return GARROW_ERROR_NOT_IMPLEMENTED;
  case arrow::StatusCode::SerializationError:
    return GARROW_ERROR_SERIALIZATION;
  case arrow::StatusCode::CodeGenError:
    return GARROW_ERROR_CODE_GENERATION;
  case arrow::StatusCode::ExpressionValidationError:
    return GARROW_ERROR_EXPRESSION_VALIDATION;
  case arrow::StatusCode::ExecutionError:
    return GARROW_ERROR_EXECUTION;
  case arrow::StatusCode::AlreadyExists:
    return GARROW_ERROR_ALREADY_EXISTS;
  default:
    return GARROW_ERROR_UNKNOWN;
  }
}

arrow::StatusCode
garrow_error_to_status_code(GError *error, arrow::StatusCode default_code)
{
  if (error->domain != GARROW_ERROR) {
    return default_code;
  }

  switch (error->code) {
  case GARROW_ERROR_OUT_OF_MEMORY:
    return arrow::StatusCode::OutOfMemory;
  case GARROW_ERROR_KEY:
    return arrow::StatusCode::KeyError;
  case GARROW_ERROR_TYPE:
    return arrow::StatusCode::TypeError;
  case GARROW_ERROR_INVALID:
    return arrow::StatusCode::Invalid;
  case GARROW_ERROR_IO:
    return arrow::StatusCode::IOError;
  case GARROW_ERROR_CAPACITY:
    return arrow::StatusCode::CapacityError;
  case GARROW_ERROR_INDEX:
    return arrow::StatusCode::IndexError;
  case GARROW_ERROR_UNKNOWN:
    return arrow::StatusCode::UnknownError;
  case GARROW_ERROR_NOT_IMPLEMENTED:
    return arrow::StatusCode::NotImplemented;
  case GARROW_ERROR_SERIALIZATION:
    return arrow::StatusCode::SerializationError;
  case GARROW_ERROR_CODE_GENERATION:
    return arrow::StatusCode::CodeGenError;
  case GARROW_ERROR_EXPRESSION_VALIDATION:
    return arrow::StatusCode::ExpressionValidationError;
  case GARROW_ERROR_EXECUTION:
    return arrow::StatusCode::ExecutionError;
  case GARROW_ERROR_ALREADY_EXISTS:
    return arrow::StatusCode::AlreadyExists;
  default:
    return default_code;
  }
}

arrow::Status
garrow_error_to_status(GError *error, arrow::StatusCode default_code, const char *context)
{
  std::stringstream message;
  message << context << ": " << g_quark_to_string(error->domain);
  message << "(" << error->code << "): ";
  message << error->message;
  auto code = garrow_error_to_status_code(error, default_code);
  g_error_free(error);
  return arrow::Status(code, message.str());
}

namespace garrow {
  gboolean
  check(GError **error, const arrow::Status &status, const char *context)
  {
    if (status.ok()) {
      return TRUE;
    } else {
      g_set_error(error,
                  GARROW_ERROR,
                  garrow_error_from_status(status),
                  "%s: %s",
                  context,
                  status.ToString().c_str());
      return FALSE;
    }
  }
} // namespace garrow
