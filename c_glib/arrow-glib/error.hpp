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

#pragma once

#include <arrow/api.h>

#include <arrow-glib/error.h>

gboolean garrow_error_check(GError **error,
                            const arrow::Status &status,
                            const char *context);
GArrowError garrow_error_from_status(const arrow::Status &status);
arrow::StatusCode
garrow_error_to_status_code(GError *error,
                            arrow::StatusCode default_code);
arrow::Status garrow_error_to_status(GError *error,
                                     arrow::StatusCode default_code,
                                     const char *context);

namespace garrow {
  gboolean check(GError **error,
                 const arrow::Status &status,
                 const char *context);

  template <typename CONTEXT_FUNC>
  gboolean check(GError **error,
                 const arrow::Status &status,
                 CONTEXT_FUNC &&context_func) {
    if (status.ok()) {
      return TRUE;
    } else {
      std::string context = context_func();
      g_set_error(error,
                  GARROW_ERROR,
                  garrow_error_from_status(status),
                  "%s: %s",
                  context.c_str(),
                  status.ToString().c_str());
      return FALSE;
    }
  }

  template <typename TYPE>
  gboolean check(GError **error,
                 const arrow::Result<TYPE> &result,
                 const char *context) {
    return check(error, result.status(), context);
  }

  template <typename TYPE, typename CONTEXT_FUNC>
  gboolean check(GError **error,
                 const arrow::Result<TYPE> &result,
                 CONTEXT_FUNC &&context_func) {
    return check(error, result.status(), context_func);
  }
}
