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

#ifdef HAVE_CONFIG_H
#  include <config.h>
#endif

#include <gandiva-glib/error.hpp>

G_BEGIN_DECLS

/**
 * SECTION: error
 * @title: GGandivaError
 * @short_description: Error code mapping between Gandiva and gandiva-glib
 *
 * #GGandivaError provides error codes corresponding to `gandiva::Status`
 * values.
 */

G_DEFINE_QUARK(ggandiva-error-quark, ggandiva_error)

static GGandivaError
ggandiva_error_code(const gandiva::Status &status)
{
  switch (status.code()) {
  case gandiva::StatusCode::OK:
    return GGANDIVA_ERROR_UNKNOWN;
  case gandiva::StatusCode::Invalid:
    return GGANDIVA_ERROR_INVALID;
  case gandiva::StatusCode::CodeGenError:
    return GGANDIVA_ERROR_CODE_GEN;
  case gandiva::StatusCode::ArrowError:
    return GGANDIVA_ERROR_ARROW;
  case gandiva::StatusCode::ExpressionValidationError:
    return GGANDIVA_ERROR_EXPRESSION_VALIDATION;
  default:
    return GGANDIVA_ERROR_UNKNOWN;
  }
}

G_END_DECLS

gboolean
ggandiva_error_check(GError **error,
                     const gandiva::Status &status,
                     const char *context)
{
  if (status.ok()) {
    return TRUE;
  } else {
    g_set_error(error,
                GGANDIVA_ERROR,
                ggandiva_error_code(status),
                "%s: %s",
                context,
                status.ToString().c_str());
    return FALSE;
  }
}
