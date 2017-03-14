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

#include <arrow-glib/error.hpp>

G_BEGIN_DECLS

/**
 * SECTION: error
 * @title: GArrowError
 * @short_description: Error code mapping between Arrow and arrow-glib
 *
 * #GArrowError provides error codes corresponding to `arrow::Status`
 * values.
 */

G_DEFINE_QUARK(garrow-error-quark, garrow_error)

static GArrowError
garrow_error_code(const arrow::Status &status)
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
  case arrow::StatusCode::UnknownError:
    return GARROW_ERROR_UNKNOWN;
  case arrow::StatusCode::NotImplemented:
    return GARROW_ERROR_NOT_IMPLEMENTED;
  default:
    return GARROW_ERROR_UNKNOWN;
  }
}

G_END_DECLS

void
garrow_error_set(GError **error,
                 const arrow::Status &status,
                 const char *context)
{
  if (status.ok()) {
    return;
  }

  g_set_error(error,
              GARROW_ERROR,
              garrow_error_code(status),
              "%s: %s",
              context,
              status.ToString().c_str());
}
