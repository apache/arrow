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

#include <glib-object.h>

G_BEGIN_DECLS

/**
 * GArrowError:
 * @GARROW_ERROR_OUT_OF_MEMORY: Out of memory error.
 * @GARROW_ERROR_KEY: Key error.
 * @GARROW_ERROR_TYPE: Type error.
 * @GARROW_ERROR_INVALID: Invalid value error.
 * @GARROW_ERROR_IO: IO error.
 * @GARROW_ERROR_UNKNOWN: Unknown error.
 * @GARROW_ERROR_NOT_IMPLEMENTED: The feature is not implemented.
 *
 * The error codes are used by all arrow-glib functions.
 *
 * They are corresponding to `arrow::Status` values.
 */
typedef enum {
  GARROW_ERROR_OUT_OF_MEMORY = 1,
  GARROW_ERROR_KEY,
  GARROW_ERROR_TYPE,
  GARROW_ERROR_INVALID,
  GARROW_ERROR_IO,
  GARROW_ERROR_UNKNOWN = 9,
  GARROW_ERROR_NOT_IMPLEMENTED = 10
} GArrowError;

#define GARROW_ERROR garrow_error_quark()

GQuark garrow_error_quark(void);

G_END_DECLS
