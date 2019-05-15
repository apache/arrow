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
 * @GARROW_ERROR_CAPACITY: Capacity error.
 * @GARROW_ERROR_INDEX: Index error.
 * @GARROW_ERROR_UNKNOWN: Unknown error.
 * @GARROW_ERROR_NOT_IMPLEMENTED: The feature is not implemented.
 * @GARROW_ERROR_SERIALIZATION: Serialization error.
 * @GARROW_ERROR_PYTHON: Python error.
 * @GARROW_ERROR_PLASMA_OBJECT_EXISTS: Object already exists on Plasma.
 * @GARROW_ERROR_PLASMA_OBJECT_NONEXISTENT: Object doesn't exist on Plasma.
 * @GARROW_ERROR_PLASMA_STORE_FULL: Store full error on Plasma.
 * @GARROW_ERROR_PLASMA_OBJECT_ALREADY_SEALED: Object already sealed on Plasma.
 * @GARROW_ERROR_CODE_GENERATION: Error generating code for expression evaluation
 *   in Gandiva.
 * @GARROW_ERROR_EXPRESSION_VALIDATION: Validation errors in expression given for code
 * generation.
 * @GARROW_ERROR_EXECUTION: Execution error while evaluating the expression against a
 * record batch.
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
  GARROW_ERROR_CAPACITY,
  GARROW_ERROR_INDEX,
  GARROW_ERROR_UNKNOWN = 9,
  GARROW_ERROR_NOT_IMPLEMENTED,
  GARROW_ERROR_SERIALIZATION,
  GARROW_ERROR_PYTHON,
  GARROW_ERROR_PLASMA_OBJECT_EXISTS = 20,
  GARROW_ERROR_PLASMA_OBJECT_NONEXISTENT,
  GARROW_ERROR_PLASMA_STORE_FULL,
  GARROW_ERROR_PLASMA_OBJECT_ALREADY_SEALED,
  GARROW_ERROR_CODE_GENERATION = 40,
  GARROW_ERROR_EXPRESSION_VALIDATION = 41,
  GARROW_ERROR_EXECUTION = 42,
} GArrowError;

#define GARROW_ERROR garrow_error_quark()

GQuark garrow_error_quark(void);

G_END_DECLS
