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
 * GGandivaError:
 * @GGANDIVA_ERROR_INVALID: Invalid value error.
 * @GGANDIVA_ERROR_CODE_GEN: Code generation error.
 * @GGANDIVA_ERROR_ARROW: Arrow error.
 * @GGANDIVA_ERROR_EXPRESSION_VALIDATION: Expression validation error.
 * @GGANDIVA_ERROR_UNKNOWN: Unknown error.
 *
 * The error codes are used by all gandiva-glib functions.
 *
 * They are corresponding to `gandiva::Status` values.
 */
typedef enum {
  GGANDIVA_ERROR_INVALID = 1,
  GGANDIVA_ERROR_CODE_GEN,
  GGANDIVA_ERROR_ARROW,
  GGANDIVA_ERROR_EXPRESSION_VALIDATION,
  GGANDIVA_ERROR_UNKNOWN,
} GGandivaError;

#define GGANDIVA_ERROR ggandiva_error_quark()

GQuark ggandiva_error_quark(void);

G_END_DECLS
