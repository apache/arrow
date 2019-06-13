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

#include <gandiva-glib/function-signature.h>

G_BEGIN_DECLS

/**
 * GGandivaResultNullableType:
 * @GGANDIVA_RESULT_NULL_IF_NULL: This means the result validity is an intersection of
 *   the validity of the children.
 * @GGANDIVA_RESULT_NULL_NEVER: This means that the result is always valid.
 * @GGANDIVA_RESULT_NULL_INTERNAL: This means that the result validity depends on some
 *   internal logic.
 *
 * They are corresponding to `gandiva::ResultNullableType` values.
 */
typedef enum {
  GGANDIVA_RESULT_NULL_IF_NULL,
  GGANDIVA_RESULT_NULL_NEVER,
  GGANDIVA_RESULT_NULL_INTERNAL
} GGandivaResultNullableType;

#define GGANDIVA_TYPE_NATIVE_FUNCTION (ggandiva_native_function_get_type())
G_DECLARE_DERIVABLE_TYPE(GGandivaNativeFunction,
                         ggandiva_native_function,
                         GGANDIVA,
                         NATIVE_FUNCTION,
                         GObject)

struct _GGandivaNativeFunctionClass
{
  GObjectClass parent_class;
};

GGandivaFunctionSignature *ggandiva_native_function_get_signature(GGandivaNativeFunction *native_function);
gboolean
ggandiva_native_function_equal(GGandivaNativeFunction *native_function,
                               GGandivaNativeFunction *other_native_function);
gchar *ggandiva_native_function_to_string(GGandivaNativeFunction *native_function);
GGandivaResultNullableType ggandiva_native_function_get_result_nullable_type(GGandivaNativeFunction *native_function);
gboolean ggandiva_native_function_need_context(GGandivaNativeFunction *native_function);
gboolean ggandiva_native_function_need_function_holder(GGandivaNativeFunction *native_function);
gboolean ggandiva_native_function_can_return_errors(GGandivaNativeFunction *native_function);

G_END_DECLS
