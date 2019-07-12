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

#include <gandiva-glib/native-function.hpp>

#include <gandiva-glib/function-signature.hpp>

G_BEGIN_DECLS

/**
 * SECTION: native-function
 * @short_description: NativeFunction class
 * @title: NativeFunction class
 * @include: gandiva-glib/gandiva-glib.h
 *
 * Since: 0.14.0
 */

typedef struct GGandivaNativeFunctionPrivate_ {
  const gandiva::NativeFunction *native_function;
} GGandivaNativeFunctionPrivate;

enum {
  PROP_NATIVE_FUNCTION = 1
};

G_DEFINE_TYPE_WITH_PRIVATE(GGandivaNativeFunction,
                           ggandiva_native_function,
                           G_TYPE_OBJECT)

#define GGANDIVA_NATIVE_FUNCTION_GET_PRIVATE(obj)      \
    static_cast<GGandivaNativeFunctionPrivate *>(      \
        ggandiva_native_function_get_instance_private( \
          GGANDIVA_NATIVE_FUNCTION(obj)))

static void
ggandiva_native_function_set_property(GObject *object,
                                      guint prop_id,
                                      const GValue *value,
                                      GParamSpec *pspec)
{
  auto priv = GGANDIVA_NATIVE_FUNCTION_GET_PRIVATE(object);
  switch (prop_id) {
  case PROP_NATIVE_FUNCTION:
    priv->native_function =
      static_cast<const gandiva::NativeFunction *>(g_value_get_pointer(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
ggandiva_native_function_init(GGandivaNativeFunction *object)
{
}

static void
ggandiva_native_function_class_init(GGandivaNativeFunctionClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->set_property = ggandiva_native_function_set_property;

  GParamSpec *spec;
  spec = g_param_spec_pointer("native-function",
                              "NativeFunction",
                              "The raw gandiva::NativeFunction *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_NATIVE_FUNCTION, spec);
}

/**
 * ggandiva_native_function_get_signature:
 * @native_function: A #GGandivaNativeFunction.
 *
 * Returns: (transfer full): A #GGandivaFunctionSignature that represents
 *   the signature of the native function.
 *
 * Since: 0.14.0
 */
GGandivaFunctionSignature *
ggandiva_native_function_get_signature(GGandivaNativeFunction *native_function)
{
  auto gandiva_native_function =
    ggandiva_native_function_get_raw(native_function);
  auto &gandiva_function_signature = gandiva_native_function->signatures().front();
  return ggandiva_function_signature_new_raw(&gandiva_function_signature);
}

/**
 * ggandiva_native_function_get_all_signatures:
 * @native_function: A #GGandivaNativeFunction.
 *
 * Returns: (transfer full): A List of #GGandivaFunctionSignature supported by
 * the native function.
 *
 * Since: ??
 */
GList *
ggandiva_native_function_get_all_signatures(GGandivaNativeFunction *native_function)
{
  auto gandiva_native_function =
  ggandiva_native_function_get_raw(native_function);
  
  GList *function_signatures = nullptr;
  for (auto& function_sig_raw : gandiva_native_function->signatures()) {
    auto function_sig = ggandiva_function_signature_new_raw(&function_sig_raw);
    function_signatures = g_list_prepend(function_signatures, function_sig);
  }
  function_signatures = g_list_reverse(function_signatures);
  
  return function_signatures;
}

/**
 * ggandiva_native_function_equal:
 * @native_function: A #GGandivaNativeFunction.
 * @other_native_function: A #GGandivaNativeFunction to be compared.
 *
 * Returns: %TRUE if both of them have the same data, %FALSE otherwise.
 *
 * Since: 0.14.0
 */
gboolean
ggandiva_native_function_equal(GGandivaNativeFunction *native_function,
                               GGandivaNativeFunction *other_native_function)
{
  auto gandiva_native_function =
    ggandiva_native_function_get_raw(native_function);
  auto gandiva_other_native_function =
    ggandiva_native_function_get_raw(other_native_function);
  return gandiva_native_function == gandiva_other_native_function;
}

/**
 * ggandiva_native_function_to_string:
 * @native_function: A #GGandivaNativeFunction.
 *
 * Returns: (transfer full):
 *   The string representation of the signature of the native function.
 *   It should be freed with g_free() when no longer needed.
 *
 * Since: 0.14.0
 */
gchar *
ggandiva_native_function_to_string(GGandivaNativeFunction *native_function)
{
  auto gandiva_native_function =
    ggandiva_native_function_get_raw(native_function);
  auto gandiva_function_signature = gandiva_native_function->signatures().front();
  return g_strdup(gandiva_function_signature.ToString().c_str());
}

/**
 * ggandiva_native_function_get_result_nullable_type:
 * @native_function: A #GGandivaNativeFunction.
 *
 * Returns:
 *   A value of #GGandivaResultNullableType.
 *
 * Since: 0.14.0
 */
GGandivaResultNullableType
ggandiva_native_function_get_result_nullable_type(GGandivaNativeFunction *native_function)
{
  auto gandiva_native_function =
    ggandiva_native_function_get_raw(native_function);
  const auto gandiva_result_nullable_type =
    gandiva_native_function->result_nullable_type();
  return ggandiva_result_nullable_type_from_raw(gandiva_result_nullable_type);
}

/**
 * ggandiva_native_function_need_context:
 * @native_function: A #GGandivaNativeFunction.
 *
 * Returns:
 *   %TRUE if the native function needs a context for evaluation,
 *   %FALSE otherwise.
 *
 * Since: 0.14.0
 */
gboolean
ggandiva_native_function_need_context(GGandivaNativeFunction *native_function)
{
  auto gandiva_native_function =
    ggandiva_native_function_get_raw(native_function);
  return gandiva_native_function->NeedsContext();
}

/**
 * ggandiva_native_function_need_function_holder:
 * @native_function: A #GGandivaNativeFunction.
 *
 * Returns:
 *   %TRUE if the native function needs a function holder for evaluation,
 *   %FALSE otherwise.
 *
 * Since: 0.14.0
 */
gboolean
ggandiva_native_function_need_function_holder(GGandivaNativeFunction *native_function)
{
  auto gandiva_native_function =
    ggandiva_native_function_get_raw(native_function);
  return gandiva_native_function->NeedsFunctionHolder();
}

/**
 * ggandiva_native_function_can_return_errors:
 * @native_function: A #GGandivaNativeFunction.
 *
 * Returns:
 *   %TRUE if the native function has the possibility of returning errors,
 *   %FALSE otherwise.
 *
 * Since: 0.14.0
 */
gboolean
ggandiva_native_function_can_return_errors(GGandivaNativeFunction *native_function)
{
  auto gandiva_native_function =
    ggandiva_native_function_get_raw(native_function);
  return gandiva_native_function->CanReturnErrors();
}

G_END_DECLS

GGandivaResultNullableType
ggandiva_result_nullable_type_from_raw(gandiva::ResultNullableType gandiva_type)
{
  switch (gandiva_type) {
  case gandiva::kResultNullIfNull:
    return GGANDIVA_RESULT_NULL_IF_NULL;
  case gandiva::kResultNullNever:
    return GGANDIVA_RESULT_NULL_NEVER;
  case gandiva::kResultNullInternal:
    return GGANDIVA_RESULT_NULL_INTERNAL;
  default:
    return GGANDIVA_RESULT_NULL_IF_NULL;
  }
}

gandiva::ResultNullableType
ggandiva_result_nullable_type_to_raw(GGandivaResultNullableType type)
{
  switch (type) {
  case GGANDIVA_RESULT_NULL_IF_NULL:
    return gandiva::kResultNullIfNull;
  case GGANDIVA_RESULT_NULL_NEVER:
    return gandiva::kResultNullNever;
  case GGANDIVA_RESULT_NULL_INTERNAL:
    return gandiva::kResultNullInternal;
  default:
    return gandiva::kResultNullIfNull;
  }
}

GGandivaNativeFunction *
ggandiva_native_function_new_raw(const gandiva::NativeFunction *gandiva_native_function)
{
  auto native_function =
    GGANDIVA_NATIVE_FUNCTION(g_object_new(GGANDIVA_TYPE_NATIVE_FUNCTION,
                                          "native-function",
                                          gandiva_native_function,
                                          NULL));
  return native_function;
}

const gandiva::NativeFunction *
ggandiva_native_function_get_raw(GGandivaNativeFunction *native_function)
{
  auto priv = GGANDIVA_NATIVE_FUNCTION_GET_PRIVATE(native_function);
  return priv->native_function;
}
