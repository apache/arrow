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

#include <gandiva-glib/function-signature.hpp>

G_BEGIN_DECLS

/**
 * SECTION: function-signature
 * @short_description: FunctionSignature class
 * @title: FunctionSignature class
 *
 * Since: 0.14.0
 */

typedef struct GGandivaFunctionSignaturePrivate_ {
  const gandiva::FunctionSignature *function_signature;
} GGandivaFunctionSignaturePrivate;

enum {
  PROP_FUNCTION_SIGNATURE = 1
};

G_DEFINE_TYPE_WITH_PRIVATE(GGandivaFunctionSignature,
                           ggandiva_function_signature,
                           G_TYPE_OBJECT)

#define GGANDIVA_FUNCTION_SIGNATURE_GET_PRIVATE(obj)      \
    static_cast<GGandivaFunctionSignaturePrivate *>(      \
        ggandiva_function_signature_get_instance_private( \
          GGANDIVA_FUNCTION_SIGNATURE(obj)))

static void
ggandiva_function_signature_set_property(GObject *object,
                                         guint prop_id,
                                         const GValue *value,
                                         GParamSpec *pspec)
{
  auto priv = GGANDIVA_FUNCTION_SIGNATURE_GET_PRIVATE(object);
  switch (prop_id) {
  case PROP_FUNCTION_SIGNATURE:
    priv->function_signature =
      static_cast<const gandiva::FunctionSignature *>(g_value_get_pointer(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
ggandiva_function_signature_init(GGandivaFunctionSignature *object)
{
}

static void
ggandiva_function_signature_class_init(GGandivaFunctionSignatureClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->set_property = ggandiva_function_signature_set_property;

  GParamSpec *spec;
  spec = g_param_spec_pointer("function_signature",
                              "FunctionSignature",
                              "The raw gandiva::FunctionSignature *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_FUNCTION_SIGNATURE, spec);
}

/**
 * ggandiva_function_signature_equal:
 * @function_signature: A #GGandivaFunctionSignature.
 * @other_function_signature: A #GGandivaFunctionSignature to be compared.
 *
 * Returns: %TRUE if both of them have the same data, %FALSE otherwise.
 *
 * Since: 0.14.0
 */
gboolean
ggandiva_function_signature_equal(GGandivaFunctionSignature *function_signature,
                                  GGandivaFunctionSignature *other_function_signature)
{
  auto gandiva_function_signature
    = ggandiva_function_signature_get_raw(function_signature);
  auto other_gandiva_function_signature
    = ggandiva_function_signature_get_raw(other_function_signature);

  return (*gandiva_function_signature) == (*other_gandiva_function_signature);
}

/**
 * ggandiva_function_signature_to_string:
 * @function_signature: A #GGandivaFunctionSignature
 *
 * Returns: The string representation of the function signature.
 *
 * Since: 0.14.0
 */
gchar *
ggandiva_function_signature_to_string(GGandivaFunctionSignature *function_signature)
{
  const auto gandiva_function_signature
    = ggandiva_function_signature_get_raw(function_signature);
  return g_strdup(gandiva_function_signature->ToString().c_str());
}

G_END_DECLS

GGandivaFunctionSignature *
ggandiva_function_signature_new_raw(const gandiva::FunctionSignature *gandiva_function_signature)
{
  auto function_signature
    = GGANDIVA_FUNCTION_SIGNATURE(g_object_new(GGANDIVA_TYPE_FUNCTION_SIGNATURE,
                                               "function_signature",
                                               gandiva_function_signature,
                                               NULL));
  return function_signature;
}

const gandiva::FunctionSignature *
ggandiva_function_signature_get_raw(GGandivaFunctionSignature *function_signature)
{
  auto priv = GGANDIVA_FUNCTION_SIGNATURE_GET_PRIVATE(function_signature);
  return priv->function_signature;
}
