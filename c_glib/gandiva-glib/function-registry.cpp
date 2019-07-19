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

#include <gandiva/function_registry.h>
#include <gandiva-glib/function-registry.h>

#include <gandiva-glib/function-signature.hpp>
#include <gandiva-glib/native-function.hpp>

G_BEGIN_DECLS

/**
 * SECTION: function-registry
 * @short_description: FunctionRegistry class
 * @title: FunctionRegistry class
 * @include: gandiva-glib/gandiva-glib.h
 *
 * Since: 0.14.0
 */

G_DEFINE_TYPE(GGandivaFunctionRegistry,
              ggandiva_function_registry,
              G_TYPE_OBJECT)

static void
ggandiva_function_registry_init(GGandivaFunctionRegistry *object)
{
}

static void
ggandiva_function_registry_class_init(GGandivaFunctionRegistryClass *klass)
{
}

/**
 * ggandiva_function_registry_new:
 *
 * Returns: A newly created #GGandivaFunctionRegistry.
 *
 * Since: 0.14.0
 */
GGandivaFunctionRegistry *
ggandiva_function_registry_new(void)
{
  return GGANDIVA_FUNCTION_REGISTRY(g_object_new(GGANDIVA_TYPE_FUNCTION_REGISTRY, NULL));
}

/**
 * ggandiva_function_registry_lookup:
 * @function_registry: A #GGandivaFunctionRegistry.
 * @function_signature: A #GGandivaFunctionSignature to be looked up.
 *
 * Returns: (transfer full) (nullable):
 *   The native functions associated to the given #GGandivaFunctionSignature.
 *
 * Since: 0.14.0
 */
GGandivaNativeFunction *
ggandiva_function_registry_lookup(GGandivaFunctionRegistry *function_registry,
                                  GGandivaFunctionSignature *function_signature)
{
  gandiva::FunctionRegistry gandiva_function_registry;
  auto gandiva_function_signature =
    ggandiva_function_signature_get_raw(function_signature);
  auto gandiva_native_function =
    gandiva_function_registry.LookupSignature(*gandiva_function_signature);
  if (gandiva_native_function) {
    return ggandiva_native_function_new_raw(gandiva_native_function);
  } else {
    return NULL;
  }
}

/**
 * ggandiva_function_registry_get_native_functions:
 * @function_registry: A #GGandivaFunctionRegistry.
 *
 * Returns: (transfer full) (element-type GGandivaNativeFunction):
 *   The native functions in the function registry.
 *
 * Since: 0.14.0
 */
GList *
ggandiva_function_registry_get_native_functions(GGandivaFunctionRegistry *function_registry)
{
  gandiva::FunctionRegistry gandiva_function_registry;

  GList *native_functions = nullptr;
  for (auto gandiva_native_function = gandiva_function_registry.begin();
       gandiva_native_function != gandiva_function_registry.end();
       ++gandiva_native_function) {
    auto native_function = ggandiva_native_function_new_raw(gandiva_native_function);
    native_functions = g_list_prepend(native_functions, native_function);
  }
  native_functions = g_list_reverse(native_functions);

  return native_functions;
}

G_END_DECLS
