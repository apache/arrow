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

#include <gandiva-glib/function-registry.hpp>
#include <gandiva-glib/native-function.hpp>

G_BEGIN_DECLS

/**
 * SECTION: function-registry
 * @short_description: FunctionRegistry class
 * @title: FunctionRegistry class
 *
 * Since: 0.14.0
 */

typedef struct GGandivaFunctionSignaturePrivate_ {
  std::shared_ptr<gandiva::FunctionRegistry> *function_registry;
} GGandivaFunctionRegistryPrivate;

enum {
  PROP_FUNCTION_REGISTRY = 1
};

G_DEFINE_TYPE_WITH_PRIVATE(GGandivaFunctionRegistry,
                           ggandiva_function_registry,
                           G_TYPE_OBJECT)

#define GGANDIVA_FUNCTION_REGISTRY_GET_PRIVATE(obj)      \
    static_cast<GGandivaFunctionRegistryPrivate *>(      \
        ggandiva_function_registry_get_instance_private( \
          GGANDIVA_FUNCTION_REGISTRY(obj)))

static void
ggandiva_function_registry_dispose(GObject *object)
{
  G_OBJECT_CLASS(ggandiva_function_registry_parent_class)->dispose(object);
}

static void
ggandiva_function_registry_finalize(GObject *object)
{
  auto priv = GGANDIVA_FUNCTION_REGISTRY_GET_PRIVATE(object);

  delete priv->function_registry;

  G_OBJECT_CLASS(ggandiva_function_registry_parent_class)->finalize(object);
}

static void
ggandiva_function_registry_set_property(GObject *object,
                                        guint prop_id,
                                        const GValue *value,
                                        GParamSpec *pspec)
{
  auto priv = GGANDIVA_FUNCTION_REGISTRY_GET_PRIVATE(object);
  switch (prop_id) {
  case PROP_FUNCTION_REGISTRY:
    {
      auto ptr = g_value_get_pointer(value);
      auto gandiva_function_registry
        = static_cast<std::shared_ptr<gandiva::FunctionRegistry> *>(ptr);
      priv->function_registry
        = new std::shared_ptr<gandiva::FunctionRegistry>(*gandiva_function_registry);
    }
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
ggandiva_function_registry_init(GGandivaFunctionRegistry *object)
{
}

static void
ggandiva_function_registry_class_init(GGandivaFunctionRegistryClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->dispose = ggandiva_function_registry_dispose;
  gobject_class->finalize = ggandiva_function_registry_finalize;
  gobject_class->set_property = ggandiva_function_registry_set_property;

  GParamSpec *spec;
  spec = g_param_spec_pointer("function_registry",
                              "FunctionRegistry",
                              "The raw gandiva::FunctionRegistry *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_FUNCTION_REGISTRY, spec);
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
  auto gandiva_function_registry
    = std::make_shared<gandiva::FunctionRegistry>();
  return ggandiva_function_registry_new_raw(&gandiva_function_registry);
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
  auto priv = GGANDIVA_FUNCTION_REGISTRY_GET_PRIVATE(function_registry);
  GList *native_function_list = nullptr;

  using iterator = gandiva::FunctionRegistry::iterator;
  const auto& gandiva_function_registry = *priv->function_registry;

  for (iterator gandiva_native_function = gandiva_function_registry->begin();
      gandiva_native_function != gandiva_function_registry->end();
      ++gandiva_native_function) {
    auto native_function = ggandiva_native_function_new_raw(gandiva_native_function);
    native_function_list = g_list_append(native_function_list, native_function);
  }

  return native_function_list;
}

G_END_DECLS

GGandivaFunctionRegistry *
ggandiva_function_registry_new_raw(
    std::shared_ptr<gandiva::FunctionRegistry> *gandiva_function_registry)
{
  auto function_registry
    = GGANDIVA_FUNCTION_REGISTRY(g_object_new(GGANDIVA_TYPE_FUNCTION_REGISTRY,
                                              "function_registry",
                                              gandiva_function_registry,
                                              NULL));
  return function_registry;
}

std::shared_ptr<gandiva::FunctionRegistry>
ggandiva_function_registry_get_raw(GGandivaFunctionRegistry *function_registry)
{
  auto priv = GGANDIVA_FUNCTION_REGISTRY_GET_PRIVATE(function_registry);
  return *priv->function_registry;
}
