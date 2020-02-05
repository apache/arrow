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

#include <gandiva-glib/native-function.h>

G_BEGIN_DECLS

#define GGANDIVA_TYPE_FUNCTION_REGISTRY (ggandiva_function_registry_get_type())
G_DECLARE_DERIVABLE_TYPE(GGandivaFunctionRegistry,
                         ggandiva_function_registry,
                         GGANDIVA,
                         FUNCTION_REGISTRY,
                         GObject)

struct _GGandivaFunctionRegistryClass
{
  GObjectClass parent_class;
};

GGandivaFunctionRegistry *ggandiva_function_registry_new(void);
GGandivaNativeFunction *
ggandiva_function_registry_lookup(GGandivaFunctionRegistry *function_registry,
                                  GGandivaFunctionSignature *function_signature);
GList *ggandiva_function_registry_get_native_functions(GGandivaFunctionRegistry *function_registry);

G_END_DECLS
