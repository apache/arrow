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

#include <arrow-glib/arrow-glib.h>

G_BEGIN_DECLS

#define GGANDIVA_TYPE_FUNCTION_SIGNATURE (ggandiva_function_signature_get_type())
G_DECLARE_DERIVABLE_TYPE(GGandivaFunctionSignature,
                         ggandiva_function_signature,
                         GGANDIVA,
                         FUNCTION_SIGNATURE,
                         GObject)

struct _GGandivaFunctionSignatureClass
{
  GObjectClass parent_class;
};

GGandivaFunctionSignature *ggandiva_function_signature_new(const gchar *base_name,
                                                           GList *parameter_types,
                                                           GArrowDataType *return_type);
gboolean ggandiva_function_signature_equal(GGandivaFunctionSignature *function_signature,
                                           GGandivaFunctionSignature *other_function_signature);
gchar *ggandiva_function_signature_to_string(GGandivaFunctionSignature *function_signature);
GArrowDataType *ggandiva_function_signature_get_return_type(GGandivaFunctionSignature *function_signature);
gchar *ggandiva_function_signature_get_base_name(GGandivaFunctionSignature *function_signature);
GList *ggandiva_function_signature_get_param_types(GGandivaFunctionSignature *function_signature);

G_END_DECLS
