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

#include <gandiva/native_function.h>

#include <gandiva-glib/native-function.h>

G_BEGIN_DECLS

GGandivaResultNullableType
ggandiva_result_nullable_type_from_raw(gandiva::ResultNullableType gandiva_type);
gandiva::ResultNullableType
ggandiva_result_nullable_type_to_raw(GGandivaResultNullableType type);

GGandivaNativeFunction *ggandiva_native_function_new_raw(const gandiva::NativeFunction *gandiva_native_function);
const gandiva::NativeFunction *ggandiva_native_function_get_raw(GGandivaNativeFunction *native_function);

G_END_DECLS
