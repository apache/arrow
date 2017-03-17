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

#include <arrow-glib/array.hpp>
#include <arrow-glib/string-array.h>

G_BEGIN_DECLS

/**
 * SECTION: string-array
 * @short_description: UTF-8 encoded string array class
 *
 * #GArrowStringArray is a class for UTF-8 encoded string array. It
 * can store zero or more UTF-8 encoded string data.
 *
 * #GArrowStringArray is immutable. You need to use
 * #GArrowStringArrayBuilder to create a new array.
 */

G_DEFINE_TYPE(GArrowStringArray,               \
              garrow_string_array,             \
              GARROW_TYPE_BINARY_ARRAY)

static void
garrow_string_array_init(GArrowStringArray *object)
{
}

static void
garrow_string_array_class_init(GArrowStringArrayClass *klass)
{
}

/**
 * garrow_string_array_get_string:
 * @array: A #GArrowStringArray.
 * @i: The index of the target value.
 *
 * Returns: The i-th UTF-8 encoded string.
 */
gchar *
garrow_string_array_get_string(GArrowStringArray *array,
                               gint64 i)
{
  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  auto arrow_string_array =
    static_cast<arrow::StringArray *>(arrow_array.get());
  gint32 length;
  auto value =
    reinterpret_cast<const gchar *>(arrow_string_array->GetValue(i, &length));
  return g_strndup(value, length);
}

G_END_DECLS
