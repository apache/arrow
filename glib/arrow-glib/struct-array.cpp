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
#include <arrow-glib/data-type.hpp>
#include <arrow-glib/struct-array.h>

G_BEGIN_DECLS

/**
 * SECTION: struct-array
 * @short_description: Struct array class
 * @include: arrow-glib/arrow-glib.h
 *
 * #GArrowStructArray is a class for struct array. It can store zero
 * or more structs. One struct has zero or more fields.
 *
 * #GArrowStructArray is immutable. You need to use
 * #GArrowStructArrayBuilder to create a new array.
 */

G_DEFINE_TYPE(GArrowStructArray,               \
              garrow_struct_array,             \
              GARROW_TYPE_ARRAY)

static void
garrow_struct_array_init(GArrowStructArray *object)
{
}

static void
garrow_struct_array_class_init(GArrowStructArrayClass *klass)
{
}

/**
 * garrow_struct_array_get_field
 * @array: A #GArrowStructArray.
 * @i: The index of the field in the struct.
 *
 * Returns: (transfer full): The i-th field.
 */
GArrowArray *
garrow_struct_array_get_field(GArrowStructArray *array,
                              gint i)
{
  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  auto arrow_struct_array =
    static_cast<arrow::StructArray *>(arrow_array.get());
  auto arrow_field = arrow_struct_array->field(i);
  return garrow_array_new_raw(&arrow_field);
}

/**
 * garrow_struct_array_get_fields
 * @array: A #GArrowStructArray.
 *
 * Returns: (element-type GArrowArray) (transfer full):
 *   The fields in the struct.
 */
GList *
garrow_struct_array_get_fields(GArrowStructArray *array)
{
  const auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  const auto arrow_struct_array =
    static_cast<const arrow::StructArray *>(arrow_array.get());

  GList *fields = NULL;
  for (auto arrow_field : arrow_struct_array->fields()) {
    GArrowArray *field = garrow_array_new_raw(&arrow_field);
    fields = g_list_prepend(fields, field);
  }

  return g_list_reverse(fields);
}

G_END_DECLS
